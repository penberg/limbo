#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

mod errors;
mod types;

use errors::MVCCError;
use mvcc_rs::persistent_storage::{s3, Storage};
use mvcc_rs::*;
use types::{DbContext, MVCCDatabaseRef, MVCCScanCursorRef, ScanCursorContext};

/// cbindgen:ignore
type Clock = clock::LocalClock;

/// cbindgen:ignore
type Db = database::Database<Clock>;

/// cbindgen:ignore
type ScanCursor = cursor::ScanCursor<'static, Clock>;

static INIT_RUST_LOG: std::sync::Once = std::sync::Once::new();

async fn storage_for(main_db_path: &str) -> database::Result<Storage> {
    // TODO: let's accept an URL instead of main_db_path here, so we can
    // pass custom S3 endpoints, options, etc.
    if cfg!(feature = "json_on_disk_storage") {
        tracing::info!("JSONonDisk storage stored in {main_db_path}-mvcc");
        return Ok(Storage::new_json_on_disk(format!("{main_db_path}-mvcc")));
    }
    if cfg!(feature = "s3_storage") {
        tracing::info!("S3 storage for {main_db_path}");
        return Storage::new_s3(s3::Options::with_create_bucket_if_not_exists(true)).await;
    }
    tracing::info!("No persistent storage for {main_db_path}");
    Ok(Storage::new_noop())
}

#[no_mangle]
pub unsafe extern "C" fn MVCCDatabaseOpen(path: *const std::ffi::c_char) -> MVCCDatabaseRef {
    INIT_RUST_LOG.call_once(|| {
        tracing_subscriber::fmt::init();
    });

    tracing::debug!("MVCCDatabaseOpen");

    let clock = clock::LocalClock::new();
    let main_db_path = unsafe { std::ffi::CStr::from_ptr(path) };
    let main_db_path = match main_db_path.to_str() {
        Ok(path) => path,
        Err(_) => {
            tracing::error!("Invalid UTF-8 path");
            return MVCCDatabaseRef::null();
        }
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();

    tracing::debug!("mvccrs: opening persistent storage for {main_db_path}");
    let storage = match runtime.block_on(storage_for(main_db_path)) {
        Ok(storage) => storage,
        Err(e) => {
            tracing::error!("Failed to open persistent storage: {e}");
            return MVCCDatabaseRef::null();
        }
    };
    let db = Db::new(clock, storage);

    runtime.block_on(db.recover()).ok();

    let ctx = DbContext { db, runtime };
    let ctx = Box::leak(Box::new(ctx));
    MVCCDatabaseRef::from(ctx)
}

#[no_mangle]
pub unsafe extern "C" fn MVCCDatabaseClose(db: MVCCDatabaseRef) {
    tracing::debug!("MVCCDatabaseClose");
    if db.is_null() {
        tracing::debug!("warning: `db` is null in MVCCDatabaseClose()");
        return;
    }
    let _ = unsafe { Box::from_raw(db.get_ref_mut()) };
}

#[no_mangle]
pub unsafe extern "C" fn MVCCTransactionBegin(db: MVCCDatabaseRef) -> u64 {
    let db = db.get_ref();
    let (db, runtime) = (&db.db, &db.runtime);
    let tx_id = runtime.block_on(async move { db.begin_tx().await });
    tracing::debug!("MVCCTransactionBegin: {tx_id}");
    tx_id
}

#[no_mangle]
pub unsafe extern "C" fn MVCCTransactionCommit(db: MVCCDatabaseRef, tx_id: u64) -> MVCCError {
    let db = db.get_ref();
    let (db, runtime) = (&db.db, &db.runtime);
    tracing::debug!("MVCCTransactionCommit: {tx_id}");
    match runtime.block_on(async move { db.commit_tx(tx_id).await }) {
        Ok(()) => MVCCError::MVCC_OK,
        Err(e) => {
            tracing::error!("MVCCTransactionCommit: {e}");
            MVCCError::MVCC_IO_ERROR_WRITE
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn MVCCTransactionRollback(db: MVCCDatabaseRef, tx_id: u64) -> MVCCError {
    let db = db.get_ref();
    let (db, runtime) = (&db.db, &db.runtime);
    tracing::debug!("MVCCTransactionRollback: {tx_id}");
    runtime.block_on(async move { db.rollback_tx(tx_id).await });
    MVCCError::MVCC_OK
}

#[no_mangle]
pub unsafe extern "C" fn MVCCDatabaseInsert(
    db: MVCCDatabaseRef,
    tx_id: u64,
    table_id: u64,
    row_id: u64,
    value_ptr: *const std::ffi::c_void,
    value_len: usize,
) -> MVCCError {
    let db = db.get_ref();
    let value = std::slice::from_raw_parts(value_ptr as *const u8, value_len);
    let data = match std::str::from_utf8(value) {
        Ok(value) => value.to_string(),
        Err(_) => {
            tracing::info!("Invalid UTF-8, let's base64 this fellow");
            use base64::{engine::general_purpose, Engine as _};
            general_purpose::STANDARD.encode(value)
        }
    };
    let (db, runtime) = (&db.db, &db.runtime);
    let id = database::RowID { table_id, row_id };
    let row = database::Row { id, data };
    tracing::debug!("MVCCDatabaseInsert: {row:?}");
    match runtime.block_on(async move { db.insert(tx_id, row).await }) {
        Ok(_) => {
            tracing::debug!("MVCCDatabaseInsert: success");
            MVCCError::MVCC_OK
        }
        Err(e) => {
            tracing::error!("MVCCDatabaseInsert: {e}");
            MVCCError::MVCC_IO_ERROR_WRITE
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn MVCCDatabaseRead(
    db: MVCCDatabaseRef,
    tx_id: u64,
    table_id: u64,
    row_id: u64,
    value_ptr: *mut *mut u8,
    value_len: *mut i64,
) -> MVCCError {
    let db = db.get_ref();
    let (db, runtime) = (&db.db, &db.runtime);

    match runtime.block_on(async move {
        let id = database::RowID { table_id, row_id };
        let maybe_row = db.read(tx_id, id).await?;
        match maybe_row {
            Some(row) => {
                tracing::debug!("Found row {row:?}");
                let str_len = row.data.len() + 1;
                let value = std::ffi::CString::new(row.data.as_bytes()).map_err(|e| {
                    mvcc_rs::errors::DatabaseError::Io(format!(
                        "Failed to transform read data into CString: {e}"
                    ))
                })?;
                unsafe {
                    *value_ptr = value.into_raw() as *mut u8;
                    *value_len = str_len as i64;
                }
            }
            None => unsafe { *value_len = -1 },
        };
        Ok::<(), mvcc_rs::errors::DatabaseError>(())
    }) {
        Ok(_) => {
            tracing::debug!("MVCCDatabaseRead: success");
            MVCCError::MVCC_OK
        }
        Err(e) => {
            tracing::error!("MVCCDatabaseRead: {e}");
            MVCCError::MVCC_IO_ERROR_READ
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn MVCCFreeStr(ptr: *mut std::ffi::c_void) {
    if ptr.is_null() {
        return;
    }
    let _ = std::ffi::CString::from_raw(ptr as *mut std::ffi::c_char);
}

#[no_mangle]
pub unsafe extern "C" fn MVCCScanCursorOpen(
    db: MVCCDatabaseRef,
    tx_id: u64,
    table_id: u64,
) -> MVCCScanCursorRef {
    tracing::debug!("MVCCScanCursorOpen()");
    // Reference is transmuted to &'static in order to be able to pass the cursor back to C.
    // The contract with C is to never use a cursor after MVCCDatabaseClose() has been called.
    let database = unsafe { std::mem::transmute::<&DbContext, &'static DbContext>(db.get_ref()) };
    let (database, runtime) = (&database.db, &database.runtime);
    match runtime
        .block_on(async move { mvcc_rs::cursor::ScanCursor::new(database, tx_id, table_id).await })
    {
        Ok(cursor) => {
            if cursor.is_empty() {
                tracing::debug!("Cursor is empty");
                return MVCCScanCursorRef {
                    ptr: std::ptr::null_mut(),
                };
            }
            tracing::debug!("Cursor open: {cursor:?}");
            MVCCScanCursorRef {
                ptr: Box::into_raw(Box::new(ScanCursorContext { cursor, db })),
            }
        }
        Err(e) => {
            tracing::error!("MVCCScanCursorOpen: {e}");
            MVCCScanCursorRef {
                ptr: std::ptr::null_mut(),
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn MVCCScanCursorClose(cursor: MVCCScanCursorRef) {
    tracing::debug!("MVCCScanCursorClose()");
    if cursor.ptr.is_null() {
        tracing::debug!("warning: `cursor` is null in MVCCScanCursorClose()");
        return;
    }
    let cursor_ctx = unsafe { Box::from_raw(cursor.ptr) };
    let db_context = cursor_ctx.db.clone();
    let runtime = &db_context.get_ref().runtime;
    runtime.block_on(async move { cursor_ctx.cursor.close().await.ok() });
}

#[no_mangle]
pub unsafe extern "C" fn MVCCScanCursorRead(
    cursor: MVCCScanCursorRef,
    value_ptr: *mut *mut u8,
    value_len: *mut i64,
) -> MVCCError {
    tracing::debug!("MVCCScanCursorRead()");
    if cursor.ptr.is_null() {
        tracing::debug!("warning: `cursor` is null in MVCCScanCursorRead()");
        return MVCCError::MVCC_IO_ERROR_READ;
    }
    let cursor_ctx = unsafe { &*cursor.ptr };
    let runtime = &cursor_ctx.db.get_ref().runtime;
    let cursor = &cursor_ctx.cursor;

    // TODO: deduplicate with MVCCDatabaseRead()
    match runtime.block_on(async move {
        let maybe_row = cursor.current_row().await?;
        match maybe_row {
            Some(row) => {
                tracing::debug!("Found row {row:?}");
                let str_len = row.data.len() + 1;
                let value = std::ffi::CString::new(row.data.as_bytes()).map_err(|e| {
                    mvcc_rs::errors::DatabaseError::Io(format!(
                        "Failed to transform read data into CString: {e}"
                    ))
                })?;
                unsafe {
                    *value_ptr = value.into_raw() as *mut u8;
                    *value_len = str_len as i64;
                }
            }
            None => unsafe { *value_len = -1 },
        };
        Ok::<(), mvcc_rs::errors::DatabaseError>(())
    }) {
        Ok(_) => {
            tracing::debug!("MVCCDatabaseRead: success");
            MVCCError::MVCC_OK
        }
        Err(e) => {
            tracing::error!("MVCCDatabaseRead: {e}");
            MVCCError::MVCC_IO_ERROR_READ
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn MVCCScanCursorNext(cursor: MVCCScanCursorRef) -> std::ffi::c_int {
    let cursor_ctx = unsafe { &mut *cursor.ptr };
    let cursor = &mut cursor_ctx.cursor;
    tracing::debug!("MVCCScanCursorNext(): {}", cursor.index);
    if cursor.forward() {
        tracing::debug!("Forwarded to {}", cursor.index);
        1
    } else {
        tracing::debug!("Forwarded to end");
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn MVCCScanCursorPosition(cursor: MVCCScanCursorRef) -> u64 {
    let cursor_ctx = unsafe { &mut *cursor.ptr };
    let cursor = &mut cursor_ctx.cursor;
    cursor
        .current_row_id()
        .map(|row_id| row_id.row_id)
        .unwrap_or(0)
}
