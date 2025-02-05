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
/// Note - We use String type in C bindings as Row type. Type is generic.
type Db = database::Database<Clock, String>;

/// cbindgen:ignore
/// Note - We use String type in C bindings as Row type. Type is generic.
type ScanCursor = cursor::ScanCursor<'static, Clock, String>;

static INIT_RUST_LOG: std::sync::Once = std::sync::Once::new();

fn storage_for(main_db_path: &str) -> database::Result<Storage> {
    // TODO: let's accept an URL instead of main_db_path here, so we can
    // pass custom S3 endpoints, options, etc.
    if cfg!(feature = "json_on_disk_storage") {
        tracing::info!("JSONonDisk storage stored in {main_db_path}-mvcc");
        return Ok(Storage::new_json_on_disk(format!("{main_db_path}-mvcc")));
    }
    if cfg!(feature = "s3_storage") {
        tracing::info!("S3 storage for {main_db_path}");
        let options = s3::Options::with_create_bucket_if_not_exists(true);
        return Storage::new_s3(options);
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

    tracing::debug!("mvccrs: opening persistent storage for {main_db_path}");
    let storage = match storage_for(main_db_path) {
        Ok(storage) => storage,
        Err(e) => {
            tracing::error!("Failed to open persistent storage: {e}");
            return MVCCDatabaseRef::null();
        }
    };
    let db = Db::new(clock, storage);

    db.recover().ok();

    let db = Box::leak(Box::new(DbContext { db }));
    MVCCDatabaseRef::from(db)
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
    let tx_id = db.begin_tx();
    tracing::debug!("MVCCTransactionBegin: {tx_id}");
    tx_id
}

#[no_mangle]
pub unsafe extern "C" fn MVCCTransactionCommit(db: MVCCDatabaseRef, tx_id: u64) -> MVCCError {
    let db = db.get_ref();
    tracing::debug!("MVCCTransactionCommit: {tx_id}");
    match db.commit_tx(tx_id) {
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
    tracing::debug!("MVCCTransactionRollback: {tx_id}");
    db.rollback_tx(tx_id);
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
    let id = database::RowID { table_id, row_id };
    let row = database::Row { id, data };
    tracing::debug!("MVCCDatabaseInsert: {row:?}");
    match db.insert(tx_id, row) {
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

    match {
        let id = database::RowID { table_id, row_id };
        let maybe_row = db.read(tx_id, id);
        match maybe_row {
            Ok(Some(row)) => {
                tracing::debug!("Found row {row:?}");
                let str_len = row.data.len() + 1;
                let value = std::ffi::CString::new(row.data.as_bytes()).unwrap_or_default();
                unsafe {
                    *value_ptr = value.into_raw() as *mut u8;
                    *value_len = str_len as i64;
                }
            }
            _ => unsafe { *value_len = -1 },
        };
        Ok::<(), mvcc_rs::errors::DatabaseError>(())
    } {
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
    let db = unsafe { std::mem::transmute::<&Db, &'static Db>(db.get_ref()) };
    match mvcc_rs::cursor::ScanCursor::new(db, tx_id, table_id) {
        Ok(cursor) => {
            if cursor.is_empty() {
                tracing::debug!("Cursor is empty");
                return MVCCScanCursorRef {
                    ptr: std::ptr::null_mut(),
                };
            }
            tracing::debug!("Cursor open: {cursor:?}");
            MVCCScanCursorRef {
                ptr: Box::into_raw(Box::new(ScanCursorContext { cursor })),
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
    let cursor = unsafe { Box::from_raw(cursor.ptr) }.cursor;
    cursor.close().ok();
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
    let cursor = cursor.get_ref();

    match {
        let maybe_row = cursor.current_row();
        match maybe_row {
            Ok(Some(row)) => {
                tracing::debug!("Found row {row:?}");
                let str_len = row.data.len() + 1;
                let value = std::ffi::CString::new(row.data.as_bytes()).unwrap_or_default();
                unsafe {
                    *value_ptr = value.into_raw() as *mut u8;
                    *value_len = str_len as i64;
                }
            }
            _ => unsafe { *value_len = -1 },
        };
        Ok::<(), mvcc_rs::errors::DatabaseError>(())
    } {
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
    let cursor = cursor.get_ref_mut();
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
    let cursor = cursor.get_ref();
    cursor
        .current_row_id()
        .map(|row_id| row_id.row_id)
        .unwrap_or(0)
}
