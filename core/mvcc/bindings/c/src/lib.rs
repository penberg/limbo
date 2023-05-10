#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

mod errors;
mod types;

use errors::MVCCError;
use mvcc_rs::*;
use types::{DbContext, MVCCDatabaseRef};

/// cbindgen:ignore
type Clock = clock::LocalClock;

/// cbindgen:ignore
type Storage = persistent_storage::JsonOnDisk;

/// cbindgen:ignore
type Inner = database::DatabaseInner<Clock, Storage>;

/// cbindgen:ignore
type Db = database::Database<Clock, Storage, tokio::sync::Mutex<Inner>>;

static INIT_RUST_LOG: std::sync::Once = std::sync::Once::new();

#[no_mangle]
pub unsafe extern "C" fn MVCCDatabaseOpen(path: *const std::ffi::c_char) -> MVCCDatabaseRef {
    INIT_RUST_LOG.call_once(|| {
        tracing_subscriber::fmt::init();
    });

    tracing::debug!("MVCCDatabaseOpen");

    let clock = clock::LocalClock::new();
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = match path.to_str() {
        Ok(path) => path,
        Err(_) => {
            tracing::error!("Invalid UTF-8 path");
            return MVCCDatabaseRef::null();
        }
    };
    tracing::debug!("mvccrs: opening persistent storage at {path}");
    let storage = crate::persistent_storage::JsonOnDisk::new(path);
    let db = Db::new(clock, storage);
    let runtime = tokio::runtime::Runtime::new().unwrap();
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
pub unsafe extern "C" fn MVCCDatabaseInsert(
    db: MVCCDatabaseRef,
    id: u64,
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
    let row = database::Row { id, data };
    tracing::debug!("MVCCDatabaseInsert: {row:?}");
    match runtime.block_on(async move {
        let tx = db.begin_tx().await;
        db.insert(tx, row).await?;
        db.commit_tx(tx).await
    }) {
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
    id: u64,
    value_ptr: *mut *mut std::ffi::c_char,
    value_len: *mut i64,
) -> MVCCError {
    let db = db.get_ref();
    let (db, runtime) = (&db.db, &db.runtime);

    match runtime.block_on(async move {
        let tx = db.begin_tx().await;
        let maybe_row = db.read(tx, id).await?;
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
                    *value_ptr = value.into_raw();
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
