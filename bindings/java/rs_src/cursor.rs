use crate::connection::Connection;
use crate::errors::ErrorCode;
use crate::utils::row_to_obj_array;
use crate::{eprint_return, eprint_return_null};
use jni::errors::JniError;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use limbo_core::IO;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Cursor {
    /// This read/write attribute specifies the number of rows to fetch at a time with `.fetchmany()`.
    /// It defaults to `1`, meaning it fetches a single row at a time.
    pub(crate) array_size: i64,

    pub(crate) conn: Connection,

    /// The `.description` attribute is a read-only sequence of 7-item, each describing a column in the result set:
    ///
    /// - `name`: The column's name (always present).
    /// - `type_code`: The data type code (always present).
    /// - `display_size`: Column's display size (optional).
    /// - `internal_size`: Column's internal size (optional).
    /// - `precision`: Numeric precision (optional).
    /// - `scale`: Numeric scale (optional).
    /// - `null_ok`: Indicates if null values are allowed (optional).
    ///
    /// The `name` and `type_code` fields are mandatory; others default to `None` if not applicable.
    ///
    /// This attribute is `None` for operations that do not return rows or if no `.execute*()` method has been invoked.
    pub(crate) description: Option<Description>,

    /// Read-only attribute that provides the number of modified rows for `INSERT`, `UPDATE`, `DELETE`,
    /// and `REPLACE` statements; it is `-1` for other statements, including CTE queries.
    /// It is only updated by the `execute()` and `executemany()` methods after the statement has run to completion.
    /// This means any resulting rows must be fetched for `rowcount` to be updated.
    pub(crate) rowcount: i64,

    pub(crate) smt: Option<Arc<Mutex<limbo_core::Statement>>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct Description {
    _name: String,
    _type_code: String,
    _display_size: Option<String>,
    _internal_size: Option<String>,
    _precision: Option<String>,
    _scale: Option<String>,
    _null_ok: Option<String>,
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursor")
            .field("array_size", &self.array_size)
            .field("description", &self.description)
            .field("rowcount", &self.rowcount)
            .finish()
    }
}

/// TODO: we should find a way to handle Error thrown by rust and how to handle those errors in java
#[no_mangle]
#[allow(improper_ctypes_definitions, clippy::arc_with_non_send_sync)]
pub extern "system" fn Java_org_github_tursodatabase_limbo_Cursor_execute<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    cursor_ptr: jlong,
    sql: JString<'local>,
) -> Result<(), JniError> {
    let sql: String = env
        .get_string(&sql)
        .expect("Could not extract query")
        .into();

    let stmt_is_dml = stmt_is_dml(&sql);
    if stmt_is_dml {
        return eprint_return!(
            "DML statements (INSERT/UPDATE/DELETE) are not fully supported in this version",
            JniError::Other(ErrorCode::STATEMENT_IS_DML)
        );
    }

    let cursor = to_cursor(cursor_ptr);
    let conn_lock = match cursor.conn.conn.lock() {
        Ok(lock) => lock,
        Err(_) => return eprint_return!("Failed to acquire connection lock", JniError::Other(-1)),
    };

    match conn_lock.prepare(&sql) {
        Ok(statement) => {
            cursor.smt = Some(Arc::new(Mutex::new(statement)));
            Ok(())
        }
        Err(e) => {
            eprint_return!(
                &format!("Failed to prepare statement: {:?}", e),
                JniError::Other(-1)
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_limbo_Cursor_fetchOne<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    cursor_ptr: jlong,
) -> JObject<'local> {
    let cursor = to_cursor(cursor_ptr);

    if let Some(smt) = &cursor.smt {
        loop {
            let mut smt_lock = match smt.lock() {
                Ok(lock) => lock,
                Err(_) => {
                    return eprint_return_null!(
                        "Failed to acquire statement lock",
                        JniError::Other(-1)
                    )
                }
            };

            match smt_lock.step() {
                Ok(limbo_core::StepResult::Row(row)) => {
                    return match row_to_obj_array(&mut env, &row) {
                        Ok(r) => r,
                        Err(e) => eprint_return_null!(&format!("{:?}", e), JniError::Other(-1)),
                    }
                }
                Ok(limbo_core::StepResult::IO) => {
                    if let Err(e) = cursor.conn.io.run_once() {
                        return eprint_return_null!(
                            &format!("IO Error: {:?}", e),
                            JniError::Other(-1)
                        );
                    }
                }
                Ok(limbo_core::StepResult::Interrupt) => return JObject::null(),
                Ok(limbo_core::StepResult::Done) => return JObject::null(),
                Ok(limbo_core::StepResult::Busy) => {
                    return eprint_return_null!("Busy error", JniError::Other(-1));
                }
                Err(e) => {
                    return eprint_return_null!(
                        format!("Step error: {:?}", e),
                        JniError::Other(-1)
                    );
                }
            };
        }
    } else {
        eprint_return_null!("No statement prepared for execution", JniError::Other(-1))
    }
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_limbo_Cursor_fetchAll<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    cursor_ptr: jlong,
) -> JObject<'local> {
    let cursor = to_cursor(cursor_ptr);

    if let Some(smt) = &cursor.smt {
        let mut rows = Vec::new();
        loop {
            let mut smt_lock = match smt.lock() {
                Ok(lock) => lock,
                Err(_) => {
                    return eprint_return_null!(
                        "Failed to acquire statement lock",
                        JniError::Other(-1)
                    )
                }
            };

            match smt_lock.step() {
                Ok(limbo_core::StepResult::Row(row)) => match row_to_obj_array(&mut env, &row) {
                    Ok(r) => rows.push(r),
                    Err(e) => return eprint_return_null!(&format!("{:?}", e), JniError::Other(-1)),
                },
                Ok(limbo_core::StepResult::IO) => {
                    if let Err(e) = cursor.conn.io.run_once() {
                        return eprint_return_null!(
                            &format!("IO Error: {:?}", e),
                            JniError::Other(-1)
                        );
                    }
                }
                Ok(limbo_core::StepResult::Interrupt) => {
                    return JObject::null();
                }
                Ok(limbo_core::StepResult::Done) => {
                    break;
                }
                Ok(limbo_core::StepResult::Busy) => {
                    return eprint_return_null!("Busy error", JniError::Other(-1));
                }
                Err(e) => {
                    return eprint_return_null!(
                        format!("Step error: {:?}", e),
                        JniError::Other(-1)
                    );
                }
            };
        }

        let array_class = env
            .find_class("[Ljava/lang/Object;")
            .expect("Failed to find Object array class");
        let result_array = env
            .new_object_array(rows.len() as i32, array_class, JObject::null())
            .expect("Failed to create new object array");

        for (i, row) in rows.into_iter().enumerate() {
            env.set_object_array_element(&result_array, i as i32, row)
                .expect("Failed to set object array element");
        }

        result_array.into()
    } else {
        eprint_return_null!("No statement prepared for execution", JniError::Other(-1))
    }
}

fn to_cursor(cursor_ptr: jlong) -> &'static mut Cursor {
    unsafe { &mut *(cursor_ptr as *mut Cursor) }
}

fn stmt_is_dml(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE")
}
