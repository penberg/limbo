use crate::errors::LimboError;
use crate::errors::Result;
use jni::objects::{JObject, JValue};
use jni::sys::jlong;
use jni::JNIEnv;
use limbo_core::Statement;

pub struct CoreStatement {
    pub(crate) stmt: Statement,
}

impl CoreStatement {
    pub fn to_ptr(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    pub fn new(stmt: Statement) -> Self {
        CoreStatement { stmt }
    }

    #[allow(dead_code)]
    pub fn drop(ptr: jlong) {
        let _boxed = unsafe { Box::from_raw(ptr as *mut CoreStatement) };
    }
}

pub fn to_statement(ptr: jlong) -> Result<&'static mut CoreStatement> {
    if ptr == 0 {
        Err(LimboError::InvalidConnectionPointer)
    } else {
        unsafe { Ok(&mut *(ptr as *mut CoreStatement)) }
    }
}

#[allow(dead_code)]
fn row_to_obj_array<'local>(
    env: &mut JNIEnv<'local>,
    row: &limbo_core::Row,
) -> Result<JObject<'local>> {
    let obj_array =
        env.new_object_array(row.values.len() as i32, "java/lang/Object", JObject::null())?;

    for (i, value) in row.values.iter().enumerate() {
        let obj = match value {
            limbo_core::Value::Null => JObject::null(),
            limbo_core::Value::Integer(i) => {
                env.new_object("java/lang/Long", "(J)V", &[JValue::Long(*i)])?
            }
            limbo_core::Value::Float(f) => {
                env.new_object("java/lang/Double", "(D)V", &[JValue::Double(*f)])?
            }
            limbo_core::Value::Text(s) => env.new_string(s)?.into(),
            limbo_core::Value::Blob(b) => env.byte_array_from_slice(b)?.into(),
        };
        if let Err(e) = env.set_object_array_element(&obj_array, i as i32, obj) {
            eprintln!("Error on parsing row: {:?}", e);
        }
    }

    Ok(obj_array.into())
}
