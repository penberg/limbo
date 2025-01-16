use crate::errors::LimboError;
use jni::objects::{JByteArray, JObject, JValue};
use jni::JNIEnv;

#[allow(dead_code)]
pub(crate) fn row_to_obj_array<'local>(
    env: &mut JNIEnv<'local>,
    row: &limbo_core::Row,
) -> Result<JObject<'local>, LimboError> {
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

pub(crate) fn utf8_byte_arr_to_str(
    env: &JNIEnv,
    bytes: JByteArray,
) -> crate::errors::Result<String> {
    let bytes = env
        .convert_byte_array(bytes)
        .map_err(|_| LimboError::CustomError("Failed to retrieve bytes".to_string()))?;
    let str = String::from_utf8(bytes).map_err(|_| {
        LimboError::CustomError("Failed to convert utf8 byte array into string".to_string())
    })?;
    Ok(str)
}
