use crate::errors::LimboError;
use jni::objects::JByteArray;
use jni::JNIEnv;

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
