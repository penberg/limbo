use std::ffi::{c_char, c_void};
#[allow(dead_code)]
#[repr(C)]
pub enum ResultCode {
    Error = -1,
    Ok = 0,
    Row = 1,
    Busy = 2,
    Io = 3,
    Interrupt = 4,
    Invalid = 5,
    Null = 6,
    NoMem = 7,
    ReadOnly = 8,
    NoData = 9,
    Done = 10,
    SyntaxErr = 11,
    ConstraintViolation = 12,
    NoSuchEntity = 13,
}

#[repr(C)]
pub enum ValueType {
    Integer = 0,
    Text = 1,
    Blob = 2,
    Real = 3,
    Null = 4,
}

#[repr(C)]
pub struct LimboValue {
    value_type: ValueType,
    value: ValueUnion,
}

#[repr(C)]
union ValueUnion {
    int_val: i64,
    real_val: f64,
    text_ptr: *const c_char,
    blob_ptr: *const c_void,
}

#[repr(C)]
struct Blob {
    data: *const u8,
    len: i64,
}

pub struct AllocPool {
    strings: Vec<String>,
    blobs: Vec<Vec<u8>>,
}

impl AllocPool {
    pub fn new() -> Self {
        AllocPool {
            strings: Vec::new(),
            blobs: Vec::new(),
        }
    }
    pub fn add_string(&mut self, s: String) -> &String {
        self.strings.push(s);
        self.strings.last().unwrap()
    }

    pub fn add_blob(&mut self, b: Vec<u8>) -> &Vec<u8> {
        self.blobs.push(b);
        self.blobs.last().unwrap()
    }
}

#[no_mangle]
pub extern "C" fn free_blob(blob_ptr: *mut c_void) {
    if blob_ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(blob_ptr as *mut Blob);
    }
}

#[allow(dead_code)]
impl ValueUnion {
    fn from_str(s: &str) -> Self {
        let cstr = std::ffi::CString::new(s).expect("Failed to create CString");
        ValueUnion {
            text_ptr: cstr.into_raw(),
        }
    }

    fn from_bytes(b: &[u8]) -> Self {
        let blob = Box::new(Blob {
            data: b.as_ptr(),
            len: b.len() as i64,
        });
        ValueUnion {
            blob_ptr: Box::into_raw(blob) as *const c_void,
        }
    }

    fn from_int(i: i64) -> Self {
        ValueUnion { int_val: i }
    }

    fn from_real(r: f64) -> Self {
        ValueUnion { real_val: r }
    }

    fn from_null() -> Self {
        ValueUnion { int_val: 0 }
    }

    pub fn to_int(&self) -> i64 {
        unsafe { self.int_val }
    }

    pub fn to_real(&self) -> f64 {
        unsafe { self.real_val }
    }

    pub fn to_str(&self) -> &str {
        unsafe {
            if self.text_ptr.is_null() {
                return "";
            }
            std::ffi::CStr::from_ptr(self.text_ptr)
                .to_str()
                .unwrap_or("")
        }
    }

    pub fn to_bytes(&self) -> &[u8] {
        let blob = unsafe { self.blob_ptr as *const Blob };
        let blob = unsafe { &*blob };
        unsafe { std::slice::from_raw_parts(blob.data, blob.len as usize) }
    }
}

impl LimboValue {
    fn new(value_type: ValueType, value: ValueUnion) -> Self {
        LimboValue { value_type, value }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_ptr(self) -> *const c_void {
        Box::into_raw(Box::new(self)) as *const c_void
    }

    pub fn from_value(value: &limbo_core::Value<'_>) -> Self {
        match value {
            limbo_core::Value::Integer(i) => {
                LimboValue::new(ValueType::Integer, ValueUnion::from_int(*i))
            }
            limbo_core::Value::Float(r) => {
                LimboValue::new(ValueType::Real, ValueUnion::from_real(*r))
            }
            limbo_core::Value::Text(s) => LimboValue::new(ValueType::Text, ValueUnion::from_str(s)),
            limbo_core::Value::Blob(b) => {
                LimboValue::new(ValueType::Blob, ValueUnion::from_bytes(b))
            }
            limbo_core::Value::Null => LimboValue::new(ValueType::Null, ValueUnion::from_null()),
        }
    }

    // The values we get from Go need to be temporarily owned by the statement until they are bound
    // then they can be cleaned up immediately afterwards
    pub fn to_value<'pool>(&self, pool: &'pool mut AllocPool) -> limbo_core::Value<'pool> {
        match self.value_type {
            ValueType::Integer => {
                if unsafe { self.value.int_val == 0 } {
                    return limbo_core::Value::Null;
                }
                limbo_core::Value::Integer(unsafe { self.value.int_val })
            }
            ValueType::Real => {
                if unsafe { self.value.real_val == 0.0 } {
                    return limbo_core::Value::Null;
                }
                limbo_core::Value::Float(unsafe { self.value.real_val })
            }
            ValueType::Text => {
                if unsafe { self.value.text_ptr.is_null() } {
                    return limbo_core::Value::Null;
                }
                let cstr = unsafe { std::ffi::CStr::from_ptr(self.value.text_ptr) };
                match cstr.to_str() {
                    Ok(utf8_str) => {
                        let owned = utf8_str.to_owned();
                        let borrowed = pool.add_string(owned);
                        limbo_core::Value::Text(borrowed)
                    }
                    Err(_) => limbo_core::Value::Null,
                }
            }
            ValueType::Blob => {
                if unsafe { self.value.blob_ptr.is_null() } {
                    return limbo_core::Value::Null;
                }
                let bytes = self.value.to_bytes();
                let borrowed = pool.add_blob(bytes.to_vec());
                limbo_core::Value::Blob(borrowed)
            }
            ValueType::Null => limbo_core::Value::Null,
        }
    }
}
