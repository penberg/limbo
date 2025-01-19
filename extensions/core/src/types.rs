use std::{fmt::Display, os::raw::c_void};

/// Error type is of type ExtError which can be
/// either a user defined error or an error code
#[repr(C)]
pub enum ResultCode {
    OK = 0,
    Error = 1,
    InvalidArgs = 2,
    Unknown = 3,
    OoM = 4,
    Corrupt = 5,
    NotFound = 6,
    AlreadyExists = 7,
    PermissionDenied = 8,
    Aborted = 9,
    OutOfRange = 10,
    Unimplemented = 11,
    Internal = 12,
    Unavailable = 13,
}

impl ResultCode {
    pub fn is_ok(&self) -> bool {
        matches!(self, ResultCode::OK)
    }
}

impl Display for ResultCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultCode::OK => write!(f, "OK"),
            ResultCode::Error => write!(f, "Error"),
            ResultCode::InvalidArgs => write!(f, "InvalidArgs"),
            ResultCode::Unknown => write!(f, "Unknown"),
            ResultCode::OoM => write!(f, "Out of Memory"),
            ResultCode::Corrupt => write!(f, "Corrupt"),
            ResultCode::NotFound => write!(f, "Not Found"),
            ResultCode::AlreadyExists => write!(f, "Already Exists"),
            ResultCode::PermissionDenied => write!(f, "Permission Denied"),
            ResultCode::Aborted => write!(f, "Aborted"),
            ResultCode::OutOfRange => write!(f, "Out of Range"),
            ResultCode::Unimplemented => write!(f, "Unimplemented"),
            ResultCode::Internal => write!(f, "Internal Error"),
            ResultCode::Unavailable => write!(f, "Unavailable"),
        }
    }
}

#[repr(C)]
#[derive(PartialEq, Debug, Eq, Clone, Copy)]
pub enum ValueType {
    Null,
    Integer,
    Float,
    Text,
    Blob,
    Error,
}

#[repr(C)]
pub struct Value {
    value_type: ValueType,
    value: *mut c_void,
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.value.is_null() {
            return write!(f, "{:?}: Null", self.value_type);
        }
        match self.value_type {
            ValueType::Null => write!(f, "Value {{ Null }}"),
            ValueType::Integer => write!(f, "Value {{ Integer: {} }}", unsafe {
                *(self.value as *const i64)
            }),
            ValueType::Float => write!(f, "Value {{ Float: {} }}", unsafe {
                *(self.value as *const f64)
            }),
            ValueType::Text => write!(f, "Value {{ Text: {:?} }}", unsafe {
                &*(self.value as *const TextValue)
            }),
            ValueType::Blob => write!(f, "Value {{ Blob: {:?} }}", unsafe {
                &*(self.value as *const Blob)
            }),
            ValueType::Error => write!(f, "Value {{ Error: {:?} }}", unsafe {
                &*(self.value as *const TextValue)
            }),
        }
    }
}

#[repr(C)]
pub struct TextValue {
    text: *const u8,
    len: u32,
}

impl std::fmt::Debug for TextValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TextValue {{ text: {:?}, len: {} }}",
            self.text, self.len
        )
    }
}

impl Default for TextValue {
    fn default() -> Self {
        Self {
            text: std::ptr::null(),
            len: 0,
        }
    }
}

impl TextValue {
    pub(crate) fn new(text: *const u8, len: usize) -> Self {
        Self {
            text,
            len: len as u32,
        }
    }

    fn as_str(&self) -> &str {
        if self.text.is_null() {
            return "";
        }
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(self.text, self.len as usize))
        }
    }
}

#[repr(C)]
pub struct Blob {
    data: *const u8,
    size: u64,
}

impl std::fmt::Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blob {{ data: {:?}, size: {} }}", self.data, self.size)
    }
}

impl Blob {
    pub fn new(data: *const u8, size: u64) -> Self {
        Self { data, size }
    }
}

impl Value {
    /// Creates a new Value with type Null
    pub fn null() -> Self {
        Self {
            value_type: ValueType::Null,
            value: std::ptr::null_mut(),
        }
    }

    /// Returns the value type of the Value
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    /// Returns the float value if the Value is the proper type
    pub fn to_float(&self) -> Option<f64> {
        if self.value.is_null() {
            return None;
        }
        match self.value_type {
            ValueType::Float => Some(unsafe { *(self.value as *const f64) }),
            ValueType::Integer => Some(unsafe { *(self.value as *const i64) as f64 }),
            ValueType::Text => {
                let txt = unsafe { &*(self.value as *const TextValue) };
                txt.as_str().parse().ok()
            }
            _ => None,
        }
    }
    /// Returns the text value if the Value is the proper type
    pub fn to_text(&self) -> Option<String> {
        if self.value_type != ValueType::Text {
            return None;
        }
        if self.value.is_null() {
            return None;
        }
        let txt = unsafe { &*(self.value as *const TextValue) };
        Some(String::from(txt.as_str()))
    }

    /// Returns the blob value if the Value is the proper type
    pub fn to_blob(&self) -> Option<Vec<u8>> {
        if self.value_type != ValueType::Blob {
            return None;
        }
        if self.value.is_null() {
            return None;
        }
        let blob = unsafe { &*(self.value as *const Blob) };
        let slice = unsafe { std::slice::from_raw_parts(blob.data, blob.size as usize) };
        Some(slice.to_vec())
    }

    /// Returns the integer value if the Value is the proper type
    pub fn to_integer(&self) -> Option<i64> {
        if self.value.is_null() {
            return None;
        }
        match self.value_type() {
            ValueType::Integer => Some(unsafe { *(self.value as *const i64) }),
            ValueType::Float => Some(unsafe { *(self.value as *const f64) } as i64),
            ValueType::Text => {
                let txt = unsafe { &*(self.value as *const TextValue) };
                txt.as_str().parse().ok()
            }
            _ => None,
        }
    }

    /// Returns the error message if the value is an error
    pub fn to_error(&self) -> Option<String> {
        if self.value_type != ValueType::Error {
            return None;
        }
        if self.value.is_null() {
            return None;
        }
        let err = unsafe { &*(self.value as *const ExtError) };
        match &err.error_type {
            ErrorType::User => {
                if err.message.is_null() {
                    return None;
                }
                let txt = unsafe { &*(err.message as *const TextValue) };
                Some(txt.as_str().to_string())
            }
            ErrorType::ErrCode { code } => Some(format!("{}", code)),
        }
    }

    /// Creates a new integer Value from an i64
    pub fn from_integer(value: i64) -> Self {
        let boxed = Box::new(value);
        Self {
            value_type: ValueType::Integer,
            value: Box::into_raw(boxed) as *mut c_void,
        }
    }

    /// Creates a new float Value from an f64
    pub fn from_float(value: f64) -> Self {
        let boxed = Box::new(value);
        Self {
            value_type: ValueType::Float,
            value: Box::into_raw(boxed) as *mut c_void,
        }
    }
    /// Creates a new text Value from a String
    pub fn from_text(s: String) -> Self {
        let buffer = s.into_boxed_str();
        let ptr = buffer.as_ptr();
        let len = buffer.len();
        std::mem::forget(buffer);
        let text_value = TextValue::new(ptr, len);
        let text_box = Box::new(text_value);
        Self {
            value_type: ValueType::Text,
            value: Box::into_raw(text_box) as *mut c_void,
        }
    }

    /// Creates a new error Value from a ResultCode
    pub fn error(err: ResultCode) -> Self {
        let error = ExtError {
            error_type: ErrorType::ErrCode { code: err },
            message: std::ptr::null_mut(),
        };
        Self {
            value_type: ValueType::Error,
            value: Box::into_raw(Box::new(error)) as *mut c_void,
        }
    }

    /// Create a new user defined error Value with a message
    pub fn custom_error(s: String) -> Self {
        let buffer = s.into_boxed_str();
        let ptr = buffer.as_ptr();
        let len = buffer.len();
        std::mem::forget(buffer);
        let text_value = TextValue::new(ptr, len);
        let text_box = Box::new(text_value);
        let error = ExtError {
            error_type: ErrorType::User,
            message: Box::into_raw(text_box) as *mut c_void,
        };
        Self {
            value_type: ValueType::Error,
            value: Box::into_raw(Box::new(error)) as *mut c_void,
        }
    }

    /// Creates a new blob Value from a Vec<u8>
    pub fn from_blob(value: Vec<u8>) -> Self {
        let boxed = Box::new(Blob::new(value.as_ptr(), value.len() as u64));
        std::mem::forget(value);
        Self {
            value_type: ValueType::Blob,
            value: Box::into_raw(boxed) as *mut c_void,
        }
    }

    /// # Safety
    /// consumes the value while freeing the underlying memory with null check.
    /// however this does assume that the type was properly constructed with
    /// the appropriate value_type and value.
    pub unsafe fn free(self) {
        if self.value.is_null() {
            return;
        }
        match self.value_type {
            ValueType::Integer => {
                let _ = Box::from_raw(self.value as *mut i64);
            }
            ValueType::Float => {
                let _ = Box::from_raw(self.value as *mut f64);
            }
            ValueType::Text => {
                let _ = Box::from_raw(self.value as *mut TextValue);
            }
            ValueType::Blob => {
                let _ = Box::from_raw(self.value as *mut Blob);
            }
            ValueType::Error => {
                let _ = Box::from_raw(self.value as *mut ExtError);
            }
            ValueType::Null => {}
        }
    }
}

#[repr(C)]
pub struct ExtError {
    pub error_type: ErrorType,
    pub message: *mut std::ffi::c_void,
}

#[repr(C)]
pub enum ErrorType {
    User,
    /// User type has a user provided message
    ErrCode {
        code: ResultCode,
    },
}
