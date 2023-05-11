use crate::Db;

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct MVCCDatabaseRef {
    ptr: *const DbContext,
}

impl MVCCDatabaseRef {
    pub fn null() -> MVCCDatabaseRef {
        MVCCDatabaseRef {
            ptr: std::ptr::null(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &DbContext {
        unsafe { &*(self.ptr) }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut DbContext {
        let ptr_mut = self.ptr as *mut DbContext;
        unsafe { &mut (*ptr_mut) }
    }
}

#[allow(clippy::from_over_into)]
impl From<&DbContext> for MVCCDatabaseRef {
    fn from(value: &DbContext) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut DbContext> for MVCCDatabaseRef {
    fn from(value: &mut DbContext) -> Self {
        Self { ptr: value }
    }
}

pub struct DbContext {
    pub(crate) db: Db,
    pub(crate) runtime: tokio::runtime::Runtime,
}

pub struct ScanCursorContext {
    pub cursor: crate::ScanCursor,
    pub db: MVCCDatabaseRef,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct MVCCScanCursorRef {
    pub ptr: *mut ScanCursorContext,
}
