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

    pub fn get_ref(&self) -> &Db {
        &unsafe { &*(self.ptr) }.db
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut Db {
        let ptr_mut = self.ptr as *mut DbContext;
        &mut unsafe { &mut (*ptr_mut) }.db
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
}

pub struct ScanCursorContext {
    pub(crate) cursor: crate::ScanCursor,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct MVCCScanCursorRef {
    pub ptr: *mut ScanCursorContext,
}

impl MVCCScanCursorRef {
    pub fn null() -> MVCCScanCursorRef {
        MVCCScanCursorRef {
            ptr: std::ptr::null_mut(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &crate::ScanCursor {
        &unsafe { &*(self.ptr) }.cursor
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut crate::ScanCursor {
        let ptr_mut = self.ptr as *mut ScanCursorContext;
        &mut unsafe { &mut (*ptr_mut) }.cursor
    }
}
