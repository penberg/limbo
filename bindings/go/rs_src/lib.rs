mod rows;
#[allow(dead_code)]
mod statement;
mod types;
use limbo_core::{Connection, Database, LimboError};
use std::{
    ffi::{c_char, c_void},
    rc::Rc,
    str::FromStr,
    sync::Arc,
};

/// # Safety
/// Safe to be called from Go with null terminated DSN string.
/// performs null check on the path.
#[no_mangle]
pub unsafe extern "C" fn db_open(path: *const c_char) -> *mut c_void {
    if path.is_null() {
        println!("Path is null");
        return std::ptr::null_mut();
    }
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = path.to_str().unwrap();
    let db_options = parse_query_str(path);
    if let Ok(io) = get_io(&db_options.path) {
        let db = Database::open_file(io.clone(), &db_options.path.to_string());
        match db {
            Ok(db) => {
                println!("Opened database: {}", path);
                let conn = db.connect();
                return LimboConn::new(conn, io).to_ptr();
            }
            Err(e) => {
                println!("Error opening database: {}", e);
                return std::ptr::null_mut();
            }
        };
    }
    std::ptr::null_mut()
}

#[allow(dead_code)]
struct LimboConn {
    conn: Rc<Connection>,
    io: Arc<dyn limbo_core::IO>,
}

impl LimboConn {
    fn new(conn: Rc<Connection>, io: Arc<dyn limbo_core::IO>) -> Self {
        LimboConn { conn, io }
    }
    #[allow(clippy::wrong_self_convention)]
    fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }

    fn from_ptr(ptr: *mut c_void) -> &'static mut LimboConn {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut LimboConn) }
    }
}

/// Close the database connection
/// # Safety
/// safely frees the connection's memory
#[no_mangle]
pub unsafe extern "C" fn db_close(db: *mut c_void) {
    if !db.is_null() {
        let _ = unsafe { Box::from_raw(db as *mut LimboConn) };
    }
}

#[allow(clippy::arc_with_non_send_sync)]
fn get_io(db_location: &DbType) -> Result<Arc<dyn limbo_core::IO>, LimboError> {
    Ok(match db_location {
        DbType::Memory => Arc::new(limbo_core::MemoryIO::new()?),
        _ => {
            return Ok(Arc::new(limbo_core::PlatformIO::new()?));
        }
    })
}

#[allow(dead_code)]
struct DbOptions {
    path: DbType,
    params: Parameters,
}

#[derive(Default, Debug, Clone)]
enum DbType {
    File(String),
    #[default]
    Memory,
}

impl std::fmt::Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbType::File(path) => write!(f, "{}", path),
            DbType::Memory => write!(f, ":memory:"),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Parameters {
    mode: Mode,
    cache: Option<Cache>,
    vfs: Option<String>,
    nolock: bool,
    immutable: bool,
    modeof: Option<String>,
}

impl FromStr for Parameters {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.contains('?') {
            return Ok(Parameters::default());
        }
        let mut params = Parameters::default();
        for param in s.split('?').nth(1).unwrap().split('&') {
            let mut kv = param.split('=');
            match kv.next() {
                Some("mode") => params.mode = kv.next().unwrap().parse().unwrap(),
                Some("cache") => params.cache = Some(kv.next().unwrap().parse().unwrap()),
                Some("vfs") => params.vfs = Some(kv.next().unwrap().to_string()),
                Some("nolock") => params.nolock = true,
                Some("immutable") => params.immutable = true,
                Some("modeof") => params.modeof = Some(kv.next().unwrap().to_string()),
                _ => {}
            }
        }
        Ok(params)
    }
}

#[derive(Default, Debug, Clone, Copy)]
enum Cache {
    Shared,
    #[default]
    Private,
}

impl FromStr for Cache {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "shared" => Ok(Cache::Shared),
            _ => Ok(Cache::Private),
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Default, Debug, Clone, Copy)]
enum Mode {
    ReadOnly,
    ReadWrite,
    #[default]
    ReadWriteCreate,
}

impl FromStr for Mode {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "readonly" | "ro" => Ok(Mode::ReadOnly),
            "readwrite" | "rw" => Ok(Mode::ReadWrite),
            "readwritecreate" | "rwc" => Ok(Mode::ReadWriteCreate),
            _ => Ok(Mode::default()),
        }
    }
}

// At this point we don't have configurable parameters but many
// DSN's are going to have query parameters
fn parse_query_str(mut path: &str) -> DbOptions {
    if path == ":memory:" {
        return DbOptions {
            path: DbType::Memory,
            params: Parameters::default(),
        };
    }
    if path.starts_with("sqlite://") {
        path = &path[10..];
    }
    if path.contains('?') {
        let parameters = Parameters::from_str(path).unwrap();
        let path = &path[..path.find('?').unwrap()];
        DbOptions {
            path: DbType::File(path.to_string()),
            params: parameters,
        }
    } else {
        DbOptions {
            path: DbType::File(path.to_string()),
            params: Parameters::default(),
        }
    }
}
