#[cfg(feature = "uuid")]
mod uuid;
#[cfg(feature = "uuid")]
pub use uuid::{exec_ts_from_uuid7, exec_uuid, exec_uuidblob, exec_uuidstr, UuidFunc};

#[derive(Debug, Clone, PartialEq)]
pub enum ExtFunc {
    #[cfg(feature = "uuid")]
    Uuid(UuidFunc),
}

impl std::fmt::Display for ExtFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "uuid")]
            ExtFunc::Uuid(uuidfn) => write!(f, "{}", uuidfn),
            _ => write!(f, "unknown"),
        }
    }
}

impl ExtFunc {
    pub fn resolve_function(name: &str, num_args: usize) -> Option<ExtFunc> {
        match name {
            #[cfg(feature = "uuid")]
            name => UuidFunc::resolve_function(name, num_args),
            _ => None,
        }
    }
}
