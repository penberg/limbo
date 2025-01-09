#[cfg(feature = "uuid")]
mod uuid;
use crate::{function::ExternalFunc, Database};
use std::sync::Arc;

use extension_api::{AggregateFunction, ExtensionApi, Result, ScalarFunction, VirtualTable};
#[cfg(feature = "uuid")]
pub use uuid::{exec_ts_from_uuid7, exec_uuid, exec_uuidblob, exec_uuidstr, UuidFunc};

impl ExtensionApi for Database {
    fn register_scalar_function(
        &self,
        name: &str,
        func: Arc<dyn ScalarFunction>,
    ) -> extension_api::Result<()> {
        let ext_func = ExternalFunc::new(name, func.clone());
        self.syms
            .borrow_mut()
            .functions
            .insert(name.to_string(), Arc::new(ext_func));
        Ok(())
    }

    fn register_aggregate_function(
        &self,
        _name: &str,
        _func: Arc<dyn AggregateFunction>,
    ) -> Result<()> {
        todo!("implement aggregate function registration");
    }

    fn register_virtual_table(&self, _name: &str, _table: Arc<dyn VirtualTable>) -> Result<()> {
        todo!("implement virtual table registration");
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExtFunc {
    #[cfg(feature = "uuid")]
    Uuid(UuidFunc),
}

#[allow(unreachable_patterns)] // TODO: remove when more extension funcs added
impl std::fmt::Display for ExtFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "uuid")]
            Self::Uuid(uuidfn) => write!(f, "{}", uuidfn),
            _ => write!(f, "unknown"),
        }
    }
}

#[allow(unreachable_patterns)]
impl ExtFunc {
    pub fn resolve_function(name: &str, num_args: usize) -> Option<ExtFunc> {
        match name {
            #[cfg(feature = "uuid")]
            name => UuidFunc::resolve_function(name, num_args),
            _ => None,
        }
    }
}

//pub fn init(db: &mut crate::Database) {
//    #[cfg(feature = "uuid")]
//    uuid::init(db);
//}
