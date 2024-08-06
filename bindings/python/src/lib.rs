use anyhow::Result;
use limbo_core::{Connection, RowResult, Value, IO};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::sync::{Arc, Mutex};

#[pyclass(unsendable)]
pub struct Database {
    _inner: Arc<Mutex<limbo_core::Database>>,
    _io: Arc<limbo_core::PlatformIO>,
}

#[pymethods]
impl Database {
    #[new]
    pub fn new(path: &str) -> Result<Self> {
        let io = Arc::new(limbo_core::PlatformIO::new()?);
        let db = limbo_core::Database::open_file(io.clone(), path)?;
        Ok(Database {
            _inner: Arc::new(Mutex::new(db)),
            _io: io,
        })
    }

    pub fn exec(&self, sql: &str) -> Result<Vec<PyObject>> {
        let conn = self
            ._inner
            .lock()
            .map_err(|_| PyException::new_err("Failed to acquire lock"))?
            .connect();
        match query(conn, self._io.clone(), sql) {
            Ok(results) => Ok(results),
            Err(e) => Err(e),
        }
    }
}

fn query(conn: Connection, io: Arc<limbo_core::PlatformIO>, sql: &str) -> Result<Vec<PyObject>> {
    Python::with_gil(|py| {
        let mut results = vec![];

        match conn.query(sql) {
            Ok(Some(ref mut rows)) => loop {
                match rows.next_row()? {
                    RowResult::Row(row) => {
                        let args = PyTuple::new_bound(
                            py,
                            row.values
                                .iter()
                                .map(|value| match value {
                                    Value::Null => py.None(),
                                    Value::Integer(i) => i.into_py(py),
                                    Value::Float(f) => f.into_py(py),
                                    Value::Text(s) => s.into_py(py),
                                    Value::Blob(b) => b.clone().clone().into_py(py),
                                })
                                .collect::<Vec<PyObject>>(),
                        )
                        .into_py(py);
                        results.push(args);
                    }
                    RowResult::IO => {
                        io.run_once()?;
                    }
                    RowResult::Done => break,
                }
            },
            Ok(None) => {}
            Err(err) => {
                return Err(anyhow::anyhow!(format!("Query failed: {:?}", err)));
            }
        }

        conn.cacheflush()?; // Cache flush if needed
        Ok(results)
    })
}

#[pymodule]
fn limbo(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Database>()?;
    Ok(())
}
