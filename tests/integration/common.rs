use limbo_core::{CheckpointStatus, Connection, Database, IO};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;

#[allow(dead_code)]
pub struct TempDatabase {
    pub path: PathBuf,
    pub io: Arc<dyn IO>,
}

#[allow(dead_code, clippy::arc_with_non_send_sync)]
impl TempDatabase {
    pub fn new(table_sql: &str) -> Self {
        let mut path = TempDir::new().unwrap().into_path();
        path.push("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
            connection.execute(table_sql, ()).unwrap();
        }
        let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new().unwrap());

        Self { path, io }
    }

    pub fn connect_limbo(&self) -> Rc<limbo_core::Connection> {
        log::debug!("conneting to limbo");
        let db = Database::open_file(self.io.clone(), self.path.to_str().unwrap()).unwrap();

        let conn = db.connect();
        log::debug!("connected to limbo");
        conn
    }
}

pub(crate) fn do_flush(conn: &Rc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<()> {
    loop {
        match conn.cacheflush()? {
            CheckpointStatus::Done => {
                break;
            }
            CheckpointStatus::IO => {
                tmp_db.io.run_once()?;
            }
        }
    }
    Ok(())
}

pub(crate) fn compare_string(a: &String, b: &String) {
    assert_eq!(a.len(), b.len(), "Strings are not equal in size!");
    let a = a.as_bytes();
    let b = b.as_bytes();

    let len = a.len();
    for i in 0..len {
        if a[i] != b[i] {
            println!(
                "Bytes differ \n\t at index: dec -> {} hex -> {:#02x} \n\t values dec -> {}!={} hex -> {:#02x}!={:#02x}",
                i, i, a[i], b[i], a[i], b[i]
            );
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TempDatabase;

    #[test]
    fn test_statement_columns() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tmp_db =
            TempDatabase::new("create table test (foo integer, bar integer, baz integer);");
        let conn = tmp_db.connect_limbo();

        let stmt = conn.prepare("select * from test;")?;

        let columns = stmt.columns();
        assert_eq!(columns.len(), 3);
        assert_eq!(&columns[0], "foo");
        assert_eq!(&columns[1], "bar");
        assert_eq!(&columns[2], "baz");

        let stmt = conn.prepare("select foo, bar from test;")?;

        let columns = stmt.columns();
        assert_eq!(columns.len(), 2);
        assert_eq!(&columns[0], "foo");
        assert_eq!(&columns[1], "bar");

        let stmt = conn.prepare("delete from test;")?;
        let columns = stmt.columns();
        assert_eq!(columns.len(), 0);

        let stmt = conn.prepare("insert into test (foo, bar, baz) values (1, 2, 3);")?;
        let columns = stmt.columns();
        assert_eq!(columns.len(), 0);

        let stmt = conn.prepare("delete from test where foo = 1")?;
        let columns = stmt.columns();
        assert_eq!(columns.len(), 0);

        Ok(())
    }
}
