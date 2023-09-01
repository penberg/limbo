use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lig_core::{Database, DatabaseRef};
use pprof::criterion::{Output, PProfProfiler};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek};
use std::sync::Arc;

struct IO {
    inner: RefCell<IOInner>,
}

struct IOInner {
    db_refs: usize,
    db_files: HashMap<DatabaseRef, File>,
}

impl lig_core::IO for IO {
    fn open(&self, path: &str) -> Result<DatabaseRef> {
        println!("Opening database file {}", path);
        let file = std::fs::File::open(path)?;
        let mut inner = self.inner.borrow_mut();
        let db_ref = inner.db_refs;
        inner.db_refs += 1;
        inner.db_files.insert(db_ref, file);
        Ok(db_ref)
    }

    fn get(&self, database_ref: DatabaseRef, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        let page_size = buf.len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let mut inner = self.inner.borrow_mut();
        let file = inner.db_files.get_mut(&database_ref).unwrap();
        let pos = (page_idx - 1) * page_size;
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        file.read_exact(buf)?;
        Ok(())
    }
}

impl IO {
    fn new() -> Self {
        Self {
            inner: RefCell::new(IOInner {
                db_refs: 0,
                db_files: HashMap::new(),
            }),
        }
    }
}

fn bench_db() -> Database {
    let io = IO::new();
    Database::open(Arc::new(io), "../testing/hello.db").unwrap()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("libsql");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    let conn = db.connect();

    let stmt = conn.prepare("SELECT 1").unwrap();
    group.bench_function("Execute prepared statement: 'SELECT 1'", |b| {
        b.iter(|| {
            let mut rows = stmt.query().unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            stmt.reset();
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
