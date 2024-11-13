use limbo_core::{OpenFlags, Page, Result, IO};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
    db: Rc<limbo_core::Database>,
    conn: Rc<limbo_core::Connection>,
}

#[allow(clippy::arc_with_non_send_sync)]
#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new(path: &str) -> Database {
        let io = Arc::new(PlatformIO { vfs: VFS::new() });
        let file = io
            .open_file(path, limbo_core::OpenFlags::None, false)
            .unwrap();
        let page_io = Rc::new(DatabaseStorage::new(file));
        let wal = Rc::new(RefCell::new(Wal {}));
        let db = limbo_core::Database::open(io, page_io, wal).unwrap();
        let conn = db.connect();
        Database { db, conn }
    }

    #[wasm_bindgen]
    pub fn exec(&self, _sql: &str) {}

    #[wasm_bindgen]
    pub fn prepare(&self, _sql: &str) -> Statement {
        let stmt = self.conn.prepare(_sql).unwrap();
        Statement {
            inner: RefCell::new(stmt),
        }
    }
}

#[wasm_bindgen]
pub struct Statement {
    inner: RefCell<limbo_core::Statement>,
}

#[wasm_bindgen]
impl Statement {
    pub fn all(&self) -> js_sys::Array {
        let array = js_sys::Array::new();
        loop {
            match self.inner.borrow_mut().step() {
                Ok(limbo_core::RowResult::Row(row)) => {
                    let row_array = js_sys::Array::new();
                    for value in row.values {
                        row_array.push(&JsValue::from_str(&value.to_string()));
                    }
                    array.push(&row_array);
                }
                Ok(limbo_core::RowResult::IO) => todo!(),
                Ok(limbo_core::RowResult::Done) => break,
                Err(e) => panic!("Error: {:?}", e),
            }
        }
        array
    }
}

pub struct File {
    vfs: VFS,
    fd: i32,
}

#[allow(dead_code)]
impl File {
    fn new(vfs: VFS, fd: i32) -> Self {
        File { vfs, fd }
    }
}

impl limbo_core::File for File {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        // TODO
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        // TODO
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<limbo_core::Completion>) -> Result<()> {
        let r = match &*c {
            limbo_core::Completion::Read(r) => r,
            _ => unreachable!(),
        };
        {
            let mut buf = r.buf_mut();
            let buf: &mut [u8] = buf.as_mut_slice();
            let nr = self.vfs.pread(self.fd, buf, pos);
            assert!(nr >= 0);
        }
        r.complete();
        Ok(())
    }

    fn pwrite(
        &self,
        _pos: usize,
        _buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        _c: Rc<limbo_core::Completion>,
    ) -> Result<()> {
        todo!()
    }

    fn sync(&self, _c: Rc<limbo_core::Completion>) -> Result<()> {
        todo!()
    }

    fn size(&self) -> Result<u64> {
        todo!()
    }
}

pub struct PlatformIO {
    vfs: VFS,
}

impl limbo_core::IO for PlatformIO {
    fn open_file(
        &self,
        path: &str,
        _flags: OpenFlags,
        _direct: bool,
    ) -> Result<Rc<dyn limbo_core::File>> {
        let fd = self.vfs.open(path);
        Ok(Rc::new(File {
            vfs: VFS::new(),
            fd,
        }))
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let random_f64 = Math_random();
        (random_f64 * i64::MAX as f64) as i64
    }

    fn get_current_time(&self) -> String {
        let date = Date::new();
        date.toISOString()
    }
}

#[wasm_bindgen]
extern "C" {
    fn Math_random() -> f64;
}

#[wasm_bindgen]
extern "C" {
    type Date;

    #[wasm_bindgen(constructor)]
    fn new() -> Date;

    #[wasm_bindgen(method, getter)]
    fn toISOString(this: &Date) -> String;
}

pub struct DatabaseStorage {
    file: Rc<dyn limbo_core::File>,
}

impl DatabaseStorage {
    pub fn new(file: Rc<dyn limbo_core::File>) -> Self {
        DatabaseStorage { file }
    }
}

#[allow(dead_code)]
struct BufferPool {}

impl limbo_core::DatabaseStorage for DatabaseStorage {
    fn read_page(&self, page_idx: usize, c: Rc<limbo_core::Completion>) -> Result<()> {
        let r = match &(*c) {
            limbo_core::Completion::Read(r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(limbo_core::LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        _page_idx: usize,
        _buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        _c: Rc<limbo_core::Completion>,
    ) -> Result<()> {
        todo!()
    }

    fn sync(&self, _c: Rc<limbo_core::Completion>) -> Result<()> {
        todo!()
    }
}

pub struct Wal {}

impl limbo_core::Wal for Wal {
    fn begin_read_tx(&self) -> Result<()> {
        Ok(())
    }

    fn end_read_tx(&self) -> Result<()> {
        Ok(())
    }

    fn find_frame(&self, _page_id: u64) -> Result<Option<u64>> {
        Ok(None)
    }

    fn begin_write_tx(&self) -> Result<()> {
        todo!()
    }

    fn end_write_tx(&self) -> Result<()> {
        todo!()
    }

    fn read_frame(
        &self,
        _frame_id: u64,
        _page: Rc<std::cell::RefCell<limbo_core::Page>>,
        _buffer_pool: Rc<limbo_core::BufferPool>,
    ) -> Result<()> {
        todo!()
    }

    fn should_checkpoint(&self) -> bool {
        false
    }

    fn append_frame(
        &mut self,
        _page: Rc<RefCell<Page>>,
        _db_size: u32,
        _pager: &limbo_core::Pager,
        _write_counter: Rc<RefCell<usize>>,
    ) -> Result<()> {
        todo!()
    }

    fn checkpoint(
        &mut self,
        _pager: &limbo_core::Pager,
        _write_counter: Rc<RefCell<usize>>,
    ) -> Result<limbo_core::CheckpointStatus> {
        todo!()
    }

    fn sync(&mut self) -> Result<limbo_core::CheckpointStatus> {
        Ok(limbo_core::CheckpointStatus::Done)
    }
}

#[wasm_bindgen(module = "/vfs.js")]
extern "C" {
    type VFS;

    #[wasm_bindgen(constructor)]
    fn new() -> VFS;

    #[wasm_bindgen(method)]
    fn open(this: &VFS, path: &str) -> i32;

    #[wasm_bindgen(method)]
    fn close(this: &VFS, fd: i32) -> bool;

    #[wasm_bindgen(method)]
    fn pwrite(this: &VFS, fd: i32, buffer: &[u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn pread(this: &VFS, fd: i32, buffer: &mut [u8], offset: usize) -> i32;
}

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}
