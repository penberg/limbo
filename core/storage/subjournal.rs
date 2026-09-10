use crate::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use crate::{turso_assert, turso_assert_eq};

use crate::{
    storage::sqlite3_ondisk::finish_read_page, Buffer, Completion, CompletionError, PageRef, Result,
};

#[derive(Clone)]
pub struct Subjournal {
    file: Arc<dyn crate::io::File>,
    in_use: Arc<AtomicBool>,
}

impl Subjournal {
    pub fn new(file: Arc<dyn crate::io::File>) -> Self {
        Self {
            file,
            in_use: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn try_use(&self) -> Result<()> {
        let result = self
            .in_use
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        if result.is_err() {
            return Err(crate::LimboError::Busy);
        }
        Ok(())
    }

    pub fn stop_use(&self) {
        let result = self
            .in_use
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst);
        turso_assert!(
            result.is_ok(),
            "try_start_use must succeed before stop_use call"
        );
    }

    pub fn in_use(&self) -> bool {
        self.in_use.load(Ordering::SeqCst)
    }

    pub fn write_page(
        &self,
        offset: u64,
        page_size: usize,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        turso_assert_eq!(
            buffer.len(),
            page_size + 4,
            "buffer length should be page_size + 4 bytes for page id"
        );
        self.file.pwrite(offset, buffer, c)
    }

    pub fn read_page_number(&self, offset: u64, page_id_buffer: Arc<Buffer>) -> Result<Completion> {
        turso_assert_eq!(
            page_id_buffer.len(),
            4,
            "page_id_buffer length should be 4 bytes"
        );
        let c = Completion::new_read(
            page_id_buffer,
            move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((buf, bytes_read)) = res else {
                    return None;
                };
                let expected = buf.len();
                if bytes_read != expected as i32 {
                    tracing::error!(
                        "subjournal short read: expected {expected} bytes, got {bytes_read}"
                    );
                    return Some(CompletionError::ShortRead {
                        page_idx: 0, // reading page number header, not a page
                        expected,
                        actual: bytes_read as usize,
                    });
                }
                None
            },
        );
        let c = self.file.pread(offset, c)?;
        Ok(c)
    }

    pub fn read_page(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        page: PageRef,
        page_size: usize,
    ) -> Result<Completion> {
        turso_assert_eq!(buffer.len(), page_size, "buffer length should be page_size");
        let c = Completion::new_read(
            buffer,
            move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((buf, bytes_read)) = res else {
                    return None;
                };
                let page_idx = page.get().id();
                if bytes_read != page_size as i32 {
                    tracing::error!(
                        "subjournal short read on page {page_idx}: expected {page_size} bytes, got {bytes_read}"
                    );
                    return Some(CompletionError::ShortRead {
                        page_idx,
                        expected: page_size,
                        actual: bytes_read as usize,
                    });
                }
                finish_read_page(page_idx, buf, page.clone());
                None
            },
        );
        let c = self.file.pread(offset, c)?;
        Ok(c)
    }

    pub fn truncate(&self, offset: u64) -> Result<Completion> {
        let c = Completion::new_trunc(move |res: Result<i32, CompletionError>| {
            let Ok(_) = res else {
                return;
            };
        });
        self.file.truncate(offset, c)
    }
}
