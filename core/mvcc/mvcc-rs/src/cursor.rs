use crate::clock::LogicalClock;
use crate::database::{Database, DatabaseInner, Result, Row};
use crate::persistent_storage::Storage;
use crate::sync::AsyncMutex;

#[derive(Debug)]
pub struct ScanCursor<
    'a,
    Clock: LogicalClock,
    StorageImpl: Storage,
    Mutex: AsyncMutex<Inner = DatabaseInner<Clock, StorageImpl>>,
> {
    pub db: &'a Database<Clock, StorageImpl, Mutex>,
    pub row_ids: Vec<u64>,
    pub index: usize,
    tx_id: u64,
}

impl<
        'a,
        Clock: LogicalClock,
        StorageImpl: Storage,
        Mutex: AsyncMutex<Inner = DatabaseInner<Clock, StorageImpl>>,
    > ScanCursor<'a, Clock, StorageImpl, Mutex>
{
    pub async fn new(
        db: &'a Database<Clock, StorageImpl, Mutex>,
    ) -> Result<ScanCursor<'a, Clock, StorageImpl, Mutex>> {
        let tx_id = db.begin_tx().await;
        let row_ids = db.scan_row_ids().await?;
        Ok(Self {
            db,
            tx_id,
            row_ids,
            index: 0,
        })
    }

    pub async fn current(&self) -> Result<Option<Row>> {
        let id = self.row_ids[self.index];
        self.db.read(self.tx_id, id).await
    }

    pub fn forward(&mut self) -> bool {
        self.index += 1;
        self.index < self.row_ids.len()
    }
}
