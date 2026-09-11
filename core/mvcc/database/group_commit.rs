use super::{LogRecord, TxID};
use crate::storage::wal::TursoRwLock;
#[cfg(test)]
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Arc;
use crate::sync::Mutex;
use rustc_hash::FxHashSet as HashSet;
use std::collections::VecDeque;

#[derive(Debug)]
pub(crate) struct QueuedCommit {
    pub ticket: u64,
    pub tx_id: TxID,
    pub log_record: LogRecord,
}

#[derive(Debug)]
pub(crate) struct GroupBatch {
    pub rest: VecDeque<QueuedCommit>,
    pub writing: QueuedCommit,
    pub advanced_through: Option<u64>,
}

impl GroupBatch {
    pub fn from_lead(writing: QueuedCommit, rest: VecDeque<QueuedCommit>) -> Self {
        Self {
            writing,
            rest,
            advanced_through: None,
        }
    }
}

#[derive(Debug)]
struct GroupState {
    next_ticket: u64,
    durable_through: u64,
    written_through: u64,
    pending: VecDeque<QueuedCommit>,
    /// Tickets whose in-flight `log_tx` was discarded before the offset was
    /// advanced. The waiter rebuilds its log record instead of hanging.
    retry: HashSet<u64>,
    /// Tx currently inside `log_tx`, before the offset advanced.
    issued: Option<TxID>,
    /// Waiters that dropped after `log_tx`. The leader finishes or rolls them
    /// back. They must not roll back themselves.
    abandoned: HashSet<TxID>,
}

pub(crate) enum GroupWork {
    Lead {
        writing: QueuedCommit,
        rest: VecDeque<QueuedCommit>,
    },
    SyncPrefix,
    None,
}

#[derive(Debug)]
pub(crate) struct CommitCoordinator {
    pub pager_commit_lock: Arc<TursoRwLock>,
    group_commit_enabled: AtomicBool,
    group: Mutex<GroupState>,
    #[cfg(test)]
    last_group_size: AtomicUsize,
}

impl CommitCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            pager_commit_lock: Arc::new(TursoRwLock::new()),
            group_commit_enabled: AtomicBool::new(true),
            group: Mutex::new(GroupState {
                next_ticket: 0,
                durable_through: 0,
                written_through: 0,
                pending: VecDeque::new(),
                retry: HashSet::default(),
                issued: None,
                abandoned: HashSet::default(),
            }),
            #[cfg(test)]
            last_group_size: AtomicUsize::new(0),
        }
    }

    pub(crate) fn group_commit_enabled(&self) -> bool {
        self.group_commit_enabled.load(Ordering::Acquire)
    }

    pub(crate) fn set_group_commit_enabled(&self, enabled: bool) {
        self.group_commit_enabled.store(enabled, Ordering::Release);
    }

    pub(crate) fn enqueue(&self, tx_id: TxID, log_record: LogRecord) -> u64 {
        let mut group = self.group.lock();
        group.next_ticket += 1;
        let ticket = group.next_ticket;
        group.pending.push_back(QueuedCommit {
            ticket,
            tx_id,
            log_record,
        });
        ticket
    }

    #[cfg(test)]
    pub(crate) fn take_pending(&self) -> VecDeque<QueuedCommit> {
        let mut group = self.group.lock();
        let batch = std::mem::take(&mut group.pending);
        #[cfg(test)]
        if !batch.is_empty() {
            self.last_group_size.store(batch.len(), Ordering::Release);
        }
        batch
    }

    pub(crate) fn take_work(&self) -> GroupWork {
        let mut group = self.group.lock();
        if !group.retry.is_empty() {
            return if group.written_through > group.durable_through {
                GroupWork::SyncPrefix
            } else {
                GroupWork::None
            };
        }
        match group.pending.pop_front() {
            Some(writing) => {
                let rest = std::mem::take(&mut group.pending);
                #[cfg(test)]
                {
                    self.last_group_size
                        .store(rest.len() + 1, Ordering::Release);
                }
                GroupWork::Lead { writing, rest }
            }
            None if group.written_through > group.durable_through => GroupWork::SyncPrefix,
            None => GroupWork::None,
        }
    }

    pub(crate) fn requeue(&self, records: impl DoubleEndedIterator<Item = QueuedCommit>) {
        let mut group = self.group.lock();
        for entry in records.rev() {
            group.pending.push_front(entry);
        }
    }

    pub(crate) fn mark_durable(&self, ticket: u64) {
        let mut group = self.group.lock();
        let ticket = dense_prefix_cap(ticket, group.written_through, &group.retry);
        group.durable_through = group.durable_through.max(ticket);
    }

    pub(crate) fn durable_through(&self) -> u64 {
        self.group.lock().durable_through
    }

    pub(crate) fn note_written(&self, ticket: u64) {
        let mut group = self.group.lock();
        if group.retry.iter().any(|&hole| hole <= ticket) {
            return;
        }
        group.written_through = group.written_through.max(ticket);
    }

    pub(crate) fn written_through(&self) -> u64 {
        self.group.lock().written_through
    }

    /// Removes `ticket` from the queue. Returns whether it was still waiting.
    pub(crate) fn drop_pending(&self, ticket: u64) -> bool {
        let mut group = self.group.lock();
        group.retry.remove(&ticket);
        if let Some(index) = group
            .pending
            .iter()
            .position(|queued| queued.ticket == ticket)
        {
            group.pending.remove(index);
            true
        } else {
            false
        }
    }

    pub(crate) fn request_retry(&self, ticket: u64) {
        let mut group = self.group.lock();
        group.retry.insert(ticket);
        if group.written_through >= ticket {
            group.written_through = ticket.saturating_sub(1);
        }
        if group.durable_through >= ticket {
            group.durable_through = ticket.saturating_sub(1);
        }
    }

    pub(crate) fn take_retry(&self, ticket: u64) -> bool {
        self.group.lock().retry.remove(&ticket)
    }

    pub(crate) fn note_write_issued(&self, tx_id: TxID) {
        self.group.lock().issued = Some(tx_id);
    }

    pub(crate) fn clear_issued(&self) {
        self.group.lock().issued = None;
    }

    pub(crate) fn abandon_if_issued(&self, tx_id: TxID) -> bool {
        let mut group = self.group.lock();
        if group.issued == Some(tx_id) {
            group.abandoned.insert(tx_id);
            true
        } else {
            false
        }
    }

    pub(crate) fn is_abandoned(&self, tx_id: TxID) -> bool {
        self.group.lock().abandoned.contains(&tx_id)
    }

    pub(crate) fn take_abandoned(&self, tx_id: TxID) -> bool {
        self.group.lock().abandoned.remove(&tx_id)
    }

    #[cfg(test)]
    pub(crate) fn last_group_size(&self) -> usize {
        self.last_group_size.load(Ordering::Acquire)
    }
}

fn dense_prefix_cap(ticket: u64, written_through: u64, retry: &HashSet<u64>) -> u64 {
    let mut cap = ticket.min(written_through);
    if let Some(hole) = retry.iter().copied().filter(|&t| t <= cap).min() {
        cap = hole.saturating_sub(1);
    }
    cap
}
