pub mod bugbase;
pub mod cli;
pub mod clock;
pub mod differential;
pub mod doublecheck;
pub mod env;
pub mod execution;
#[expect(dead_code)]
pub mod file;
pub mod io;
pub mod memory;

pub const FAULT_ERROR_MSG: &str = "Injected Fault";

pub trait SimIO: turso_core::IO {
    fn inject_fault(&self, fault: bool);

    /// Inject faults selectively per database file.
    /// Each entry in `faults` is `(path_stem, should_fault)`.
    /// Files whose path contains a given stem get that fault setting.
    fn inject_fault_selective(&self, faults: &[(&str, bool)]);

    fn print_stats(&self);

    fn syncing(&self) -> bool;

    fn close_files(&self);

    /// Discard writes not covered by a completed successful file sync.
    fn power_loss(&self) -> turso_core::Result<()>;

    fn persist_files(&self) -> anyhow::Result<()>;
}
