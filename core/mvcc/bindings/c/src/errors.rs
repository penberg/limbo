#[repr(C)]
pub enum MVCCError {
    MVCC_OK = 0,
    MVCC_IO_ERROR_READ = 266,
    MVCC_IO_ERROR_WRITE = 778,
}
