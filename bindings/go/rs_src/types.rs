#[repr(C)]
pub enum ResultCode {
    Error = -1,
    Ok = 0,
    Row = 1,
    Busy = 2,
    Done = 3,
    Io = 4,
    Interrupt = 5,
    Invalid = 6,
    Null = 7,
    NoMem = 8,
    ReadOnly = 9,
}
