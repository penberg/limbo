/// Common results that different functions can return in limbo.
pub enum LimboResult {
    /// Couldn't acquire a lock
    Busy,
    Ok,
}
