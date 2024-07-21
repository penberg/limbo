use crate::sqlite3;

pub fn sqlite3_safety_check_sick_or_ok(_db: &sqlite3) -> bool {
    match _db.e_open_state {
        crate::SQLITE_STATE_SICK | crate::SQLITE_STATE_OPEN | crate::SQLITE_STATE_BUSY => true,
        _ => {
            eprintln!("Invalid database state: {}", _db.e_open_state);
            false
        }
    }
}
