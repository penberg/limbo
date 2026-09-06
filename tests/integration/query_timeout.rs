use crate::common::TempDatabase;
use std::time::Duration;
use turso_core::vdbe::StepResult;

fn run_until_terminal(stmt: &mut turso_core::Statement) -> turso_core::Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Row | StepResult::Yield => continue,
            result => return Ok(result),
        }
    }
}

#[turso_macros::test]
fn query_timeout_milliseconds(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    assert_eq!(conn.get_query_timeout_ms(), 0u64);

    conn.set_query_timeout(Duration::from_micros(123_456));
    assert_eq!(conn.get_query_timeout_ms(), 123u64);

    conn.set_query_timeout(Duration::MAX);
    assert_eq!(conn.get_query_timeout_ms(), u64::MAX);

    conn.set_query_timeout(Duration::ZERO);
    assert_eq!(conn.get_query_timeout_ms(), 0u64);
}

#[turso_macros::test]
fn query_timeout_interrupts_long_running_query(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER);")?;
    for i in 0..200 {
        conn.execute(format!("INSERT INTO t VALUES ({i});"))?;
    }
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt = conn.prepare("SELECT a.x FROM t a, t b, t c, t d, t e;")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Interrupt),
        "expected interrupt, got {result:?}"
    );
    Ok(())
}

#[turso_macros::test]
fn query_timeout_allows_short_running_query(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt = conn.prepare("SELECT 1 AS value;")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Done),
        "expected done, got {result:?}"
    );
    Ok(())
}
