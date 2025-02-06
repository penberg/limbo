use crate::common::TempDatabase;
use limbo_core::{StepResult, Value};

#[test]
fn test_statement_reset_bind() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?")?;

    stmt.bind_at(1.try_into()?, Value::Integer(1));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(row.values[0].to_value(), Value::Integer(1));
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    stmt.reset();

    stmt.bind_at(1.try_into()?, Value::Integer(2));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(row.values[0].to_value(), Value::Integer(2));
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    Ok(())
}

#[test]
fn test_statement_bind() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?, ?1, :named, ?3, ?4")?;

    stmt.bind_at(1.try_into()?, Value::Text("hello"));

    let i = stmt.parameters().index(":named").unwrap();
    stmt.bind_at(i, Value::Integer(42));

    stmt.bind_at(3.try_into()?, Value::Blob(&[0x1, 0x2, 0x3]));

    stmt.bind_at(4.try_into()?, Value::Float(0.5));

    assert_eq!(stmt.parameters().count(), 4);

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(s) = row.values[0].to_value() {
                    assert_eq!(s, "hello")
                }

                if let Value::Text(s) = row.values[1].to_value() {
                    assert_eq!(s, "hello")
                }

                if let Value::Integer(i) = row.values[2].to_value() {
                    assert_eq!(i, 42)
                }

                if let Value::Blob(v) = row.values[3].to_value() {
                    assert_eq!(v, &vec![0x1 as u8, 0x2, 0x3])
                }

                if let Value::Float(f) = row.values[4].to_value() {
                    assert_eq!(f, 0.5)
                }
            }
            StepResult::IO => {
                tmp_db.io.run_once()?;
            }
            StepResult::Interrupt => break,
            StepResult::Done => break,
            StepResult::Busy => panic!("Database is busy"),
        };
    }
    Ok(())
}
