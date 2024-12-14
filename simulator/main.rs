use limbo_core::{Connection, Database, File, OpenFlags, PlatformIO, Result, RowResult, IO};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::cell::RefCell;
use std::fmt::Display;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;

use anarchist_readable_name_generator_lib::readable_name_custom;

struct SimulatorEnv {
    opts: SimulatorOpts,
    tables: Vec<Table>,
    connections: Vec<SimConnection>,
    io: Arc<SimulatorIO>,
    db: Arc<Database>,
    rng: ChaCha8Rng,
}

#[derive(Clone)]
enum SimConnection {
    Connected(Rc<Connection>),
    Disconnected,
}

#[derive(Debug, Copy, Clone)]
enum SimulatorMode {
    Random,
    Workload,
}

#[derive(Debug)]
struct SimulatorOpts {
    ticks: usize,
    max_connections: usize,
    max_tables: usize,
    // this next options are the distribution of workload where read_percent + write_percent +
    // delete_percent == 100%
    read_percent: usize,
    write_percent: usize,
    delete_percent: usize,
    mode: SimulatorMode,
    page_size: usize,
}

trait Arbitrary {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self;
}

trait ArbitraryFrom<T> {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &T) -> Self;
}

struct Table {
    rows: Vec<Vec<Value>>,
    name: String,
    columns: Vec<Column>,
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let columns = (1..rng.gen_range(1..128))
            .map(|_| Column::arbitrary(rng))
            .collect();
        Table {
            rows: Vec::new(),
            name,
            columns,
        }
    }
}

#[derive(Clone)]
struct Column {
    name: String,
    column_type: ColumnType,
    primary: bool,
    unique: bool,
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let column_type = ColumnType::arbitrary(rng);
        Column {
            name,
            column_type,
            primary: false,
            unique: false,
        }
    }
}

#[derive(Clone)]
enum ColumnType {
    Integer,
    Float,
    Text,
    Blob,
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        match rng.gen_range(0..4) {
            0 => ColumnType::Integer,
            1 => ColumnType::Float,
            2 => ColumnType::Text,
            3 => ColumnType::Blob,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl ArbitraryFrom<Vec<&Value>> for Value {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Vec<&Value>) -> Self {
        if t.is_empty() {
            return Value::Null;
        }

        let index = rng.gen_range(0..t.len());
        t[index].clone()
    }
}

impl ArbitraryFrom<ColumnType> for Value {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &ColumnType) -> Self {
        match t {
            ColumnType::Integer => Value::Integer(rng.gen_range(i64::MIN..i64::MAX)),
            ColumnType::Float => Value::Float(rng.gen_range(-1e10..1e10)),
            ColumnType::Text => Value::Text(gen_random_text(rng)),
            ColumnType::Blob => Value::Blob(gen_random_text(rng).as_bytes().to_vec()),
        }
    }
}

struct LTValue(Value);

impl ArbitraryFrom<Vec<&Value>> for LTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Vec<&Value>) -> Self {
        if t.is_empty() {
            return LTValue(Value::Null);
        }

        let index = rng.gen_range(0..t.len());
        LTValue::arbitrary_from(rng, t[index])
    }
}

impl ArbitraryFrom<Value> for LTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Value) -> Self {
        match t {
            Value::Integer(i) => LTValue(Value::Integer(rng.gen_range(i64::MIN..*i - 1))),
            Value::Float(f) => LTValue(Value::Float(rng.gen_range(-1e10..*f - 1.0))),
            Value::Text(t) => {
                // Either shorten the string, or make at least one character smaller and mutate the rest
                let mut t = t.clone();
                if rng.gen_bool(0.01) {
                    t.pop();
                    LTValue(Value::Text(t))
                } else {
                    let index = rng.gen_range(0..t.len());
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    t[index] -= 1;
                    // Mutate the rest of the string
                    for i in (index + 1)..t.len() {
                        t[i] = rng.gen_range(0..=255);
                    }
                    let t = t.into_iter().map(|c| c as u8 as char).collect::<String>();
                    LTValue(Value::Text(t))
                }
            }
            Value::Blob(b) => {
                // Either shorten the blob, or make at least one byte smaller and mutate the rest
                let mut b = b.clone();
                if rng.gen_bool(0.01) {
                    b.pop();
                    LTValue(Value::Blob(b))
                } else {
                    let index = rng.gen_range(0..b.len());
                    b[index] -= 1;
                    // Mutate the rest of the blob
                    for i in (index + 1)..b.len() {
                        b[i] = rng.gen_range(0..=255);
                    }
                    LTValue(Value::Blob(b))
                }
            }
            _ => unreachable!(),
        }
    }
}

struct GTValue(Value);

impl ArbitraryFrom<Vec<&Value>> for GTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Vec<&Value>) -> Self {
        if t.is_empty() {
            return GTValue(Value::Null);
        }

        let index = rng.gen_range(0..t.len());
        GTValue::arbitrary_from(rng, t[index])
    }
}

impl ArbitraryFrom<Value> for GTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Value) -> Self {
        match t {
            Value::Integer(i) => GTValue(Value::Integer(rng.gen_range(*i..i64::MAX))),
            Value::Float(f) => GTValue(Value::Float(rng.gen_range(*f..1e10))),
            Value::Text(t) => {
                // Either lengthen the string, or make at least one character smaller and mutate the rest
                let mut t = t.clone();
                if rng.gen_bool(0.01) {
                    t.push(rng.gen_range(0..=255) as u8 as char);
                    GTValue(Value::Text(t))
                } else {
                    let index = rng.gen_range(0..t.len());
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    t[index] += 1;
                    // Mutate the rest of the string
                    for i in (index + 1)..t.len() {
                        t[i] = rng.gen_range(0..=255);
                    }
                    let t = t.into_iter().map(|c| c as u8 as char).collect::<String>();
                    GTValue(Value::Text(t))
                }
            }
            Value::Blob(b) => {
                // Either lengthen the blob, or make at least one byte smaller and mutate the rest
                let mut b = b.clone();
                if rng.gen_bool(0.01) {
                    b.push(rng.gen_range(0..=255));
                    GTValue(Value::Blob(b))
                } else {
                    let index = rng.gen_range(0..b.len());
                    b[index] += 1;
                    // Mutate the rest of the blob
                    for i in (index + 1)..b.len() {
                        b[i] = rng.gen_range(0..=255);
                    }
                    GTValue(Value::Blob(b))
                }
            }
            _ => unreachable!(),
        }
    }
}

enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Eq(String, Value),
    Gt(String, Value),
    Lt(String, Value),
}

enum Query {
    Create(Create),
    Select(Select),
    Insert(Insert),
    Delete(Delete),
}

struct Create {
    table: Table,
}

impl Arbitrary for Create {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        Create {
            table: Table::arbitrary(rng),
        }
    }
}

struct Select {
    table: String,
    predicate: Predicate,
}

impl ArbitraryFrom<Vec<Table>> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Vec<Table>) -> Self {
        let table = rng.gen_range(0..t.len());
        Select {
            table: t[table].name.clone(),
            predicate: Predicate::arbitrary_from(rng, &t[table]),
        }
    }
}

impl ArbitraryFrom<Vec<&Table>> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Vec<&Table>) -> Self {
        let table = rng.gen_range(0..t.len());
        Select {
            table: t[table].name.clone(),
            predicate: Predicate::arbitrary_from(rng, t[table]),
        }
    }
}

struct Insert {
    table: String,
    values: Vec<Value>,
}

impl ArbitraryFrom<Table> for Insert {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Table) -> Self {
        let values = t
            .columns
            .iter()
            .map(|c| Value::arbitrary_from(rng, &c.column_type))
            .collect();
        Insert {
            table: t.name.clone(),
            values,
        }
    }
}

struct Delete {
    table: String,
    predicate: Predicate,
}

impl ArbitraryFrom<Table> for Delete {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Table) -> Self {
        Delete {
            table: t.name.clone(),
            predicate: Predicate::arbitrary_from(rng, t),
        }
    }
}

impl ArbitraryFrom<Table> for Query {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Table) -> Self {
        match rng.gen_range(0..=200) {
            0 => Query::Create(Create::arbitrary(rng)),
            1..=100 => Query::Select(Select::arbitrary_from(rng, &vec![t])),
            101..=200 => Query::Insert(Insert::arbitrary_from(rng, t)),
            // todo: This branch is currently never taken, as DELETE is not yet implemented.
            //       Change this when DELETE is implemented.
            201..=300 => Query::Delete(Delete::arbitrary_from(rng, t)),
            _ => unreachable!(),
        }
    }
}

struct CompoundPredicate(Predicate);
struct SimplePredicate(Predicate);

impl ArbitraryFrom<(&Table, bool)> for SimplePredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (t, b): &(&Table, bool)) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..t.columns.len());
        let column = &t.columns[column_index];
        let column_values = t.rows.iter().map(|r| &r[column_index]).collect::<Vec<_>>();
        // Pick an operator
        let operator = match rng.gen_range(0..3) {
            0 => {
                if *b {
                    Predicate::Eq(
                        column.name.clone(),
                        Value::arbitrary_from(rng, &column_values),
                    )
                } else {
                    Predicate::Eq(
                        column.name.clone(),
                        Value::arbitrary_from(rng, &column.column_type),
                    )
                }
            }
            1 => Predicate::Gt(
                column.name.clone(),
                match b {
                    true => GTValue::arbitrary_from(rng, &column_values).0,
                    false => LTValue::arbitrary_from(rng, &column_values).0,
                },
            ),
            2 => Predicate::Lt(
                column.name.clone(),
                match b {
                    true => LTValue::arbitrary_from(rng, &column_values).0,
                    false => GTValue::arbitrary_from(rng, &column_values).0,
                },
            ),
            _ => unreachable!(),
        };

        SimplePredicate(operator)
    }
}

impl ArbitraryFrom<(&Table, bool)> for CompoundPredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (t, b): &(&Table, bool)) -> Self {
        // Decide if you want to create an AND or an OR
        CompoundPredicate(if rng.gen_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if *b {
                Predicate::And(
                    (0..rng.gen_range(1..=3))
                        .map(|_| SimplePredicate::arbitrary_from(rng, &(*t, true)).0)
                        .collect(),
                )
            } else {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(1..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();

                let len = booleans.len();

                // Make sure at least one of them is false
                if booleans.iter().all(|b| *b) {
                    booleans[rng.gen_range(0..len)] = false;
                }

                Predicate::And(
                    booleans
                        .iter()
                        .map(|b| SimplePredicate::arbitrary_from(rng, &(*t, *b)).0)
                        .collect(),
                )
            }
        } else {
            // An OR for true requires at least one of its children to be true
            // An OR for false requires each of its children to be false
            if *b {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(1..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();
                let len = booleans.len();
                // Make sure at least one of them is true
                if booleans.iter().all(|b| !*b) {
                    booleans[rng.gen_range(0..len)] = true;
                }

                Predicate::Or(
                    booleans
                        .iter()
                        .map(|b| SimplePredicate::arbitrary_from(rng, &(*t, *b)).0)
                        .collect(),
                )
            } else {
                Predicate::Or(
                    (0..rng.gen_range(1..=3))
                        .map(|_| SimplePredicate::arbitrary_from(rng, &(*t, false)).0)
                        .collect(),
                )
            }
        })
    }
}

impl ArbitraryFrom<Table> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &Table) -> Self {
        let b = rng.gen_bool(0.5);
        CompoundPredicate::arbitrary_from(rng, &(t, b)).0
    }
}

impl ArbitraryFrom<(&str, &Value)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (c, t): &(&str, &Value)) -> Self {
        match rng.gen_range(0..3) {
            0 => Predicate::Eq(c.to_string(), (*t).clone()),
            1 => Predicate::Gt(c.to_string(), LTValue::arbitrary_from(rng, *t).0),
            2 => Predicate::Lt(c.to_string(), LTValue::arbitrary_from(rng, *t).0),
            _ => unreachable!(),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::And(predicates) => {
                if predicates.is_empty() {
                    // todo: Make this TRUE when the bug is fixed
                    write!(f, "TRUE")
                } else {
                    write!(f, "(")?;
                    for (i, p) in predicates.iter().enumerate() {
                        if i != 0 {
                            write!(f, " AND ")?;
                        }
                        write!(f, "{}", p)?;
                    }
                    write!(f, ")")
                }
            }
            Predicate::Or(predicates) => {
                if predicates.is_empty() {
                    write!(f, "FALSE")
                } else {
                    write!(f, "(")?;
                    for (i, p) in predicates.iter().enumerate() {
                        if i != 0 {
                            write!(f, " OR ")?;
                        }
                        write!(f, "{}", p)?;
                    }
                    write!(f, ")")
                }
            }
            Predicate::Eq(name, value) => write!(f, "{} = {}", name, value),
            Predicate::Gt(name, value) => write!(f, "{} > {}", name, value),
            Predicate::Lt(name, value) => write!(f, "{} < {}", name, value),
        }
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Query::Create(Create { table }) => write!(f, "{}", table.to_create_str()),
            Query::Select(Select {
                table,
                predicate: guard,
            }) => write!(f, "SELECT * FROM {} WHERE {}", table, guard),
            Query::Insert(Insert { table, values }) => {
                write!(f, "INSERT INTO {} VALUES (", table)?;
                for (i, v) in values.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
            Query::Delete(Delete {
                table,
                predicate: guard,
            }) => write!(f, "DELETE FROM {} WHERE {}", table, guard),
        }
    }
}

#[allow(clippy::arc_with_non_send_sync)]
fn main() {
    let _ = env_logger::try_init();
    let seed = match std::env::var("SEED") {
        Ok(seed) => seed.parse::<u64>().unwrap(),
        Err(_) => rand::thread_rng().next_u64(),
    };
    println!("Seed: {}", seed);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let (read_percent, write_percent, delete_percent) = {
        let mut remaining = 100;
        let read_percent = rng.gen_range(0..=remaining);
        remaining -= read_percent;
        let write_percent = rng.gen_range(0..=remaining);
        remaining -= write_percent;
        let delete_percent = remaining;
        (read_percent, write_percent, delete_percent)
    };

    let opts = SimulatorOpts {
        ticks: rng.gen_range(0..4096),
        max_connections: 1, // TODO: for now let's use one connection as we didn't implement
        // correct transactions procesing
        max_tables: rng.gen_range(0..128),
        read_percent,
        write_percent,
        delete_percent,
        mode: SimulatorMode::Workload,
        page_size: 4096, // TODO: randomize this too
    };
    let io = Arc::new(SimulatorIO::new(seed, opts.page_size).unwrap());

    let mut path = TempDir::new().unwrap().into_path();
    path.push("simulator.db");
    println!("path to db '{:?}'", path);
    let db = match Database::open_file(io.clone(), path.as_path().to_str().unwrap()) {
        Ok(db) => db,
        Err(e) => {
            panic!("error opening simulator test file {:?}: {:?}", path, e);
        }
    };

    let connections = vec![SimConnection::Disconnected; opts.max_connections];
    let mut env = SimulatorEnv {
        opts,
        tables: Vec::new(),
        connections,
        rng,
        io,
        db,
    };

    println!("Initial opts {:?}", env.opts);

    for _ in 0..env.opts.ticks {
        let connection_index = env.rng.gen_range(0..env.opts.max_connections);
        let mut connection = env.connections[connection_index].clone();

        match &mut connection {
            SimConnection::Connected(conn) => {
                let disconnect = env.rng.gen_ratio(1, 100);
                if disconnect {
                    log::info!("disconnecting {}", connection_index);
                    let _ = conn.close();
                    env.connections[connection_index] = SimConnection::Disconnected;
                } else {
                    match process_connection(&mut env, conn) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!("error {}", err);
                            break;
                        }
                    }
                }
            }
            SimConnection::Disconnected => {
                log::info!("disconnecting {}", connection_index);
                env.connections[connection_index] = SimConnection::Connected(env.db.connect());
            }
        }
    }

    env.io.print_stats();
}

fn property_insert_select(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) {
    // Get a random table
    let table = env.rng.gen_range(0..env.tables.len());

    // Pick a random column
    let column_index = env.rng.gen_range(0..env.tables[table].columns.len());
    let column = &env.tables[table].columns[column_index].clone();

    let mut rng = env.rng.clone();

    // Generate a random value of the column type
    let value = Value::arbitrary_from(&mut rng, &column.column_type);

    // Create a whole new row
    let mut row = Vec::new();
    for (i, column) in env.tables[table].columns.iter().enumerate() {
        if i == column_index {
            row.push(value.clone());
        } else {
            let value = Value::arbitrary_from(&mut rng, &column.column_type);
            row.push(value);
        }
    }

    // Insert the row
    let query = Query::Insert(Insert {
        table: env.tables[table].name.clone(),
        values: row.clone(),
    });
    let _ = get_all_rows(env, conn, query.to_string().as_str()).unwrap();
    // Shadow operation on the table
    env.tables[table].rows.push(row.clone());

    // Create a query that selects the row
    let query = Query::Select(Select {
        table: env.tables[table].name.clone(),
        predicate: Predicate::Eq(column.name.clone(), value),
    });

    // Get all rows
    let rows = get_all_rows(env, conn, query.to_string().as_str()).unwrap();

    // Check that the row is there
    assert!(rows.iter().any(|r| r == &row));
}

fn property_select_all(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) {
    // Get a random table
    let table = env.rng.gen_range(0..env.tables.len());

    // Create a query that selects all rows
    let query = Query::Select(Select {
        table: env.tables[table].name.clone(),
        predicate: Predicate::And(Vec::new()),
    });

    // Get all rows
    let rows = get_all_rows(env, conn, query.to_string().as_str()).unwrap();

    // Check that all rows are there
    assert_eq!(rows.len(), env.tables[table].rows.len());
    for row in &env.tables[table].rows {
        assert!(rows.iter().any(|r| r == row));
    }
}

fn process_connection(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) -> Result<()> {
    if env.tables.is_empty() {
        maybe_add_table(env, conn)?;
    }

    match env.opts.mode {
        SimulatorMode::Random => {
            match env.rng.gen_range(0..2) {
                // Randomly insert a value and check that the select result contains it.
                0 => property_insert_select(env, conn),
                // Check that the current state of the in-memory table is the same as the one in the
                // database.
                1 => property_select_all(env, conn),
                // Perform a random query, update the in-memory table with the result.
                2 => {
                    let table_index = env.rng.gen_range(0..env.tables.len());
                    let query = Query::arbitrary_from(&mut env.rng, &env.tables[table_index]);
                    let rows = get_all_rows(env, conn, query.to_string().as_str())?;
                    env.tables[table_index].rows = rows;
                }
                _ => unreachable!(),
            }
        }
        SimulatorMode::Workload => {
            let picked = env.rng.gen_range(0..100);

            if env.rng.gen_ratio(1, 100) {
                maybe_add_table(env, conn)?;
            }

            if picked < env.opts.read_percent {
                let query = Select::arbitrary_from(&mut env.rng, &env.tables);

                let _ = get_all_rows(env, conn, Query::Select(query).to_string().as_str())?;
            } else if picked < env.opts.read_percent + env.opts.write_percent {
                let table_index = env.rng.gen_range(0..env.tables.len());
                let column_index = env.rng.gen_range(0..env.tables[table_index].columns.len());
                let column = &env.tables[table_index].columns[column_index].clone();
                let mut rng = env.rng.clone();
                let value = Value::arbitrary_from(&mut rng, &column.column_type);
                let mut row = Vec::new();
                for (i, column) in env.tables[table_index].columns.iter().enumerate() {
                    if i == column_index {
                        row.push(value.clone());
                    } else {
                        let value = Value::arbitrary_from(&mut rng, &column.column_type);
                        row.push(value);
                    }
                }
                let query = Query::Insert(Insert {
                    table: env.tables[table_index].name.clone(),
                    values: row.clone(),
                });
                let _ = get_all_rows(env, conn, query.to_string().as_str())?;
                env.tables[table_index].rows.push(row.clone());
            } else {
                let table_index = env.rng.gen_range(0..env.tables.len());
                let query = Query::Select(Select {
                    table: env.tables[table_index].name.clone(),
                    predicate: Predicate::And(Vec::new()),
                });
                let _ = get_all_rows(env, conn, query.to_string().as_str())?;
            }
        }
    }

    Ok(())
}

fn compare_equal_rows(a: &[Vec<Value>], b: &[Vec<Value>]) {
    assert_eq!(a.len(), b.len(), "lengths are different");
    for (r1, r2) in a.iter().zip(b) {
        for (v1, v2) in r1.iter().zip(r2) {
            assert_eq!(v1, v2, "values are different");
        }
    }
}

fn maybe_add_table(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) -> Result<()> {
    if env.tables.len() < env.opts.max_tables {
        let table = Table {
            rows: Vec::new(),
            name: Name::arbitrary(&mut env.rng).0,
            columns: (1..env.rng.gen_range(1..128))
                .map(|_| Column::arbitrary(&mut env.rng))
                .collect(),
        };
        let rows = get_all_rows(env, conn, table.to_create_str().as_str())?;
        log::debug!("{:?}", rows);
        let rows = get_all_rows(
            env,
            conn,
            format!(
                "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND name = '{}';",
                table.name
            )
            .as_str(),
        )?;
        log::debug!("{:?}", rows);
        assert!(rows.len() == 1);
        let as_text = match &rows[0][0] {
            Value::Text(t) => t,
            _ => unreachable!(),
        };
        assert!(
            *as_text != table.to_create_str(),
            "table was not inserted correctly"
        );
        env.tables.push(table);
    }
    Ok(())
}

struct Name(String);

impl Arbitrary for Name {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = readable_name_custom("_", rng);
        Name(name.replace("-", "_"))
    }
}

impl Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn gen_random_text<T: Rng>(rng: &mut T) -> String {
    let big_text = rng.gen_ratio(1, 1000);
    if big_text {
        let max_size: u64 = 2 * 1024 * 1024 * 1024;
        let size = rng.gen_range(1024..max_size);
        let mut name = String::new();
        for i in 0..size {
            name.push(((i % 26) as u8 + b'A') as char);
        }
        name
    } else {
        let name = readable_name_custom("_", rng);
        name.replace("-", "_")
    }
}

fn get_all_rows(
    env: &mut SimulatorEnv,
    conn: &mut Rc<Connection>,
    query: &str,
) -> Result<Vec<Vec<Value>>> {
    log::info!("running query '{}'", &query[0..query.len().min(4096)]);
    let mut out = Vec::new();
    let rows = conn.query(query);
    if rows.is_err() {
        let err = rows.err();
        log::error!(
            "Error running query '{}': {:?}",
            &query[0..query.len().min(4096)],
            err
        );
        return Err(err.unwrap());
    }
    let rows = rows.unwrap();
    assert!(rows.is_some());
    let mut rows = rows.unwrap();
    'rows_loop: loop {
        env.io.inject_fault(env.rng.gen_ratio(1, 10000));
        match rows.next_row()? {
            RowResult::Row(row) => {
                let mut r = Vec::new();
                for el in &row.values {
                    let v = match el {
                        limbo_core::Value::Null => Value::Null,
                        limbo_core::Value::Integer(i) => Value::Integer(*i),
                        limbo_core::Value::Float(f) => Value::Float(*f),
                        limbo_core::Value::Text(t) => Value::Text(t.to_string()),
                        limbo_core::Value::Blob(b) => Value::Blob(b.to_vec()),
                    };
                    r.push(v);
                }

                out.push(r);
            }
            RowResult::IO => {
                env.io.inject_fault(env.rng.gen_ratio(1, 10000));
                if env.io.run_once().is_err() {
                    log::info!("query inject fault");
                    break 'rows_loop;
                }
            }
            RowResult::Done => {
                break;
            }
        }
    }
    Ok(out)
}

struct SimulatorIO {
    inner: Box<dyn IO>,
    fault: RefCell<bool>,
    files: RefCell<Vec<Rc<SimulatorFile>>>,
    rng: RefCell<ChaCha8Rng>,
    nr_run_once_faults: RefCell<usize>,
    page_size: usize,
}

impl SimulatorIO {
    fn new(seed: u64, page_size: usize) -> Result<Self> {
        let inner = Box::new(PlatformIO::new()?);
        let fault = RefCell::new(false);
        let files = RefCell::new(Vec::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let nr_run_once_faults = RefCell::new(0);
        Ok(Self {
            inner,
            fault,
            files,
            rng,
            nr_run_once_faults,
            page_size,
        })
    }

    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
        for file in self.files.borrow().iter() {
            file.inject_fault(fault);
        }
    }

    fn print_stats(&self) {
        println!("run_once faults: {}", self.nr_run_once_faults.borrow());
        for file in self.files.borrow().iter() {
            file.print_stats();
        }
    }
}

impl IO for SimulatorIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        _direct: bool,
    ) -> Result<Rc<dyn limbo_core::File>> {
        let inner = self.inner.open_file(path, flags, false)?;
        let file = Rc::new(SimulatorFile {
            inner,
            fault: RefCell::new(false),
            nr_pread_faults: RefCell::new(0),
            nr_pwrite_faults: RefCell::new(0),
            reads: RefCell::new(0),
            writes: RefCell::new(0),
            syncs: RefCell::new(0),
            page_size: self.page_size,
        });
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn run_once(&self) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_run_once_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.run_once().unwrap();
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().next_u64() as i64
    }

    fn get_current_time(&self) -> String {
        "2024-01-01 00:00:00".to_string()
    }
}

struct SimulatorFile {
    inner: Rc<dyn File>,
    fault: RefCell<bool>,
    nr_pread_faults: RefCell<usize>,
    nr_pwrite_faults: RefCell<usize>,
    writes: RefCell<usize>,
    reads: RefCell<usize>,
    syncs: RefCell<usize>,
    page_size: usize,
}

impl SimulatorFile {
    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }

    fn print_stats(&self) {
        println!(
            "pread faults: {}, pwrite faults: {}, reads: {}, writes: {}, syncs: {}",
            *self.nr_pread_faults.borrow(),
            *self.nr_pwrite_faults.borrow(),
            *self.reads.borrow(),
            *self.writes.borrow(),
            *self.syncs.borrow(),
        );
    }
}

impl limbo_core::File for SimulatorFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if *self.fault.borrow() {
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        if *self.fault.borrow() {
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.unlock_file()
    }

    fn pread(&self, pos: usize, c: Rc<limbo_core::Completion>) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_pread_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        *self.reads.borrow_mut() += 1;
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        c: Rc<limbo_core::Completion>,
    ) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_pwrite_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        *self.writes.borrow_mut() += 1;
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Rc<limbo_core::Completion>) -> Result<()> {
        *self.syncs.borrow_mut() += 1;
        self.inner.sync(c)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {
        self.inner.unlock_file().expect("Failed to unlock file");
    }
}

impl ColumnType {
    pub fn as_str(&self) -> &str {
        match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::Float => "FLOAT",
            ColumnType::Text => "TEXT",
            ColumnType::Blob => "BLOB",
        }
    }
}

impl Table {
    pub fn to_create_str(&self) -> String {
        let mut out = String::new();

        out.push_str(format!("CREATE TABLE {} (", self.name).as_str());

        assert!(!self.columns.is_empty());
        for column in &self.columns {
            out.push_str(format!("{} {},", column.name, column.column_type.as_str()).as_str());
        }
        // remove last comma
        out.pop();

        out.push_str(");");
        out
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(t) => write!(f, "'{}'", t),
            Value::Blob(b) => write!(f, "{}", to_sqlite_blob(b)),
        }
    }
}

fn to_sqlite_blob(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{:02X}", b)).collect();
    format!("X'{}'", hex)
}
