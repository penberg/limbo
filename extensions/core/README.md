# Limbo extension API

The `limbo_ext` crate simplifies the creation and registration of libraries meant to extend the functionality of `Limbo`, that can be loaded
like traditional `sqlite3` extensions, but are able to be written in much more ergonomic Rust.
 
---

## Currently supported features

 - [ x ] **Scalar Functions**: Create scalar functions using the `scalar` macro.
 - [ x ] **Aggregate Functions**: Define aggregate functions with `AggregateDerive` macro and `AggFunc` trait.
 - [ x ]  **Virtual tables**: Create a module for a virtual table with the `VTabModuleDerive` macro and `VTabCursor` trait.
 - [] **VFS Modules** 
---

## Installation

Add the crate to your `Cargo.toml`:

```toml

[features]
static = ["limbo_ext/static"]

[dependencies]
limbo_ext = { path = "path/to/limbo/extensions/core", features = ["static"] } # temporary until crate is published


# mimalloc is required if you intend on linking dynamically. It is imported for you by the register_extension
# macro, so no configuration is needed. But it must be added to your Cargo.toml
[target.'cfg(not(target_family = "wasm"))'.dependencies]
mimalloc = { version = "*", default-features = false }


# NOTE: Crate must be of type `cdylib` if you wish to link dynamically
[lib]
crate-type = ["cdylib", "lib"]
```

`cargo build` will output a shared library that can be loaded by the following options:

#### **CLI:**
    `.load target/debug/libyour_crate_name`

#### **SQL:**
   `SELECT load_extension('target/debug/libyour_crate_name')`


Extensions can be registered with the `register_extension!` macro:

```rust

register_extension!{
    scalars: { double }, // name of your function, if different from attribute name
    aggregates: { Percentile },
    vtabs: { CsvVTable },
}
```

### Scalar Example:
```rust
use limbo_ext::{register_extension, Value, scalar};

/// Annotate each with the scalar macro, specifying the name you would like to call it with
/// and optionally, an alias.. e.g. SELECT double(4); or SELECT twice(4);
#[scalar(name = "double", alias = "twice")]
fn double(&self, args: &[Value]) -> Value {
    if let Some(arg) = args.first() {
        match arg.value_type() {
            ValueType::Float => {
                let val = arg.to_float().unwrap();
                Value::from_float(val * 2.0)
            }
            ValueType::Integer => {
                let val = arg.to_integer().unwrap();
                Value::from_integer(val * 2)
            }
        }
    } else {
        Value::null()
    }
}
```

### Aggregates Example:

```rust

use limbo_ext::{register_extension, AggregateDerive, AggFunc, Value};
/// annotate your struct with the AggregateDerive macro, and it must implement the below AggFunc trait
#[derive(AggregateDerive)]
struct Percentile;

impl AggFunc for Percentile {
    /// The state to track during the steps
    type State = (Vec<f64>, Option<f64>, Option<String>); // Tracks the values, Percentile, and errors

    /// Define the name you wish to call your function by. 
    /// e.g. SELECT percentile(value, 40);
     const NAME: &str = "percentile";

    /// Define the number of expected arguments for your function.
     const ARGS: i32 = 2;

    /// Define a function called on each row/value in a relevant group/column
    fn step(state: &mut Self::State, args: &[Value]) {
        let (values, p_value, error) = state;

        if let (Some(y), Some(p)) = (
            args.first().and_then(Value::to_float),
            args.get(1).and_then(Value::to_float),
        ) {
            if !(0.0..=100.0).contains(&p) {
                *error = Some("Percentile P must be between 0 and 100.".to_string());
                return;
            }

            if let Some(existing_p) = *p_value {
                if (existing_p - p).abs() >= 0.001 {
                    *error = Some("P values must remain consistent.".to_string());
                    return;
                }
            } else {
                *p_value = Some(p);
            }

            values.push(y);
        }
    }
    /// A function to finalize the state into a value to be returned as a result
    /// or an error (if you chose to track an error state as well)
    fn finalize(state: Self::State) -> Value {
        let (mut values, p_value, error) = state;

        if let Some(error) = error {
            return Value::custom_error(error);
        }

        if values.is_empty() {
            return Value::null();
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = values.len() as f64;
        let p = p_value.unwrap();
        let index = (p * (n - 1.0) / 100.0).floor() as usize;

        Value::from_float(values[index])
    }
}
```

### Virtual Table Example:

```rust

/// Example: A virtual table that operates on a CSV file as a database table.
/// This example assumes that the CSV file is located at "data.csv" in the current directory.
#[derive(Debug, VTabModuleDerive)]
struct CsvVTable;

impl VTabModule for CsvVTable {
    type VCursor = CsvCursor;
    /// Declare the name for your virtual table
    const NAME: &'static str = "csv_data";

    /// Declare the table schema and call `api.declare_virtual_table` with the schema sql.
    fn connect(api: &ExtensionApi) -> ResultCode {
        let sql = "CREATE TABLE csv_data(
            name TEXT,
            age TEXT,
            city TEXT
        )";
        api.declare_virtual_table(Self::NAME, sql)
    }

    /// Open to return a new cursor: In this simple example, the CSV file is read completely into memory on connect.
    fn open() -> Self::VCursor {
        // Read CSV file contents from "data.csv"
        let csv_content = fs::read_to_string("data.csv").unwrap_or_default();
        // For simplicity, we'll ignore the header row.
        let rows: Vec<Vec<String>> = csv_content
            .lines()
            .skip(1)
            .map(|line| {
                line.split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            })
            .collect();
        CsvCursor { rows, index: 0 }
    }

    /// Filter through result columns. (not used in this simple example)
    fn filter(_cursor: &mut Self::VCursor, _arg_count: i32, _args: &[Value]) -> ResultCode {
        ResultCode::OK
    }

    /// Return the value for the column at the given index in the current row.
    fn column(cursor: &Self::VCursor, idx: u32) -> Value {
        cursor.column(idx)
    }

    /// Next advances the cursor to the next row.
    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        if cursor.index < cursor.rows.len() - 1 {
            cursor.index += 1;
            ResultCode::OK
        } else {
            ResultCode::EOF
        }
    }

    /// Return true if the cursor is at the end.
    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.index >= cursor.rows.len()
    }
}

/// The cursor for iterating over CSV rows.
#[derive(Debug)]
struct CsvCursor {
    rows: Vec<Vec<String>>,
    index: usize,
}

/// Implement the VTabCursor trait for your cursor type
impl VTabCursor for CsvCursor {
    fn next(&mut self) -> ResultCode {
        CsvCursor::next(self)
    }

    fn eof(&self) -> bool {
        self.index >= self.rows.len()
    }

    fn column(&self, idx: u32) -> Value {
        let row = &self.rows[self.index];
        if (idx as usize) < row.len() {
            Value::from_text(&row[idx as usize])
        } else {
            Value::null()
        }
    }

    fn rowid(&self) -> i64 {
        self.index as i64
    }
}
```
