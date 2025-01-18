# Limbo extension API

The `limbo_ext` crate simplifies the creation and registration of libraries meant to extend the functionality of `Limbo`, that can be loaded
like traditional `sqlite3` extensions, but are able to be written in much more ergonomic Rust.
 
---

## Currently supported features

 - [ x ] **Scalar Functions**: Create scalar functions using the `scalar` macro.
 - [ x ] **Aggregate Functions**: Define aggregate functions with `AggregateDerive` macro and `AggFunc` trait.
 - []  **Virtual tables**: TODO
---

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
limbo_ext = { path = "path/to/limbo/extensions/core" } # temporary until crate is published
```

**NOTE** Crate must be of type `cdylib`

```
[lib]
crate-type = ["cdylib"]
```

`cargo build` will output a shared library that can be loaded with `.load target/debug/libyour_crate_name`



Extensions can be registered with the `register_extension!` macro:

```rust

register_extension!{
    scalars: { double }, // name of your function, if different from attribute name
    aggregates: { Percentile },
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


