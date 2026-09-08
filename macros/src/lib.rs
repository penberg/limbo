//! =============================================================================
//! Antithesis Assertion Macros
//! =============================================================================
//!
//! These macros define correctness properties for [Antithesis](https://antithesis.com/)
//! autonomous testing. They wrap the [Antithesis SDK](https://docs.rs/antithesis_sdk)
//! assertion macros and double as standard Rust assertions, giving us a single assertion
//! layer that works both in normal builds and under Antithesis fuzzing.
//!
//! ## The `antithesis` feature flag
//!
//! All macros compile to different code depending on whether `--features antithesis`
//! is enabled:
//!
//! - **Without** the feature: macros behave like their `std` counterparts (`assert!`,
//!   `debug_assert!`, `unreachable!`, etc.), or are no-ops for observational macros.
//! - **With** the feature: macros additionally report to the Antithesis SDK. On failure,
//!   "always" assertions print an error to stderr and call `std::process::exit(0)` instead
//!   of panicking. This clean exit lets Antithesis properly process the property violation.
//!
//! ## Five categories of assertions
//!
//! 1. **Condition assertions** ([`turso_assert!`], [`turso_debug_assert!`])
//!    Drop-in replacements for `assert!`/`debug_assert!` that also report to Antithesis.
//!
//! 2. **Sometimes assertions** ([`turso_assert_sometimes!`],
//!    [`turso_assert_sometimes_greater_than!`], [`turso_assert_sometimes_less_than!`],
//!    [`turso_assert_sometimes_greater_than_or_equal!`],
//!    [`turso_assert_sometimes_less_than_or_equal!`])
//!    Observational only — never panic. Tell Antithesis "this condition should be true at
//!    least once across all test runs." Useful for verifying the fuzzer explores both sides
//!    of a branch.
//!
//! 3. **Boolean guidance** ([`turso_assert_some!`], [`turso_assert_all!`])
//!    Multi-condition assertions that provide better guidance to the Antithesis fuzzer.
//!    `some` = at least one condition must be true (OR), `all` = every condition must be true (AND).
//!    Panics on failure (or exit(0) with antithesis feature).
//!
//! 4. **Reachability assertions** ([`turso_assert_reachable!`],
//!    [`turso_assert_unreachable!`], [`turso_soft_unreachable!`])
//!    Verify whether code paths are or aren't hit during testing.
//!
//! 5. **Comparison assertions** ([`turso_assert_eq!`], [`turso_assert_ne!`],
//!    [`turso_assert_greater_than!`], [`turso_assert_greater_than_or_equal!`],
//!    [`turso_assert_less_than!`], [`turso_assert_less_than_or_equal!`])
//!    Typed comparison assertions that provide richer information to Antithesis than a
//!    plain `turso_assert!(a > b)`.
//!
//! ## Note: fuzzer guidance temporarily disabled
//!
//! The Antithesis SDK provides specialized `numeric_guidance_helper!` and
//! `boolean_guidance_helper!` macros that give the fuzzer detailed numeric/boolean
//! values to guide exploration. These were previously used by comparison and boolean
//! macros but are currently disabled (replaced with plain `assert_always_or_unreachable!`
//! / `assert_sometimes!`) while Antithesis investigates an issue.
//!
//! The macros still work correctly as assertions — they just don't give the fuzzer
//! as much detail to work with. Guidance will be restored when the issue is resolved.
//!
//! ## Quick reference
//!
//! | Macro | Antithesis SDK | Panics? | Notes |
//! |-------|---------------|---------|-------|
//! | `turso_assert!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | Drop-in for `assert!` |
//! | `turso_debug_assert!` | `assert_always_or_unreachable!` | Debug only (exit(0) w/ feature) | Drop-in for `debug_assert!` |
//! | `turso_assert_sometimes!` | `assert_sometimes!` | Never | Observational only |
//! | `turso_assert_some!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | OR of named conditions |
//! | `turso_assert_all!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | AND of named conditions |
//! | `turso_assert_reachable!` | *(no-op)* | Never | Pending better SQL generation |
//! | `turso_assert_unreachable!` | `assert_unreachable!` | Yes (exit(0) w/ feature) | Hard unreachable |
//! | `turso_soft_unreachable!` | `assert_unreachable!` | Never | Soft signal, no-op w/o feature |
//! | `turso_assert_eq!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | Drop-in for `assert_eq!` |
//! | `turso_assert_ne!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | Drop-in for `assert_ne!` |
//! | `turso_assert_greater_than!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | `left > right` |
//! | `turso_assert_greater_than_or_equal!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | `left >= right` |
//! | `turso_assert_less_than!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | `left < right` |
//! | `turso_assert_less_than_or_equal!` | `assert_always_or_unreachable!` | Yes (exit(0) w/ feature) | `left <= right` |
//! | `turso_assert_sometimes_greater_than!` | `assert_sometimes!` | Never | Observational `left > right` |
//! | `turso_assert_sometimes_less_than!` | `assert_sometimes!` | Never | Observational `left < right` |
//! | `turso_assert_sometimes_greater_than_or_equal!` | `assert_sometimes!` | Never | Observational `left >= right` |
//! | `turso_assert_sometimes_less_than_or_equal!` | `assert_sometimes!` | Never | Observational `left <= right` |

extern crate proc_macro;
mod atomic_enum;
mod codspeed;
mod ext;
mod test;

// Import assertion proc macro implementations
mod assert;
#[path = "trace_stack.rs"]
mod trace_stack_impl;

use assert::{
    comparison_auto_message, details_debug_check, details_format_args, details_json,
    expr_to_lit_str, BooleanGuidanceInput, ComparisonAssertInput, ConditionAssertInput,
    MessageAssertInput,
};
use proc_macro::{token_stream::IntoIter, Group, TokenStream, TokenTree};
use quote::quote;
use std::collections::HashMap;
use syn::{parse_macro_input, LitStr};

/// Generate a runtime check that panics if `ANTITHESIS_OUTPUT_DIR` is not set.
/// Uses `std::sync::Once` so the actual env var lookup only happens once per call site.
fn antithesis_env_check() -> proc_macro2::TokenStream {
    quote! {
        {
            static __TURSO_ANTITHESIS_ENV_CHECK: std::sync::Once = std::sync::Once::new();
            __TURSO_ANTITHESIS_ENV_CHECK.call_once(|| {
                if std::env::var_os("ANTITHESIS_OUTPUT_DIR").is_none() {
                    panic!("Do not use --cfg antithesis unless running on Antithesis.");
                }
            });
        }
    }
}

/// A procedural macro that derives a `Description` trait for enums.
/// This macro extracts documentation comments (specified with `/// Description...`) for enum variants
/// and generates an implementation for `get_description`, which returns the associated description.
#[proc_macro_derive(Description, attributes(desc))]
pub fn derive_description_from_doc(item: TokenStream) -> TokenStream {
    // Convert the TokenStream into an iterator of TokenTree
    let mut tokens = item.into_iter();

    let mut enum_name = String::new();

    // Vector to store enum variants and their associated payloads (if any)
    let mut enum_variants: Vec<(String, Option<String>)> = Vec::<(String, Option<String>)>::new();

    // HashMap to store descriptions associated with each enum variant
    let mut variant_description_map: HashMap<String, String> = HashMap::new();

    // Parses the token stream to extract the enum name and its variants
    while let Some(token) = tokens.next() {
        match token {
            TokenTree::Ident(ident) if ident.to_string() == "enum" => {
                // Get the enum name
                if let Some(TokenTree::Ident(name)) = tokens.next() {
                    enum_name = name.to_string();
                }
            }
            TokenTree::Group(group) => {
                let mut group_tokens_iter: IntoIter = group.stream().into_iter();

                let mut last_seen_desc: Option<String> = None;
                while let Some(token) = group_tokens_iter.next() {
                    match token {
                        TokenTree::Punct(punct) => {
                            if punct.to_string() == "#" {
                                last_seen_desc = process_description(&mut group_tokens_iter);
                            }
                        }
                        TokenTree::Ident(ident) => {
                            // Capture the enum variant name and associate it with its description
                            let ident_str = ident.to_string();

                            // this is a quick fix for derive(EnumDiscriminants)
                            if ident_str == "strum_discriminants" {
                                continue;
                            }

                            // this is a quick fix for repr
                            if ident_str == "repr" {
                                continue;
                            }

                            if let Some(desc) = &last_seen_desc {
                                variant_description_map.insert(ident_str.clone(), desc.clone());
                            }
                            enum_variants.push((ident_str, None));
                            last_seen_desc = None;
                        }
                        TokenTree::Group(group) => {
                            // Capture payload information for the current enum variant
                            if let Some(last_variant) = enum_variants.last_mut() {
                                last_variant.1 = Some(process_payload(group));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    generate_get_description(enum_name, &variant_description_map, enum_variants)
}

#[proc_macro_attribute]
pub fn codspeed_criterion_benchmark(attr: TokenStream, input: TokenStream) -> TokenStream {
    codspeed::criterion_benchmark_attribute(attr, input)
}

#[proc_macro_attribute]
pub fn divan_bench(attr: TokenStream, input: TokenStream) -> TokenStream {
    codspeed::divan_bench_attribute(attr, input)
}

/// Processes a Rust docs to extract the description string.
fn process_description(token_iter: &mut IntoIter) -> Option<String> {
    if let Some(TokenTree::Group(doc_group)) = token_iter.next() {
        let mut doc_group_iter = doc_group.stream().into_iter();
        // Skip the `desc` and `(` tokens to reach the actual description
        doc_group_iter.next();
        doc_group_iter.next();
        if let Some(TokenTree::Literal(description)) = doc_group_iter.next() {
            return Some(description.to_string());
        }
    }
    None
}

/// Processes the payload of an enum variant to extract variable names (ignoring types).
fn process_payload(payload_group: Group) -> String {
    let payload_group_iter = payload_group.stream().into_iter();
    let mut variable_name_list = String::from("");
    let mut is_variable_name = true;
    for token in payload_group_iter {
        match token {
            TokenTree::Ident(ident) => {
                if is_variable_name {
                    variable_name_list.push_str(&format!("{ident},"));
                }
                is_variable_name = false;
            }
            TokenTree::Punct(punct) => {
                if punct.to_string() == "," {
                    is_variable_name = true;
                }
            }
            _ => {}
        }
    }
    format!("{{ {variable_name_list} }}").to_string()
}
/// Generates the `get_description` implementation for the processed enum.
fn generate_get_description(
    enum_name: String,
    variant_description_map: &HashMap<String, String>,
    enum_variants: Vec<(String, Option<String>)>,
) -> TokenStream {
    let mut all_enum_arms = String::from("");
    for (variant, payload) in enum_variants {
        let payload = payload.unwrap_or("".to_string());
        let desc;
        if let Some(description) = variant_description_map.get(&variant) {
            desc = format!("Some({description})");
        } else {
            desc = "None".to_string();
        }
        all_enum_arms.push_str(&format!("{enum_name}::{variant} {payload} => {desc},\n"));
    }

    let enum_impl = format!(
        "impl {enum_name}  {{
     pub fn get_description(&self) -> Option<&str> {{
     match self {{
     {all_enum_arms}
     }}
     }}
     }}"
    );
    enum_impl
        .parse()
        .expect("generated code should be valid Rust")
}

/// Register your extension with 'core' by providing the relevant functions
///```ignore
///use turso_ext::{register_extension, scalar, Value, AggregateDerive, AggFunc};
///
/// register_extension!{ scalars: { return_one }, aggregates: { SumPlusOne } }
///
///#[scalar(name = "one")]
///fn return_one(args: &[Value]) -> Value {
///  return Value::from_integer(1);
///}
///
///#[derive(AggregateDerive)]
///struct SumPlusOne;
///
///impl AggFunc for SumPlusOne {
///   type State = i64;
///   const NAME: &'static str = "sum_plus_one";
///   const ARGS: i32 = 1;
///
///   fn step(state: &mut Self::State, args: &[Value]) {
///      let Some(val) = args[0].to_integer() else {
///        return;
///      };
///      *state += val;
///     }
///
///     fn finalize(state: Self::State) -> Value {
///        Value::from_integer(state + 1)
///     }
///}
///
/// ```
#[proc_macro]
pub fn register_extension(input: TokenStream) -> TokenStream {
    ext::register_extension(input)
}

/// Declare a scalar function for your extension. This requires the name:
/// #[scalar(name = "example")] of what you wish to call your function with.
/// ```ignore
/// use turso_ext::{scalar, Value};
/// #[scalar(name = "double", alias = "twice")] // you can provide an <optional> alias
/// fn double(args: &[Value]) -> Value {
///       let arg = args.get(0).unwrap();
///       match arg.value_type() {
///           ValueType::Float => {
///               let val = arg.to_float().unwrap();
///               Value::from_float(val * 2.0)
///           }
///           ValueType::Integer => {
///               let val = arg.to_integer().unwrap();
///               Value::from_integer(val * 2)
///           }
///       }
///   } else {
///       Value::null()
///   }
/// }
/// ```
#[proc_macro_attribute]
pub fn scalar(attr: TokenStream, input: TokenStream) -> TokenStream {
    ext::scalar(attr, input)
}

/// Derive a context-aware scalar function for your extension by deriving
/// `ScalarDerive` on a struct that implements the `ScalarFunc` trait.
///
/// The associated `State` is built once per registration via `init`, shared by
/// reference across every call, and dropped when the function is unregistered or
/// the owning connection is dropped. The derived `register_<Struct>` entry point
/// can be listed directly in the `scalars: { .. }` section of `register_extension!`.
/// ```ignore
/// use turso_ext::{register_extension, ScalarDerive, ScalarFunc, Value};
///
/// #[derive(ScalarDerive)]
/// struct Multiply;
///
/// impl ScalarFunc for Multiply {
///     type State = i64;
///     const NAME: &'static str = "ctx_multiply";
///     fn init() -> Self::State {
///         3
///     }
///     fn call(state: &Self::State, args: &[Value]) -> Value {
///         Value::from_integer(args[0].to_integer().unwrap_or_default() * *state)
///     }
/// }
/// ```
#[proc_macro_derive(ScalarDerive)]
pub fn derive_scalar(input: TokenStream) -> TokenStream {
    ext::derive_scalar(input)
}

/// Define an aggregate function for your extension by deriving
/// AggregateDerive on a struct that implements the AggFunc trait.
/// ```ignore
/// use turso_ext::{register_extension, Value, AggregateDerive, AggFunc};
///
///#[derive(AggregateDerive)]
///struct SumPlusOne;
///
///impl AggFunc for SumPlusOne {
///   type State = i64;
///   type Error = &'static str;
///   const NAME: &'static str = "sum_plus_one";
///   const ARGS: i32 = 1;
///   fn step(state: &mut Self::State, args: &[Value]) {
///      let Some(val) = args[0].to_integer() else {
///        return;
///     };
///     *state += val;
///     }
///     fn finalize(state: Self::State) -> Result<Value, Self::Error> {
///        Ok(Value::from_integer(state + 1))
///     }
///}
/// ```
#[proc_macro_derive(AggregateDerive)]
pub fn derive_agg_func(input: TokenStream) -> TokenStream {
    ext::derive_agg_func(input)
}

/// Macro to derive a VTabModule for your extension. This macro will generate
/// the necessary functions to register your module with core. You must implement
/// the VTabModule, VTable, and VTabCursor traits.
/// ```ignore
/// #[derive(Debug, VTabModuleDerive)]
/// struct CsvVTabModule;
///
/// impl VTabModule for CsvVTabModule {
///  type Table = CsvTable;
///  const NAME: &'static str = "csv_data";
///  const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
///
///   /// Declare your virtual table and its schema
///  fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
///     let schema = "CREATE TABLE csv_data (
///             name TEXT,
///             age TEXT,
///             city TEXT
///         )".into();
///     Ok((schema, CsvTable {}))
///  }
/// }
///
/// struct CsvTable {}
///
/// // Implement the VTable trait for your virtual table
/// impl VTable for CsvTable {
///  type Cursor = CsvCursor;
///  type Error = &'static str;
///
///  /// Open the virtual table and return a cursor
///  fn open(&self) -> Result<Self::Cursor, Self::Error> {
///     let csv_content = fs::read_to_string("data.csv").unwrap_or_default();
///     let rows: Vec<Vec<String>> = csv_content
///         .lines()
///         .skip(1)
///         .map(|line| {
///             line.split(',')
///                 .map(|s| s.trim().to_string())
///                 .collect()
///         })
///         .collect();
///     Ok(CsvCursor { rows, index: 0 })
///  }
///
/// /// **Optional** methods for non-readonly tables:
///
///  /// Update the row with the provided values, return the new rowid
///  fn update(&mut self, rowid: i64, args: &[Value]) -> Result<Option<i64>, Self::Error> {
///      Ok(None)// return Ok(None) for read-only
///  }
///
///  /// Insert a new row with the provided values, return the new rowid
///  fn insert(&mut self, args: &[Value]) -> Result<(), Self::Error> {
///      Ok(()) //
///  }
///
///  /// Delete the row with the provided rowid
///  fn delete(&mut self, rowid: i64) -> Result<(), Self::Error> {
///    Ok(())
///  }
///
///  /// Destroy the virtual table. Any cleanup logic for when the table is deleted comes heres
///  fn destroy(&mut self) -> Result<(), Self::Error> {
///     Ok(())
///  }
/// }
///
///  #[derive(Debug)]
/// struct CsvCursor {
///   rows: Vec<Vec<String>>,
///   index: usize,
/// }
///
/// impl CsvCursor {
///   /// Returns the value for a given column index.
///   fn column(&self, idx: u32) -> Result<Value, Self::Error> {
///       let row = &self.rows[self.index];
///       if (idx as usize) < row.len() {
///           Value::from_text(&row[idx as usize])
///       } else {
///           Value::null()
///       }
///   }
/// }
///
/// // Implement the VTabCursor trait for your virtual cursor
/// impl VTabCursor for CsvCursor {
///  type Error = &'static str;
///
///  /// Filter the virtual table based on arguments (omitted here for simplicity)
///  fn filter(&mut self, _args: &[Value], _idx_info: Option<(&str, i32)>) -> ResultCode {
///      ResultCode::OK
///  }
///
///  /// Move the cursor to the next row
///  fn next(&mut self) -> ResultCode {
///     if self.index < self.rows.len() - 1 {
///         self.index += 1;
///         ResultCode::OK
///     } else {
///         ResultCode::EOF
///     }
///  }
///
///  fn eof(&self) -> bool {
///      self.index >= self.rows.len()
///  }
///
///  /// Return the value for a given column index
///  fn column(&self, idx: u32) -> Result<Value, Self::Error> {
///      self.column(idx)
///  }
///
///  fn rowid(&self) -> i64 {
///      self.index as i64
///  }
/// }
///
#[proc_macro_derive(VTabModuleDerive)]
pub fn derive_vtab_module(input: TokenStream) -> TokenStream {
    ext::derive_vtab_module(input)
}

/// ```ignore
/// use turso_ext::{ExtResult as Result, VfsDerive, VfsExtension, VfsFile};
///
/// // Your struct must also impl Default
/// #[derive(VfsDerive, Default)]
/// struct ExampleFS;
///
///
/// struct ExampleFile {
///    file: std::fs::File,
///
///
/// impl VfsExtension for ExampleFS {
///    /// The name of your vfs module
///    const NAME: &'static str = "example";
///
///    type File = ExampleFile;
///
///    fn open(&self, path: &str, flags: i32, _direct: bool) -> Result<Self::File> {
///        let file = OpenOptions::new()
///            .read(true)
///            .write(true)
///            .create(flags & 1 != 0)
///            .open(path)
///            .map_err(|_| ResultCode::Error)?;
///        Ok(TestFile { file })
///    }
///
///    fn run_once(&self) -> Result<()> {
///    // (optional) method to cycle/advance IO, if your extension is asynchronous
///        Ok(())
///    }
///
///    fn close(&self, file: Self::File) -> Result<()> {
///    // (optional) method to close or drop the file
///        Ok(())
///    }
///
///    fn generate_random_number(&self) -> i64 {
///    // (optional) method to generate random number. Used for testing
///        let mut buf = [0u8; 8];
///        getrandom::fill(&mut buf).unwrap();
///        i64::from_ne_bytes(buf)
///    }
///
///   fn get_current_time(&self) -> String {
///    // (optional) method to generate random number. Used for testing
///        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
///    }
///
///
/// impl VfsFile for ExampleFile {
///    fn read(
///        &mut self,
///        buf: &mut [u8],
///        count: usize,
///        offset: i64,
///    ) -> Result<i32> {
///        if file.file.seek(SeekFrom::Start(offset as u64)).is_err() {
///            return Err(ResultCode::Error);
///        }
///        file.file
///            .read(&mut buf[..count])
///            .map_err(|_| ResultCode::Error)
///            .map(|n| n as i32)
///    }
///
///    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> Result<i32> {
///        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
///            return Err(ResultCode::Error);
///        }
///        self.file
///            .write(&buf[..count])
///            .map_err(|_| ResultCode::Error)
///            .map(|n| n as i32)
///    }
///
///    fn sync(&self) -> Result<()> {
///        self.file.sync_all().map_err(|_| ResultCode::Error)
///    }
///
///    fn size(&self) -> i64 {
///      self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
///   }
///}
///
///```
#[proc_macro_derive(VfsDerive)]
pub fn derive_vfs_module(input: TokenStream) -> TokenStream {
    ext::derive_vfs_module(input)
}

/// match_ignore_ascii_case will generate trie-like tree matching from normal match expression.
/// example:
/// ```ignore
///     match_ignore_ascii_case!(match input {
///        b"AB" => TokenType::TK_ABORT,
///        b"AC" => TokenType::TK_ACTION,
///        _ => TokenType::TK_ID,
///    })
/// ```
#[proc_macro]
pub fn match_ignore_ascii_case(input: TokenStream) -> TokenStream {
    ext::match_ignore_ascci_case(input)
}

/// Derive macro for creating atomic wrappers for enums
///
/// Supports:
/// - Unit variants
/// - Variants with single bool/u8/i8 fields
/// - Named or unnamed fields
///
/// Algorithm:
/// - Uses u8 representation, splitting bits for variant discriminant and field data
/// - For bool fields: high bit for bool, lower 7 bits for discriminant
/// - For u8/i8 fields: uses u16 internally (8 bits discriminant, 8 bits data)
///
/// Example:
/// ```ignore
/// #[derive(AtomicEnum)]
/// enum TransactionState {
///     Write { schema_did_change: bool },
///     Read,
///     PendingUpgrade,
///     None,
/// }
/// ```
#[proc_macro_derive(AtomicEnum)]
pub fn derive_atomic_enum(input: TokenStream) -> TokenStream {
    atomic_enum::derive_atomic_enum_inner(input)
}

/// Test macro for `core_tester` crate
///
/// Generates a runnable Rust test from the following function signature
///
/// ```ignore
/// fn test_x(db: TempDatabase) -> Result<()> {}
/// // Or
/// fn test_y(db: TempDatabase) {}
/// ```
///
/// Macro accepts the following arguments
///
/// - `mvcc` flag: creates an additional test that will run the same code with MVCC enabled
/// - `encryption` flag: creates an additional `_encrypted` variant that passes
///   `Some(EncryptionOpts)` to the test function (the plain variant passes `None`).
///   The function parameter must be `Option<EncryptionOpts>`.
/// - `path` arg: specifies the name of the database to be created
/// - `init_sql` arg: specifies the SQL query that will be run by `rusqlite` before initializing the Turso database
///
/// Example (TempDatabase):
/// ```no_run,rust
/// #[turso_macros::test(mvcc, path = "test.db", init_sql = "CREATE TABLE test_rowid (id INTEGER PRIMARY KEY);")]
/// fn test_integer_primary_key(tmp_db: TempDatabase) -> anyhow::Result<()> {
///     // Code goes here to test
///     Ok(())
/// }
/// ```
///
/// Example (encryption):
/// ```no_run,rust
/// #[turso_macros::test(encryption)]
/// fn test_restart() {
///     // `encrypted` is injected by the macro: false for the plain variant,
///     // true for the _encrypted variant.
///     let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
///     // test body
/// }
/// ```
#[proc_macro_attribute]
pub fn test(args: TokenStream, input: TokenStream) -> TokenStream {
    test::test_macro_attribute(args, input)
}

/// Wrap a function body in a stack trace guard and a `turso_stack` tracing span.
///
/// With no arguments, the label is inferred as `module_path!()::function_name`
/// and the span name is the function name.
/// A string literal argument overrides the label.
///
/// ```ignore
/// #[turso_macros::trace_stack]
/// fn prepare_select_plan(...) { ... }
///
/// #[turso_macros::trace_stack("select:translate")]
/// fn translate_select(...) { ... }
///
/// #[turso_macros::trace_stack(detail = stmt_kind(&stmt))]
/// fn translate_inner(stmt: ast::Stmt, ...) { ... }
/// ```
#[proc_macro_attribute]
pub fn trace_stack(attr: TokenStream, input: TokenStream) -> TokenStream {
    trace_stack_impl::trace_stack_attribute(attr, input)
}

/// Wrap a function body in an allocation-site scope.
///
/// The argument must be an expression that converts into
/// `crate::alloc::AllocationSite`.
#[proc_macro_attribute]
pub fn allocation_site(attr: TokenStream, input: TokenStream) -> TokenStream {
    let site: proc_macro2::TokenStream = attr.into();
    let mut function = parse_macro_input!(input as syn::ItemFn);
    let body = function.block;
    function.block = Box::new(syn::parse_quote!({
        #[cfg(feature = "allocation_metric")]
        let _turso_allocation_site_guard =
            crate::alloc::enter_allocation_site(#site);
        #body
    }));
    TokenStream::from(quote!(#function))
}

/// Controls the `#[cfg(not(antithesis))]` fallback in "always" comparison macros.
#[allow(clippy::enum_variant_names)]
enum ComparisonFallback {
    /// `assert!(left OP right, fmt_args)` — for gt, gte, lt, lte
    AssertOp,
    /// `assert_eq!(left, right, fmt_args)`
    AssertEq,
    /// `assert_ne!(left, right, fmt_args)`
    AssertNe,
}

enum ConditionAssertKind {
    Assert,
    DebugAssert,
}

enum BooleanCombinator {
    Some,
    All,
}

fn emit_condition_assert(
    file_path: &str,
    input: ConditionAssertInput,
    kind: ConditionAssertKind,
) -> proc_macro2::TokenStream {
    let cond = &input.condition;
    let msg = input
        .message
        .clone()
        .unwrap_or_else(|| expr_to_lit_str(cond));
    let prefixed = prefix_message(file_path, &msg);
    let details = details_json(&input.details);

    let fmt_args = details_format_args(&msg, &input.details);
    // Without the antithesis cfg, a debug assertion must not evaluate its
    // condition in release builds: `debug_assert!` keeps the expression
    // inside the `if cfg!(debug_assertions)` block, so the compiler can drop
    // it together with the check. Binding it to `__turso_cond` first would
    // keep every condition with a fallible or non-trivial callee alive.
    let assert_call = match kind {
        ConditionAssertKind::Assert => quote! { assert!(#cond, #fmt_args); },
        ConditionAssertKind::DebugAssert => quote! { debug_assert!(#cond, #fmt_args); },
    };
    let exit_msg = quote! {
        eprint!("[antithesis] assertion failed: ");
        eprintln!(#fmt_args);
        eprintln!("exiting with code 0 because antithesis already captured this failure");
    };

    let env_check = antithesis_env_check();
    quote! {
        {
            #[cfg(antithesis)]
            {
                let __turso_cond = #cond;
                #env_check
                antithesis_sdk::assert_always_or_unreachable!(__turso_cond, #prefixed, #details);
                if !__turso_cond {
                    #exit_msg
                    std::process::exit(0);
                }
            }
            #[cfg(not(antithesis))]
            { #assert_call }
        }
    }
}

fn emit_boolean_guidance(
    file_path: &str,
    input: BooleanGuidanceInput,
    combinator: BooleanCombinator,
) -> proc_macro2::TokenStream {
    let prefixed = prefix_message(file_path, &input.message);
    let details = details_json(&input.details);

    let conds: Vec<_> = input.conditions.iter().map(|c| &c.condition).collect();

    // Build the combined condition using separate quote! arms to avoid
    // multi-character punct interpolation issues with `||` and `&&`.
    let combined_cond = match combinator {
        BooleanCombinator::Some => {
            if conds.is_empty() {
                quote! { false }
            } else {
                let mut iter = conds.iter();
                let first = iter.next().unwrap();
                let mut combined = quote! { #first };
                for c in iter {
                    combined = quote! { #combined || #c };
                }
                combined
            }
        }
        BooleanCombinator::All => {
            if conds.is_empty() {
                quote! { true }
            } else {
                let mut iter = conds.iter();
                let first = iter.next().unwrap();
                let mut combined = quote! { #first };
                for c in iter {
                    combined = quote! { #combined && #c };
                }
                combined
            }
        }
    };

    let fmt_args = details_format_args(&input.message, &input.details);
    let env_check = antithesis_env_check();
    quote! {
        {
            let __turso_cond = #combined_cond;
            #[cfg(antithesis)]
            {
                #env_check
                antithesis_sdk::assert_always_or_unreachable!(__turso_cond, #prefixed, #details);
                if !__turso_cond {
                    eprint!("[antithesis] assertion failed: ");
                    eprintln!(#fmt_args);
                    eprintln!("exiting with code 0 because antithesis already captured this failure");
                    std::process::exit(0);
                }
            }
            #[cfg(not(antithesis))]
            { assert!(__turso_cond, #fmt_args); }
        }
    }
}

fn emit_always_comparison(
    file_path: &str,
    input: ComparisonAssertInput,
    op_str: &str,
    fallback: ComparisonFallback,
) -> proc_macro2::TokenStream {
    let op_tokens: proc_macro2::TokenStream = op_str.parse().unwrap();
    let left = &input.left;
    let right = &input.right;
    let msg = input
        .message
        .clone()
        .unwrap_or_else(|| comparison_auto_message(left, right, op_str));
    let prefixed = prefix_message(file_path, &msg);
    let details = details_json(&input.details);
    let fmt_args = details_format_args(&msg, &input.details);

    let fallback_assert = match fallback {
        ComparisonFallback::AssertOp => {
            quote! { assert!(__turso_left #op_tokens __turso_right, #fmt_args); }
        }
        ComparisonFallback::AssertEq => {
            quote! { assert_eq!(__turso_left, __turso_right, #fmt_args); }
        }
        ComparisonFallback::AssertNe => {
            quote! { assert_ne!(__turso_left, __turso_right, #fmt_args); }
        }
    };

    let env_check = antithesis_env_check();
    quote! {
        {
            let ref __turso_left = #left;
            let ref __turso_right = #right;
            #[cfg(antithesis)]
            {
                #env_check
                antithesis_sdk::assert_always_or_unreachable!(__turso_left #op_tokens __turso_right, #prefixed, #details);
                if !(__turso_left #op_tokens __turso_right) {
                    eprint!("[antithesis] assertion failed: ");
                    eprintln!(#fmt_args);
                    eprintln!("exiting with code 0 because antithesis already captured this failure");
                    std::process::exit(0);
                }
            }
            #[cfg(not(antithesis))]
            { #fallback_assert }
        }
    }
}

fn emit_sometimes_comparison(
    file_path: &str,
    input: ComparisonAssertInput,
    op_str: &str,
) -> proc_macro2::TokenStream {
    let op_tokens: proc_macro2::TokenStream = op_str.parse().unwrap();
    let left = &input.left;
    let right = &input.right;
    let msg = input
        .message
        .clone()
        .unwrap_or_else(|| comparison_auto_message(left, right, op_str));
    let prefixed = prefix_message(file_path, &msg);
    let details = details_json(&input.details);
    let debug_check = details_debug_check(&input.details);

    let env_check = antithesis_env_check();
    quote! {
        {
            let ref __turso_left = #left;
            let ref __turso_right = #right;
            #[cfg(antithesis)]
            {
                #env_check
                antithesis_sdk::assert_sometimes!(__turso_left #op_tokens __turso_right, #prefixed, #details);
            }
            #[cfg(not(antithesis))]
            {
                let _ = (__turso_left, __turso_right);
                #debug_check
            }
        }
    }
}

/// Drop-in replacement for [`assert!`] that additionally reports to the
/// [Antithesis SDK](https://docs.rs/antithesis_sdk) when compiled with
/// `--features antithesis`.
///
/// Maps to `antithesis_sdk::assert_always_or_unreachable!` — the condition must be
/// true every time this line is reached, but it is OK if the line is never reached.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves exactly like [`assert!`] (panics on failure).
/// - **With `antithesis` feature**: reports the property to Antithesis via
///   `assert_always_or_unreachable!`. On failure, prints the error to stderr and calls
///   `std::process::exit(0)` instead of panicking. The clean exit lets Antithesis properly
///   process the property violation. The normal `assert!` is skipped in this path.
///
/// # Parameters
///
/// - `condition` — boolean expression to check.
/// - `"message"` *(optional)* — human-readable description.
///   Auto-generated from the   condition expression if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details forwarded to Antithesis
///   as JSON and included in the panic/exit message.
///
/// # Usage
///
/// ```ignore
/// turso_assert!(condition);
/// turso_assert!(condition, "message");
/// turso_assert!(condition, "message", { "key": value });
/// ```
///
/// # Examples
///
/// ```ignore
/// // Simple condition (auto-generates message from expression)
/// turso_assert_less_than_or_equal!(value, PageSize::MAX as usize);
///
/// // With explicit message
/// turso_assert_greater_than!(page_idx, 0, "page index must be positive");
///
/// // With structured details for Antithesis
/// turso_assert_greater_than_or_equal!(
///     available_space, required_space,
///     "not enough space on page",
///     { "available": available_space, "required": required_space }
/// );
/// ```
///
/// # When to use
///
/// Use `turso_assert!` anywhere you would use `assert!`. It is the default choice for
/// invariants that must always hold. Prefer [`turso_debug_assert!`] for expensive checks
/// that should only run in debug builds.
#[proc_macro]
pub fn turso_assert(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ConditionAssertInput);
    emit_condition_assert(&file_path, input, ConditionAssertKind::Assert).into()
}

/// Drop-in replacement for [`debug_assert!`] that additionally reports to the
/// [Antithesis SDK](https://docs.rs/antithesis_sdk) when compiled with
/// `--features antithesis`.
///
/// Maps to `antithesis_sdk::assert_always_or_unreachable!`.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves exactly like [`debug_assert!`] — panics
///   on failure in debug builds, compiled out in release builds.
/// - **With `antithesis` feature**: reports the property to Antithesis via
///   `assert_always_or_unreachable!`. On failure, prints the error to stderr and calls
///   `std::process::exit(0)` instead of panicking. The clean exit lets Antithesis properly
///   process the property violation. The normal `debug_assert!` is skipped in this path.
///
/// # Parameters
///
/// Same as [`turso_assert!`]:
/// - `condition` — boolean expression to check.
/// - `"message"` *(optional)* — human-readable description.
/// - `{ "key": value, ... }` *(optional)* — structured details for Antithesis.
///
/// # Usage
///
/// ```ignore
/// turso_debug_assert!(condition);
/// turso_debug_assert!(condition, "message");
/// turso_debug_assert!(condition, "message", { "key": value });
/// ```
///
/// # Examples
///
/// ```ignore
/// turso_debug_assert!(value <= PageSize::MAX as usize);
/// turso_debug_assert!(matches!(self.page_type(), Ok(PageType::TableInterior)));
/// ```
///
/// # When to use
///
/// Use `turso_debug_assert!` for invariant checks that are too expensive for release
/// builds (e.g., scanning a list to verify sorted order).
#[proc_macro]
pub fn turso_debug_assert(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ConditionAssertInput);
    emit_condition_assert(&file_path, input, ConditionAssertKind::DebugAssert).into()
}

/// Observational assertion: tells Antithesis that a condition should be true
/// **at least once** across all test runs. Never panics.
///
/// Maps to `antithesis_sdk::assert_sometimes!`.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: evaluates the condition expression (preserving any
///   side effects) but discards the result. Effectively a no-op.
/// - **With `antithesis` feature**: reports to Antithesis via `assert_sometimes!`.
///   Antithesis will flag it if the condition is *never* true across the entire test
///   campaign, indicating the fuzzer failed to explore that state.
///
/// This macro **never panics or exits** — it is purely observational.
///
/// # Parameters
///
/// - `condition` — boolean expression to observe.
/// - `"message"` *(optional)* — human-readable description.
/// - `{ "key": value, ... }` *(optional)* — structured details for Antithesis.
///
/// # Usage
///
/// ```ignore
/// turso_assert_sometimes!(condition);
/// turso_assert_sometimes!(condition, "message");
/// turso_assert_sometimes!(condition, "message", { "key": value });
/// ```
///
/// # When to use
///
/// Use `turso_assert_sometimes!` to verify the fuzzer explores both branches of a
/// condition. For example, if a cache can be either hit or miss, place a
/// `turso_assert_sometimes!` on each branch to ensure Antithesis tests both paths.
#[proc_macro]
pub fn turso_assert_sometimes(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ConditionAssertInput);
    let cond = &input.condition;
    let msg = input
        .message
        .clone()
        .unwrap_or_else(|| expr_to_lit_str(cond));
    let prefixed = prefix_message(&file_path, &msg);
    let details = details_json(&input.details);

    let debug_check = details_debug_check(&input.details);
    let env_check = antithesis_env_check();
    quote! {
        {
            let __turso_cond = #cond;
            #[cfg(antithesis)]
            {
                #env_check
                antithesis_sdk::assert_sometimes!(__turso_cond, #prefixed, #details);
            }
            #[cfg(not(antithesis))]
            {
                let _ = __turso_cond;
                #debug_check
            }
        }
    }
    .into()
}

/// Asserts that **at least one** of multiple named conditions is true whenever this
/// line is reached (logical OR). All conditions are evaluated.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: panics if none of the conditions are true (like `assert!`).
/// - **With `antithesis` feature**: reports the property to Antithesis via
///   `assert_always_or_unreachable!`. On failure, prints the error to stderr and calls
///   `std::process::exit(0)` instead of panicking.
///
/// # Parameters
///
/// - `{name: condition, ...}` — named boolean conditions in curly braces. Names are
///   labels for Antithesis reporting.
/// - `"message"` — human-readable description (required).
/// - `{ "key": value, ... }` *(optional)* — structured details for Antithesis.
///
/// # Usage
///
/// ```ignore
/// turso_assert_some!(
///     {is_leaf: page.is_leaf(), is_interior: page.is_interior()},
///     "page must be either leaf or interior"
/// );
///
/// turso_assert_some!(
///     {has_data: !row.is_empty(), has_rowid: row.rowid().is_some()},
///     "row must have data or a rowid",
///     { "table": table_name }
/// );
/// ```
///
/// # When to use
///
/// Use whenever a condition has a logical OR.
#[proc_macro]
pub fn turso_assert_some(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as BooleanGuidanceInput);
    emit_boolean_guidance(&file_path, input, BooleanCombinator::Some).into()
}

/// Asserts that **all** named conditions are true whenever this line is reached
/// (logical AND).
///
/// # Behavior
///
/// - **Without `antithesis` feature**: panics if any condition is false (like `assert!`).
/// - **With `antithesis` feature**: reports the property to Antithesis via
///   `assert_always_or_unreachable!`. On failure, prints the error to stderr and calls
///   `std::process::exit(0)` instead of panicking.
///
/// # Parameters
///
/// - `{name: condition, ...}` — named boolean conditions in curly braces.
/// - `"message"` — human-readable description (required).
/// - `{ "key": value, ... }` *(optional)* — structured details for Antithesis.
///
/// # Usage
///
/// ```ignore
/// turso_assert_all!(
///     {valid_size: page_size > 0, within_bounds: offset < page_size},
///     "page header fields must all be valid"
/// );
///
/// // With structured details
/// turso_assert_all!(
///     {has_header: header.is_some(), correct_magic: magic == EXPECTED_MAGIC},
///     "database file must be well-formed",
///     { "magic": magic }
/// );
/// ```
///
/// # When to use
///
/// Use whenever a condition has a logical AND.
#[proc_macro]
pub fn turso_assert_all(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as BooleanGuidanceInput);
    emit_boolean_guidance(&file_path, input, BooleanCombinator::All).into()
}

/// Asserts that a code path is reached **at least once** during Antithesis testing. No-op in
/// non-antithesis builds.
///
/// # Parameters
///
/// - `"message"` — human-readable description of the code path (required).
/// - `{ "key": value, ... }` *(optional)* — structured details for Antithesis.
///
/// # Usage
///
/// ```ignore
/// turso_assert_reachable!("opcode: Init");
/// turso_assert_reachable!("checkpoint", { "frames": frame_count });
/// ```
///
/// # Examples
///
/// ```ignore
/// turso_assert_reachable!("opcode: Add");
/// turso_assert_reachable!("checkpoint");
/// ```
///
/// # When to use
///
/// Place at code paths that should be exercised by the fuzzer.
#[proc_macro]
pub fn turso_assert_reachable(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as MessageAssertInput);
    let prefixed = prefix_message(&file_path, &input.message);
    let details = details_json(&input.details);
    let debug_check = details_debug_check(&input.details);

    let env_check = antithesis_env_check();
    quote! {
        {
            #[cfg(antithesis)]
            {
                #env_check
                antithesis_sdk::assert_reachable!(#prefixed, #details);
            }
            #[cfg(not(antithesis))]
            {
                #debug_check
            }
        }
    }
    .into()
}

/// Asserts that a code path is **never** reached. This is a hard assertion — it will
/// terminate the program if the path is executed.
///
/// Maps to `antithesis_sdk::assert_unreachable!`.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: calls [`unreachable!`] (panics with the message).
/// - **With `antithesis` feature**: reports to Antithesis via `assert_unreachable!`,
///   prints the error, then calls `exit(0)`.
///
/// # Parameters
///
/// - `"message"` — human-readable description of why this path is unreachable (required).
/// - `{ "key": value, ... }` *(optional)* — structured details for Antithesis and the
///   panic/exit message.
///
/// # Usage
///
/// ```ignore
/// turso_assert_unreachable!("message");
/// turso_assert_unreachable!("message", { "key": value });
/// ```
///
/// # When to use
///
/// Use for code paths that represent logic errors — places that should genuinely never
/// execute. For paths that are unexpected but not impossible (e.g., fallback branches
/// that indicate a likely bug but shouldn't panic), prefer [`turso_soft_unreachable!`] instead.
#[proc_macro]
pub fn turso_assert_unreachable(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as MessageAssertInput);
    let msg = &input.message;
    let prefixed = prefix_message(&file_path, msg);
    let details = details_json(&input.details);
    let fmt_args = details_format_args(msg, &input.details);

    let env_check = antithesis_env_check();
    quote! {
        {
            #[cfg(antithesis)]
            {
                #env_check
                antithesis_sdk::assert_unreachable!(#prefixed, #details);
                eprint!("[antithesis] unreachable assertion reached: ");
                eprintln!(#fmt_args);
                eprintln!("exiting with code 0 because antithesis already captured this failure");
                std::process::exit(0);
            }
            #[cfg(not(antithesis))]
            { unreachable!(#fmt_args) }
        }
    }
    .into()
}

/// Soft unreachable: signals to Antithesis that this code path should never be reached,
/// but does **not** panic or exit. Without the `antithesis` feature, this is a no-op.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: complete no-op — no evaluation, no side effects.
/// - **With `antithesis` feature**: reports to Antithesis, then execution continues normally.
///
/// This is the key difference from [`turso_assert_unreachable!`]: soft_unreachable only
/// *reports* to Antithesis, while the hard version terminates the process.
///
/// # Parameters
///
/// - `"message"` — human-readable description (required).
/// - `{ "key": value, ... }` *(optional)* — structured details for Antithesis.
///
/// # Usage
///
/// ```ignore
/// turso_soft_unreachable!("message");
/// turso_soft_unreachable!("message", { "key": value });
/// ```
///
/// # Examples
///
/// ```ignore
/// // In the pager, marking unexpected-but-recoverable states
/// turso_soft_unreachable!("wal_state() called on database without WAL");
/// turso_soft_unreachable!(
///     "Cannot set ptrmap entry for header/ptrmap page or invalid page",
///     { "page": db_page_no_to_update }
/// );
/// ```
///
/// # When to use
///
/// Use for code paths that are unexpected but not impossible — places where you want
/// Antithesis to flag the issue but don't want to crash the program, for example in error
/// handling paths and fallback branches that indicate a likely bug but should degrade
/// gracefully in production.
#[proc_macro]
pub fn turso_soft_unreachable(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as MessageAssertInput);
    let prefixed = prefix_message(&file_path, &input.message);
    let details = details_json(&input.details);
    let debug_check = details_debug_check(&input.details);

    let env_check = antithesis_env_check();
    quote! {
        {
            #[cfg(antithesis)]
            {
                #env_check
                antithesis_sdk::assert_unreachable!(#prefixed, #details);
            }
            #[cfg(not(antithesis))]
            {
                #debug_check
            }
        }
    }
    .into()
}

/// Asserts that `left > right`, providing richer comparison information to Antithesis
/// than a plain `turso_assert!(a > b)`.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves like `assert!(left > right, ...)`.
/// - **With `antithesis` feature**: reports to Antithesis, then on failure prints the
///   error and calls `std::process::exit(0)`.
///
/// # Parameters
///
/// - `left` — left-hand operand.
/// - `right` — right-hand operand.
/// - `"message"` *(optional)* — auto-generated as `"left > right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_greater_than!(left, right);
/// turso_assert_greater_than!(left, right, "message");
/// turso_assert_greater_than!(left, right, "message", { "key": value });
/// ```
///
/// # Examples
///
/// ```ignore
/// turso_assert_greater_than!(page_idx, 0);
/// turso_assert_greater_than!(
///     root_page_num, 0,
///     "Largest root page number cannot be 0"
/// );
/// ```
#[proc_macro]
pub fn turso_assert_greater_than(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_always_comparison(&file_path, input, ">", ComparisonFallback::AssertOp).into()
}

/// Asserts that `left >= right`, providing richer comparison information to Antithesis
/// than a plain `turso_assert!(a >= b)`.
///
/// Maps to `antithesis_sdk::assert_always_or_unreachable!` with `left >= right`.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves like `assert!(left >= right, ...)`.
/// - **With `antithesis` feature**: reports to Antithesis, then on failure prints the
///   error and calls `std::process::exit(0)`.
///
/// # Parameters
///
/// - `left`, `right` — operands to compare.
/// - `"message"` *(optional)* — auto-generated as `"left >= right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_greater_than_or_equal!(left, right);
/// turso_assert_greater_than_or_equal!(left, right, "message");
/// turso_assert_greater_than_or_equal!(left, right, "message", { "key": value });
/// ```
#[proc_macro]
pub fn turso_assert_greater_than_or_equal(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_always_comparison(&file_path, input, ">=", ComparisonFallback::AssertOp).into()
}

/// Asserts that `left < right`, providing richer comparison information to Antithesis
/// than a plain `turso_assert!(a < b)`.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves like `assert!(left < right, ...)`.
/// - **With `antithesis` feature**: reports to Antithesis, then on failure prints the
///   error and calls `std::process::exit(0)`.
///
/// # Parameters
///
/// - `left`, `right` — operands to compare.
/// - `"message"` *(optional)* — auto-generated as `"left < right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_less_than!(left, right);
/// turso_assert_less_than!(left, right, "message");
/// turso_assert_less_than!(left, right, "message", { "key": value });
/// ```
#[proc_macro]
pub fn turso_assert_less_than(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_always_comparison(&file_path, input, "<", ComparisonFallback::AssertOp).into()
}

/// Asserts that `left <= right`, providing richer comparison information to Antithesis
/// than a plain `turso_assert!(a <= b)`.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves like `assert!(left <= right, ...)`.
/// - **With `antithesis` feature**: reports to Antithesis, then on failure prints the
///   error and calls `std::process::exit(0)`.
///
/// # Parameters
///
/// - `left`, `right` — operands to compare.
/// - `"message"` *(optional)* — auto-generated as `"left <= right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_less_than_or_equal!(left, right);
/// turso_assert_less_than_or_equal!(left, right, "message");
/// turso_assert_less_than_or_equal!(left, right, "message", { "key": value });
/// ```
#[proc_macro]
pub fn turso_assert_less_than_or_equal(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_always_comparison(&file_path, input, "<=", ComparisonFallback::AssertOp).into()
}

/// Drop-in replacement for [`assert_eq!`] that additionally reports to the
/// [Antithesis SDK](https://docs.rs/antithesis_sdk) in Antithesis builds (`--cfg=antithesis`).
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves exactly like [`assert_eq!`].
/// - **With `antithesis` feature**: reports to Antithesis, then on failure prints the
///   error and calls `std::process::exit(0)`.
///
/// # Parameters
///
/// - `left`, `right` — values to compare for equality.
/// - `"message"` *(optional)* — auto-generated as `"left == right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_eq!(left, right);
/// turso_assert_eq!(left, right, "message");
/// turso_assert_eq!(left, right, "message", { "key": value });
/// ```
///
/// # Examples
///
/// ```ignore
/// turso_assert_eq!(QueryMode::new(&cmd), mode);
/// turso_assert_eq!(joined_tables.len(), 1, "expected only one joined table");
/// ```
#[proc_macro]
pub fn turso_assert_eq(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_always_comparison(&file_path, input, "==", ComparisonFallback::AssertEq).into()
}

/// Drop-in replacement for [`assert_ne!`] that additionally reports to the
/// [Antithesis SDK](https://docs.rs/antithesis_sdk) in Antithesis builds (`--cfg=antithesis`).
///
/// # Behavior
///
/// - **Without `antithesis` feature**: behaves exactly like [`assert_ne!`].
/// - **With `antithesis` feature**: reports to Antithesis, then on failure prints the
///   error and calls `std::process::exit(0)`.
///
/// # Parameters
///
/// - `left`, `right` — values to compare for inequality.
/// - `"message"` *(optional)* — auto-generated as `"left != right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_ne!(left, right);
/// turso_assert_ne!(left, right, "message");
/// turso_assert_ne!(left, right, "message", { "key": value });
/// ```
#[proc_macro]
pub fn turso_assert_ne(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_always_comparison(&file_path, input, "!=", ComparisonFallback::AssertNe).into()
}

/// Observational assertion: tells Antithesis that `left > right` should be true
/// **at least once** across all test runs. Never panics.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: evaluates both operands, then discards the result. No-op.
/// - **With `antithesis` feature**: reports the failure to Antithesis, then continues execution.
///
/// # Parameters
///
/// - `left`, `right` — operands to compare.
/// - `"message"` *(optional)* — auto-generated as `"left > right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_sometimes_greater_than!(left, right);
/// turso_assert_sometimes_greater_than!(left, right, "message");
/// turso_assert_sometimes_greater_than!(left, right, "message", { "key": value });
/// ```
///
/// # When to use
///
/// Use to verify the fuzzer explores states where one value exceeds another. For example,
/// ensuring the WAL sometimes grows beyond a certain threshold.
#[proc_macro]
pub fn turso_assert_sometimes_greater_than(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_sometimes_comparison(&file_path, input, ">").into()
}

/// Observational assertion: tells Antithesis that `left < right` should be true
/// **at least once** across all test runs. Never panics.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: evaluates both operands, then discards the result. No-op.
/// - **With `antithesis` feature**: reports to Antithesis via `assert_sometimes!`.
///
/// # Parameters
///
/// - `left`, `right` — operands to compare.
/// - `"message"` *(optional)* — auto-generated as `"left < right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_sometimes_less_than!(left, right);
/// turso_assert_sometimes_less_than!(left, right, "message");
/// turso_assert_sometimes_less_than!(left, right, "message", { "key": value });
/// ```
#[proc_macro]
pub fn turso_assert_sometimes_less_than(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_sometimes_comparison(&file_path, input, "<").into()
}

/// Observational assertion: tells Antithesis that `left >= right` should be true
/// **at least once** across all test runs. Never panics.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: evaluates both operands, then discards the result. No-op.
/// - **With `antithesis` feature**: reports to Antithesis via `assert_sometimes!`.
///
/// # Parameters
///
/// - `left`, `right` — operands to compare.
/// - `"message"` *(optional)* — auto-generated as `"left >= right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_sometimes_greater_than_or_equal!(left, right);
/// turso_assert_sometimes_greater_than_or_equal!(left, right, "message");
/// turso_assert_sometimes_greater_than_or_equal!(left, right, "message", { "key": value });
/// ```
#[proc_macro]
pub fn turso_assert_sometimes_greater_than_or_equal(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_sometimes_comparison(&file_path, input, ">=").into()
}

/// Observational assertion: tells Antithesis that `left <= right` should be true
/// **at least once** across all test runs. Never panics.
///
/// # Behavior
///
/// - **Without `antithesis` feature**: evaluates both operands, then discards the result. No-op.
/// - **With `antithesis` feature**: reports to Antithesis via `assert_sometimes!`.
///
/// # Parameters
///
/// - `left`, `right` — operands to compare.
/// - `"message"` *(optional)* — auto-generated as `"left <= right"` if omitted.
/// - `{ "key": value, ... }` *(optional)* — structured details.
///
/// # Usage
///
/// ```ignore
/// turso_assert_sometimes_less_than_or_equal!(left, right);
/// turso_assert_sometimes_less_than_or_equal!(left, right, "message");
/// turso_assert_sometimes_less_than_or_equal!(left, right, "message", { "key": value });
/// ```
#[proc_macro]
pub fn turso_assert_sometimes_less_than_or_equal(input: TokenStream) -> TokenStream {
    let file_path = get_caller_file(&input);
    let input = parse_macro_input!(input as ComparisonAssertInput);
    emit_sometimes_comparison(&file_path, input, "<=").into()
}

fn get_caller_file(input: &TokenStream) -> String {
    let mut iter = input.clone().into_iter();
    if let Some(first_token) = iter.next() {
        first_token.span().start().file()
    } else {
        "unknown".to_string()
    }
}

fn prefix_message(file_path: &str, msg: &LitStr) -> LitStr {
    LitStr::new(&format!("[{}] {}", file_path, msg.value()), msg.span())
}
