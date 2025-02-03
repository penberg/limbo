mod args;
use args::{RegisterExtensionInput, ScalarInfo};
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput, ItemFn};
extern crate proc_macro;
use proc_macro::{token_stream::IntoIter, Group, TokenStream, TokenTree};
use std::collections::HashMap;

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
                    variable_name_list.push_str(&format!("{},", ident));
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
    format!("{{ {} }}", variable_name_list).to_string()
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
            desc = format!("Some({})", description);
        } else {
            desc = "None".to_string();
        }
        all_enum_arms.push_str(&format!(
            "{}::{} {} => {},\n",
            enum_name, variant, payload, desc
        ));
    }

    let enum_impl = format!(
        "impl {}  {{ 
     pub fn get_description(&self) -> Option<&str> {{
     match self {{
     {}
     }}
     }}
     }}",
        enum_name, all_enum_arms
    );
    enum_impl.parse().unwrap()
}

/// Declare a scalar function for your extension. This requires the name:
/// #[scalar(name = "example")] of what you wish to call your function with.
/// Your function __must__ use the signature: `fn (args: &[Value]) -> Value`
/// with proper spelling.
/// ```ignore
/// use limbo_ext::{scalar, Value};
/// #[scalar(name = "double", alias = "twice")] // you can provide an <optional> alias
/// fn double(args: &[Value]) -> Value {
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
    let ast = parse_macro_input!(input as ItemFn);
    let fn_name = &ast.sig.ident;
    let scalar_info = parse_macro_input!(attr as ScalarInfo);
    let name = &scalar_info.name;
    let register_fn_name = format_ident!("register_{}", fn_name);
    let fn_body = &ast.block;
    let alias_check = if let Some(alias) = &scalar_info.alias {
        quote! {
            let Ok(alias_c_name) = std::ffi::CString::new(#alias) else {
                return ::limbo_ext::ResultCode::Error;
            };
            (api.register_scalar_function)(
                api.ctx,
                alias_c_name.as_ptr(),
                #fn_name,
            );
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[no_mangle]
        pub unsafe extern "C" fn #register_fn_name(
            api: *const ::limbo_ext::ExtensionApi
        ) -> ::limbo_ext::ResultCode {
            if api.is_null() {
                return ::limbo_ext::ResultCode::Error;
            }
            let api = unsafe { &*api };
            let Ok(c_name) = std::ffi::CString::new(#name) else {
                return ::limbo_ext::ResultCode::Error;
            };
            (api.register_scalar_function)(
                api.ctx,
                c_name.as_ptr(),
                #fn_name,
            );
            #alias_check
            ::limbo_ext::ResultCode::OK
        }

        #[no_mangle]
        pub unsafe extern "C" fn #fn_name(
            argc: i32,
            argv: *const ::limbo_ext::Value
        ) -> ::limbo_ext::Value {
            let args = if argv.is_null() || argc <= 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(argv, argc as usize) }
            };
            #fn_body
        }
    };

    TokenStream::from(expanded)
}

/// Define an aggregate function for your extension by deriving
/// AggregateDerive on a struct that implements the AggFunc trait.
/// ```ignore
/// use limbo_ext::{register_extension, Value, AggregateDerive, AggFunc};
///
///#[derive(AggregateDerive)]
///struct SumPlusOne;
///
///impl AggFunc for SumPlusOne {
///   type State = i64;
///   const NAME: &'static str = "sum_plus_one";
///   const ARGS: i32 = 1;
///   fn step(state: &mut Self::State, args: &[Value]) {
///      let Some(val) = args[0].to_integer() else {
///        return;
///     };
///     *state += val;
///     }
///     fn finalize(state: Self::State) -> Value {
///        Value::from_integer(state + 1)
///     }
///}
/// ```
#[proc_macro_derive(AggregateDerive)]
pub fn derive_agg_func(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let step_fn_name = format_ident!("{}_step", struct_name);
    let finalize_fn_name = format_ident!("{}_finalize", struct_name);
    let init_fn_name = format_ident!("{}_init", struct_name);
    let register_fn_name = format_ident!("register_{}", struct_name);

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            pub extern "C" fn #init_fn_name() -> *mut ::limbo_ext::AggCtx {
                let state = Box::new(<#struct_name as ::limbo_ext::AggFunc>::State::default());
                let ctx = Box::new(::limbo_ext::AggCtx {
                    state: Box::into_raw(state) as *mut ::std::os::raw::c_void,
                });
                Box::into_raw(ctx)
            }

            #[no_mangle]
            pub extern "C" fn #step_fn_name(
                ctx: *mut ::limbo_ext::AggCtx,
                argc: i32,
                argv: *const ::limbo_ext::Value,
            ) {
                unsafe {
                    let ctx = &mut *ctx;
                    let state = &mut *(ctx.state as *mut <#struct_name as ::limbo_ext::AggFunc>::State);
                    let args = std::slice::from_raw_parts(argv, argc as usize);
                    <#struct_name as ::limbo_ext::AggFunc>::step(state, args);
                }
            }

            #[no_mangle]
            pub extern "C" fn #finalize_fn_name(
                ctx: *mut ::limbo_ext::AggCtx
            ) -> ::limbo_ext::Value {
                unsafe {
                    let ctx = &mut *ctx;
                    let state = Box::from_raw(ctx.state as *mut <#struct_name as ::limbo_ext::AggFunc>::State);
                    <#struct_name as ::limbo_ext::AggFunc>::finalize(*state)
                }
            }

            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *const ::limbo_ext::ExtensionApi
            ) -> ::limbo_ext::ResultCode {
                if api.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                let api = &*api;
                let name_str = #struct_name::NAME;
                let c_name = match std::ffi::CString::new(name_str) {
                    Ok(cname) => cname,
                    Err(_) => return ::limbo_ext::ResultCode::Error,
                };

                (api.register_aggregate_function)(
                    api.ctx,
                    c_name.as_ptr(),
                    #struct_name::ARGS,
                    #struct_name::#init_fn_name
                        as ::limbo_ext::InitAggFunction,
                    #struct_name::#step_fn_name
                        as ::limbo_ext::StepFunction,
                    #struct_name::#finalize_fn_name
                        as ::limbo_ext::FinalizeFunction,
                )
            }
        }
    };

    TokenStream::from(expanded)
}

/// Macro to derive a VTabModule for your extension. This macro will generate
/// the necessary functions to register your module with core. You must implement
/// the VTabModule trait for your struct, and the VTabCursor trait for your cursor.
/// ```ignore
///#[derive(Debug, VTabModuleDerive)]
///struct CsvVTab;
///impl VTabModule for CsvVTab {
///    type VCursor = CsvCursor;
///    const NAME: &'static str = "csv_data";
///
///    /// Declare the schema for your virtual table
///    fn connect(api: &ExtensionApi) -> ResultCode {
///        let sql = "CREATE TABLE csv_data(
///            name TEXT,
///            age TEXT,
///            city TEXT
///        )";
///        api.declare_virtual_table(Self::NAME, sql)
///    }
///    /// Open the virtual table and return a cursor
///  fn open() -> Self::VCursor {
///       let csv_content = fs::read_to_string("data.csv").unwrap_or_default();
///       let rows: Vec<Vec<String>> = csv_content
///           .lines()
///           .skip(1)
///           .map(|line| {
///               line.split(',')
///                   .map(|s| s.trim().to_string())
///                   .collect()
///           })
///           .collect();
///       CsvCursor { rows, index: 0 }
///   }
///   /// Filter the virtual table based on arguments (omitted here for simplicity)
///   fn filter(_cursor: &mut Self::VCursor, _arg_count: i32, _args: &[Value]) -> ResultCode {
///       ResultCode::OK
///   }
///   /// Return the value for a given column index
///   fn column(cursor: &Self::VCursor, idx: u32) -> Value {
///      cursor.column(idx)
///  }
///  /// Move the cursor to the next row
///  fn next(cursor: &mut Self::VCursor) -> ResultCode {
///      if cursor.index < cursor.rows.len() - 1 {
///          cursor.index += 1;
///          ResultCode::OK
///      } else {
///          ResultCode::EOF
///      }
///  }
///  fn eof(cursor: &Self::VCursor) -> bool {
///      cursor.index >= cursor.rows.len()
///  }
///  #[derive(Debug)]
/// struct CsvCursor {
///   rows: Vec<Vec<String>>,
///   index: usize,
///
/// impl CsvCursor {
///   /// Returns the value for a given column index.
///   fn column(&self, idx: u32) -> Value {
///       let row = &self.rows[self.index];
///       if (idx as usize) < row.len() {
///           Value::from_text(&row[idx as usize])
///       } else {
///           Value::null()
///       }
///   }
/// // Implement the VTabCursor trait for your virtual cursor
/// impl VTabCursor for CsvCursor {
///   fn next(&mut self) -> ResultCode {
///       Self::next(self)
///   }
///  fn eof(&self) -> bool {
///      self.index >= self.rows.len()
///  }
///  fn column(&self, idx: u32) -> Value {
///      self.column(idx)
///  }
///  fn rowid(&self) -> i64 {
///      self.index as i64
///  }
///
#[proc_macro_derive(VTabModuleDerive)]
pub fn derive_vtab_module(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let register_fn_name = format_ident!("register_{}", struct_name);
    let connect_fn_name = format_ident!("connect_{}", struct_name);
    let open_fn_name = format_ident!("open_{}", struct_name);
    let filter_fn_name = format_ident!("filter_{}", struct_name);
    let column_fn_name = format_ident!("column_{}", struct_name);
    let next_fn_name = format_ident!("next_{}", struct_name);
    let eof_fn_name = format_ident!("eof_{}", struct_name);

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            unsafe extern "C" fn #connect_fn_name(
                db: *const ::std::ffi::c_void,
            ) -> ::limbo_ext::ResultCode {
                if db.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let api = unsafe { &*(db as *const ExtensionApi) };
                <#struct_name as ::limbo_ext::VTabModule>::connect(api)
            }

            #[no_mangle]
            unsafe extern "C" fn #open_fn_name(
            ) -> *mut ::std::ffi::c_void {
                let cursor = <#struct_name as ::limbo_ext::VTabModule>::open();
                Box::into_raw(Box::new(cursor)) as *mut ::std::ffi::c_void
            }

            #[no_mangle]
            unsafe extern "C" fn #filter_fn_name(
                cursor: *mut ::std::ffi::c_void,
                argc: i32,
                argv: *const ::limbo_ext::Value,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                let args = std::slice::from_raw_parts(argv, argc as usize);
                <#struct_name as ::limbo_ext::VTabModule>::filter(cursor, argc, args)
            }

            #[no_mangle]
            unsafe extern "C" fn #column_fn_name(
                cursor: *mut ::std::ffi::c_void,
                idx: u32,
            ) -> ::limbo_ext::Value {
                if cursor.is_null() {
                    return ::limbo_ext::Value::error(ResultCode::Error);
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                <#struct_name as ::limbo_ext::VTabModule>::column(cursor, idx)
            }

            #[no_mangle]
            unsafe extern "C" fn #next_fn_name(
                cursor: *mut ::std::ffi::c_void,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                <#struct_name as ::limbo_ext::VTabModule>::next(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #eof_fn_name(
                cursor: *mut ::std::ffi::c_void,
            ) -> bool {
                if cursor.is_null() {
                    return true;
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                <#struct_name as ::limbo_ext::VTabModule>::eof(cursor)
            }

            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *const ::limbo_ext::ExtensionApi
            ) -> ::limbo_ext::ResultCode {
                if api.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                let api = &*api;
                let name = <#struct_name as ::limbo_ext::VTabModule>::NAME;
                // name needs to be a c str FFI compatible, NOT CString
                let name_c = std::ffi::CString::new(name).unwrap();

                let module = ::limbo_ext::VTabModuleImpl {
                    name: name_c.as_ptr(),
                    connect: Self::#connect_fn_name,
                    open: Self::#open_fn_name,
                    filter: Self::#filter_fn_name,
                    column: Self::#column_fn_name,
                    next: Self::#next_fn_name,
                    eof: Self::#eof_fn_name,
                };

                (api.register_module)(api.ctx, name_c.as_ptr(), module)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Register your extension with 'core' by providing the relevant functions
///```ignore
///use limbo_ext::{register_extension, scalar, Value, AggregateDerive, AggFunc};
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
    let input_ast = parse_macro_input!(input as RegisterExtensionInput);
    let RegisterExtensionInput {
        aggregates,
        scalars,
        vtabs,
    } = input_ast;

    let scalar_calls = scalars.iter().map(|scalar_ident| {
        let register_fn =
            syn::Ident::new(&format!("register_{}", scalar_ident), scalar_ident.span());
        quote! {
            {
                let result = unsafe { #register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });

    let aggregate_calls = aggregates.iter().map(|agg_ident| {
        let register_fn = syn::Ident::new(&format!("register_{}", agg_ident), agg_ident.span());
        quote! {
            {
                let result = unsafe{ #agg_ident::#register_fn(api)};
                if !result.is_ok() {
                    return result;
                }
            }
        }
    });
    let vtab_calls = vtabs.iter().map(|vtab_ident| {
        let register_fn = syn::Ident::new(&format!("register_{}", vtab_ident), vtab_ident.span());
        quote! {
            {
                let result = unsafe{ #vtab_ident::#register_fn(api)};
                if result == ::limbo_ext::ResultCode::OK {
                    let result = <#vtab_ident as ::limbo_ext::VTabModule>::connect(api);
                    if !result.is_ok() {
                        return result;
                     }
                }
            }
        }
    });
    let static_aggregates = aggregate_calls.clone();
    let static_scalars = scalar_calls.clone();
    let static_vtabs = vtab_calls.clone();

    let expanded = quote! {
    #[cfg(not(target_family = "wasm"))]
    #[cfg(not(feature = "static"))]
    #[global_allocator]
    static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

            #[cfg(feature = "static")]
            pub unsafe extern "C" fn register_extension_static(api: &::limbo_ext::ExtensionApi) -> ::limbo_ext::ResultCode {
                let api = unsafe { &*api };
                #(#static_scalars)*

                #(#static_aggregates)*

                #(#static_vtabs)*

                ::limbo_ext::ResultCode::OK
              }

            #[cfg(not(feature = "static"))]
            #[no_mangle]
            pub unsafe extern "C" fn register_extension(api: &::limbo_ext::ExtensionApi) -> ::limbo_ext::ResultCode {
                let api = unsafe { &*api };
                #(#scalar_calls)*

                #(#aggregate_calls)*

                #(#vtab_calls)*

                ::limbo_ext::ResultCode::OK
            }
        };

    TokenStream::from(expanded)
}
