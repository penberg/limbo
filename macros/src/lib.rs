mod args;
use args::{ArgsAttr, ArgsSpec};
use quote::{format_ident, quote};
use syn::{parse_macro_input, Attribute, Block, DeriveInput, ItemFn, LitStr};
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

/// Macro to transform the preferred API for scalar functions in extensions into
/// an FFI-compatible function signature while validating argc
#[proc_macro_attribute]
pub fn export_scalar(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_body: &Block = &input_fn.block;

    let mut extracted_spec: Option<ArgsSpec> = None;
    let mut arg_err = None;
    let kept_attrs: Vec<Attribute> = input_fn
        .attrs
        .into_iter()
        .filter_map(|attr| {
            if attr.path().is_ident("args") {
                let parsed_attr = match attr.parse_args::<ArgsAttr>() {
                    Ok(p) => p,
                    Err(err) => {
                        arg_err = Some(err.to_compile_error());
                        return None;
                    }
                };
                extracted_spec = Some(parsed_attr.spec);
                None
            } else {
                Some(attr)
            }
        })
        .collect();
    input_fn.attrs = kept_attrs;
    if let Some(arg_err) = arg_err {
        return arg_err.into();
    }
    let spec = match extracted_spec {
        Some(s) => s,
        None => {
            return syn::Error::new_spanned(
                fn_name,
                "Expected an attribute with integer or range: #[args(1)] #[args(0..2)], etc.",
            )
            .to_compile_error()
            .into()
        }
    };
    let arg_check = match spec {
        ArgsSpec::Exact(exact_count) => {
            quote! {
                if argc != #exact_count {
                    log::error!(
                        "{} was called with {} arguments, expected exactly {}",
                        stringify!(#fn_name),
                        argc,
                        #exact_count
                    );
                    return ::limbo_ext::Value::null();
                }
            }
        }
        ArgsSpec::Range {
            lower,
            upper,
            inclusive: true,
        } => {
            quote! {
                if !(#lower..=#upper).contains(&argc) {
                    log::error!(
                        "{} was called with {} arguments, expected {}..={} range",
                        stringify!(#fn_name),
                        argc,
                        #lower,
                        #upper
                    );
                    return ::limbo_ext::Value::null();
                }
            }
        }
        ArgsSpec::Range {
            lower,
            upper,
            inclusive: false,
        } => {
            quote! {
                if !(#lower..#upper).contains(&argc) {
                    log::error!(
                        "{} was called with {} arguments, expected {}..{} (exclusive)",
                        stringify!(#fn_name),
                        argc,
                        #lower,
                        #upper
                    );
                    return ::limbo_ext::Value::null();
                }
            }
        }
    };
    let expanded = quote! {
        #[export_name = stringify!(#fn_name)]
        extern "C" fn #fn_name(argc: i32, argv: *const ::limbo_ext::Value) -> ::limbo_ext::Value {
            #arg_check

            // from_raw_parts doesn't currently accept null ptr
            if argc == 0 || argv.is_null() {
                log::debug!("{} was called with no arguments", stringify!(#fn_name));
                let args: &[::limbo_ext::Value] = &[];
                #fn_body
            } else {
                let ptr_slice = unsafe {
                    std::slice::from_raw_parts(argv, argc as usize)
                };
                let args: &[::limbo_ext::Value] = ptr_slice;
                #fn_body
            }
        }
    };
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn declare_aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    let meta = parse_macro_input!(attr as ItemFn);
    let mut aggregate_name = None;
    for arg in meta.attrs.iter() {
        if arg.path().is_ident("name") {
            let _ = arg.parse_nested_meta(|nv| match nv.value() {
                Ok(val) => {
                    let s: LitStr = val.parse()?;
                    aggregate_name = Some(s);
                    Ok(())
                }
                Err(e) => Err(e),
            });
        }
    }
    let Some(aggregate_name) = aggregate_name else {
        return syn::Error::new_spanned(
            meta,
            "Expected an attribute with a name: #[aggregate(name = \"sum\")]",
        )
        .to_compile_error()
        .into();
    };

    let input = parse_macro_input!(item as DeriveInput);
    let struct_name = &input.ident;

    let step_fn_name = format_ident!("{}_step", struct_name.to_string().to_lowercase());
    let finalize_fn_name = format_ident!("{}_finalize", struct_name.to_string().to_lowercase());
    let register_fn_name = format_ident!("register_{}", struct_name.to_string().to_lowercase());

    let expanded = quote! {
        #input

        extern "C" fn #step_fn_name(
            ctx: *mut ::limbo_extension::AggregateCtx,
            argc: i32,
            argv: *const ::limbo_extension::Value,
        ) {
            unsafe {
                let state = &mut *(ctx.state as *mut #struct_name);
                let args = std::slice::from_raw_parts(argv, argc as usize);
                state.step(args);
            }
        }

        extern "C" fn #finalize_fn_name(
            ctx: *mut ::limbo_extension::AggregateCtx
        ) -> ::limbo_extension::Value {
            unsafe {
                let state = Box::from_raw(ctx.state as *mut #struct_name);
                state.finalize()
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn #register_fn_name(
            api: *const ::limbo_extension::ExtensionApi
        ) -> ::limbo_extension::ResultCode {
            let cname = std::ffi::CString::new(#aggregate_name).unwrap();
            ((*api).register_aggregate_function)(
                (*api).ctx,
                cname.as_ptr(),
                #step_fn_name,
                #finalize_fn_name,
            )
        }
    };

    TokenStream::from(expanded)
}
