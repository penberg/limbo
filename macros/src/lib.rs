mod args;
use args::RegisterExtensionInput;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};
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

#[proc_macro_derive(ScalarDerive)]
pub fn derive_scalar(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let register_fn_name = format_ident!("register_{}", struct_name);
    let exec_fn_name = format_ident!("{}_exec", struct_name);

    let alias_check = quote! {
       if let Some(alias) = scalar.alias() {
            let alias_c_name = std::ffi::CString::new(alias).unwrap();

            (api.register_scalar_function)(
                api.ctx,
                alias_c_name.as_ptr(),
                #exec_fn_name,
            );
        }
    };

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *const ::limbo_ext::ExtensionApi
            ) -> ::limbo_ext::ResultCode {
                if api.is_null() {
                    return ::limbo_ext::RESULT_ERROR;
                }
                let api = unsafe { &*api };

                let scalar = #struct_name;
                let name = scalar.name();
                let c_name = std::ffi::CString::new(name).unwrap();

                (api.register_scalar_function)(
                    api.ctx,
                    c_name.as_ptr(),
                    #exec_fn_name,
                );

                #alias_check

                ::limbo_ext::RESULT_OK
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn #exec_fn_name(
            argc: i32,
            argv: *const ::limbo_ext::Value
        ) -> ::limbo_ext::Value {
            let scalar = #struct_name;
            let args_slice = if argv.is_null() || argc <= 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(argv, argc as usize) }
            };
            scalar.call(args_slice)
        }
    };

    TokenStream::from(expanded)
}

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
                    return ::limbo_ext::RESULT_ERROR;
                }

                let api = &*api;
                let agg = #struct_name;
                let name_str = agg.name();
                let c_name = match std::ffi::CString::new(name_str) {
                    Ok(cname) => cname,
                    Err(_) => return ::limbo_ext::RESULT_ERROR,
                };

                (api.register_aggregate_function)(
                    api.ctx,
                    c_name.as_ptr(),
                    agg.args(),
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

#[proc_macro]
pub fn register_extension(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as RegisterExtensionInput);

    let RegisterExtensionInput {
        aggregates,
        scalars,
    } = input_ast;

    let scalar_calls = scalars.iter().map(|scalar_ident| {
        let register_fn =
            syn::Ident::new(&format!("register_{}", scalar_ident), scalar_ident.span());
        quote! {
            {
                let result = unsafe { #scalar_ident::#register_fn(api)};
                if result != 0 {
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
                if result != 0 {
                    return result;
                }
            }
        }
    });

    let expanded = quote! {
        #[no_mangle]
        pub extern "C" fn register_extension(api: &::limbo_ext::ExtensionApi) -> i32 {
            let api = unsafe { &*api };
            #(#scalar_calls)*

            #(#aggregate_calls)*

            ::limbo_ext::RESULT_OK
        }
    };

    TokenStream::from(expanded)
}
