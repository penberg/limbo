use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::token::Eq;
use syn::{Ident, LitStr, Token};

pub(crate) struct RegisterExtensionInput {
    pub aggregates: Vec<Ident>,
    pub scalars: Vec<Ident>,
}

impl syn::parse::Parse for RegisterExtensionInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut aggregates = Vec::new();
        let mut scalars = Vec::new();

        while !input.is_empty() {
            if input.peek(syn::Ident) && input.peek2(Token![:]) {
                let section_name: Ident = input.parse()?;
                input.parse::<Token![:]>()?;

                if section_name == "aggregates" || section_name == "scalars" {
                    let content;
                    syn::braced!(content in input);

                    let parsed_items = Punctuated::<Ident, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();

                    if section_name == "aggregates" {
                        aggregates = parsed_items;
                    } else {
                        scalars = parsed_items;
                    }

                    if input.peek(Token![,]) {
                        input.parse::<Token![,]>()?;
                    }
                } else {
                    return Err(syn::Error::new(section_name.span(), "Unknown section"));
                }
            } else {
                return Err(input.error("Expected aggregates: or scalars: section"));
            }
        }

        Ok(Self {
            aggregates,
            scalars,
        })
    }
}

pub(crate) struct ScalarInfo {
    pub name: String,
    pub alias: Option<String>,
}

impl ScalarInfo {
    pub fn new(name: String, alias: Option<String>) -> Self {
        Self { name, alias }
    }
}

impl syn::parse::Parse for ScalarInfo {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        let mut name = None;
        let mut alias = None;
        while !input.is_empty() {
            if let Ok(ident) = input.parse::<Ident>() {
                if ident.to_string().as_str() == "name" {
                    let _ = input.parse::<Eq>();
                    name = Some(input.parse::<LitStr>()?);
                } else if ident.to_string().as_str() == "alias" {
                    let _ = input.parse::<Eq>();
                    alias = Some(input.parse::<LitStr>()?);
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        let Some(name) = name else {
            return Err(input.error("Expected name"));
        };
        Ok(Self::new(name.value(), alias.map(|i| i.value())))
    }
}
