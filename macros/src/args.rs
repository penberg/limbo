use syn::punctuated::Punctuated;
use syn::{Ident, Token};

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
                let content;
                syn::braced!(content in input);

                if section_name == "aggregates" {
                    aggregates = Punctuated::<Ident, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
                } else if section_name == "scalars" {
                    scalars = Punctuated::<Ident, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
                } else {
                    return Err(syn::Error::new(section_name.span(), "Unknown section"));
                }
            } else {
                return Err(input.error("Expected aggregates: or scalars: section"));
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self {
            aggregates,
            scalars,
        })
    }
}
