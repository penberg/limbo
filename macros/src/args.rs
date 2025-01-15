use syn::parse::{Parse, ParseStream, Result as ParseResult};
use syn::{LitInt, Token};
#[derive(Debug)]
pub enum ArgsSpec {
    Exact(i32),
    Range {
        lower: i32,
        upper: i32,
        inclusive: bool,
    },
}

pub struct ArgsAttr {
    pub spec: ArgsSpec,
}

impl Parse for ArgsAttr {
    fn parse(input: ParseStream) -> ParseResult<Self> {
        if input.peek(LitInt) {
            let start_lit = input.parse::<LitInt>()?;
            let start_val = start_lit.base10_parse::<i32>()?;

            if input.is_empty() {
                return Ok(ArgsAttr {
                    spec: ArgsSpec::Exact(start_val),
                });
            }
            if input.peek(Token![..=]) {
                let _dots = input.parse::<Token![..=]>()?;
                let end_lit = input.parse::<LitInt>()?;
                let end_val = end_lit.base10_parse::<i32>()?;
                Ok(ArgsAttr {
                    spec: ArgsSpec::Range {
                        lower: start_val,
                        upper: end_val,
                        inclusive: true,
                    },
                })
            } else if input.peek(Token![..]) {
                let _dots = input.parse::<Token![..]>()?;
                let end_lit = input.parse::<LitInt>()?;
                let end_val = end_lit.base10_parse::<i32>()?;
                Ok(ArgsAttr {
                    spec: ArgsSpec::Range {
                        lower: start_val,
                        upper: end_val,
                        inclusive: false,
                    },
                })
            } else {
                Err(syn::Error::new_spanned(
                    start_lit,
                    "Expected '..' or '..=' for a range, or nothing for a single integer.",
                ))
            }
        } else {
            Err(syn::Error::new(
                input.span(),
                "Expected an integer or a range expression, like `0`, `0..2`, or `0..=2`.",
            ))
        }
    }
}
