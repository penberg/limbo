use sqlite3_parser::lexer::sql::{TokenType, Tokenizer};
use sqlite3_parser::lexer::Scanner;

use std::env;
use std::fs::read;
use std::str;

/// Tokenize specified files (and do some checks)
fn main() {
    use TokenType::*;
    let args = env::args();
    for arg in args.skip(1) {
        let input = read(arg.clone()).unwrap();
        let tokenizer = Tokenizer::new();
        let mut s = Scanner::new(tokenizer);
        loop {
            match s.scan(&input) {
                Ok((_, None, _)) => break,
                Err(err) => {
                    //eprintln!("{} at line: {}, column: {}", err, s.line(), s.column());
                    eprintln!("Err: {err} in {arg}");
                    break;
                }
                Ok((_, Some((token, token_type)), _)) => match token_type {
                    TK_TEMP => debug_assert!(
                        b"TEMP".eq_ignore_ascii_case(token)
                            || b"TEMPORARY".eq_ignore_ascii_case(token)
                    ),
                    TK_EQ => debug_assert!(b"=" == token || b"==" == token),
                    TK_NE => debug_assert!(b"<>" == token || b"!=" == token),
                    //TK_STRING => debug_assert!(),
                    //TK_ID => debug_assert!(),
                    //TK_VARIABLE => debug_assert!(),
                    TK_BLOB => debug_assert!(
                        token.len() % 2 == 0 && token.iter().all(u8::is_ascii_hexdigit)
                    ),
                    TK_INTEGER => {
                        if token.len() > 2
                            && token[0] == b'0'
                            && (token[1] == b'x' || token[1] == b'X')
                        {
                            if let Err(err) =
                                i64::from_str_radix(str::from_utf8(&token[2..]).unwrap(), 16)
                            {
                                eprintln!("Err: {err} in {arg}");
                            }
                        } else {
                            /*let raw = str::from_utf8(token).unwrap();
                            let res = raw.parse::<i64>();
                            if res.is_err() {
                                eprintln!("Err: {} in {}", res.unwrap_err(), arg);
                            }*/
                            debug_assert!(token.iter().all(u8::is_ascii_digit))
                        }
                    }
                    TK_FLOAT => {
                        debug_assert!(str::from_utf8(token).unwrap().parse::<f64>().is_ok())
                    }
                    TK_CTIME_KW => debug_assert!(
                        b"CURRENT_DATE".eq_ignore_ascii_case(token)
                            || b"CURRENT_TIME".eq_ignore_ascii_case(token)
                            || b"CURRENT_TIMESTAMP".eq_ignore_ascii_case(token)
                    ),
                    TK_JOIN_KW => debug_assert!(
                        b"CROSS".eq_ignore_ascii_case(token)
                            || b"FULL".eq_ignore_ascii_case(token)
                            || b"INNER".eq_ignore_ascii_case(token)
                            || b"LEFT".eq_ignore_ascii_case(token)
                            || b"NATURAL".eq_ignore_ascii_case(token)
                            || b"OUTER".eq_ignore_ascii_case(token)
                            || b"RIGHT".eq_ignore_ascii_case(token)
                    ),
                    TK_LIKE_KW => debug_assert!(
                        b"GLOB".eq_ignore_ascii_case(token)
                            || b"LIKE".eq_ignore_ascii_case(token)
                            || b"REGEXP".eq_ignore_ascii_case(token)
                    ),
                    _ => match token_type.as_str() {
                        Some(str) => {
                            debug_assert!(str.eq_ignore_ascii_case(str::from_utf8(token).unwrap()))
                        }
                        _ => {
                            println!("'{}', {:?}", str::from_utf8(token).unwrap(), token_type);
                        }
                    },
                },
            }
        }
    }
}
