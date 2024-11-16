use fallible_iterator::FallibleIterator;
use std::env;
use std::fs::read;
use std::panic;

#[cfg(not(feature = "YYNOERRORRECOVERY"))]
use sqlite3_parser::lexer::sql::Error;
use sqlite3_parser::lexer::sql::Parser;

/// Parse specified files and print all commands.
fn main() {
    env_logger::init();
    let args = env::args();
    for arg in args.skip(1) {
        println!("{arg}");
        let result = panic::catch_unwind(|| {
            let input = read(arg.clone()).unwrap();
            let mut parser = Parser::new(input.as_ref());
            loop {
                match parser.next() {
                    Ok(None) => break,
                    Err(err) => {
                        eprintln!("Err: {err} in {arg}");
                        #[cfg(feature = "YYNOERRORRECOVERY")]
                        break;
                        #[cfg(not(feature = "YYNOERRORRECOVERY"))]
                        if let Error::ParserError(..) = err {
                        } else {
                            break;
                        }
                    }
                    Ok(Some(cmd)) => {
                        println!("{cmd}");
                    }
                }
            }
        });
        if let Err(e) = result {
            eprintln!("Panic: {e:?} in {arg}");
        }
    }
}
