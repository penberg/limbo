%token_type { i32 }

// An extra argument to the constructor for the parser, which is available
// to all actions.
%extra_context {ctx: Context}

%left PLUS MINUS.
%left DIVIDE TIMES.

%include {

use log::{debug, error, log_enabled, Level, LevelFilter, Metadata, Record, SetLoggerError};

pub struct Context {
    expr: Option<Expr>,
}

#[derive(Debug)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug)]
pub enum Expr {
    Number(i32),
    Binary(Operator, Box<Expr>, Box<Expr>),
}
impl Expr {
    fn binary(op: Operator, lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary(op, Box::new(lhs), Box::new(rhs))
    }
}

fn main() {
    init_logger().unwrap();

    let r = Context { expr: None };
    let mut p = yyParser::new(r);
    p.Parse(TokenType::INTEGER, Some(5));
    p.Parse(TokenType::PLUS, None);
    p.Parse(TokenType::INTEGER, Some(10));
    p.Parse(TokenType::TIMES, None);
    p.Parse(TokenType::INTEGER, Some(4));
    p.Parse(TokenType::EOF, None);
    p.ParseFinalize();
    let s = format!("{:?}", p.ctx.expr);
    assert_eq!(s, "Some(Binary(Add, Number(5), Binary(Multiply, Number(10), Number(4))))");

    let r = Context { expr: None };
    let mut p = yyParser::new(r);
    p.Parse(TokenType::INTEGER, Some(15));
    p.Parse(TokenType::DIVIDE, None);
    p.Parse(TokenType::INTEGER, Some(5));
    p.Parse(TokenType::EOF, None);
    p.ParseFinalize();
    let s = format!("{:?}", p.ctx.expr);
    assert_eq!(s, "Some(Binary(Divide, Number(15), Number(5)))");

    let r = Context { expr: None };
    let mut p = yyParser::new(r);
    p.Parse(TokenType::INTEGER, Some(50));
    p.Parse(TokenType::PLUS, None);
    p.Parse(TokenType::INTEGER, Some(125));
    p.Parse(TokenType::EOF, None);
    p.ParseFinalize();
    let s = format!("{:?}", p.ctx.expr);
    assert_eq!(s, "Some(Binary(Add, Number(50), Number(125)))");

    let r = Context { expr: None };
    let mut p = yyParser::new(r);
    p.Parse(TokenType::INTEGER, Some(50));
    p.Parse(TokenType::TIMES, None);
    p.Parse(TokenType::INTEGER, Some(125));
    p.Parse(TokenType::PLUS, None);
    p.Parse(TokenType::INTEGER, Some(125));
    p.Parse(TokenType::EOF, None);
    p.ParseFinalize();
    let s = format!("{:?}", p.ctx.expr);
    assert_eq!(s, "Some(Binary(Add, Binary(Multiply, Number(50), Number(125)), Number(125)))");
}

static LOGGER: Logger = Logger;
struct Logger;

impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Debug
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {
    }
}

fn init_logger() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER)?;
    log::set_max_level(LevelFilter::Debug);
    Ok(())
}
}

%syntax_error {
    let _ = yymajor;
    println!("near token {:?}: syntax error", yyminor);
}

program ::= expr(A). { self.ctx.expr = Some(A); }

%type expr { Expr }
expr(A) ::= expr(B) MINUS expr(C). { A = Expr::binary(Operator::Subtract, B, C); }
expr(A) ::= expr(B) PLUS expr(C). { A = Expr::binary(Operator::Add, B, C); }
expr(A) ::= expr(B) TIMES expr(C). { A = Expr::binary(Operator::Multiply, B, C); }
expr(A) ::= expr(B) DIVIDE expr(C). { A = Expr::binary(Operator::Divide, B, C); }

expr(A) ::= INTEGER(B). { A = Expr::Number(B.unwrap()); }
