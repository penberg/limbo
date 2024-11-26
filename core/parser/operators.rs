use super::ast::Operator;
use super::tokenizer::{SqlTokenKind, SqlTokenStream};

pub(crate) fn peek_operator(input: &SqlTokenStream) -> Option<(Operator, u8)> {
    match input.peek_kind(0) {
        Some(SqlTokenKind::Eq) => Some((Operator::Eq, 30)),
        Some(SqlTokenKind::Neq) => Some((Operator::NotEq, 30)),
        Some(SqlTokenKind::Lt) => Some((Operator::Lt, 30)),
        Some(SqlTokenKind::Le) => Some((Operator::LtEq, 30)),
        Some(SqlTokenKind::Gt) => Some((Operator::Gt, 30)),
        Some(SqlTokenKind::Ge) => Some((Operator::GtEq, 30)),
        Some(SqlTokenKind::Not) => Some((Operator::Not, 25)),
        Some(SqlTokenKind::And) => Some((Operator::And, 20)),
        Some(SqlTokenKind::Or) => Some((Operator::Or, 10)),
        Some(SqlTokenKind::NotLike) => Some((Operator::NotLike, 10)),
        Some(SqlTokenKind::Like) => Some((Operator::Like, 10)),
        Some(SqlTokenKind::In) => Some((Operator::In, 10)),
        Some(SqlTokenKind::NotIn) => Some((Operator::NotIn, 10)),
        Some(SqlTokenKind::Between) => Some((Operator::Between, 10)),
        Some(SqlTokenKind::Glob) => Some((Operator::Glob, 10)),
        Some(SqlTokenKind::Plus) => Some((Operator::Plus, 40)),
        Some(SqlTokenKind::Minus) => Some((Operator::Minus, 40)),
        Some(SqlTokenKind::Asterisk) => Some((Operator::Multiply, 50)),
        Some(SqlTokenKind::Slash) => Some((Operator::Divide, 50)),
        _ => None,
    }
}
