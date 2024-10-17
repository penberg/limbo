use super::ast::Operator;
use super::tokenizer::SqlToken;

pub(crate) fn peek_operator(input: &[SqlToken]) -> Option<(Operator, u8)> {
    match input.get(0) {
        Some(SqlToken::Eq) => Some((Operator::Eq, 2)),
        Some(SqlToken::Neq) => Some((Operator::NotEq, 2)),
        Some(SqlToken::Lt) => Some((Operator::Lt, 2)),
        Some(SqlToken::Le) => Some((Operator::LtEq, 2)),
        Some(SqlToken::Gt) => Some((Operator::Gt, 2)),
        Some(SqlToken::Ge) => Some((Operator::GtEq, 2)),
        Some(SqlToken::And) => Some((Operator::And, 1)),
        Some(SqlToken::Or) => Some((Operator::Or, 0)),
        Some(SqlToken::Like) => Some((Operator::Like, 0)),
        Some(SqlToken::Plus) => Some((Operator::Plus, 3)),
        Some(SqlToken::Minus) => Some((Operator::Minus, 3)),
        Some(SqlToken::Asterisk) => Some((Operator::Multiply, 4)),
        Some(SqlToken::Slash) => Some((Operator::Divide, 4)),
        _ => None,
    }
}
