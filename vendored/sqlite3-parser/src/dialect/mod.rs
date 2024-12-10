//! SQLite dialect

use std::fmt::Formatter;
use std::str;
use uncased::UncasedStr;

mod token;
pub use token::TokenType;

/// Token value (lexeme)
#[derive(Clone, Copy)]
pub struct Token<'i>(pub usize, pub &'i [u8], pub usize);

pub(crate) fn sentinel(start: usize) -> Token<'static> {
    Token(start, b"", start)
}

impl Token<'_> {
    /// Access token value
    pub fn unwrap(self) -> String {
        from_bytes(self.1)
    }
}

impl std::fmt::Debug for Token<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Token").field(&self.1).finish()
    }
}

impl TokenType {
    // TODO try Cow<&'static, str> (Borrowed<&'static str> for keyword and Owned<String> for below),
    // => Syntax error on keyword will be better
    // => `from_token` will become unnecessary
    pub(crate) fn to_token(self, start: usize, value: &[u8], end: usize) -> Token<'_> {
        Token(start, value, end)
    }
}

pub(crate) fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
pub(crate) const MAX_KEYWORD_LEN: usize = 17;

/// Check if `word` is a keyword
pub fn keyword_token(word: &[u8]) -> Option<TokenType> {
    KEYWORDS
        .get(UncasedStr::new(unsafe { str::from_utf8_unchecked(word) }))
        .copied()
}

pub(crate) fn is_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = name.as_bytes();
    is_identifier_start(bytes[0])
        && (bytes.len() == 1 || bytes[1..].iter().all(|b| is_identifier_continue(*b)))
}

pub(crate) fn is_identifier_start(b: u8) -> bool {
    b.is_ascii_uppercase() || b == b'_' || b.is_ascii_lowercase() || b > b'\x7F'
}

pub(crate) fn is_identifier_continue(b: u8) -> bool {
    b == b'$'
        || b.is_ascii_digit()
        || b.is_ascii_uppercase()
        || b == b'_'
        || b.is_ascii_lowercase()
        || b > b'\x7F'
}

// keyword may become an identifier
// see %fallback in parse.y
pub(crate) fn from_token(_ty: u16, value: Token) -> String {
    from_bytes(value.1)
}

impl TokenType {
    /// Return the associated string (mainly for testing)
    pub const fn as_str(&self) -> Option<&'static str> {
        use TokenType::*;
        Some(match self {
            TK_ABORT => "ABORT",
            TK_ACTION => "ACTION",
            TK_ADD => "ADD",
            TK_AFTER => "AFTER",
            TK_ALL => "ALL",
            TK_ALTER => "ALTER",
            TK_ANALYZE => "ANALYZE",
            TK_ALWAYS => "ALWAYS",
            TK_AND => "AND",
            TK_AS => "AS",
            TK_ASC => "ASC",
            TK_ATTACH => "ATTACH",
            TK_AUTOINCR => "AUTOINCREMENT",
            TK_BEFORE => "BEFORE",
            TK_BEGIN => "BEGIN",
            TK_BETWEEN => "BETWEEN",
            TK_BY => "BY",
            TK_CASCADE => "CASCADE",
            TK_CASE => "CASE",
            TK_CAST => "CAST",
            TK_CHECK => "CHECK",
            TK_COLLATE => "COLLATE",
            TK_COLUMNKW => "COLUMN",
            TK_COMMIT => "COMMIT",
            TK_CONFLICT => "CONFLICT",
            TK_CONSTRAINT => "CONSTRAINT",
            TK_CREATE => "CREATE",
            TK_CURRENT => "CURRENT",
            TK_DATABASE => "DATABASE",
            TK_DEFAULT => "DEFAULT",
            TK_DEFERRABLE => "DEFERRABLE",
            TK_DEFERRED => "DEFERRED",
            TK_DELETE => "DELETE",
            TK_DESC => "DESC",
            TK_DETACH => "DETACH",
            TK_DISTINCT => "DISTINCT",
            TK_DO => "DO",
            TK_DROP => "DROP",
            TK_EACH => "EACH",
            TK_ELSE => "ELSE",
            TK_END => "END",
            TK_ESCAPE => "ESCAPE",
            TK_EXCEPT => "EXCEPT",
            TK_EXCLUDE => "EXCLUDE",
            TK_EXCLUSIVE => "EXCLUSIVE",
            TK_EXISTS => "EXISTS",
            TK_EXPLAIN => "EXPLAIN",
            TK_FAIL => "FAIL",
            TK_FILTER => "FILTER",
            TK_FIRST => "FIRST",
            TK_FOLLOWING => "FOLLOWING",
            TK_FOR => "FOR",
            TK_FOREIGN => "FOREIGN",
            TK_FROM => "FROM",
            TK_GENERATED => "GENERATED",
            TK_GROUP => "GROUP",
            TK_GROUPS => "GROUPS",
            TK_HAVING => "HAVING",
            TK_IF => "IF",
            TK_IGNORE => "IGNORE",
            TK_IMMEDIATE => "IMMEDIATE",
            TK_IN => "IN",
            TK_INDEX => "INDEX",
            TK_INDEXED => "INDEXED",
            TK_INITIALLY => "INITIALLY",
            TK_INSERT => "INSERT",
            TK_INSTEAD => "INSTEAD",
            TK_INTERSECT => "INTERSECT",
            TK_INTO => "INTO",
            TK_IS => "IS",
            TK_ISNULL => "ISNULL",
            TK_JOIN => "JOIN",
            TK_KEY => "KEY",
            TK_LAST => "LAST",
            TK_LIMIT => "LIMIT",
            TK_MATCH => "MATCH",
            TK_MATERIALIZED => "MATERIALIZED",
            TK_NO => "NO",
            TK_NOT => "NOT",
            TK_NOTHING => "NOTHING",
            TK_NOTNULL => "NOTNULL",
            TK_NULL => "NULL",
            TK_NULLS => "NULLS",
            TK_OF => "OF",
            TK_OFFSET => "OFFSET",
            TK_ON => "ON",
            TK_OR => "OR",
            TK_ORDER => "ORDER",
            TK_OTHERS => "OTHERS",
            TK_OVER => "OVER",
            TK_PARTITION => "PARTITION",
            TK_PLAN => "PLAN",
            TK_PRAGMA => "PRAGMA",
            TK_PRECEDING => "PRECEDING",
            TK_PRIMARY => "PRIMARY",
            TK_QUERY => "QUERY",
            TK_RAISE => "RAISE",
            TK_RANGE => "RANGE",
            TK_RECURSIVE => "RECURSIVE",
            TK_REFERENCES => "REFERENCES",
            TK_REINDEX => "REINDEX",
            TK_RELEASE => "RELEASE",
            TK_RENAME => "RENAME",
            TK_REPLACE => "REPLACE",
            TK_RETURNING => "RETURNING",
            TK_RESTRICT => "RESTRICT",
            TK_ROLLBACK => "ROLLBACK",
            TK_ROW => "ROW",
            TK_ROWS => "ROWS",
            TK_SAVEPOINT => "SAVEPOINT",
            TK_SELECT => "SELECT",
            TK_SET => "SET",
            TK_TABLE => "TABLE",
            TK_TEMP => "TEMP", // or TEMPORARY
            TK_TIES => "TIES",
            TK_THEN => "THEN",
            TK_TO => "TO",
            TK_TRANSACTION => "TRANSACTION",
            TK_TRIGGER => "TRIGGER",
            TK_UNBOUNDED => "UNBOUNDED",
            TK_UNION => "UNION",
            TK_UNIQUE => "UNIQUE",
            TK_UPDATE => "UPDATE",
            TK_USING => "USING",
            TK_VACUUM => "VACUUM",
            TK_VALUES => "VALUES",
            TK_VIEW => "VIEW",
            TK_VIRTUAL => "VIRTUAL",
            TK_WHEN => "WHEN",
            TK_WHERE => "WHERE",
            TK_WINDOW => "WINDOW",
            TK_WITH => "WITH",
            TK_WITHOUT => "WITHOUT",
            TK_BITAND => "&",
            TK_BITNOT => "~",
            TK_BITOR => "|",
            TK_COMMA => ",",
            TK_CONCAT => "||",
            TK_DOT => ".",
            TK_EQ => "=", // or ==
            TK_GT => ">",
            TK_GE => ">=",
            TK_LP => "(",
            TK_LSHIFT => "<<",
            TK_LE => "<=",
            TK_LT => "<",
            TK_MINUS => "-",
            TK_NE => "<>", // or !=
            TK_PLUS => "+",
            TK_REM => "%",
            TK_RP => "",
            TK_RSHIFT => ">>",
            TK_SEMI => ";",
            TK_SLASH => "/",
            TK_STAR => "*",
            _ => return None,
        })
    }
}
