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
        match self {
            TK_ABORT => Some("ABORT"),
            TK_ACTION => Some("ACTION"),
            TK_ADD => Some("ADD"),
            TK_AFTER => Some("AFTER"),
            TK_ALL => Some("ALL"),
            TK_ALTER => Some("ALTER"),
            TK_ANALYZE => Some("ANALYZE"),
            TK_ALWAYS => Some("ALWAYS"),
            TK_AND => Some("AND"),
            TK_AS => Some("AS"),
            TK_ASC => Some("ASC"),
            TK_ATTACH => Some("ATTACH"),
            TK_AUTOINCR => Some("AUTOINCREMENT"),
            TK_BEFORE => Some("BEFORE"),
            TK_BEGIN => Some("BEGIN"),
            TK_BETWEEN => Some("BETWEEN"),
            TK_BY => Some("BY"),
            TK_CASCADE => Some("CASCADE"),
            TK_CASE => Some("CASE"),
            TK_CAST => Some("CAST"),
            TK_CHECK => Some("CHECK"),
            TK_COLLATE => Some("COLLATE"),
            TK_COLUMNKW => Some("COLUMN"),
            TK_COMMIT => Some("COMMIT"),
            TK_CONFLICT => Some("CONFLICT"),
            TK_CONSTRAINT => Some("CONSTRAINT"),
            TK_CREATE => Some("CREATE"),
            TK_CURRENT => Some("CURRENT"),
            TK_DATABASE => Some("DATABASE"),
            TK_DEFAULT => Some("DEFAULT"),
            TK_DEFERRABLE => Some("DEFERRABLE"),
            TK_DEFERRED => Some("DEFERRED"),
            TK_DELETE => Some("DELETE"),
            TK_DESC => Some("DESC"),
            TK_DETACH => Some("DETACH"),
            TK_DISTINCT => Some("DISTINCT"),
            TK_DO => Some("DO"),
            TK_DROP => Some("DROP"),
            TK_EACH => Some("EACH"),
            TK_ELSE => Some("ELSE"),
            TK_END => Some("END"),
            TK_ESCAPE => Some("ESCAPE"),
            TK_EXCEPT => Some("EXCEPT"),
            TK_EXCLUDE => Some("EXCLUDE"),
            TK_EXCLUSIVE => Some("EXCLUSIVE"),
            TK_EXISTS => Some("EXISTS"),
            TK_EXPLAIN => Some("EXPLAIN"),
            TK_FAIL => Some("FAIL"),
            TK_FILTER => Some("FILTER"),
            TK_FIRST => Some("FIRST"),
            TK_FOLLOWING => Some("FOLLOWING"),
            TK_FOR => Some("FOR"),
            TK_FOREIGN => Some("FOREIGN"),
            TK_FROM => Some("FROM"),
            TK_GENERATED => Some("GENERATED"),
            TK_GROUP => Some("GROUP"),
            TK_GROUPS => Some("GROUPS"),
            TK_HAVING => Some("HAVING"),
            TK_IF => Some("IF"),
            TK_IGNORE => Some("IGNORE"),
            TK_IMMEDIATE => Some("IMMEDIATE"),
            TK_IN => Some("IN"),
            TK_INDEX => Some("INDEX"),
            TK_INDEXED => Some("INDEXED"),
            TK_INITIALLY => Some("INITIALLY"),
            TK_INSERT => Some("INSERT"),
            TK_INSTEAD => Some("INSTEAD"),
            TK_INTERSECT => Some("INTERSECT"),
            TK_INTO => Some("INTO"),
            TK_IS => Some("IS"),
            TK_ISNULL => Some("ISNULL"),
            TK_JOIN => Some("JOIN"),
            TK_KEY => Some("KEY"),
            TK_LAST => Some("LAST"),
            TK_LIMIT => Some("LIMIT"),
            TK_MATCH => Some("MATCH"),
            TK_MATERIALIZED => Some("MATERIALIZED"),
            TK_NO => Some("NO"),
            TK_NOT => Some("NOT"),
            TK_NOTHING => Some("NOTHING"),
            TK_NOTNULL => Some("NOTNULL"),
            TK_NULL => Some("NULL"),
            TK_NULLS => Some("NULLS"),
            TK_OF => Some("OF"),
            TK_OFFSET => Some("OFFSET"),
            TK_ON => Some("ON"),
            TK_OR => Some("OR"),
            TK_ORDER => Some("ORDER"),
            TK_OTHERS => Some("OTHERS"),
            TK_OVER => Some("OVER"),
            TK_PARTITION => Some("PARTITION"),
            TK_PLAN => Some("PLAN"),
            TK_PRAGMA => Some("PRAGMA"),
            TK_PRECEDING => Some("PRECEDING"),
            TK_PRIMARY => Some("PRIMARY"),
            TK_QUERY => Some("QUERY"),
            TK_RAISE => Some("RAISE"),
            TK_RANGE => Some("RANGE"),
            TK_RECURSIVE => Some("RECURSIVE"),
            TK_REFERENCES => Some("REFERENCES"),
            TK_REINDEX => Some("REINDEX"),
            TK_RELEASE => Some("RELEASE"),
            TK_RENAME => Some("RENAME"),
            TK_REPLACE => Some("REPLACE"),
            TK_RETURNING => Some("RETURNING"),
            TK_RESTRICT => Some("RESTRICT"),
            TK_ROLLBACK => Some("ROLLBACK"),
            TK_ROW => Some("ROW"),
            TK_ROWS => Some("ROWS"),
            TK_SAVEPOINT => Some("SAVEPOINT"),
            TK_SELECT => Some("SELECT"),
            TK_SET => Some("SET"),
            TK_TABLE => Some("TABLE"),
            TK_TEMP => Some("TEMP"), // or TEMPORARY
            TK_TIES => Some("TIES"),
            TK_THEN => Some("THEN"),
            TK_TO => Some("TO"),
            TK_TRANSACTION => Some("TRANSACTION"),
            TK_TRIGGER => Some("TRIGGER"),
            TK_UNBOUNDED => Some("UNBOUNDED"),
            TK_UNION => Some("UNION"),
            TK_UNIQUE => Some("UNIQUE"),
            TK_UPDATE => Some("UPDATE"),
            TK_USING => Some("USING"),
            TK_VACUUM => Some("VACUUM"),
            TK_VALUES => Some("VALUES"),
            TK_VIEW => Some("VIEW"),
            TK_VIRTUAL => Some("VIRTUAL"),
            TK_WHEN => Some("WHEN"),
            TK_WHERE => Some("WHERE"),
            TK_WINDOW => Some("WINDOW"),
            TK_WITH => Some("WITH"),
            TK_WITHOUT => Some("WITHOUT"),
            TK_BITAND => Some("&"),
            TK_BITNOT => Some("~"),
            TK_BITOR => Some("|"),
            TK_COMMA => Some(","),
            TK_CONCAT => Some("||"),
            TK_DOT => Some("."),
            TK_EQ => Some("="), // or ==
            TK_GT => Some(">"),
            TK_GE => Some(">="),
            TK_LP => Some("("),
            TK_LSHIFT => Some("<<"),
            TK_LE => Some("<="),
            TK_LT => Some("<"),
            TK_MINUS => Some("-"),
            TK_NE => Some("<>"), // or !=
            TK_PLUS => Some("+"),
            TK_REM => Some("%"),
            TK_RP => Some(")"),
            TK_RSHIFT => Some(">>"),
            TK_SEMI => Some(";"),
            TK_SLASH => Some("/"),
            TK_STAR => Some("*"),
            _ => None,
        }
    }
}
