use crate::ast::{
    check::ColumnCount, AlterTable, AlterTableBody, As, Cmd, ColumnConstraint, ColumnDefinition,
    CommonTableExpr, CompoundOperator, CompoundSelect, CreateTableBody, CreateTypeBody,
    CreateVirtualTable, DeferSubclause, Distinctness, DomainConstraint, EqpFormat, Expr,
    ForeignKeyClause, FrameBound, FrameClause, FrameExclude, FrameMode, FromClause, FunctionTail,
    GeneratedColumnType, GroupBy, Indexed, IndexedColumn, InitDeferredPred, InsertBody,
    JoinConstraint, JoinOperator, JoinType, JoinedSelectTable, LikeOperator, Limit, Literal,
    Materialized, Name, NamedColumnConstraint, NamedTableConstraint, NullsOrder, OneSelect,
    Operator, Over, PragmaBody, PragmaValue, QualifiedName, RefAct, RefArg, ResolveType,
    ResultColumn, Select, SelectBody, SelectTable, Set, SortOrder, SortedColumn, Stmt,
    TableConstraint, TableOptions, TransactionType, TriggerCmd, TriggerEvent, TriggerTime, Type,
    TypeField, TypeOperator, TypeParam, TypeSize, UnaryOperator, Update, Upsert, UpsertDo,
    UpsertIndex, Variable, Window, WindowDef, With,
};
use crate::error::Error;
use crate::lexer::{Lexer, Token};
use crate::token::TokenType::{self, *};
use crate::Result;
use std::collections::HashMap;
use std::num::NonZeroU32;
use turso_macros::match_ignore_ascii_case;

macro_rules! peek_expect {
    ( $parser:expr, $( $x:ident ),* $(,)?) => {
        {
            let token = $parser.peek_no_eof()?;
            match token.token_type  {
                $($x => token,)*
                tt => {
                    let token_len = token.value.len();
                    // handle fallback TK_ID
                    match (TK_ID, tt.fallback_id_if_ok()) {
                        $(($x, TK_ID) => token,)*
                        _ => {
                            let token_text = token.to_utf8();
                            let offset = $parser.offset();
                            return Err(Error::ParseUnexpectedToken {
                                parsed_offset: ($parser.offset(), token_len).into(),
                                expected: &[
                                    $($x,)*
                                ],
                                got: tt,
                                token_text: token_text.clone(),
                                offset,
                                expected_display: crate::token::TokenType::format_expected_tokens(&[$($x,)*]),
                            })
                        }
                    }
                }
            }
        }
    };
}

macro_rules! eat_assert {
    ( $parser:expr, $( $x:ident ),* $(,)?) => {
        {
            let token = $parser.eat().unwrap().unwrap();

            #[cfg(debug_assertions)]
            match token.token_type {
                $($x => token,)*
                tt => {
                    // handle fallback TK_ID
                    match (TK_ID, tt.fallback_id_if_ok()) {
                        $(($x, TK_ID) => token,)*
                        _ => {
                            panic!(
                                "Expected token {:?}, got {:?}",
                                &[ $($x,)* ],
                                tt
                            );
                        }
                    }
                }
            }

            #[cfg(not(debug_assertions))]
            token // in release mode, we assume the caller has checked the token type
        }
    };
}

macro_rules! eat_expect {
    ( $parser:expr, $( $x:ident ),* $(,)?) => {
        {
            peek_expect!($parser, $( $x ),*);
            eat_assert!($parser, $( $x ),*)
        }
    };
}

#[inline(always)]
fn from_bytes_as_str(bytes: &[u8]) -> &str {
    unsafe { str::from_utf8_unchecked(bytes) }
}

#[inline(always)]
fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

#[inline]
fn join_type_from_bytes(s: &[u8]) -> Result<JoinType> {
    match_ignore_ascii_case!(match s {
        b"CROSS" => Ok(JoinType::INNER | JoinType::CROSS),
        b"FULL" => Ok(JoinType::LEFT | JoinType::RIGHT | JoinType::OUTER),
        b"INNER" => Ok(JoinType::INNER),
        b"LEFT" => Ok(JoinType::LEFT | JoinType::OUTER),
        b"NATURAL" => Ok(JoinType::NATURAL),
        b"RIGHT" => Ok(JoinType::RIGHT | JoinType::OUTER),
        b"OUTER" => Ok(JoinType::OUTER),
        _ => Err(Error::Custom(format!(
            "unsupported JOIN type: {:?}",
            str::from_utf8(s)
        ))),
    })
}

#[inline]
fn new_join_type(n0: &[u8], n1: Option<&[u8]>, n2: Option<&[u8]>) -> Result<JoinType> {
    let mut jt = join_type_from_bytes(n0)?;

    if let Some(n1) = n1 {
        jt |= join_type_from_bytes(n1)?;
    }

    if let Some(n2) = n2 {
        jt |= join_type_from_bytes(n2)?;
    }

    if (jt & (JoinType::INNER | JoinType::OUTER)) == (JoinType::INNER | JoinType::OUTER)
        || (jt & (JoinType::OUTER | JoinType::LEFT | JoinType::RIGHT)) == JoinType::OUTER
    {
        return Err(Error::Custom(format!(
            "unsupported JOIN type: {:?} {:?} {:?}",
            from_bytes_as_str(n0),
            from_bytes_as_str(n1.unwrap_or(&[])),
            from_bytes_as_str(n2.unwrap_or(&[])),
        )));
    }

    Ok(jt)
}

/// True if `e` is a bare subquery, possibly wrapped in one or more layers of
/// single-element parentheses (e.g. `(SELECT ...)`, `((SELECT ...))`).
fn is_bare_subquery(e: &Expr) -> bool {
    match e {
        Expr::Subquery(_) => true,
        Expr::Parenthesized(inner) => inner.len() == 1 && is_bare_subquery(&inner[0]),
        _ => false,
    }
}

/// Unwrap a bare subquery previously confirmed by [`is_bare_subquery`].
fn into_bare_subquery(e: Box<Expr>) -> Select {
    match *e {
        Expr::Subquery(select) => select,
        Expr::Parenthesized(mut inner) => into_bare_subquery(inner.pop().expect("single element")),
        _ => unreachable!("into_bare_subquery called on a non-subquery expression"),
    }
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,

    /// The current token being processed
    current_token: Token<'a>,
    peekable: bool,

    /// Last assigned id of positional variable
    /// Parser tracks that in order to properly auto-assign variable ids in correct order for anonymous parameters '?'
    last_variable_id: u32,
    named_variables: HashMap<&'a [u8], NonZeroU32>,
    /// Tracks STRUCT/UNION nesting depth to prevent stack overflow from deeply nested types
    type_nesting_depth: u32,
    /// Current expression recursion depth of the parser, bounded by [`MAX_EXPR_DEPTH`]
    expr_nesting_depth: u32,
    /// Height of the most recently parsed expression (`1 + max(child heights)`,
    /// like SQLite's `Expr.nHeight`), bounded by [`MAX_EXPR_DEPTH`]
    last_expr_height: usize,
}

/// Maximum query expression depth, our equivalent of SQLite's
/// `SQLITE_MAX_EXPR_DEPTH` (default 1000). Kept lower because our recursive
/// translator/optimizer uses larger stack frames per nesting level, so a
/// 1000-deep tree still overflows a default 8 MiB thread stack in debug builds.
pub const MAX_EXPR_DEPTH: usize = 100;

impl<'a> Iterator for Parser<'a> {
    type Item = Result<Cmd>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.mark(|p| p.next_cmd()) {
            Ok(None) => None, // EOF
            Ok(Some(cmd)) => Some(Ok(cmd)),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Parser<'a> {
    #[inline(always)]
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            lexer: Lexer::new(input),
            peekable: false,
            current_token: Token::new(&input[..0], TokenType::TK_NONE),
            last_variable_id: 0,
            named_variables: HashMap::new(),
            type_nesting_depth: 0,
            expr_nesting_depth: 0,
            last_expr_height: 0,
        }
    }

    fn create_variable(&mut self, token: &'a [u8]) -> Result<Expr> {
        debug_assert!(!token.is_empty());
        if token == b"?" {
            // Rewrite anonymous variables in encounter order
            self.last_variable_id += 1;
            let index = NonZeroU32::new(self.last_variable_id).unwrap();
            Ok(Expr::Variable(Variable::indexed(index)))
        } else if token[0] == b'?' {
            let variable_str = std::str::from_utf8(&token[1..])
                .map_err(|e| Error::Custom(format!("non-utf8 positional variable id: {e}")))?;
            let variable_id = variable_str
                .parse::<u32>()
                .map_err(|e| Error::Custom(format!("non-integer positional variable id: {e}")))?;
            if variable_id == 0 {
                return Err(Error::Custom(
                    "variable number must be between ?1 and ?250000".to_string(),
                ));
            }
            if variable_id > 250000 {
                return Err(Error::Custom(
                    "variable number must be between ?1 and ?250000".to_string(),
                ));
            }
            self.last_variable_id = self.last_variable_id.max(variable_id);
            let index = NonZeroU32::new(variable_id).unwrap();
            // An explicit ?N is distinct from an anonymous ?: its "?N"
            // spelling is its name (sqlite3_bind_parameter_name returns it,
            // bind_parameter_index resolves it), derived from the index on
            // demand rather than allocated per marker.
            Ok(Expr::Variable(Variable::numbered(index)))
        } else {
            debug_assert!(matches!(token[0], b':' | b'@' | b'$'));
            let index = if let Some(index) = self.named_variables.get(token).copied() {
                index
            } else {
                self.last_variable_id += 1;
                let index = NonZeroU32::new(self.last_variable_id).unwrap();
                self.named_variables.insert(token, index);
                index
            };
            Ok(Expr::Variable(Variable::named(
                from_bytes_as_str(token),
                index,
            )))
        }
    }

    #[inline(always)]
    pub fn offset(&self) -> usize {
        if !self.peekable {
            // not peekable means current token already consumed
            // so just take lexer offset
            self.lexer.offset
        } else {
            self.lexer.offset - self.current_token.value.len()
        }
    }

    // entrypoint of parsing
    pub fn next_cmd(&mut self) -> Result<Option<Cmd>> {
        self.last_variable_id = 0;
        self.named_variables.clear();

        // consumes prefix SEMI
        while let Some(token) = self.peek()? {
            if token.token_type == TK_SEMI {
                eat_assert!(self, TK_SEMI);
            } else {
                break;
            }
        }

        let result = match self.peek()? {
            None => None, // EOF
            Some(token) => match token.token_type {
                TK_EXPLAIN => {
                    eat_assert!(self, TK_EXPLAIN);

                    if self.peek_no_eof()?.token_type == TK_QUERY {
                        eat_assert!(self, TK_QUERY);
                        eat_expect!(self, TK_PLAN);
                        let format = self.parse_explain_query_plan_format()?;
                        Some(Cmd::ExplainQueryPlan {
                            stmt: self.parse_stmt()?,
                            format,
                        })
                    } else {
                        Some(Cmd::Explain(self.parse_stmt()?))
                    }
                }
                _ => Some(Cmd::Stmt(self.parse_stmt()?)),
            },
        };

        let mut found_semi = false;
        loop {
            match self.peek()? {
                None => break,
                Some(token) if token.token_type == TK_SEMI => {
                    found_semi = true;
                    eat_assert!(self, TK_SEMI);
                }
                Some(token) => {
                    if !found_semi {
                        let tt = token.token_type;
                        let token_text = token.to_utf8();
                        let offset = self.offset();
                        return Err(Error::ParseUnexpectedToken {
                            parsed_offset: (offset, 1).into(),
                            expected: &[TK_SEMI],
                            got: tt,
                            token_text,
                            offset,
                            expected_display: crate::token::TokenType::format_expected_tokens(&[
                                TK_SEMI,
                            ]),
                        });
                    }

                    break;
                }
            }
        }

        Ok(result)
    }

    /// Parse the optional `FORMAT=JSON` / `FORMAT=TEXT` clause after `EXPLAIN QUERY PLAN`.
    fn parse_explain_query_plan_format(&mut self) -> Result<EqpFormat> {
        let starts_format_clause = matches!(
            self.peek()?,
            Some(token)
                if token.token_type == TK_ID && token.value.eq_ignore_ascii_case(b"FORMAT")
        );
        if !starts_format_clause {
            return Ok(EqpFormat::Text);
        }
        eat_assert!(self, TK_ID);
        eat_expect!(self, TK_EQ);
        let token = eat_expect!(self, TK_ID);
        match_ignore_ascii_case!(match token.value {
            b"JSON" => Ok(EqpFormat::Json),
            b"TEXT" => Ok(EqpFormat::Text),
            _ => Err(Error::Custom(format!(
                "unknown EXPLAIN QUERY PLAN format: {} (supported formats: TEXT, JSON)",
                String::from_utf8_lossy(token.value)
            ))),
        })
    }

    #[inline(always)]
    fn consume_lexer_without_whitespaces_or_comments(&mut self) -> Option<Result<Token<'a>>> {
        debug_assert!(!self.peekable);
        loop {
            let tok = self.lexer.next();
            if let Some(Ok(ref token)) = tok {
                if token.token_type.is_none() {
                    continue; // white space or comment
                }
            }

            return tok;
        }
    }

    fn next_token(&mut self) -> Result<Option<&Token<'a>>> {
        debug_assert!(!self.peekable);
        let mut next = self.consume_lexer_without_whitespaces_or_comments();

        fn get_token(tt: TokenType) -> TokenType {
            match tt {
                TK_ID | TK_STRING | TK_JOIN_KW | TK_UNION | TK_EXCEPT | TK_INTERSECT
                | TK_GENERATED | TK_WITHOUT | TK_COLUMNKW | TK_WINDOW | TK_FILTER | TK_OVER
                | TK_WITHIN => TK_ID,
                _ => tt.fallback_id_if_ok(),
            }
        }

        if let Some(Ok(ref mut tok)) = next {
            /*
             ** The following three functions are called immediately after the tokenizer
             ** reads the keywords WINDOW, OVER and FILTER, respectively, to determine
             ** whether the token should be treated as a keyword or an SQL identifier.
             ** This cannot be handled by the usual lemon %fallback method, due to
             ** the ambiguity in some constructions. e.g.
             **
             **   SELECT sum(x) OVER ...
             **
             ** In the above, "OVER" might be a keyword, or it might be an alias for the
             ** sum(x) expression. If a "%fallback ID OVER" directive were added to
             ** grammar, then SQLite would always treat "OVER" as an alias, making it
             ** impossible to call a window-function without a FILTER clause.
             **
             ** WINDOW is treated as a keyword if:
             **
             **   * the following token is an identifier, or a keyword that can fallback
             **     to being an identifier, and
             **   * the token after than one is TK_AS.
             **
             ** OVER is a keyword if:
             **
             **   * the previous token was TK_RP, and
             **   * the next token is either TK_LP or an identifier.
             **
             ** FILTER is a keyword if:
             **
             **   * the previous token was TK_RP, and
             **   * the next token is TK_LP.
             **
             ** UNION is a keyword if:
             **
             **   * the next token is TK_ALL|TK_SELECT|TK_VALUES.
             **
             ** EXCEPT is a keyword if:
             **
             **   * the next token is TK_SELECT|TK_VALUES.
             **
             ** INTERSECT is a keyword if:
             **
             **   * the next token is TK_SELECT|TK_VALUES.
             **
             ** COLUMNKW is a keyword if:
             **
             **   * the previous token is TK_ADD|TK_RENAME|TK_DROP.
             **
             ** GENERATED is a keyword if:
             **
             **   * the next token is TK_ALWAYS.
             **   * the token after than one is TK_AS.
             **
             ** WITHOUT is a keyword if:
             **
             **   * the previous token is TK_RP|TK_COMMA.
             **   * the next token is TK_ID.
             */
            match tok.token_type {
                TK_WINDOW => {
                    let can_be_window = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => return Ok(false),
                            Some(tok) => match get_token(tok?.token_type) {
                                TK_ID => {}
                                _ => return Ok(false),
                            },
                        }

                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type {
                                TK_AS => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_window {
                        tok.token_type = TK_ID;
                    }
                }
                TK_OVER => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_over = {
                        if prev_tt == TK_RP {
                            self.try_parse(|p| {
                                match p.consume_lexer_without_whitespaces_or_comments() {
                                    None => Ok(false),
                                    Some(tok) => match get_token(tok?.token_type) {
                                        TK_LP | TK_ID => Ok(true),
                                        _ => Ok(false),
                                    },
                                }
                            })?
                        } else {
                            false
                        }
                    };

                    if !can_be_over {
                        tok.token_type = TK_ID;
                    }
                }
                TK_FILTER => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_filter = {
                        if prev_tt == TK_RP {
                            self.try_parse(|p| {
                                match p.consume_lexer_without_whitespaces_or_comments() {
                                    None => Ok(false),
                                    Some(tok) => match tok?.token_type {
                                        TK_LP => Ok(true),
                                        _ => Ok(false),
                                    },
                                }
                            })?
                        } else {
                            false
                        }
                    };

                    if !can_be_filter {
                        tok.token_type = TK_ID;
                    }
                }
                TK_WITHIN => {
                    // WITHIN is a keyword only in `<aggregate>(...) WITHIN GROUP (...)`:
                    // the previous token must be `)` and the next token must be GROUP.
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_within = if prev_tt == TK_RP {
                        self.try_parse(|p| {
                            match p.consume_lexer_without_whitespaces_or_comments() {
                                None => Ok(false),
                                Some(tok) => match tok?.token_type {
                                    TK_GROUP => Ok(true),
                                    _ => Ok(false),
                                },
                            }
                        })?
                    } else {
                        false
                    };

                    if !can_be_within {
                        tok.token_type = TK_ID;
                    }
                }
                TK_UNION => {
                    let can_be_union = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type {
                                TK_ALL | TK_SELECT | TK_VALUES => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_union {
                        tok.token_type = TK_ID;
                    }
                }
                TK_EXCEPT | TK_INTERSECT => {
                    let can_be_except = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type {
                                TK_SELECT | TK_VALUES => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_except {
                        tok.token_type = TK_ID;
                    }
                }
                TK_COLUMNKW => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_columnkw =
                        matches!(prev_tt, TK_ADD | TK_RENAME | TK_DROP | TK_ALTER);

                    if !can_be_columnkw {
                        tok.token_type = TK_ID;
                    }
                }
                TK_GENERATED => {
                    let can_be_generated = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => return Ok(false),
                            Some(tok) => match tok?.token_type {
                                TK_ALWAYS => {}
                                _ => return Ok(false),
                            },
                        }

                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type {
                                TK_AS => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_generated {
                        tok.token_type = TK_ID;
                    }
                }
                TK_WITHOUT => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_without = match prev_tt {
                        TK_RP | TK_COMMA => self.try_parse(|p| {
                            match p.consume_lexer_without_whitespaces_or_comments() {
                                None => Ok(false),
                                Some(tok) => match get_token(tok?.token_type) {
                                    TK_ID => Ok(true),
                                    _ => Ok(false),
                                },
                            }
                        })?,
                        _ => false,
                    };

                    if !can_be_without {
                        tok.token_type = TK_ID;
                    }
                }
                _ => {}
            }
        }

        match next {
            None => Ok(None), // EOF
            Some(Ok(tok)) => {
                self.current_token = tok;
                self.peekable = true;
                Ok(Some(&self.current_token))
            }
            Some(Err(err)) => Err(err),
        }
    }

    #[inline]
    fn mark<F, R>(&mut self, exc: F) -> Result<R>
    where
        F: FnOnce(&mut Self) -> Result<R>,
    {
        let old_peekable = self.peekable;
        let old_current_token = self.current_token.clone();
        let start_offset = self.lexer.offset;
        let result = exc(self);
        if result.is_err() {
            self.peekable = old_peekable;
            self.current_token = old_current_token;
            self.lexer.offset = start_offset;
        }
        result
    }

    #[inline]
    fn try_parse<F, R>(&mut self, exc: F) -> R
    where
        F: FnOnce(&mut Self) -> R,
    {
        debug_assert!(!self.peekable);
        let start_offset = self.lexer.offset;
        let result = exc(self);
        self.peekable = false;
        self.lexer.offset = start_offset;
        result
    }

    /// Get the next token from the lexer
    #[inline]
    fn eat(&mut self) -> Result<Option<Token<'a>>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => {
                let result = tok.clone();
                self.peekable = false; // Clear the peek mark after consuming
                Ok(Some(result))
            }
        }
    }

    /// Peek at the next token without consuming it
    #[inline]
    fn peek(&mut self) -> Result<Option<&Token<'a>>> {
        if self.peekable {
            return Ok(Some(&self.current_token));
        }

        self.next_token()
    }

    #[inline]
    fn eat_no_eof(&mut self) -> Result<Token<'a>> {
        match self.eat()? {
            None => Err(Error::ParseUnexpectedEOF),
            Some(token) => Ok(token),
        }
    }

    #[inline]
    fn peek_no_eof(&mut self) -> Result<&Token<'a>> {
        match self.peek()? {
            None => Err(Error::ParseUnexpectedEOF),
            Some(token) => Ok(token),
        }
    }

    fn parse_stmt(&mut self) -> Result<Stmt> {
        let tok = peek_expect!(
            self,
            TK_BEGIN,
            TK_COMMIT,
            TK_END,
            TK_ROLLBACK,
            TK_SAVEPOINT,
            TK_RELEASE,
            TK_CREATE,
            TK_SELECT,
            TK_VALUES,
            TK_WITH,
            TK_ANALYZE,
            TK_ATTACH,
            TK_DETACH,
            TK_PRAGMA,
            TK_VACUUM,
            TK_ALTER,
            TK_DELETE,
            TK_DROP,
            TK_INSERT,
            TK_REPLACE,
            TK_UPDATE,
            TK_REINDEX,
            TK_OPTIMIZE
        );

        match tok.token_type {
            TK_BEGIN => self.parse_begin(),
            TK_COMMIT | TK_END => self.parse_commit(),
            TK_ROLLBACK => self.parse_rollback(),
            TK_SAVEPOINT => self.parse_savepoint(),
            TK_RELEASE => self.parse_release(),
            TK_CREATE => self.parse_create_stmt(),
            TK_SELECT | TK_VALUES => Ok(Stmt::Select(self.parse_select()?)),
            TK_WITH => self.parse_with_stmt(),
            TK_ANALYZE => self.parse_analyze(),
            TK_ATTACH => self.parse_attach(),
            TK_DETACH => self.parse_detach(),
            TK_PRAGMA => self.parse_pragma(),
            TK_VACUUM => self.parse_vacuum(),
            TK_ALTER => self.parse_alter(),
            TK_DELETE => self.parse_delete(),
            TK_DROP => self.parse_drop_stmt(),
            TK_INSERT | TK_REPLACE => self.parse_insert(),
            TK_UPDATE => self.parse_update(),
            TK_REINDEX => self.parse_reindex(),
            TK_OPTIMIZE => self.parse_optimize(),
            _ => unreachable!(),
        }
    }

    fn parse_nm(&mut self) -> Result<Name> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_LBRACKET {
                // Bracket-quoted identifier: [name] or [multi word name]
                return self.parse_bracket_quoted_name();
            }
        }
        let tok = eat_expect!(self, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW);
        Ok(Name::from_bytes(tok.as_bytes()))
    }

    /// After `peek()` returned `TK_LBRACKET`, report whether the brackets are
    /// empty (`[]`). Empty brackets are an array-type suffix, not a
    /// bracket-quoted identifier. `peek()` leaves the lexer offset right after
    /// the peeked token, so the next raw byte decides.
    fn peeked_empty_brackets(&self) -> bool {
        self.lexer.input.get(self.lexer.offset) == Some(&b']')
    }

    /// Parse a bracket-quoted identifier like `[name]` or `[ multi word name]`.
    fn parse_bracket_quoted_name(&mut self) -> Result<Name> {
        eat_assert!(self, TK_LBRACKET);
        let start = self.lexer.offset;
        let rest = &self.lexer.input[start..];
        let end_pos = rest
            .iter()
            .position(|&b| b == b']')
            .ok_or(Error::ParseUnexpectedEOF)?;
        let raw = &self.lexer.input[start..start + end_pos];
        let name = String::from_utf8_lossy(raw).into_owned();
        // Advance lexer past the closing `]`
        self.lexer.offset = start + end_pos + 1;
        Ok(Name::bracketed(name))
    }

    fn parse_transopt(&mut self) -> Result<Option<Name>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type {
                TK_TRANSACTION => {
                    eat_assert!(self, TK_TRANSACTION);
                    match self.peek()? {
                        Some(tok) => match tok.token_type {
                            TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_LBRACKET => {
                                Ok(Some(self.parse_nm()?))
                            }
                            _ => Ok(None),
                        },
                        _ => Ok(None),
                    }
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_begin(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_BEGIN);

        let transtype = match self.peek()? {
            None => None,
            Some(tok) => match tok.token_type {
                TK_DEFERRED => {
                    eat_assert!(self, TK_DEFERRED);
                    Some(TransactionType::Deferred)
                }
                TK_IMMEDIATE => {
                    eat_assert!(self, TK_IMMEDIATE);
                    Some(TransactionType::Immediate)
                }
                TK_EXCLUSIVE => {
                    eat_assert!(self, TK_EXCLUSIVE);
                    Some(TransactionType::Exclusive)
                }
                TK_CONCURRENT => {
                    eat_assert!(self, TK_CONCURRENT);
                    Some(TransactionType::Concurrent)
                }
                _ => None,
            },
        };

        Ok(Stmt::Begin {
            typ: transtype,
            name: self.parse_transopt()?,
        })
    }

    fn parse_commit(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_COMMIT, TK_END);
        Ok(Stmt::Commit {
            name: self.parse_transopt()?,
        })
    }

    fn parse_rollback(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ROLLBACK);

        let tx_name = self.parse_transopt()?;

        let savepoint_name = match self.peek()? {
            None => None,
            Some(tok) => {
                if tok.token_type == TK_TO {
                    eat_assert!(self, TK_TO);

                    if self.peek_no_eof()?.token_type == TK_SAVEPOINT {
                        eat_assert!(self, TK_SAVEPOINT);
                    }

                    Some(self.parse_nm()?)
                } else {
                    None
                }
            }
        };

        Ok(Stmt::Rollback {
            tx_name,
            savepoint_name,
        })
    }

    fn parse_savepoint(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_SAVEPOINT);
        Ok(Stmt::Savepoint {
            name: self.parse_nm()?,
        })
    }

    fn parse_release(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_RELEASE);

        if self.peek_no_eof()?.token_type == TK_SAVEPOINT {
            eat_assert!(self, TK_SAVEPOINT);
        }

        Ok(Stmt::Release {
            name: self.parse_nm()?,
        })
    }

    fn parse_create_view(&mut self, temporary: bool) -> Result<Stmt> {
        eat_assert!(self, TK_VIEW);
        let if_not_exists = self.parse_if_not_exists()?;
        let view_name = self.parse_fullname(false)?;
        if temporary {
            if let Some(ref db_name) = view_name.db_name {
                if !db_name.as_str().eq_ignore_ascii_case("TEMP") {
                    return Err(Error::Custom(
                        "temporary table name must be unqualified".to_owned(),
                    ));
                }
            }
        }
        let columns = self.parse_eid_list(true)?;
        eat_expect!(self, TK_AS);
        let select = self.parse_select()?;
        if let (ColumnCount::Fixed(n), false) = (select.column_count(), columns.is_empty()) {
            if n != columns.len() {
                return Err(Error::Custom(format!(
                    "expected {} columns for {} but got {}",
                    columns.len(),
                    view_name,
                    n
                )));
            }
        }

        Ok(Stmt::CreateView {
            temporary,
            if_not_exists,
            view_name,
            columns,
            select,
        })
    }

    fn parse_create_materialized_view(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_MATERIALIZED);
        eat_expect!(self, TK_VIEW);
        let if_not_exists = self.parse_if_not_exists()?;
        let view_name = self.parse_fullname(false)?;
        let columns = self.parse_eid_list(false)?;
        eat_expect!(self, TK_AS);
        let select = self.parse_select()?;
        Ok(Stmt::CreateMaterializedView {
            if_not_exists,
            view_name,
            columns,
            select,
        })
    }

    fn parse_vtab_arg(&mut self) -> Result<String> {
        let start_idx = self.offset();

        loop {
            match self.peek_no_eof()?.token_type {
                TK_LP => {
                    let mut lp_count: usize = 0;
                    loop {
                        let tok = self.eat_no_eof()?;
                        match tok.token_type {
                            TK_LP => {
                                lp_count += 1;
                            }
                            TK_RP => {
                                lp_count -= 1; // FIXME: no need to check underflow
                                if lp_count == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                TK_COMMA | TK_RP => break,
                _ => {
                    self.eat_no_eof()?;
                }
            }
        }

        // minus 1 because lexer already consumed TK_COMMA or TK_RP
        let end_idx = self.offset();
        if start_idx == end_idx {
            return Err(Error::Custom("unexpected COMMA in vtab args".to_owned()));
        }

        Ok(from_bytes(&self.lexer.input[start_idx..end_idx]))
    }

    fn parse_create_virtual(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_VIRTUAL);
        eat_expect!(self, TK_TABLE);
        let if_not_exists = self.parse_if_not_exists()?;
        let tbl_name = self.parse_fullname(false)?;
        eat_expect!(self, TK_USING);
        let module_name = self.parse_nm()?;
        let args = match self.peek()? {
            Some(tok) => match tok.token_type {
                TK_LP => {
                    eat_assert!(self, TK_LP);
                    let mut result = vec![];
                    loop {
                        if self.peek_no_eof()?.token_type == TK_RP {
                            break; // handle empty args case
                        }

                        result.push(self.parse_vtab_arg()?);
                        let tok = peek_expect!(self, TK_COMMA, TK_RP);
                        match tok.token_type {
                            TK_COMMA => {
                                eat_assert!(self, TK_COMMA);
                            }
                            TK_RP => break,
                            _ => unreachable!(),
                        }
                    }

                    eat_assert!(self, TK_RP); // already checks in loop
                    result
                }
                _ => vec![],
            },
            _ => vec![],
        };

        Ok(Stmt::CreateVirtualTable(CreateVirtualTable {
            if_not_exists,
            tbl_name,
            module_name,
            args,
        }))
    }

    fn parse_create_stmt(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_CREATE);
        let mut first_tok = peek_expect!(
            self,
            TK_TEMP,
            TK_TABLE,
            TK_VIRTUAL,
            TK_VIEW,
            TK_INDEX,
            TK_UNIQUE,
            TK_TRIGGER,
            TK_MATERIALIZED,
            TK_TYPE,
            TK_ID
        );
        let mut temp = false;
        if first_tok.token_type == TK_TEMP {
            eat_assert!(self, TK_TEMP);
            temp = true;
            first_tok = peek_expect!(self, TK_TABLE, TK_VIEW, TK_TRIGGER);
        }

        match first_tok.token_type {
            TK_TABLE => self.parse_create_table(temp),
            TK_VIEW => self.parse_create_view(temp),
            TK_MATERIALIZED => self.parse_create_materialized_view(),
            TK_TRIGGER => self.parse_create_trigger(temp),
            TK_VIRTUAL => self.parse_create_virtual(),
            TK_INDEX | TK_UNIQUE => self.parse_create_index(),
            TK_TYPE => self.parse_create_type(),
            TK_ID if first_tok.to_utf8().eq_ignore_ascii_case("DOMAIN") => {
                self.parse_create_domain()
            }
            TK_ID if first_tok.to_utf8().eq_ignore_ascii_case("SEQUENCE") => {
                self.parse_create_sequence()
            }
            _ => Err(Error::ParseError(format!(
                "unexpected token: {}",
                first_tok.to_utf8()
            )))?,
        }
    }

    fn parse_with_stmt(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        debug_assert!(with.is_some());
        let first_tok =
            peek_expect!(self, TK_SELECT, TK_VALUES, TK_UPDATE, TK_DELETE, TK_INSERT, TK_REPLACE,);

        match first_tok.token_type {
            TK_SELECT | TK_VALUES => Ok(Stmt::Select(self.parse_select_without_cte(with)?)),
            TK_UPDATE => self.parse_update_without_cte(with),
            TK_DELETE => self.parse_delete_without_cte(with),
            TK_INSERT | TK_REPLACE => self.parse_insert_without_cte(with),
            _ => unreachable!(),
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_IF {
                eat_assert!(self, TK_IF);
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }

        eat_expect!(self, TK_NOT);
        eat_expect!(self, TK_EXISTS);
        Ok(true)
    }

    fn parse_fullname(&mut self, allow_alias: bool) -> Result<QualifiedName> {
        let first_name = self.parse_nm()?;

        let second_name = if let Some(tok) = self.peek()? {
            if tok.token_type == TK_DOT {
                eat_assert!(self, TK_DOT);
                Some(self.parse_nm()?)
            } else {
                None
            }
        } else {
            None
        };

        let alias_name = if allow_alias {
            if let Some(tok) = self.peek()? {
                if tok.token_type == TK_AS {
                    eat_assert!(self, TK_AS);
                    Some(self.parse_nm()?)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(second_name) = second_name {
            Ok(QualifiedName {
                db_name: Some(first_name),
                name: second_name,
                alias: alias_name,
            })
        } else {
            Ok(QualifiedName {
                db_name: None,
                name: first_name,
                alias: alias_name,
            })
        }
    }

    fn parse_reindex_fullname(&mut self) -> Result<QualifiedName> {
        let first_name = self.parse_reindex_nm()?;

        let second_name = if let Some(tok) = self.peek()? {
            if tok.token_type == TK_DOT {
                eat_assert!(self, TK_DOT);
                Some(self.parse_reindex_nm()?)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(second_name) = second_name {
            Ok(QualifiedName {
                db_name: Some(first_name),
                name: second_name,
                alias: None,
            })
        } else {
            Ok(QualifiedName {
                db_name: None,
                name: first_name,
                alias: None,
            })
        }
    }

    fn parse_reindex_nm(&mut self) -> Result<Name> {
        let offset = self.offset();
        let token = self.peek_no_eof()?;
        if Self::is_reindex_compound_operator_name(token) {
            let token_len = token.value.len();
            let token_text = token.to_utf8();
            return Err(Error::ParseUnexpectedToken {
                parsed_offset: (offset, token_len).into(),
                expected: &[TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW, TK_LBRACKET],
                got: token.token_type,
                token_text,
                offset,
                expected_display: crate::token::TokenType::format_expected_tokens(&[
                    TK_ID,
                    TK_STRING,
                    TK_INDEXED,
                    TK_JOIN_KW,
                    TK_LBRACKET,
                ]),
            });
        }
        self.parse_nm()
    }

    fn is_reindex_compound_operator_name(token: &Token<'_>) -> bool {
        matches!(token.token_type, TK_UNION | TK_EXCEPT | TK_INTERSECT)
            || matches!(token.token_type, TK_ID)
                && (token.as_bytes().eq_ignore_ascii_case(b"union")
                    || token.as_bytes().eq_ignore_ascii_case(b"except")
                    || token.as_bytes().eq_ignore_ascii_case(b"intersect"))
    }

    fn parse_signed(&mut self) -> Result<Box<Expr>> {
        peek_expect!(self, TK_FLOAT, TK_INTEGER, TK_PLUS, TK_MINUS);

        let expr = self.parse_expr_operand()?;
        match expr.as_ref() {
            Expr::Unary(_, inner) => match inner.as_ref() {
                Expr::Literal(Literal::Numeric(_)) => Ok(expr),
                _ => Err(Error::Custom(
                    "Expected a numeric literal after unary operator".to_string(),
                )),
            },
            _ => Ok(expr),
        }
    }

    fn parse_type(&mut self) -> Result<Option<Type>> {
        let mut type_name = if let Some(tok) = self.peek()? {
            match tok.token_type.fallback_id_if_ok() {
                TK_ID | TK_STRING => {
                    let tok = eat_assert!(self, TK_ID, TK_STRING);
                    from_bytes(tok.as_bytes())
                }
                TK_LBRACKET if !self.peeked_empty_brackets() => {
                    self.parse_bracket_quoted_name()?.as_str().to_owned()
                }
                _ => return Ok(None),
            }
        } else {
            return Ok(None);
        };

        // Inline STRUCT/UNION types (e.g. `col STRUCT(x INT)`) are only valid
        // inside CREATE TYPE field lists (tracked by type_nesting_depth > 0).
        // In column definitions and CAST expressions, reject them.
        let is_struct = type_name.eq_ignore_ascii_case("STRUCT");
        let is_union = type_name.eq_ignore_ascii_case("UNION");
        if (is_struct || is_union) && self.peek()?.is_some_and(|t| t.token_type == TK_LP) {
            if self.type_nesting_depth == 0 {
                return Err(Error::ParseError(
                    "inline STRUCT/UNION column types are not supported; \
                     use CREATE TYPE to define a named type first, then use the type name in the column definition".to_owned(),
                ));
            }
            // Inside a CREATE TYPE field list — consume the field list but
            // store only the bare type name. The nested fields are validated
            // during schema loading (from_create_type), not here.
            let _fields = self.parse_type_field_list()?;
        }

        while let Some(tok) = self.peek()? {
            match tok.token_type.fallback_id_if_ok() {
                TK_ID | TK_STRING => {
                    let tok = eat_assert!(self, TK_ID, TK_STRING);
                    type_name.push(' ');
                    type_name.push_str(from_bytes_as_str(tok.value));
                }
                TK_LBRACKET if !self.peeked_empty_brackets() => {
                    let name = self.parse_bracket_quoted_name()?;
                    type_name.push(' ');
                    type_name.push_str(name.as_str());
                }
                _ => break,
            }
        }

        let size = if let Some(tok) = self.peek()? {
            if tok.token_type == TK_LP {
                eat_assert!(self, TK_LP);
                let first_size = self.parse_signed()?;
                let tok = eat_expect!(self, TK_RP, TK_COMMA);
                match tok.token_type {
                    TK_RP => Some(TypeSize::MaxSize(first_size)),
                    TK_COMMA => {
                        let second_size = self.parse_signed()?;
                        eat_expect!(self, TK_RP);
                        Some(TypeSize::TypeSize(first_size, second_size))
                    }
                    _ => unreachable!(),
                }
            } else {
                None
            }
        } else {
            None
        };

        // Check for array suffix(es) [], [][], etc.
        let mut array_dimensions = 0u32;
        while self.peek()?.is_some_and(|t| t.token_type == TK_LBRACKET) {
            eat_assert!(self, TK_LBRACKET);
            eat_expect!(self, TK_RBRACKET);
            array_dimensions += 1;
        }

        Ok(Some(Type {
            name: type_name,
            size,
            array_dimensions,
        }))
    }

    /// Parse a parenthesized list of `name Type` pairs for STRUCT/UNION type declarations.
    /// Expects the opening `(` to be the next token.
    fn parse_type_field_list(&mut self) -> Result<Vec<TypeField>> {
        self.type_nesting_depth += 1;
        if self.type_nesting_depth > 32 {
            self.type_nesting_depth -= 1;
            return Err(Error::ParseError(
                "STRUCT/UNION nesting depth exceeds maximum (32)".to_owned(),
            ));
        }
        let result = self.parse_type_field_list_inner();
        self.type_nesting_depth -= 1;
        result
    }

    fn parse_type_field_list_inner(&mut self) -> Result<Vec<TypeField>> {
        eat_expect!(self, TK_LP);
        let mut fields = Vec::new();
        loop {
            // Parse field name (use parse_nm to handle keywords-as-identifiers)
            let name = self.parse_nm()?;
            // Parse field type (recursive — allows nested STRUCT/UNION)
            let field_type = self.parse_type()?.ok_or_else(|| {
                Error::ParseError("expected type after field name in STRUCT/UNION".to_owned())
            })?;
            fields.push(TypeField { name, field_type });
            match self.peek()? {
                Some(tok) if tok.token_type == TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                }
                _ => break,
            }
        }
        eat_expect!(self, TK_RP);
        Ok(fields)
    }

    /// SQLite understands these operators, listed in precedence1 order
    /// (top to bottom / highest to lowest):
    ///
    /// Operators 2
    /// 11 -> ~ [expr]    + [expr]    - [expr]
    /// 10 -> [expr] COLLATE (collation-name) 3
    /// 9 -> ||   ->   ->>
    /// 8 -> *   /   %
    /// 7 -> +   -
    /// 6 -> &  |   <<  >>
    /// 5 -> [expr] ESCAPE [escape-character-expr] 4
    /// 4 -> <  >  <=  >=
    /// 3 -> =  ==  <>  !=  IS   IS NOT
    ///     IS DISTINCT FROM   IS NOT DISTINCT FROM
    ///     [expr] BETWEEN5 [expr] AND [expr]
    ///     IN5  MATCH5  LIKE5  REGEXP5  GLOB5
    ///     [expr] ISNULL  [expr] NOTNULL   [expr] NOT NULL
    /// 2   NOT [expr]
    /// 1 -> AND
    /// 0 -> OR
    ///
    /// this function detect precedence by peeking first token of operator
    /// after parsing a operand (binary operator)
    fn current_token_precedence(&mut self) -> Result<Option<u8>> {
        let tok = self.peek()?;
        if tok.is_none() {
            return Ok(None);
        }

        match tok.unwrap().token_type {
            TK_OR => Ok(Some(0)),
            TK_AND => Ok(Some(1)),
            TK_NOT => Ok(Some(3)), // NOT is 3 because of binary operator
            TK_EQ | TK_NE | TK_IS | TK_BETWEEN | TK_IN | TK_MATCH | TK_LIKE_KW | TK_ISNULL
            | TK_NOTNULL => Ok(Some(3)),
            TK_LT | TK_GT | TK_LE | TK_GE | TK_ARRAY_CONTAINS | TK_ARRAY_OVERLAP => Ok(Some(4)),
            TK_ESCAPE => Ok(None), // ESCAPE will be consumed after parsing MATCH|LIKE_KW
            TK_BITAND | TK_BITOR | TK_LSHIFT | TK_RSHIFT => Ok(Some(6)),
            TK_PLUS | TK_MINUS => Ok(Some(7)),
            TK_STAR | TK_SLASH | TK_REM => Ok(Some(8)),
            TK_CONCAT | TK_PTR => Ok(Some(9)),
            TK_COLLATE => Ok(Some(10)),
            // no need 11 because its for unary operators
            TK_LBRACKET => Ok(Some(12)), // subscript: expr[n]
            _ => Ok(None),
        }
    }

    fn parse_distinct(&mut self) -> Result<Option<Distinctness>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type {
                TK_DISTINCT => {
                    eat_assert!(self, TK_DISTINCT);
                    Ok(Some(Distinctness::Distinct))
                }
                TK_ALL => {
                    eat_assert!(self, TK_ALL);
                    Ok(Some(Distinctness::All))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_filter_clause(&mut self) -> Result<Option<Box<Expr>>> {
        // Report height 0 when there is no FILTER clause so callers folding this
        // into their own height are not misled by a stale value.
        self.last_expr_height = 0;
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type {
                TK_FILTER => {
                    eat_assert!(self, TK_FILTER);
                }
                _ => return Ok(None),
            },
        }

        eat_expect!(self, TK_LP);
        eat_expect!(self, TK_WHERE);
        // `parse_expr` leaves the filter expression's height in `last_expr_height`.
        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_RP);
        Ok(Some(expr))
    }

    fn parse_frame_opt(&mut self) -> Result<Option<FrameClause>> {
        // Tallest frame-bound expression, reported via `last_expr_height`; the
        // non-expression bounds (`UNBOUNDED`, `CURRENT ROW`) contribute nothing.
        let mut max_h = 0usize;
        // No frame clause parsed yet: report height 0 for the early returns below.
        self.last_expr_height = 0;
        let range_or_rows = match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type {
                TK_RANGE => {
                    eat_assert!(self, TK_RANGE);
                    FrameMode::Range
                }
                TK_ROWS => {
                    eat_assert!(self, TK_ROWS);
                    FrameMode::Rows
                }
                TK_GROUPS => {
                    eat_assert!(self, TK_GROUPS);
                    FrameMode::Groups
                }
                _ => return Ok(None),
            },
        };

        let has_end = match self.peek_no_eof()?.token_type {
            TK_BETWEEN => {
                eat_assert!(self, TK_BETWEEN);
                true
            }
            _ => false,
        };

        let start = match self.peek_no_eof()?.token_type {
            TK_UNBOUNDED => {
                eat_assert!(self, TK_UNBOUNDED);
                eat_expect!(self, TK_PRECEDING);
                FrameBound::UnboundedPreceding
            }
            TK_CURRENT => {
                eat_assert!(self, TK_CURRENT);
                eat_expect!(self, TK_ROW);
                FrameBound::CurrentRow
            }
            _ => {
                let expr = self.parse_expr(0)?;
                max_h = max_h.max(self.last_expr_height);
                let tok = eat_expect!(self, TK_PRECEDING, TK_FOLLOWING);
                match tok.token_type {
                    TK_PRECEDING => FrameBound::Preceding(expr),
                    TK_FOLLOWING => FrameBound::Following(expr),
                    _ => unreachable!(),
                }
            }
        };

        let end = if has_end {
            eat_expect!(self, TK_AND);

            Some(match self.peek_no_eof()?.token_type {
                TK_UNBOUNDED => {
                    eat_assert!(self, TK_UNBOUNDED);
                    eat_expect!(self, TK_FOLLOWING);
                    FrameBound::UnboundedFollowing
                }
                TK_CURRENT => {
                    eat_assert!(self, TK_CURRENT);
                    eat_expect!(self, TK_ROW);
                    FrameBound::CurrentRow
                }
                _ => {
                    let expr = self.parse_expr(0)?;
                    max_h = max_h.max(self.last_expr_height);
                    let tok = eat_expect!(self, TK_PRECEDING, TK_FOLLOWING);
                    match tok.token_type {
                        TK_PRECEDING => FrameBound::Preceding(expr),
                        TK_FOLLOWING => FrameBound::Following(expr),
                        _ => unreachable!(),
                    }
                }
            })
        } else {
            None
        };

        let exclude = match self.peek()? {
            None => None,
            Some(tok) => match tok.token_type {
                TK_EXCLUDE => {
                    eat_assert!(self, TK_EXCLUDE);
                    let tok = eat_expect!(self, TK_NO, TK_CURRENT, TK_GROUP, TK_TIES);
                    match tok.token_type {
                        TK_NO => {
                            eat_expect!(self, TK_OTHERS);
                            Some(FrameExclude::NoOthers)
                        }
                        TK_CURRENT => {
                            eat_expect!(self, TK_ROW);
                            Some(FrameExclude::CurrentRow)
                        }
                        TK_GROUP => Some(FrameExclude::Group),
                        TK_TIES => Some(FrameExclude::Ties),
                        _ => unreachable!(),
                    }
                }
                _ => None,
            },
        };

        self.last_expr_height = max_h;
        Ok(Some(FrameClause {
            mode: range_or_rows,
            start,
            end,
            exclude,
        }))
    }

    fn parse_window(&mut self) -> Result<Window> {
        let name = match self.peek()? {
            None => None,
            Some(tok) => match tok.token_type {
                TK_PARTITION | TK_ORDER | TK_RANGE | TK_ROWS | TK_GROUPS => None,
                tt => match tt.fallback_id_if_ok() {
                    TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => Some(self.parse_nm()?),
                    _ => None,
                },
            },
        };

        // Tallest sub-expression across PARTITION BY / ORDER BY / frame bounds,
        // reported via `last_expr_height` so a function's window clause folds into
        // its height (later passes recurse through the window's expressions).
        let mut max_h = 0usize;
        let partition_by = match self.peek()? {
            Some(tok) if tok.token_type == TK_PARTITION => {
                eat_assert!(self, TK_PARTITION);
                eat_expect!(self, TK_BY);
                let partition_by = self.parse_nexpr_list()?;
                max_h = max_h.max(self.last_expr_height);
                partition_by
            }
            _ => vec![],
        };

        let order_by = self.parse_order_by()?;
        max_h = max_h.max(self.last_expr_height);
        let frame_clause = self.parse_frame_opt()?;
        max_h = max_h.max(self.last_expr_height);
        self.last_expr_height = max_h;
        Ok(Window {
            base: name,
            partition_by,
            order_by,
            frame_clause,
        })
    }

    fn parse_over_clause(&mut self) -> Result<Option<Over>> {
        // Report height 0 unless a window definition with expressions is parsed.
        self.last_expr_height = 0;
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type {
                TK_OVER => {
                    eat_assert!(self, TK_OVER);
                }
                _ => return Ok(None),
            },
        }

        let tok = peek_expect!(self, TK_LP, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW);
        match tok.token_type {
            TK_LP => {
                eat_assert!(self, TK_LP);
                // `parse_window` leaves its tallest sub-expression height behind.
                let window = self.parse_window()?;
                eat_expect!(self, TK_RP);
                Ok(Some(Over::Window(window)))
            }
            _ => Ok(Some(Over::Name(self.parse_nm()?))),
        }
    }

    fn parse_filter_over(&mut self) -> Result<FunctionTail> {
        let filter_clause = self.parse_filter_clause()?;
        // Fold the FILTER and OVER sub-expression heights together so callers can
        // account for expressions the translator later walks in these clauses.
        let mut max_h = self.last_expr_height;
        let over_clause = self.parse_over_clause()?;
        max_h = max_h.max(self.last_expr_height);
        self.last_expr_height = max_h;
        Ok(FunctionTail {
            filter_clause,
            over_clause,
        })
    }

    /// Parses an optional `WITHIN GROUP (ORDER BY ...)` clause used by ordered-set
    /// aggregates. The `ORDER BY` is mandatory once `WITHIN GROUP` is present.
    fn parse_within_group(&mut self) -> Result<Vec<SortedColumn>> {
        // Report height 0 unless a WITHIN GROUP (ORDER BY ...) is parsed, in which
        // case `parse_order_by` leaves the tallest sort expression's height.
        self.last_expr_height = 0;
        match self.peek()? {
            Some(tok) if tok.token_type == TK_WITHIN => {
                eat_assert!(self, TK_WITHIN);
                eat_expect!(self, TK_GROUP);
                eat_expect!(self, TK_LP);
                let order_by = self.parse_order_by()?;
                if order_by.is_empty() {
                    return Err(Error::Custom(
                        "WITHIN GROUP requires an ORDER BY clause".to_owned(),
                    ));
                }
                eat_expect!(self, TK_RP);
                Ok(order_by)
            }
            _ => Ok(vec![]),
        }
    }

    fn parse_raise_type(&mut self) -> Result<ResolveType> {
        let tok = eat_expect!(self, TK_ROLLBACK, TK_ABORT, TK_FAIL);

        match tok.token_type {
            TK_ROLLBACK => Ok(ResolveType::Rollback),
            TK_ABORT => Ok(ResolveType::Abort),
            TK_FAIL => Ok(ResolveType::Fail),
            _ => unreachable!(),
        }
    }

    fn parse_expr_operand(&mut self) -> Result<Box<Expr>> {
        // Height of the operand. Leaves keep this default; branches that recurse
        // into sub-expressions overwrite it with `1 + max(child heights)` so the
        // enclosing chain guard (see `parse_expr_inner`) sees their true depth.
        self.last_expr_height = 1;

        let tok = peek_expect!(
            self,
            TK_LP,
            TK_CAST,
            TK_CTIME_KW,
            TK_RAISE,
            TK_ID,
            TK_STRING,
            TK_INDEXED,
            TK_JOIN_KW,
            TK_NULL,
            TK_BLOB,
            TK_FLOAT,
            TK_INTEGER,
            TK_VARIABLE,
            TK_NOT,
            TK_BITNOT,
            TK_PLUS,
            TK_MINUS,
            TK_EXISTS,
            TK_CASE,
            TK_LBRACKET,
            TK_DEFAULT,
        );

        match tok.token_type {
            TK_DEFAULT => {
                eat_assert!(self, TK_DEFAULT);
                Ok(Box::new(Expr::Default))
            }
            TK_LP => {
                eat_assert!(self, TK_LP);
                match self.peek_no_eof()?.token_type {
                    TK_WITH | TK_SELECT | TK_VALUES => {
                        let select = self.parse_select()?;
                        eat_expect!(self, TK_RP);
                        // Subquery is compiled separately: a leaf for height.
                        self.last_expr_height = 1;
                        Ok(Box::new(Expr::Subquery(select)))
                    }
                    _ => {
                        let exprs = self.parse_nexpr_list()?;
                        eat_expect!(self, TK_RP);
                        // `parse_nexpr_list` left the tallest element's height.
                        self.last_expr_height += 1;
                        Ok(Box::new(Expr::Parenthesized(exprs)))
                    }
                }
            }
            TK_NULL => {
                eat_assert!(self, TK_NULL);
                Ok(Box::new(Expr::Literal(Literal::Null)))
            }
            TK_BLOB => {
                let tok = eat_assert!(self, TK_BLOB);
                Ok(Box::new(Expr::Literal(Literal::Blob(from_bytes(
                    tok.value,
                )))))
            }
            TK_FLOAT => {
                let tok = eat_assert!(self, TK_FLOAT);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TK_INTEGER => {
                let tok = eat_assert!(self, TK_INTEGER);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TK_VARIABLE => {
                let tok = eat_assert!(self, TK_VARIABLE);
                Ok(Box::new(self.create_variable(tok.value)?))
            }
            TK_CAST => {
                eat_assert!(self, TK_CAST);
                eat_expect!(self, TK_LP);
                let expr = self.parse_expr(0)?;
                eat_expect!(self, TK_AS);
                let typ = self.parse_type()?;
                eat_expect!(self, TK_RP);
                self.last_expr_height += 1;
                Ok(Box::new(Expr::Cast {
                    expr,
                    type_name: typ,
                }))
            }
            TK_CTIME_KW => {
                let tok = eat_assert!(self, TK_CTIME_KW);
                match_ignore_ascii_case!(match tok.value {
                    b"CURRENT_DATE" => Ok(Box::new(Expr::Literal(Literal::CurrentDate))),
                    b"CURRENT_TIME" => Ok(Box::new(Expr::Literal(Literal::CurrentTime))),
                    b"CURRENT_TIMESTAMP" => Ok(Box::new(Expr::Literal(Literal::CurrentTimestamp))),
                    _ => unreachable!(),
                })
            }
            TK_NOT => {
                eat_assert!(self, TK_NOT);
                let expr = self.parse_expr(2)?; // NOT precedence is 2
                self.last_expr_height += 1;
                Ok(Box::new(Expr::Unary(UnaryOperator::Not, expr)))
            }
            TK_BITNOT => {
                eat_assert!(self, TK_BITNOT);
                let expr = self.parse_expr(11)?; // BITNOT precedence is 11
                self.last_expr_height += 1;
                Ok(Box::new(Expr::Unary(UnaryOperator::BitwiseNot, expr)))
            }
            TK_PLUS => {
                eat_assert!(self, TK_PLUS);
                let expr = self.parse_expr(11)?; // PLUS precedence is 11
                self.last_expr_height += 1;
                Ok(Box::new(Expr::Unary(UnaryOperator::Positive, expr)))
            }
            TK_MINUS => {
                eat_assert!(self, TK_MINUS);
                let expr = self.parse_expr(11)?; // MINUS precedence is 11
                self.last_expr_height += 1;
                Ok(Box::new(Expr::Unary(UnaryOperator::Negative, expr)))
            }
            TK_EXISTS => {
                eat_assert!(self, TK_EXISTS);
                eat_expect!(self, TK_LP);
                let select = self.parse_select()?;
                eat_expect!(self, TK_RP);
                // Subquery is compiled separately: a leaf for height.
                self.last_expr_height = 1;
                Ok(Box::new(Expr::Exists(select)))
            }
            TK_CASE => {
                eat_assert!(self, TK_CASE);
                // Tallest of the base/when/then/else sub-expressions.
                let mut max_h = 0usize;
                let base = if self.peek_no_eof()?.token_type != TK_WHEN {
                    let base = self.parse_expr(0)?;
                    max_h = max_h.max(self.last_expr_height);
                    Some(base)
                } else {
                    None
                };

                eat_expect!(self, TK_WHEN);
                let first_when = self.parse_expr(0)?;
                max_h = max_h.max(self.last_expr_height);
                eat_expect!(self, TK_THEN);
                let first_then = self.parse_expr(0)?;
                max_h = max_h.max(self.last_expr_height);
                let mut when_then_pairs = vec![(first_when, first_then)];

                while let Some(tok) = self.peek()? {
                    if tok.token_type != TK_WHEN {
                        break;
                    }

                    eat_assert!(self, TK_WHEN);
                    let when = self.parse_expr(0)?;
                    max_h = max_h.max(self.last_expr_height);
                    eat_expect!(self, TK_THEN);
                    let then = self.parse_expr(0)?;
                    max_h = max_h.max(self.last_expr_height);
                    when_then_pairs.push((when, then));
                }

                let else_expr = if let Some(ok) = self.peek()? {
                    if ok.token_type == TK_ELSE {
                        eat_assert!(self, TK_ELSE);
                        let else_expr = self.parse_expr(0)?;
                        max_h = max_h.max(self.last_expr_height);
                        Some(else_expr)
                    } else {
                        None
                    }
                } else {
                    None
                };

                eat_expect!(self, TK_END);
                self.last_expr_height = 1 + max_h;
                Ok(Box::new(Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                }))
            }
            TK_RAISE => {
                eat_assert!(self, TK_RAISE);
                eat_expect!(self, TK_LP);

                let (resolve, shorthand) = match self.peek_no_eof()?.token_type {
                    TK_IGNORE => {
                        eat_assert!(self, TK_IGNORE);
                        (ResolveType::Ignore, false)
                    }
                    // RAISE('message') shorthand — defaults to ABORT
                    TK_STRING => (ResolveType::Abort, true),
                    _ => (self.parse_raise_type()?, false),
                };

                let expr = if resolve != ResolveType::Ignore {
                    if !shorthand {
                        eat_expect!(self, TK_COMMA);
                    }
                    Some(self.parse_expr(0)?)
                } else {
                    None
                };

                eat_expect!(self, TK_RP);
                if expr.is_some() {
                    self.last_expr_height += 1;
                }
                Ok(Box::new(Expr::Raise(resolve, expr)))
            }
            TK_LBRACKET => {
                // Bracket-quoted identifier: [name] or [multi word name],
                // optionally qualified: [tbl].[col] or [db].[tbl].[col]
                let name = self.parse_bracket_quoted_name()?;
                let second_name = if self.peek()?.is_some_and(|t| t.token_type == TK_DOT) {
                    eat_assert!(self, TK_DOT);
                    Some(self.parse_nm()?)
                } else {
                    None
                };
                let third_name = if second_name.is_some()
                    && self.peek()?.is_some_and(|t| t.token_type == TK_DOT)
                {
                    eat_assert!(self, TK_DOT);
                    Some(self.parse_nm()?)
                } else {
                    None
                };
                match (second_name, third_name) {
                    (Some(second), Some(third)) => {
                        Ok(Box::new(Expr::DoublyQualified(name, second, third)))
                    }
                    (Some(second), None) => Ok(Box::new(Expr::Qualified(name, second))),
                    _ => Ok(Box::new(Expr::Id(name))),
                }
            }
            _ => {
                let can_be_lit_str = tok.token_type == TK_STRING;

                // can be either Literal::String or Name - so we parse raw value early and decide later
                let tok = eat_expect!(self, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW);
                let name = tok.value;

                // Check for ARRAY[...] literal
                if name.eq_ignore_ascii_case(b"array") {
                    if let Some(tok) = self.peek()? {
                        if tok.token_type == TK_LBRACKET {
                            eat_assert!(self, TK_LBRACKET);
                            let elements = self.parse_expr_list()?;
                            eat_expect!(self, TK_RBRACKET);
                            // `parse_expr_list` left the tallest element's height.
                            self.last_expr_height += 1;
                            // Desugar ARRAY[...] into array(...) function call
                            return Ok(Box::new(Expr::FunctionCall {
                                name: Name::from_bytes(b"array"),
                                distinctness: None,
                                args: elements,
                                order_by: vec![],
                                within_group: vec![],
                                filter_over: FunctionTail {
                                    filter_clause: None,
                                    over_clause: None,
                                },
                            }));
                        }
                    }
                }

                let second_name = if let Some(tok) = self.peek()? {
                    if tok.token_type == TK_DOT {
                        eat_assert!(self, TK_DOT);
                        Some(self.parse_nm()?)
                    } else if tok.token_type == TK_LP {
                        if can_be_lit_str {
                            let token = self.peek_no_eof()?;
                            let token_text = token.to_utf8();
                            let offset = self.offset();
                            return Err(Error::ParseUnexpectedToken {
                                parsed_offset: (self.offset() - name.len(), name.len()).into(),
                                got: TK_STRING,
                                expected: &[TK_ID, TK_INDEXED, TK_JOIN_KW],
                                token_text,
                                offset,
                                expected_display: crate::token::TokenType::format_expected_tokens(
                                    &[TK_ID, TK_INDEXED, TK_JOIN_KW],
                                ),
                            });
                        } // can not be literal string in function name

                        eat_assert!(self, TK_LP);
                        let tok = self.peek_no_eof()?;
                        match tok.token_type {
                            TK_STAR => {
                                eat_assert!(self, TK_STAR);
                                eat_expect!(self, TK_RP);
                                let filter_over = self.parse_filter_over()?;
                                // No arguments, but later passes still walk any
                                // FILTER/OVER sub-expressions, so fold their
                                // height into this node's height.
                                self.last_expr_height += 1;
                                return Ok(Box::new(Expr::FunctionCallStar {
                                    name: Name::from_bytes(name),
                                    filter_over,
                                }));
                            }
                            _ => {
                                let distinct = self.parse_distinct()?;
                                let exprs = self.parse_expr_list()?;
                                // Height is the tallest sub-expression later
                                // passes can descend into: the arguments plus the
                                // ORDER BY / WITHIN GROUP / FILTER / OVER clauses.
                                let mut clause_height = self.last_expr_height;
                                let order_by = self.parse_order_by()?;
                                clause_height = clause_height.max(self.last_expr_height);
                                eat_expect!(self, TK_RP);
                                let within_group = self.parse_within_group()?;
                                clause_height = clause_height.max(self.last_expr_height);
                                let filter_over = self.parse_filter_over()?;
                                clause_height = clause_height.max(self.last_expr_height);
                                self.last_expr_height = 1 + clause_height;
                                return Ok(Box::new(Expr::FunctionCall {
                                    name: Name::from_bytes(name),
                                    distinctness: distinct,
                                    args: exprs,
                                    order_by,
                                    within_group,
                                    filter_over,
                                }));
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let third_name = if let Some(tok) = self.peek()? {
                    if tok.token_type == TK_DOT {
                        debug_assert!(second_name.is_some());
                        eat_assert!(self, TK_DOT);
                        Some(self.parse_nm()?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(second_name) = second_name {
                    if let Some(third_name) = third_name {
                        Ok(Box::new(Expr::DoublyQualified(
                            Name::from_bytes(name),
                            second_name,
                            third_name,
                        )))
                    } else {
                        Ok(Box::new(Expr::Qualified(
                            Name::from_bytes(name),
                            second_name,
                        )))
                    }
                } else if can_be_lit_str {
                    Ok(Box::new(Expr::Literal(Literal::String(from_bytes(name)))))
                } else {
                    match_ignore_ascii_case!(match name {
                        b"true" => {
                            Ok(Box::new(Expr::Literal(Literal::True)))
                        }
                        b"false" => {
                            Ok(Box::new(Expr::Literal(Literal::False)))
                        }
                        _ => Ok(Box::new(Expr::Id(Name::from_bytes(name)))),
                    })
                }
            }
        }
    }

    #[allow(clippy::vec_box)]
    fn parse_expr_list(&mut self) -> Result<Vec<Box<Expr>>> {
        let mut exprs = vec![];
        // Tallest element, reported via `last_expr_height` so the enclosing node
        // (function call, IN list, ...) can fold it into its own height.
        let mut max_h = 0usize;
        while let Some(tok) = self.peek()? {
            match tok.token_type.fallback_id_if_ok() {
                TK_LP | TK_CAST | TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_NULL
                | TK_BLOB | TK_FLOAT | TK_INTEGER | TK_VARIABLE | TK_CTIME_KW | TK_NOT
                | TK_BITNOT | TK_PLUS | TK_MINUS | TK_EXISTS | TK_CASE | TK_LBRACKET => {}
                _ => break,
            }

            exprs.push(self.parse_expr(0)?);
            max_h = max_h.max(self.last_expr_height);
            match self.peek_no_eof()?.token_type {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                }
                _ => break,
            }
        }

        self.last_expr_height = max_h;
        Ok(exprs)
    }

    /// Parse an expression, bounding both the parser's recursion depth and the
    /// height of the resulting tree by [`MAX_EXPR_DEPTH`]. On return,
    /// `last_expr_height` holds the height of the returned expression.
    fn parse_expr(&mut self, precedence: u8) -> Result<Box<Expr>> {
        self.expr_nesting_depth += 1;
        if self.expr_nesting_depth as usize > MAX_EXPR_DEPTH {
            self.expr_nesting_depth -= 1;
            return Err(Error::ParseError(format!(
                "Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})"
            )));
        }
        let result = self.parse_expr_inner(precedence);
        self.expr_nesting_depth -= 1;
        result
    }

    fn parse_expr_inner(&mut self, precedence: u8) -> Result<Box<Expr>> {
        let mut result = self.parse_expr_operand()?;
        // Running height of `result`, maintained bottom-up so that a left-deep
        // chain (`a OR b OR c ...`), which this loop consumes iteratively, is
        // bounded too. Check the operand up front: it can already be over the
        // limit on its own without being followed by an operator.
        let mut result_height = self.last_expr_height;
        if result_height > MAX_EXPR_DEPTH {
            return Err(Error::ParseError(format!(
                "Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})"
            )));
        }

        loop {
            let pre = match self.current_token_precedence()? {
                Some(pre) if pre < precedence => break,
                None => break, // no more ops
                Some(pre) => pre,
            };

            let mut tok = self.peek_no_eof()?;
            let mut not = false;
            // Set by arms that replace `result` with a fresh leaf (e.g. `x IN ()`)
            // instead of wrapping it, so its height resets to 1.
            let mut leaf = false;
            if tok.token_type == TK_NOT {
                eat_assert!(self, TK_NOT);
                tok = peek_expect!(
                    self, TK_BETWEEN, TK_IN, TK_MATCH, TK_LIKE_KW, TK_NULL, // `NOT NULL`
                );
                not = true;
            }

            result = match tok.token_type {
                TK_NULL => {
                    // special case `NOT NULL`
                    debug_assert!(not); // FIXME: not always true because of current_token_precedence
                    eat_assert!(self, TK_NULL);
                    Box::new(Expr::NotNull(result))
                }
                TK_OR => {
                    eat_assert!(self, TK_OR);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Or,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_AND => {
                    eat_assert!(self, TK_AND);
                    Box::new(Expr::Binary(
                        result,
                        Operator::And,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_EQ => {
                    eat_assert!(self, TK_EQ);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Equals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_NE => {
                    eat_assert!(self, TK_NE);
                    Box::new(Expr::Binary(
                        result,
                        Operator::NotEquals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_IS => {
                    eat_assert!(self, TK_IS);

                    let not = match self.peek_no_eof()?.token_type {
                        TK_NOT => {
                            eat_assert!(self, TK_NOT);
                            true
                        }
                        _ => false,
                    };

                    let op = match self.peek_no_eof()?.token_type {
                        TK_DISTINCT => {
                            eat_assert!(self, TK_DISTINCT);
                            eat_expect!(self, TK_FROM);
                            if not {
                                Operator::Is
                            } else {
                                Operator::IsNot
                            }
                        }
                        _ => {
                            if not {
                                Operator::IsNot
                            } else {
                                Operator::Is
                            }
                        }
                    };

                    Box::new(Expr::Binary(result, op, self.parse_expr(pre + 1)?))
                }
                TK_BETWEEN => {
                    eat_assert!(self, TK_BETWEEN);
                    let start = self.parse_expr(pre)?;
                    let start_height = self.last_expr_height;
                    eat_expect!(self, TK_AND);
                    // Use pre + 1 so that same-precedence operators (like IS NOT NULL)
                    // bind to the whole BETWEEN expression, not just the end value
                    let end = self.parse_expr(pre + 1)?;
                    self.last_expr_height = start_height.max(self.last_expr_height);
                    Box::new(Expr::Between {
                        lhs: result,
                        not,
                        start,
                        end,
                    })
                }
                TK_IN => {
                    eat_assert!(self, TK_IN);
                    let tok = self.peek_no_eof()?;
                    match tok.token_type {
                        TK_LP => {
                            eat_assert!(self, TK_LP);
                            let tok = self.peek_no_eof()?;
                            match tok.token_type {
                                TK_SELECT | TK_WITH | TK_VALUES => {
                                    let select = self.parse_select()?;
                                    eat_expect!(self, TK_RP);
                                    // The subquery is compiled separately, so it
                                    // counts as a leaf for this expression's height.
                                    self.last_expr_height = 1;
                                    Box::new(Expr::InSelect {
                                        lhs: result,
                                        not,
                                        rhs: select,
                                    })
                                }
                                _ => {
                                    let exprs = self.parse_expr_list()?;
                                    eat_expect!(self, TK_RP);
                                    // Expressions in the form:
                                    // lhs IN ()
                                    // lhs NOT IN ()
                                    // can be simplified to constants 0 (false) and 1 (true), respectively.
                                    //
                                    // todo: should check if lhs has a function. If so, this optimization cannot
                                    // be done.
                                    if exprs.is_empty() {
                                        let name = if not { "1" } else { "0" };
                                        // Simplified to a constant leaf.
                                        leaf = true;
                                        Box::new(Expr::Literal(Literal::Numeric(name.into())))
                                    } else if exprs.len() == 1 && is_bare_subquery(&exprs[0]) {
                                        // `x IN ((SELECT ...))` is subquery membership,
                                        // the same as `x IN (SELECT ...)`: an empty
                                        // subquery yields 0/1, not NULL. This matches
                                        // SQLite. A list of two or more values, or a
                                        // subquery embedded in a larger expression, stays
                                        // a value list.
                                        self.last_expr_height = 1;
                                        Box::new(Expr::InSelect {
                                            lhs: result,
                                            not,
                                            rhs: into_bare_subquery(
                                                exprs.into_iter().next().expect("one element"),
                                            ),
                                        })
                                    } else {
                                        Box::new(Expr::InList {
                                            lhs: result,
                                            rhs: exprs,
                                            not,
                                        })
                                    }
                                }
                            }
                        }
                        _ => {
                            let name = self.parse_fullname(false)?;
                            let mut exprs = vec![];
                            if let Some(tok) = self.peek()? {
                                if tok.token_type == TK_LP {
                                    eat_assert!(self, TK_LP);
                                    exprs = self.parse_expr_list()?;
                                    eat_expect!(self, TK_RP);
                                }
                            }

                            Box::new(Expr::InTable {
                                lhs: result,
                                not,
                                rhs: name,
                                args: exprs,
                            })
                        }
                    }
                }
                TK_MATCH | TK_LIKE_KW => {
                    let tok = eat_assert!(self, TK_MATCH, TK_LIKE_KW);
                    let op = match tok.token_type {
                        TK_MATCH => LikeOperator::Match,
                        TK_LIKE_KW => match_ignore_ascii_case!(match tok.value {
                            b"LIKE" => LikeOperator::Like,
                            b"GLOB" => LikeOperator::Glob,
                            b"REGEXP" => LikeOperator::Regexp,
                            _ => unreachable!(),
                        }),
                        _ => unreachable!(),
                    };

                    // Use pre + 1 so that same-precedence operators (like IS NOT NULL)
                    // bind to the whole LIKE expression, not just the pattern
                    let expr = self.parse_expr(pre + 1)?;
                    let rhs_height = self.last_expr_height;
                    let escape = if let Some(tok) = self.peek()? {
                        if tok.token_type == TK_ESCAPE {
                            eat_assert!(self, TK_ESCAPE);
                            let escape = self.parse_expr(pre + 1)?;
                            self.last_expr_height = rhs_height.max(self.last_expr_height);
                            Some(escape)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    Box::new(Expr::Like {
                        lhs: result,
                        not,
                        op,
                        rhs: expr,
                        escape,
                    })
                }
                TK_ISNULL => {
                    eat_assert!(self, TK_ISNULL);
                    Box::new(Expr::IsNull(result))
                }
                TK_NOTNULL => {
                    eat_assert!(self, TK_NOTNULL);
                    Box::new(Expr::NotNull(result))
                }
                TK_LT => {
                    eat_assert!(self, TK_LT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Less,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_GT => {
                    eat_assert!(self, TK_GT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Greater,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_LE => {
                    eat_assert!(self, TK_LE);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LessEquals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_GE => {
                    eat_assert!(self, TK_GE);
                    Box::new(Expr::Binary(
                        result,
                        Operator::GreaterEquals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_ESCAPE => unreachable!(),
                TK_BITAND => {
                    eat_assert!(self, TK_BITAND);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseAnd,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_BITOR => {
                    eat_assert!(self, TK_BITOR);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseOr,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_LSHIFT => {
                    eat_assert!(self, TK_LSHIFT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LeftShift,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_RSHIFT => {
                    eat_assert!(self, TK_RSHIFT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::RightShift,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_PLUS => {
                    eat_assert!(self, TK_PLUS);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Add,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_MINUS => {
                    eat_assert!(self, TK_MINUS);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Subtract,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_STAR => {
                    eat_assert!(self, TK_STAR);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Multiply,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_SLASH => {
                    eat_assert!(self, TK_SLASH);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Divide,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_REM => {
                    eat_assert!(self, TK_REM);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Modulus,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_ARRAY_CONTAINS => {
                    eat_assert!(self, TK_ARRAY_CONTAINS);
                    Box::new(Expr::Binary(
                        result,
                        Operator::ArrayContains,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_ARRAY_OVERLAP => {
                    eat_assert!(self, TK_ARRAY_OVERLAP);
                    Box::new(Expr::Binary(
                        result,
                        Operator::ArrayOverlap,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_CONCAT => {
                    eat_assert!(self, TK_CONCAT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Concat,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_PTR => {
                    let tok = eat_assert!(self, TK_PTR);
                    let op = if tok.value.len() == 2 {
                        Operator::ArrowRight
                    } else {
                        Operator::ArrowRightShift
                    };

                    Box::new(Expr::Binary(result, op, self.parse_expr(pre + 1)?))
                }
                TK_COLLATE => Box::new(Expr::Collate(result, self.parse_collate()?.unwrap())),
                TK_LBRACKET => {
                    eat_assert!(self, TK_LBRACKET);
                    let first = self.parse_expr(0)?;
                    let first_height = self.last_expr_height;
                    // Slice syntax: expr[start:end]
                    if self.peek()?.is_some_and(|t| t.token_type == TK_COLON) {
                        eat_assert!(self, TK_COLON);
                        let second = self.parse_expr(0)?;
                        self.last_expr_height = first_height.max(self.last_expr_height);
                        eat_expect!(self, TK_RBRACKET);
                        // Desugar to array_slice(expr, start, end)
                        Box::new(Expr::FunctionCall {
                            name: Name::from_bytes(b"array_slice"),
                            distinctness: None,
                            args: vec![result, first, second],
                            order_by: vec![],
                            within_group: vec![],
                            filter_over: FunctionTail {
                                filter_clause: None,
                                over_clause: None,
                            },
                        })
                    } else {
                        // Desugar expr[index] into array_element(expr, index)
                        eat_expect!(self, TK_RBRACKET);
                        Box::new(Expr::FunctionCall {
                            name: Name::from_bytes(b"array_element"),
                            distinctness: None,
                            args: vec![result, first],
                            order_by: vec![],
                            within_group: vec![],
                            filter_over: FunctionTail {
                                filter_clause: None,
                                over_clause: None,
                            },
                        })
                    }
                }
                _ => unreachable!(),
            };
            // Each iteration wraps the previous `result` in a new node, growing
            // the tree by one level; `last_expr_height` holds the height of the
            // tallest sub-expression parsed in this iteration.
            result_height = if leaf {
                1
            } else {
                1 + result_height.max(self.last_expr_height)
            };
            if result_height > MAX_EXPR_DEPTH {
                return Err(Error::ParseError(format!(
                    "Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})"
                )));
            }
        }

        self.last_expr_height = result_height;
        Ok(result)
    }

    fn parse_collate(&mut self) -> Result<Option<Name>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_COLLATE {
                eat_assert!(self, TK_COLLATE);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        if self.peek()?.is_some_and(|t| t.token_type == TK_LBRACKET) {
            return Ok(Some(self.parse_bracket_quoted_name()?));
        }
        let tok = eat_expect!(self, TK_ID, TK_STRING);
        Ok(Some(Name::from_bytes(tok.as_bytes())))
    }

    fn parse_sort_order(&mut self) -> Result<Option<SortOrder>> {
        match self.peek()? {
            Some(tok) if tok.token_type == TK_ASC => {
                eat_assert!(self, TK_ASC);
                Ok(Some(SortOrder::Asc))
            }
            Some(tok) if tok.token_type == TK_DESC => {
                eat_assert!(self, TK_DESC);
                Ok(Some(SortOrder::Desc))
            }
            _ => Ok(None),
        }
    }

    fn parse_eid(&mut self) -> Result<IndexedColumn> {
        let nm = self.parse_nm()?;
        let collate = self.parse_collate()?;
        let sort_order = self.parse_sort_order()?;
        Ok(IndexedColumn {
            col_name: nm,
            collation_name: collate,
            order: sort_order,
        })
    }

    fn parse_eid_list(&mut self, check_dup: bool) -> Result<Vec<IndexedColumn>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_LP {
                eat_assert!(self, TK_LP);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        let mut columns = vec![self.parse_eid()?];
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    columns.push(self.parse_eid()?);

                    // check dup
                    if check_dup {
                        let las_col = columns.last().unwrap();
                        for col in &columns[..columns.len() - 1] {
                            if col.col_name == las_col.col_name {
                                return Err(Error::Custom(format!(
                                    "duplicate column name: {}",
                                    col.col_name
                                )));
                            }
                        }
                    }
                }
                _ => break,
            }
        }
        eat_expect!(self, TK_RP);

        Ok(columns)
    }

    fn parse_common_table_expr(&mut self) -> Result<CommonTableExpr> {
        let nm = self.parse_nm()?;
        let eid_list = self.parse_eid_list(false)?;
        eat_expect!(self, TK_AS);
        let wqas = match self.peek_no_eof()?.token_type {
            TK_MATERIALIZED => {
                eat_assert!(self, TK_MATERIALIZED);
                Materialized::Yes
            }
            TK_NOT => {
                eat_assert!(self, TK_NOT);
                eat_expect!(self, TK_MATERIALIZED);
                Materialized::No
            }
            _ => Materialized::Any,
        };
        eat_expect!(self, TK_LP);
        let select = self.parse_select()?;
        eat_expect!(self, TK_RP);
        Ok(CommonTableExpr {
            tbl_name: nm,
            columns: eid_list,
            materialized: wqas,
            select,
        })
    }

    fn parse_with(&mut self) -> Result<Option<With>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_WITH {
                eat_assert!(self, TK_WITH);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let recursive = if self.peek_no_eof()?.token_type == TK_RECURSIVE {
            eat_assert!(self, TK_RECURSIVE);
            true
        } else {
            false
        };

        let mut ctes = vec![self.parse_common_table_expr()?];

        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    let cte = self.parse_common_table_expr()?;
                    if ctes.iter().any(|existing| {
                        existing
                            .tbl_name
                            .as_str()
                            .eq_ignore_ascii_case(cte.tbl_name.as_str())
                    }) {
                        return Err(Error::Custom(format!(
                            "duplicate WITH table name: {}",
                            cte.tbl_name
                        )));
                    }
                    ctes.push(cte);
                }
                _ => break,
            }
        }

        Ok(Some(With { recursive, ctes }))
    }

    fn parse_as(&mut self) -> Result<Option<As>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.fallback_id_if_ok() {
                TK_AS => {
                    eat_assert!(self, TK_AS);
                    Ok(Some(As::As(self.parse_nm()?)))
                }
                TK_STRING | TK_ID | TK_LBRACKET => Ok(Some(As::Elided(self.parse_nm()?))),
                _ => Ok(None),
            },
        }
    }

    fn parse_window_defn(&mut self) -> Result<WindowDef> {
        let name = self.parse_nm()?;
        eat_expect!(self, TK_AS);
        eat_expect!(self, TK_LP);
        let window = self.parse_window()?;
        eat_expect!(self, TK_RP);
        Ok(WindowDef { name, window })
    }

    fn parse_window_clause(&mut self) -> Result<Vec<WindowDef>> {
        match self.peek()? {
            None => return Ok(vec![]),
            Some(tok) => match tok.token_type {
                TK_WINDOW => {
                    eat_assert!(self, TK_WINDOW);
                }
                _ => return Ok(vec![]),
            },
        }

        let mut result = vec![self.parse_window_defn()?];
        while let Some(tok) = self.peek()? {
            match tok.token_type {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    result.push(self.parse_window_defn()?);
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn parse_group_by(&mut self) -> Result<Option<GroupBy>> {
        let has_group_by = match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type {
                TK_GROUP => {
                    eat_assert!(self, TK_GROUP);
                    eat_expect!(self, TK_BY);
                    true
                }
                TK_HAVING => false,
                _ => return Ok(None),
            },
        };

        let exprs = if has_group_by {
            self.parse_nexpr_list()?
        } else {
            vec![]
        };

        let having = match self.peek()? {
            Some(tok) if tok.token_type == TK_HAVING => {
                eat_assert!(self, TK_HAVING);
                Some(self.parse_expr(0)?)
            }
            _ => None,
        };

        if !exprs.is_empty() || having.is_some() {
            Ok(Some(GroupBy { exprs, having }))
        } else {
            Ok(None)
        }
    }

    fn parse_where(&mut self) -> Result<Option<Box<Expr>>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type {
                TK_WHERE => {
                    eat_assert!(self, TK_WHERE);
                    let expr = self.parse_expr(0)?;
                    Ok(Some(expr))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_indexed(&mut self) -> Result<Option<Indexed>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type {
                TK_INDEXED => {
                    eat_assert!(self, TK_INDEXED);
                    eat_expect!(self, TK_BY);
                    Ok(Some(Indexed::IndexedBy(self.parse_nm()?)))
                }
                TK_NOT => {
                    eat_assert!(self, TK_NOT);
                    eat_expect!(self, TK_INDEXED);
                    Ok(Some(Indexed::NotIndexed))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_nm_list(&mut self) -> Result<Vec<Name>> {
        let mut names = vec![self.parse_nm()?];

        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    names.push(self.parse_nm()?);
                }
                _ => break,
            }
        }

        Ok(names)
    }

    fn parse_nm_list_opt(&mut self) -> Result<Vec<Name>> {
        match self.peek()? {
            Some(tok) if tok.token_type == TK_LP => {
                eat_assert!(self, TK_LP);
            }
            _ => return Ok(vec![]),
        }

        let result = self.parse_nm_list()?;
        eat_expect!(self, TK_RP);
        Ok(result)
    }

    fn parse_on_using(&mut self) -> Result<Option<JoinConstraint>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type {
                TK_ON => {
                    eat_assert!(self, TK_ON);
                    let expr = self.parse_expr(0)?;
                    Ok(Some(JoinConstraint::On(expr)))
                }
                TK_USING => {
                    eat_assert!(self, TK_USING);
                    eat_expect!(self, TK_LP);
                    let names = self.parse_nm_list()?;
                    eat_expect!(self, TK_RP);
                    Ok(Some(JoinConstraint::Using(names)))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_joined_tables(&mut self) -> Result<Vec<JoinedSelectTable>> {
        let mut result = vec![];
        while let Some(tok) = self.peek()? {
            let op = match tok.token_type {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    JoinOperator::Comma
                }
                TK_JOIN => {
                    eat_assert!(self, TK_JOIN);
                    JoinOperator::TypedJoin(None)
                }
                TK_JOIN_KW => {
                    let jkw = eat_assert!(self, TK_JOIN_KW);
                    let tok = eat_expect!(self, TK_JOIN, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW);

                    match tok.token_type {
                        TK_JOIN => JoinOperator::TypedJoin(Some(new_join_type(
                            jkw.as_bytes(),
                            None,
                            None,
                        )?)),
                        _ => {
                            let name_1 = tok.value;
                            let tok = eat_expect!(
                                self, TK_JOIN, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW,
                            );

                            match tok.token_type {
                                TK_JOIN => JoinOperator::TypedJoin(Some(new_join_type(
                                    jkw.as_bytes(),
                                    Some(name_1),
                                    None,
                                )?)),
                                _ => {
                                    let name_2 = tok.value;
                                    eat_expect!(self, TK_JOIN);
                                    JoinOperator::TypedJoin(Some(new_join_type(
                                        jkw.as_bytes(),
                                        Some(name_1),
                                        Some(name_2),
                                    )?))
                                }
                            }
                        }
                    }
                }
                _ => break,
            };

            let tok = peek_expect!(
                self,
                TK_ID,
                TK_STRING,
                TK_INDEXED,
                TK_JOIN_KW,
                TK_LP,
                TK_LBRACKET
            );

            match tok.token_type.fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_LBRACKET => {
                    let name = self.parse_fullname(false)?;
                    match self.peek()? {
                        None => {
                            result.push(JoinedSelectTable {
                                operator: op,
                                table: Box::new(SelectTable::Table(name, None, None)),
                                constraint: None,
                            });
                        }
                        Some(tok) => match tok.token_type {
                            TK_LP => {
                                eat_assert!(self, TK_LP);
                                let exprs = self.parse_expr_list()?;
                                eat_expect!(self, TK_RP);
                                let alias = self.parse_as()?;
                                let on_using = self.parse_on_using()?;
                                result.push(JoinedSelectTable {
                                    operator: op,
                                    table: Box::new(SelectTable::TableCall(name, exprs, alias)),
                                    constraint: on_using,
                                });
                            }
                            _ => {
                                let alias = self.parse_as()?;
                                let indexed = self.parse_indexed()?;
                                let on_using = self.parse_on_using()?;
                                result.push(JoinedSelectTable {
                                    operator: op,
                                    table: Box::new(SelectTable::Table(name, alias, indexed)),
                                    constraint: on_using,
                                });
                            }
                        },
                    }
                }
                TK_LP => {
                    eat_assert!(self, TK_LP);
                    match self.peek_no_eof()?.token_type {
                        TK_SELECT | TK_WITH | TK_VALUES => {
                            let select = self.parse_select()?;
                            eat_expect!(self, TK_RP);
                            let alias = self.parse_as()?;
                            let on_using = self.parse_on_using()?;
                            result.push(JoinedSelectTable {
                                operator: op,
                                table: Box::new(SelectTable::Select(select, alias)),
                                constraint: on_using,
                            });
                        }
                        _ => {
                            let fr = self.parse_from_clause()?;
                            eat_expect!(self, TK_RP);
                            let alias = self.parse_as()?;
                            let on_using = self.parse_on_using()?;
                            result.push(JoinedSelectTable {
                                operator: op,
                                table: Box::new(SelectTable::Sub(fr, alias)),
                                constraint: on_using,
                            });
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        Ok(result)
    }

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        let tok = peek_expect!(
            self,
            TK_ID,
            TK_STRING,
            TK_INDEXED,
            TK_JOIN_KW,
            TK_LP,
            TK_LBRACKET
        );

        match tok.token_type.fallback_id_if_ok() {
            TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_LBRACKET => {
                let name = self.parse_fullname(false)?;
                match self.peek()? {
                    None => Ok(FromClause {
                        select: Box::new(SelectTable::Table(name, None, None)),
                        joins: vec![],
                    }),
                    Some(tok) => match tok.token_type {
                        TK_LP => {
                            eat_assert!(self, TK_LP);
                            let exprs = self.parse_expr_list()?;
                            eat_expect!(self, TK_RP);
                            let alias = self.parse_as()?;
                            Ok(FromClause {
                                select: Box::new(SelectTable::TableCall(name, exprs, alias)),
                                joins: self.parse_joined_tables()?,
                            })
                        }
                        _ => {
                            let alias = self.parse_as()?;
                            let indexed = self.parse_indexed()?;
                            Ok(FromClause {
                                select: Box::new(SelectTable::Table(name, alias, indexed)),
                                joins: self.parse_joined_tables()?,
                            })
                        }
                    },
                }
            }
            TK_LP => {
                eat_assert!(self, TK_LP);
                match self.peek_no_eof()?.token_type {
                    TK_SELECT | TK_WITH | TK_VALUES => {
                        let select = self.parse_select()?;
                        eat_expect!(self, TK_RP);
                        let alias = self.parse_as()?;
                        Ok(FromClause {
                            select: Box::new(SelectTable::Select(select, alias)),
                            joins: self.parse_joined_tables()?,
                        })
                    }
                    _ => {
                        let fr = self.parse_from_clause()?;
                        eat_expect!(self, TK_RP);
                        let alias = self.parse_as()?;
                        Ok(FromClause {
                            select: Box::new(SelectTable::Sub(fr, alias)),
                            joins: self.parse_joined_tables()?,
                        })
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn parse_from_clause_opt(&mut self) -> Result<Option<FromClause>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) if tok.token_type == TK_FROM => {
                eat_assert!(self, TK_FROM);
            }
            _ => return Ok(None),
        }

        Ok(Some(self.parse_from_clause()?))
    }

    fn parse_select_column(&mut self) -> Result<ResultColumn> {
        match self.peek_no_eof()?.token_type.fallback_id_if_ok() {
            TK_STAR => {
                eat_assert!(self, TK_STAR);
                Ok(ResultColumn::Star)
            }
            tt => {
                // dot STAR case
                if tt == TK_ID
                    || tt == TK_STRING
                    || tt == TK_INDEXED
                    || tt == TK_JOIN_KW
                    || tt == TK_LBRACKET
                {
                    if let Ok(res) = self.mark(|p| -> Result<ResultColumn> {
                        let name = p.parse_nm()?;
                        eat_expect!(p, TK_DOT);
                        eat_expect!(p, TK_STAR);
                        Ok(ResultColumn::TableStar(name))
                    }) {
                        return Ok(res);
                    }
                }

                let expr_start = self.offset();
                let expr = self.parse_expr(0)?;
                let expr_end = self.offset();
                let alias = self.parse_as()?;
                // When there is no explicit AS alias, use the original SQL
                // text of the expression as an implicit column name. This
                // matches SQLite's behavior of preserving the verbatim
                // expression text as the column name.
                let alias = alias.or_else(|| {
                    let text = std::str::from_utf8(&self.lexer.input[expr_start..expr_end])
                        .ok()?
                        .trim();
                    if text.is_empty() {
                        None
                    } else {
                        Some(As::ImplicitColumnName(Name::exact(text.to_string())))
                    }
                });
                Ok(ResultColumn::Expr(expr, alias))
            }
        }
    }

    fn parse_select_columns(&mut self) -> Result<Vec<ResultColumn>> {
        let mut result = vec![self.parse_select_column()?];

        while let Some(tok) = self.peek()? {
            if tok.token_type == TK_COMMA {
                eat_assert!(self, TK_COMMA);
            } else {
                break;
            }

            result.push(self.parse_select_column()?);
        }

        Ok(result)
    }

    #[allow(clippy::vec_box)]
    fn parse_nexpr_list(&mut self) -> Result<Vec<Box<Expr>>> {
        let first = self.parse_expr(0)?;
        // Tallest element, reported via `last_expr_height` for the caller.
        let mut max_h = self.last_expr_height;
        let mut result = vec![first];
        while let Some(tok) = self.peek()? {
            if tok.token_type == TK_COMMA {
                eat_assert!(self, TK_COMMA);
            } else {
                break;
            }

            result.push(self.parse_expr(0)?);
            max_h = max_h.max(self.last_expr_height);
        }

        self.last_expr_height = max_h;
        Ok(result)
    }

    fn parse_one_select(&mut self) -> Result<OneSelect> {
        let tok = eat_expect!(self, TK_SELECT, TK_VALUES);
        match tok.token_type {
            TK_SELECT => {
                let distinct = self.parse_distinct()?;
                let collist = self.parse_select_columns()?;
                let from = self.parse_from_clause_opt()?;
                let where_clause = self.parse_where()?;
                let group_by = self.parse_group_by()?;
                let window_clause = self.parse_window_clause()?;
                Ok(OneSelect::Select {
                    distinctness: distinct,
                    columns: collist,
                    from,
                    where_clause,
                    group_by,
                    window_clause,
                })
            }
            TK_VALUES => {
                eat_expect!(self, TK_LP);
                let mut values = vec![self.parse_nexpr_list()?];
                eat_expect!(self, TK_RP);

                while let Some(tok) = self.peek()? {
                    if tok.token_type == TK_COMMA {
                        eat_assert!(self, TK_COMMA);
                    } else {
                        break;
                    }

                    eat_expect!(self, TK_LP);
                    values.push(self.parse_nexpr_list()?);
                    if values.last().unwrap().len() != values[0].len() {
                        return Err(Error::Custom(
                            "all VALUES must have the same number of terms".to_owned(),
                        ));
                    }

                    eat_expect!(self, TK_RP);
                }

                Ok(OneSelect::Values(values))
            }
            _ => unreachable!(),
        }
    }

    fn parse_select_body(&mut self) -> Result<SelectBody> {
        let select = self.parse_one_select()?;
        let mut compounds = vec![];
        while let Some(tok) = self.peek()? {
            let op = match tok.token_type {
                TK_UNION => {
                    eat_assert!(self, TK_UNION);
                    if self.peek_no_eof()?.token_type == TK_ALL {
                        eat_assert!(self, TK_ALL);
                        CompoundOperator::UnionAll
                    } else {
                        CompoundOperator::Union
                    }
                }
                TK_EXCEPT => {
                    eat_assert!(self, TK_EXCEPT);
                    CompoundOperator::Except
                }
                TK_INTERSECT => {
                    eat_assert!(self, TK_INTERSECT);
                    CompoundOperator::Intersect
                }
                _ => break,
            };

            compounds.push(CompoundSelect {
                operator: op,
                select: self.parse_one_select()?,
            });
        }

        Ok(SelectBody { select, compounds })
    }

    fn parse_sorted_column(&mut self) -> Result<SortedColumn> {
        let expr = self.parse_expr(0)?;
        let sort_order = self.parse_sort_order()?;

        let nulls = match self.peek()? {
            Some(tok) if tok.token_type == TK_NULLS => {
                eat_assert!(self, TK_NULLS);
                let tok = eat_expect!(self, TK_FIRST, TK_LAST);
                match tok.token_type {
                    TK_FIRST => Some(NullsOrder::First),
                    TK_LAST => Some(NullsOrder::Last),
                    _ => unreachable!(),
                }
            }
            _ => None,
        };

        Ok(SortedColumn {
            expr,
            order: sort_order,
            nulls,
        })
    }

    fn parse_sort_list(&mut self) -> Result<Vec<SortedColumn>> {
        let mut columns = vec![self.parse_sorted_column()?];
        // Tallest sort expression, reported via `last_expr_height` so callers that
        // fold these clauses into their own height (function calls) see them.
        let mut max_h = self.last_expr_height;
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    columns.push(self.parse_sorted_column()?);
                    max_h = max_h.max(self.last_expr_height);
                }
                _ => break,
            }
        }

        self.last_expr_height = max_h;
        Ok(columns)
    }

    fn parse_order_by(&mut self) -> Result<Vec<SortedColumn>> {
        // No sort expressions parsed yet: report height 0 so callers folding this
        // clause in do not pick up a stale height from an earlier expression.
        self.last_expr_height = 0;
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_ORDER {
                eat_assert!(self, TK_ORDER);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        eat_expect!(self, TK_BY);
        self.parse_sort_list()
    }

    fn parse_limit(&mut self) -> Result<Option<Limit>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_LIMIT {
                eat_assert!(self, TK_LIMIT);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let expr = self.parse_expr(0)?;
        let (limit, offset) = match self.peek()? {
            Some(tok) => match tok.token_type {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    let offset = expr;
                    let limit = self.parse_expr(0)?;
                    (limit, Some(offset))
                }
                TK_OFFSET => {
                    eat_assert!(self, TK_OFFSET);
                    let limit = expr;
                    let offset = self.parse_expr(0)?;
                    (limit, Some(offset))
                }
                _ => (expr, None),
            },
            _ => (expr, None),
        };

        Ok(Some(Limit {
            expr: limit,
            offset,
        }))
    }

    fn parse_select_without_cte(&mut self, with: Option<With>) -> Result<Select> {
        let body = self.parse_select_body()?;
        let order_by = self.parse_order_by()?;
        let limit = self.parse_limit()?;
        if !order_by.is_empty() || limit.is_some() {
            if let Some(tok) = self.peek()? {
                let op_name = match tok.token_type {
                    TK_UNION => {
                        eat_assert!(self, TK_UNION);
                        match self.peek()? {
                            Some(tok) if tok.token_type == TK_ALL => "UNION ALL",
                            _ => "UNION",
                        }
                    }
                    TK_EXCEPT => "EXCEPT",
                    TK_INTERSECT => "INTERSECT",
                    _ => "",
                };
                if !op_name.is_empty() {
                    let clause = if order_by.is_empty() {
                        "LIMIT"
                    } else {
                        "ORDER BY"
                    };
                    return Err(Error::Custom(format!(
                        "{clause} clause should come after {op_name} not before"
                    )));
                }
            }
        }
        Ok(Select {
            with,
            body,
            order_by,
            limit,
        })
    }

    fn parse_select(&mut self) -> Result<Select> {
        let with = self.parse_with()?;
        self.parse_select_without_cte(with)
    }

    fn parse_primary_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_PRIMARY);
        eat_expect!(self, TK_KEY);
        eat_expect!(self, TK_LP);
        let columns = self.parse_sort_list()?;
        let auto_increment = self.parse_auto_increment()?;
        eat_expect!(self, TK_RP);
        let conflict_clause = self.parse_on_conflict()?;
        Ok(TableConstraint::PrimaryKey {
            columns,
            auto_increment,
            conflict_clause,
        })
    }

    fn parse_unique_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_UNIQUE);
        eat_expect!(self, TK_LP);
        let columns = self.parse_sort_list()?;
        eat_expect!(self, TK_RP);
        let conflict_clause = self.parse_on_conflict()?;
        Ok(TableConstraint::Unique {
            columns,
            conflict_clause,
        })
    }

    fn parse_check_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_CHECK);
        eat_expect!(self, TK_LP);
        let start = self.offset();
        let expr = self.parse_expr(0)?;
        let source = self.check_constraint_source(start);
        eat_expect!(self, TK_RP);
        Ok(TableConstraint::Check { expr, source })
    }

    /// The text between a CHECK constraint's parens exactly as the user wrote
    /// it, whitespace-trimmed, the way SQLite keeps it for constraint error
    /// messages. `start` is the offset right after the opening paren; the
    /// expression must already be parsed so the closing paren is the peeked
    /// token.
    fn check_constraint_source(&self, start: usize) -> Option<String> {
        let end = self.offset();
        let raw = self.lexer.input.get(start..end)?;
        let text = std::str::from_utf8(raw).ok()?;
        Some(
            text.trim_matches(|c: char| c.is_ascii_whitespace())
                .to_owned(),
        )
    }

    fn parse_foreign_key_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_FOREIGN);
        eat_expect!(self, TK_KEY);
        peek_expect!(self, TK_LP); // make sure we have columns
        let columns = self.parse_eid_list(false)?;
        peek_expect!(self, TK_REFERENCES);
        let clause = self.parse_foreign_key_clause()?;
        let defer_clause = self.parse_defer_subclause()?;
        Ok(TableConstraint::ForeignKey {
            columns,
            clause,
            defer_clause,
        })
    }

    fn parse_named_table_constraints(&mut self) -> Result<Vec<NamedTableConstraint>> {
        let mut result = vec![];

        while let Some(tok) = self.peek()? {
            match tok.token_type {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                }
                TK_CONSTRAINT | TK_PRIMARY | TK_UNIQUE | TK_CHECK | TK_FOREIGN => {}
                _ => break,
            }

            let name = match self.peek_no_eof()?.token_type {
                TK_CONSTRAINT => {
                    eat_assert!(self, TK_CONSTRAINT);
                    Some(self.parse_nm()?)
                }
                _ => None,
            };

            let tok = peek_expect!(self, TK_PRIMARY, TK_UNIQUE, TK_CHECK, TK_FOREIGN);

            match tok.token_type {
                TK_PRIMARY => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_primary_table_constraint()?,
                    });
                }
                TK_UNIQUE => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_unique_table_constraint()?,
                    });
                }
                TK_CHECK => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_check_table_constraint()?,
                    });
                }
                TK_FOREIGN => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_foreign_key_table_constraint()?,
                    });
                }
                _ => unreachable!(),
            }
        }

        Ok(result)
    }

    fn parse_table_option(&mut self, options: &mut TableOptions) -> Result<()> {
        match self.peek()? {
            Some(tok) => match tok.token_type.fallback_id_if_ok() {
                TK_WITHOUT => {
                    let without_tok = eat_assert!(self, TK_WITHOUT);
                    let rowid_tok = eat_expect!(self, TK_ID);
                    match_ignore_ascii_case!(match rowid_tok.value {
                        b"ROWID" => {
                            // Preserve the original text "WITHOUT ROWID" with original capitalization
                            let without_text = from_bytes(without_tok.as_bytes());
                            let rowid_text = from_bytes(rowid_tok.as_bytes());
                            options.without_rowid_text =
                                Some(format!("{without_text} {rowid_text}"));
                            Ok(())
                        }
                        _ => Err(Error::Custom(format!(
                            "unknown table option: {} {}",
                            from_bytes(without_tok.as_bytes()),
                            from_bytes(rowid_tok.as_bytes())
                        ))),
                    })
                }
                TK_ID => {
                    let tok = eat_assert!(self, TK_ID);
                    match_ignore_ascii_case!(match tok.value {
                        b"STRICT" => {
                            // Preserve the original text "STRICT" with original capitalization
                            options.strict_text = Some(from_bytes(tok.as_bytes()));
                            Ok(())
                        }
                        _ => Err(Error::Custom(format!(
                            "unknown table option: {}",
                            from_bytes(tok.as_bytes())
                        ))),
                    })
                }
                _ => Ok(()),
            },
            _ => Ok(()),
        }
    }

    fn parse_table_options(&mut self) -> Result<TableOptions> {
        let mut result = TableOptions::empty();
        self.parse_table_option(&mut result)?;
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    self.parse_table_option(&mut result)?;
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn parse_create_table_args(&mut self, tbl_name: &QualifiedName) -> Result<CreateTableBody> {
        let tok = eat_expect!(self, TK_LP, TK_AS);

        match tok.token_type {
            TK_AS => Ok(CreateTableBody::AsSelect(self.parse_select()?)),
            TK_LP => {
                let mut columns = vec![self.parse_column_definition(false)?];
                let mut generated_count = 0;
                let mut has_primary_key = false;
                for cs in &columns[0].constraints {
                    match cs.constraint {
                        ColumnConstraint::Generated { .. } => generated_count += 1,
                        ColumnConstraint::PrimaryKey { .. } => has_primary_key = true,
                        _ => {}
                    }
                }

                let mut constraints = vec![];
                loop {
                    match self.peek()? {
                        Some(tok) if tok.token_type == TK_COMMA => {
                            eat_assert!(self, TK_COMMA);
                            match self.peek_no_eof()?.token_type {
                                TK_CONSTRAINT | TK_PRIMARY | TK_UNIQUE | TK_CHECK | TK_FOREIGN => {
                                    constraints = self.parse_named_table_constraints()?;
                                    for cons in &constraints {
                                        if let TableConstraint::PrimaryKey { .. } = cons.constraint
                                        {
                                            has_primary_key = true;
                                            break;
                                        }
                                    }

                                    break;
                                }
                                _ => {
                                    columns.push(self.parse_column_definition(false)?);
                                    for cs in &columns.last().unwrap().constraints {
                                        match cs.constraint {
                                            ColumnConstraint::Generated { .. } => {
                                                generated_count += 1
                                            }
                                            ColumnConstraint::PrimaryKey { .. } => {
                                                has_primary_key = true
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                        _ => break,
                    }
                }

                // larger than or equal columns len means we have only generated columns
                if generated_count >= columns.len() {
                    return Err(Error::ParseError(
                        "must have at least one non-generated column".to_owned(),
                    ));
                }

                eat_expect!(self, TK_RP);
                let options = self.parse_table_options()?;

                // strict check: every column must have a datatype specified.
                // Type names are validated later in the translator to allow custom types.
                if options.contains_strict() {
                    for c in &columns {
                        if c.col_type.is_none() {
                            return Err(Error::Custom(format!(
                                "missing datatype for {}.{}",
                                tbl_name, c.col_name
                            )));
                        }
                    }
                }

                // primary key check
                if options.contains_without_rowid() && !has_primary_key {
                    return Err(Error::Custom(format!(
                        "PRIMARY KEY missing on table {tbl_name}"
                    )));
                }

                Ok(CreateTableBody::ColumnsAndConstraints {
                    columns,
                    constraints,
                    options,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_create_table(&mut self, temporary: bool) -> Result<Stmt> {
        eat_assert!(self, TK_TABLE);
        let if_not_exists = self.parse_if_not_exists()?;
        let tbl_name = self.parse_fullname(false)?;
        if temporary {
            if let Some(ref db_name) = tbl_name.db_name {
                if !db_name.as_str().eq_ignore_ascii_case("TEMP") {
                    return Err(Error::Custom(
                        "temporary table name must be unqualified".to_owned(),
                    ));
                }
            }
        }

        let body = self.parse_create_table_args(&tbl_name)?;
        Ok(Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        })
    }

    fn parse_analyze(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ANALYZE);
        let name = match self.peek()? {
            Some(tok) => match tok.token_type.fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_LBRACKET => {
                    Some(self.parse_fullname(false)?)
                }
                _ => None,
            },
            _ => None,
        };

        Ok(Stmt::Analyze { name })
    }

    fn parse_attach(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ATTACH);
        if self.peek_no_eof()?.token_type == TK_DATABASE {
            eat_assert!(self, TK_DATABASE);
        }

        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_AS);
        let db_name = self.parse_expr(0)?;
        let key = match self.peek()? {
            Some(tok) => match tok.token_type {
                TK_KEY => {
                    eat_assert!(self, TK_KEY);
                    Some(self.parse_expr(0)?)
                }
                _ => None,
            },
            _ => None,
        };

        Ok(Stmt::Attach { expr, db_name, key })
    }

    fn parse_detach(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_DETACH);
        if self.peek_no_eof()?.token_type == TK_DATABASE {
            eat_assert!(self, TK_DATABASE);
        }

        Ok(Stmt::Detach {
            name: self.parse_expr(0)?,
        })
    }

    fn parse_pragma_value(&mut self) -> Result<PragmaValue> {
        match self.peek_no_eof()?.token_type.fallback_id_if_ok() {
            TK_ON | TK_DELETE | TK_DEFAULT => {
                let tok = eat_assert!(self, TK_ON, TK_DELETE, TK_DEFAULT);
                Ok(Box::new(Expr::Literal(Literal::Keyword(from_bytes(
                    tok.as_bytes(),
                )))))
            }
            TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_LBRACKET => {
                Ok(Box::new(Expr::Name(self.parse_nm()?)))
            }
            tt if tt
                .as_str()
                .is_some_and(|s| s.chars().next().is_some_and(|c| c.is_ascii_alphabetic())) =>
            {
                // Accept any SQL keyword as a pragma value (e.g. PRAGMA journal_mode=OFF)
                let keyword = tt.as_str().unwrap().to_owned();
                self.eat()?; // consume the peeked token
                Ok(Box::new(Expr::Literal(Literal::Keyword(keyword))))
            }
            _ => self.parse_signed(),
        }
    }

    fn parse_pragma(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_PRAGMA);
        let name = self.parse_fullname(false)?;
        match self.peek()? {
            Some(tok) => match tok.token_type {
                TK_EQ => {
                    eat_assert!(self, TK_EQ);
                    Ok(Stmt::Pragma {
                        name,
                        body: Some(PragmaBody::Equals(self.parse_pragma_value()?)),
                    })
                }
                TK_LP => {
                    eat_assert!(self, TK_LP);
                    let value = self.parse_pragma_value()?;
                    eat_expect!(self, TK_RP);
                    Ok(Stmt::Pragma {
                        name,
                        body: Some(PragmaBody::Call(value)),
                    })
                }
                _ => Ok(Stmt::Pragma { name, body: None }),
            },
            _ => Ok(Stmt::Pragma { name, body: None }),
        }
    }

    fn parse_vacuum(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_VACUUM);

        let name = match self.peek()? {
            Some(tok) => match tok.token_type.fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_LBRACKET => Some(self.parse_nm()?),
                _ => None,
            },
            _ => None,
        };

        let into = match self.peek()? {
            Some(tok) if tok.token_type == TK_INTO => {
                eat_assert!(self, TK_INTO);
                Some(self.parse_expr(0)?)
            }
            _ => None,
        };

        Ok(Stmt::Vacuum { name, into })
    }

    fn parse_term(&mut self) -> Result<Box<Expr>> {
        peek_expect!(
            self,
            TK_NULL,
            TK_BLOB,
            TK_STRING,
            TK_FLOAT,
            TK_INTEGER,
            TK_CTIME_KW,
        );

        self.parse_expr_operand()
    }

    fn parse_default_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_DEFAULT);
        let tok = peek_expect!(
            self,
            TK_NULL,
            TK_BLOB,
            TK_STRING,
            TK_FLOAT,
            TK_INTEGER,
            TK_CTIME_KW,
            TK_LP,
            TK_PLUS,
            TK_MINUS,
            TK_ID,
            TK_INDEXED,
            TK_LBRACKET,
        );

        match tok.token_type {
            TK_LP => {
                eat_assert!(self, TK_LP);
                let expr = self.parse_expr(0)?;
                eat_expect!(self, TK_RP);
                Ok(ColumnConstraint::Default(Box::new(Expr::Parenthesized(
                    vec![expr],
                ))))
            }
            TK_PLUS => {
                eat_assert!(self, TK_PLUS);
                Ok(ColumnConstraint::Default(Box::new(Expr::Unary(
                    UnaryOperator::Positive,
                    self.parse_term()?,
                ))))
            }
            TK_MINUS => {
                eat_assert!(self, TK_MINUS);
                Ok(ColumnConstraint::Default(Box::new(Expr::Unary(
                    UnaryOperator::Negative,
                    self.parse_term()?,
                ))))
            }
            TK_NULL | TK_BLOB | TK_STRING | TK_FLOAT | TK_INTEGER | TK_CTIME_KW => {
                Ok(ColumnConstraint::Default(self.parse_term()?))
            }
            _ => {
                let name = self.parse_nm()?;
                let expr = if name.as_str().eq_ignore_ascii_case("true") {
                    Expr::Literal(Literal::True)
                } else if name.as_str().eq_ignore_ascii_case("false") {
                    Expr::Literal(Literal::False)
                } else {
                    Expr::Id(name)
                };
                Ok(ColumnConstraint::Default(Box::new(expr)))
            }
        }
    }

    fn parse_resolve_type(&mut self) -> Result<ResolveType> {
        match self.peek_no_eof()?.token_type {
            TK_IGNORE => {
                eat_assert!(self, TK_IGNORE);
                Ok(ResolveType::Ignore)
            }
            TK_REPLACE => {
                eat_assert!(self, TK_REPLACE);
                Ok(ResolveType::Replace)
            }
            _ => Ok(self.parse_raise_type()?),
        }
    }

    fn parse_on_conflict(&mut self) -> Result<Option<ResolveType>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type {
                TK_ON => {
                    eat_assert!(self, TK_ON);
                    eat_expect!(self, TK_CONFLICT);
                }
                _ => return Ok(None),
            },
        }

        Ok(Some(self.parse_resolve_type()?))
    }

    fn parse_or_conflict(&mut self) -> Result<Option<ResolveType>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type {
                TK_OR => {
                    eat_assert!(self, TK_OR);
                }
                _ => return Ok(None),
            },
        }

        Ok(Some(self.parse_resolve_type()?))
    }

    fn parse_auto_increment(&mut self) -> Result<bool> {
        match self.peek()? {
            None => Ok(false),
            Some(tok) => match tok.token_type {
                TK_AUTOINCR => {
                    eat_assert!(self, TK_AUTOINCR);
                    Ok(true)
                }
                _ => Ok(false),
            },
        }
    }

    fn parse_not_null_column_constraint(&mut self) -> Result<ColumnConstraint> {
        let has_not = match self.peek_no_eof()?.token_type {
            TK_NOT => {
                eat_assert!(self, TK_NOT);
                true
            }
            _ => false,
        };

        eat_expect!(self, TK_NULL);
        Ok(ColumnConstraint::NotNull {
            nullable: !has_not,
            conflict_clause: self.parse_on_conflict()?,
        })
    }

    fn parse_primary_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_PRIMARY);
        eat_expect!(self, TK_KEY);
        let sort_order = self.parse_sort_order()?;
        let conflict_clause = self.parse_on_conflict()?;
        let autoincr = self.parse_auto_increment()?;

        Ok(ColumnConstraint::PrimaryKey {
            order: sort_order,
            conflict_clause,
            auto_increment: autoincr,
        })
    }

    fn parse_unique_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_UNIQUE);
        Ok(ColumnConstraint::Unique(self.parse_on_conflict()?))
    }

    fn parse_check_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_CHECK);
        eat_expect!(self, TK_LP);
        let start = self.offset();
        let expr = self.parse_expr(0)?;
        let source = self.check_constraint_source(start);
        eat_expect!(self, TK_RP);
        Ok(ColumnConstraint::Check { expr, source })
    }

    fn parse_ref_act(&mut self) -> Result<RefAct> {
        let tok = eat_expect!(self, TK_SET, TK_CASCADE, TK_RESTRICT, TK_NO);

        match tok.token_type {
            TK_SET => {
                let tok = eat_expect!(self, TK_NULL, TK_DEFAULT);
                match tok.token_type {
                    TK_NULL => Ok(RefAct::SetNull),
                    TK_DEFAULT => Ok(RefAct::SetDefault),
                    _ => unreachable!(),
                }
            }
            TK_CASCADE => Ok(RefAct::Cascade),
            TK_RESTRICT => Ok(RefAct::Restrict),
            TK_NO => {
                eat_expect!(self, TK_ACTION);
                Ok(RefAct::NoAction)
            }
            _ => unreachable!(),
        }
    }

    fn parse_ref_args(&mut self) -> Result<Vec<RefArg>> {
        let mut result = vec![];

        while let Some(tok) = self.peek()? {
            match tok.token_type {
                TK_MATCH => {
                    eat_assert!(self, TK_MATCH);
                    result.push(RefArg::Match(self.parse_nm()?));
                }
                TK_ON => {
                    eat_assert!(self, TK_ON);
                    let tok = eat_expect!(self, TK_INSERT, TK_DELETE, TK_UPDATE);
                    match tok.token_type {
                        TK_INSERT => result.push(RefArg::OnInsert(self.parse_ref_act()?)),
                        TK_DELETE => result.push(RefArg::OnDelete(self.parse_ref_act()?)),
                        TK_UPDATE => result.push(RefArg::OnUpdate(self.parse_ref_act()?)),
                        _ => unreachable!(),
                    }
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn parse_foreign_key_clause(&mut self) -> Result<ForeignKeyClause> {
        eat_assert!(self, TK_REFERENCES);
        let name = self.parse_nm()?;
        let eid_list = self.parse_eid_list(false)?;
        let ref_args = self.parse_ref_args()?;
        Ok(ForeignKeyClause {
            tbl_name: name,
            columns: eid_list,
            args: ref_args,
        })
    }

    fn parse_defer_subclause(&mut self) -> Result<Option<DeferSubclause>> {
        let has_not = match self.peek()? {
            Some(tok) => match tok.token_type {
                TK_DEFERRABLE => false,
                TK_NOT => {
                    eat_assert!(self, TK_NOT);
                    true
                }
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        eat_expect!(self, TK_DEFERRABLE);

        let init = match self.peek()? {
            Some(tok) => match tok.token_type {
                TK_INITIALLY => {
                    eat_assert!(self, TK_INITIALLY);
                    let tok = eat_expect!(self, TK_DEFERRED, TK_IMMEDIATE);
                    match tok.token_type {
                        TK_DEFERRED => Some(InitDeferredPred::InitiallyDeferred),
                        TK_IMMEDIATE => Some(InitDeferredPred::InitiallyImmediate),
                        _ => unreachable!(),
                    }
                }
                _ => None,
            },
            _ => None,
        };

        Ok(Some(DeferSubclause {
            deferrable: !has_not,
            init_deferred: init,
        }))
    }

    fn parse_reference_column_constraint(&mut self) -> Result<ColumnConstraint> {
        let clause = self.parse_foreign_key_clause()?;
        let defer_clause = self.parse_defer_subclause()?;
        Ok(ColumnConstraint::ForeignKey {
            clause,
            defer_clause,
        })
    }

    fn parse_generated_column_constraint(&mut self) -> Result<ColumnConstraint> {
        let tok = eat_assert!(self, TK_GENERATED, TK_AS);
        match tok.token_type {
            TK_GENERATED => {
                eat_expect!(self, TK_ALWAYS);
                eat_expect!(self, TK_AS);
            }
            TK_AS => {}
            _ => unreachable!(),
        }

        eat_expect!(self, TK_LP);
        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_RP);

        let typ = match self.peek()? {
            Some(tok) => match tok.token_type.fallback_id_if_ok() {
                TK_ID => {
                    let tok = eat_assert!(self, TK_ID);
                    let s = from_bytes(tok.as_bytes());
                    if s.eq_ignore_ascii_case("STORED") {
                        Some(GeneratedColumnType::Stored)
                    } else if s.eq_ignore_ascii_case("VIRTUAL") {
                        Some(GeneratedColumnType::Virtual)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        };

        Ok(ColumnConstraint::Generated { expr, typ })
    }

    fn parse_named_column_constraints(
        &mut self,
        in_alter: bool,
    ) -> Result<Vec<NamedColumnConstraint>> {
        let mut result = vec![];
        let mut has_primary_key = false;
        let mut has_default = false;
        let mut has_generated = false;

        loop {
            let name = match self.peek()? {
                Some(tok) => match tok.token_type {
                    TK_CONSTRAINT => {
                        eat_assert!(self, TK_CONSTRAINT);
                        Some(self.parse_nm()?)
                    }
                    _ => None,
                },
                _ => None,
            };

            if name.is_some() {
                match self.peek()? {
                    Some(tok)
                        if matches!(
                            tok.token_type,
                            TK_DEFAULT
                                | TK_NOT
                                | TK_NULL
                                | TK_PRIMARY
                                | TK_UNIQUE
                                | TK_CHECK
                                | TK_REFERENCES
                                | TK_COLLATE
                                | TK_GENERATED
                                | TK_AS
                        ) => {}
                    Some(tok) if matches!(tok.token_type, TK_COMMA | TK_RP | TK_SEMI) => {
                        // SQLite accepts trailing "CONSTRAINT <name>" in a column definition.
                        // It does not produce a real column constraint, so stop parsing here.
                        break;
                    }
                    None => break,
                    _ => {
                        peek_expect!(
                            self,
                            TK_DEFAULT,
                            TK_NOT,
                            TK_NULL,
                            TK_PRIMARY,
                            TK_UNIQUE,
                            TK_CHECK,
                            TK_REFERENCES,
                            TK_COLLATE,
                            TK_GENERATED,
                            TK_AS,
                        );
                    }
                }
            }

            match self.peek()? {
                Some(tok) => match tok.token_type {
                    TK_DEFAULT => {
                        if has_generated {
                            return Err(Error::Custom(
                                "a generated column cannot have a DEFAULT value".to_owned(),
                            ));
                        }
                        has_default = true;
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_default_column_constraint()?,
                        });
                    }
                    TK_NOT | TK_NULL => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_not_null_column_constraint()?,
                        });
                    }
                    TK_PRIMARY => {
                        if in_alter {
                            return Err(Error::Custom(
                                "Cannot add a PRIMARY KEY column in ALTER TABLE statement"
                                    .to_owned(),
                            ));
                        }

                        if has_primary_key {
                            return Err(Error::Custom(
                                "multiple PRIMARY KEY constraints on a single column".to_owned(),
                            ));
                        }
                        has_primary_key = true;

                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_primary_column_constraint()?,
                        });
                    }
                    TK_UNIQUE => {
                        if in_alter {
                            return Err(Error::Custom(
                                "Cannot add a UNIQUE column in ALTER TABLE statement".to_owned(),
                            ));
                        }

                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_unique_column_constraint()?,
                        });
                    }
                    TK_CHECK => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_check_column_constraint()?,
                        });
                    }
                    TK_REFERENCES => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_reference_column_constraint()?,
                        });
                    }
                    TK_COLLATE => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: ColumnConstraint::Collate {
                                collation_name: self.parse_collate()?.unwrap(),
                            },
                        });
                    }
                    TK_GENERATED | TK_AS => {
                        if has_default {
                            return Err(Error::Custom(
                                "a generated column cannot have a DEFAULT value".to_owned(),
                            ));
                        }
                        has_generated = true;
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_generated_column_constraint()?,
                        });
                    }
                    _ => break,
                },
                _ => break,
            }
        }

        Ok(result)
    }

    pub fn parse_column_definition(&mut self, in_alter: bool) -> Result<ColumnDefinition> {
        let col_name = self.parse_nm()?;
        let col_type = self.parse_type()?;
        let constraints = self.parse_named_column_constraints(in_alter)?;
        Ok(ColumnDefinition {
            col_name,
            col_type,
            constraints,
        })
    }

    fn parse_alter(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ALTER);
        eat_expect!(self, TK_TABLE);
        let tbl_name = self.parse_fullname(false)?;
        let tok = eat_expect!(self, TK_ADD, TK_DROP, TK_RENAME, TK_ALTER);

        match tok.token_type {
            TK_ADD => {
                if self.peek_no_eof()?.token_type == TK_COLUMNKW {
                    eat_assert!(self, TK_COLUMNKW);
                }

                Ok(Stmt::AlterTable(AlterTable {
                    name: tbl_name,
                    body: AlterTableBody::AddColumn(self.parse_column_definition(true)?),
                }))
            }
            TK_DROP => {
                if self.peek_no_eof()?.token_type == TK_COLUMNKW {
                    eat_assert!(self, TK_COLUMNKW);
                }

                Ok(Stmt::AlterTable(AlterTable {
                    name: tbl_name,
                    body: AlterTableBody::DropColumn(self.parse_nm()?),
                }))
            }
            TK_RENAME => {
                let col_name = match self.peek_no_eof()?.token_type.fallback_id_if_ok() {
                    TK_COLUMNKW => {
                        eat_assert!(self, TK_COLUMNKW);
                        Some(self.parse_nm()?)
                    }
                    TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_LBRACKET => {
                        Some(self.parse_nm()?)
                    }
                    _ => None,
                };

                eat_expect!(self, TK_TO);
                let to_name = self.parse_nm()?;

                if let Some(col_name) = col_name {
                    Ok(Stmt::AlterTable(AlterTable {
                        name: tbl_name,
                        body: AlterTableBody::RenameColumn {
                            old: col_name,
                            new: to_name,
                        },
                    }))
                } else {
                    Ok(Stmt::AlterTable(AlterTable {
                        name: tbl_name,
                        body: AlterTableBody::RenameTo(to_name),
                    }))
                }
            }
            TK_ALTER => {
                eat_expect!(self, TK_COLUMNKW);
                let col_name = self.parse_nm()?;

                eat_expect!(self, TK_TO);

                let new = self.parse_column_definition(true)?;

                Ok(Stmt::AlterTable(AlterTable {
                    name: tbl_name,
                    body: AlterTableBody::AlterColumn { old: col_name, new },
                }))
            }
            _ => unreachable!(),
        }
    }

    fn parse_create_index_using(&mut self) -> Result<Option<Name>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_USING {
                eat_assert!(self, TK_USING);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        Ok(Some(self.parse_nm()?))
    }

    fn parse_with_parameter(&mut self) -> Result<(Name, Box<Expr>)> {
        let name = self.parse_nm()?;
        eat_expect!(self, TK_EQ);
        let value = self.parse_term()?;
        Ok((name, value))
    }

    fn parse_with_parameters(&mut self) -> Result<Vec<(Name, Box<Expr>)>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_WITH {
                eat_assert!(self, TK_WITH);
            } else {
                return Ok(Vec::new());
            }
        } else {
            return Ok(Vec::new());
        }

        eat_expect!(self, TK_LP);
        let mut parameters = vec![];
        let mut first = true;
        loop {
            let tok = peek_expect!(self, TK_RP, TK_COMMA, TK_ID);
            if tok.token_type == TK_RP {
                break;
            }
            if !first {
                eat_expect!(self, TK_COMMA);
            }
            first = false;
            parameters.push(self.parse_with_parameter()?);
        }
        eat_expect!(self, TK_RP);

        Ok(parameters)
    }

    fn parse_create_index(&mut self) -> Result<Stmt> {
        let tok = eat_assert!(self, TK_INDEX, TK_UNIQUE);
        let has_unique = tok.token_type == TK_UNIQUE;
        if has_unique {
            eat_expect!(self, TK_INDEX);
        }

        let if_not_exists = self.parse_if_not_exists()?;
        let idx_name = self.parse_fullname(false)?;
        eat_expect!(self, TK_ON);
        let tbl_name = self.parse_nm()?;

        let using = self.parse_create_index_using()?;

        eat_expect!(self, TK_LP);
        let columns = self.parse_sort_list()?;
        eat_expect!(self, TK_RP);

        let with_clause = self.parse_with_parameters()?;
        let where_clause = self.parse_where()?;

        Ok(Stmt::CreateIndex {
            if_not_exists,
            idx_name,
            tbl_name,
            using,
            columns,
            with_clause,
            where_clause,
            unique: has_unique,
        })
    }

    fn parse_set(&mut self) -> Result<Set> {
        let tok = peek_expect!(
            self,
            TK_LP,
            TK_ID,
            TK_STRING,
            TK_JOIN_KW,
            TK_INDEXED,
            TK_LBRACKET
        );

        match tok.token_type {
            TK_LP => {
                eat_assert!(self, TK_LP);
                let names = self.parse_nm_list()?;
                eat_expect!(self, TK_RP);
                eat_expect!(self, TK_EQ);
                Ok(Set {
                    col_names: names,
                    expr: self.parse_expr(0)?,
                })
            }
            _ => {
                let name = self.parse_nm()?;
                // Check for array subscript: col[n] = value
                // Desugar SET col[n] = val → SET col = array_set_element(col, n, val)
                if self.peek()?.is_some_and(|t| t.token_type == TK_LBRACKET) {
                    eat_assert!(self, TK_LBRACKET);
                    let idx_expr = self.parse_expr(0)?;
                    eat_expect!(self, TK_RBRACKET);
                    eat_expect!(self, TK_EQ);
                    let val_expr = self.parse_expr(0)?;
                    let col_ref = Box::new(Expr::Id(name.clone()));
                    Ok(Set {
                        col_names: vec![name],
                        expr: Box::new(Expr::FunctionCall {
                            name: Name::from_bytes(b"array_set_element"),
                            distinctness: None,
                            args: vec![col_ref, idx_expr, val_expr],
                            order_by: vec![],
                            within_group: vec![],
                            filter_over: FunctionTail {
                                filter_clause: None,
                                over_clause: None,
                            },
                        }),
                    })
                } else {
                    eat_expect!(self, TK_EQ);
                    Ok(Set {
                        col_names: vec![name],
                        expr: self.parse_expr(0)?,
                    })
                }
            }
        }
    }

    fn parse_set_list(&mut self) -> Result<Vec<Set>> {
        let mut results = vec![self.parse_set()?];
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    results.push(self.parse_set()?);
                }
                _ => break,
            }
        }

        Ok(results)
    }

    fn parse_returning(&mut self) -> Result<Vec<ResultColumn>> {
        match self.peek()? {
            Some(tok) if tok.token_type == TK_RETURNING => {
                eat_assert!(self, TK_RETURNING);
            }
            _ => return Ok(vec![]),
        }

        self.parse_select_columns()
    }

    fn parse_upsert(&mut self) -> Result<(Option<Box<Upsert>>, Vec<ResultColumn>)> {
        match self.peek()? {
            Some(tok) => match tok.token_type {
                TK_ON => {
                    eat_assert!(self, TK_ON);
                }
                TK_RETURNING => {
                    return Ok((None, self.parse_returning()?));
                }
                _ => return Ok((None, vec![])),
            },
            _ => return Ok((None, vec![])),
        }

        eat_expect!(self, TK_CONFLICT);
        let targets = match self.peek_no_eof()?.token_type {
            TK_LP => {
                eat_assert!(self, TK_LP);
                let result = self.parse_sort_list()?;
                eat_expect!(self, TK_RP);
                result
            }
            _ => vec![],
        };

        let where_clause = if !targets.is_empty() {
            self.parse_where()?
        } else {
            None
        };

        eat_expect!(self, TK_DO);

        let tok = eat_expect!(self, TK_NOTHING, TK_UPDATE);
        let do_clause = match tok.token_type {
            TK_NOTHING => UpsertDo::Nothing,
            TK_UPDATE => {
                eat_expect!(self, TK_SET);
                let set_list = self.parse_set_list()?;
                let update_where_clause = self.parse_where()?;
                UpsertDo::Set {
                    sets: set_list,
                    where_clause: update_where_clause,
                }
            }
            _ => unreachable!(),
        };

        if !targets.is_empty() {
            let (next, returning) = self.parse_upsert()?;
            Ok((
                Some(Box::new(Upsert {
                    index: Some(UpsertIndex {
                        targets,
                        where_clause,
                    }),
                    do_clause,
                    next,
                })),
                returning,
            ))
        } else {
            Ok((
                Some(Box::new(Upsert {
                    index: None,
                    do_clause,
                    next: None,
                })),
                self.parse_returning()?,
            ))
        }
    }

    fn parse_trigger_insert_cmd(&mut self) -> Result<TriggerCmd> {
        let tok = eat_assert!(self, TK_INSERT, TK_REPLACE);
        let resolve_type = match tok.token_type {
            TK_INSERT => self.parse_or_conflict()?,
            TK_REPLACE => Some(ResolveType::Replace),
            _ => unreachable!(),
        };

        eat_expect!(self, TK_INTO);
        let tbl_name = self.parse_nm()?;
        let col_names = self.parse_nm_list_opt()?;
        let select = self.parse_select()?;
        let (upsert, returning) = self.parse_upsert()?;
        if !returning.is_empty() {
            return Err(Error::Custom(
                "cannot use RETURNING in a trigger".to_owned(),
            ));
        }
        Ok(TriggerCmd::Insert {
            or_conflict: resolve_type,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        })
    }

    fn parse_trigger_update_cmd(&mut self) -> Result<TriggerCmd> {
        eat_assert!(self, TK_UPDATE);
        let or_conflict = self.parse_or_conflict()?;
        let tbl_name = self.parse_nm()?;
        eat_expect!(self, TK_SET);
        let sets = self.parse_set_list()?;
        let from = self.parse_from_clause_opt()?;
        let where_clause = self.parse_where()?;
        if matches!(self.peek()?, Some(tok) if tok.token_type == TK_RETURNING) {
            return Err(Error::Custom(
                "cannot use RETURNING in a trigger".to_owned(),
            ));
        }
        Ok(TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
        })
    }

    fn parse_trigger_delete_cmd(&mut self) -> Result<TriggerCmd> {
        eat_assert!(self, TK_DELETE);
        eat_expect!(self, TK_FROM);
        let tbl_name = self.parse_nm()?;
        let where_clause = self.parse_where()?;
        if matches!(self.peek()?, Some(tok) if tok.token_type == TK_RETURNING) {
            return Err(Error::Custom(
                "cannot use RETURNING in a trigger".to_owned(),
            ));
        }
        Ok(TriggerCmd::Delete {
            tbl_name,
            where_clause,
        })
    }

    fn parse_trigger_cmd(&mut self) -> Result<TriggerCmd> {
        let tok = peek_expect!(
            self, TK_INSERT, TK_REPLACE, TK_UPDATE, TK_DELETE, TK_WITH, TK_SELECT, TK_VALUES,
        );

        let result = match tok.token_type {
            TK_WITH | TK_SELECT | TK_VALUES => TriggerCmd::Select(self.parse_select()?),
            TK_INSERT | TK_REPLACE => self.parse_trigger_insert_cmd()?,
            TK_UPDATE => self.parse_trigger_update_cmd()?,
            TK_DELETE => self.parse_trigger_delete_cmd()?,
            _ => unreachable!(),
        };

        eat_expect!(self, TK_SEMI);
        Ok(result)
    }

    fn parse_create_trigger(&mut self, temporary: bool) -> Result<Stmt> {
        eat_assert!(self, TK_TRIGGER);

        let if_not_exists = self.parse_if_not_exists()?;
        let trigger_name = self.parse_fullname(false)?;

        let trigger_time = match self.peek_no_eof()?.token_type {
            TK_BEFORE => {
                eat_assert!(self, TK_BEFORE);
                Some(TriggerTime::Before)
            }
            TK_AFTER => {
                eat_assert!(self, TK_AFTER);
                Some(TriggerTime::After)
            }
            TK_INSTEAD => {
                eat_assert!(self, TK_INSTEAD);
                eat_expect!(self, TK_OF);
                Some(TriggerTime::InsteadOf)
            }
            _ => None,
        };

        let tok = eat_expect!(self, TK_INSERT, TK_UPDATE, TK_DELETE);
        let trigger_event = match tok.token_type {
            TK_INSERT => TriggerEvent::Insert,
            TK_DELETE => TriggerEvent::Delete,
            TK_UPDATE => match self.peek_no_eof()?.token_type {
                TK_OF => {
                    eat_assert!(self, TK_OF);
                    TriggerEvent::UpdateOf(self.parse_nm_list()?)
                }
                _ => TriggerEvent::Update,
            },
            _ => unreachable!(),
        };

        eat_expect!(self, TK_ON);
        let tbl_name = self.parse_fullname(false)?;

        let foreach_clause = match self.peek_no_eof()?.token_type {
            TK_FOR => {
                eat_assert!(self, TK_FOR);
                eat_expect!(self, TK_EACH);
                eat_expect!(self, TK_ROW);
                true
            }
            _ => false,
        };

        let when_clause = match self.peek()? {
            Some(tok) if tok.token_type == TK_WHEN => {
                eat_assert!(self, TK_WHEN);
                Some(self.parse_expr(0)?)
            }
            _ => None,
        };

        eat_expect!(self, TK_BEGIN);

        let mut cmds = vec![self.parse_trigger_cmd()?];
        while let TK_UPDATE | TK_REPLACE | TK_INSERT | TK_DELETE | TK_WITH | TK_SELECT | TK_VALUES =
            self.peek_no_eof()?.token_type
        {
            cmds.push(self.parse_trigger_cmd()?);
        }

        eat_expect!(self, TK_END);

        Ok(Stmt::CreateTrigger {
            temporary,
            if_not_exists,
            trigger_name,
            time: trigger_time,
            event: trigger_event,
            tbl_name,
            for_each_row: foreach_clause,
            when_clause,
            commands: cmds,
        })
    }

    fn parse_delete_without_cte(&mut self, with: Option<With>) -> Result<Stmt> {
        eat_assert!(self, TK_DELETE);
        eat_expect!(self, TK_FROM);
        let tbl_name = self.parse_fullname(true)?;
        let indexed = self.parse_indexed()?;
        let where_clause = self.parse_where()?;
        let returning = self.parse_returning()?;
        Ok(Stmt::Delete {
            with,
            tbl_name,
            indexed,
            where_clause,
            returning,
        })
    }

    fn parse_delete(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        self.parse_delete_without_cte(with)
    }

    fn parse_if_exists(&mut self) -> Result<bool> {
        match self.peek()? {
            Some(tok) if tok.token_type == TK_IF => {
                eat_assert!(self, TK_IF);
                eat_expect!(self, TK_EXISTS);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Parse `CREATE TYPE [IF NOT EXISTS] name AS STRUCT(...) | UNION(...)`
    /// or `CREATE TYPE [IF NOT EXISTS] name[(param, ...)] BASE base_type ...`
    fn parse_create_type(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_TYPE);
        let if_not_exists = self.parse_if_not_exists()?;

        // Parse type name
        let name_tok = self.eat()?;
        let type_name = match name_tok {
            Some(tok) if tok.token_type == TK_ID => from_bytes(tok.as_bytes()),
            _ => return Err(Error::ParseError("expected type name".to_owned())),
        };

        // Check for AS STRUCT/UNION syntax
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_AS {
                eat_assert!(self, TK_AS);
                let next = self.peek()?;
                match next {
                    Some(t) if t.token_type.fallback_id_if_ok() == TK_ID => {
                        let kw = from_bytes_as_str(t.as_bytes());
                        let is_struct = kw.eq_ignore_ascii_case("STRUCT");
                        let is_union = kw.eq_ignore_ascii_case("UNION");
                        if !is_struct && !is_union {
                            return Err(Error::ParseError(format!(
                                "expected STRUCT or UNION after AS, got {kw}"
                            )));
                        }
                        eat_assert!(self, TK_ID);
                        let fields = self.parse_type_field_list()?;
                        let body = if is_struct {
                            CreateTypeBody::Struct(fields)
                        } else {
                            CreateTypeBody::Union(fields)
                        };
                        return Ok(Stmt::CreateType {
                            if_not_exists,
                            type_name,
                            body,
                        });
                    }
                    _ => {
                        return Err(Error::ParseError(
                            "expected STRUCT or UNION after AS".to_owned(),
                        ))
                    }
                }
            }
        }

        // Parse optional parameter list: (name [type], name [type], ...)
        let mut params = Vec::new();
        if let Some(tok) = self.peek()? {
            if tok.token_type == TK_LP {
                eat_assert!(self, TK_LP);
                loop {
                    let param_tok = self.eat()?;
                    let param_name = match param_tok {
                        Some(t) if t.token_type == TK_ID => from_bytes(t.as_bytes()),
                        _ => return Err(Error::ParseError("expected parameter name".to_owned())),
                    };
                    // Check for optional type annotation (next token is ID, not comma/RP)
                    let param_ty = match self.peek()? {
                        Some(t) if t.token_type == TK_ID => {
                            let ty_tok = self.eat()?;
                            Some(from_bytes(ty_tok.unwrap().as_bytes()).to_lowercase())
                        }
                        _ => None,
                    };
                    params.push(TypeParam {
                        name: param_name,
                        ty: param_ty,
                    });
                    match self.peek()? {
                        Some(tok) if tok.token_type == TK_COMMA => {
                            eat_assert!(self, TK_COMMA);
                        }
                        _ => break,
                    }
                }
                eat_expect!(self, TK_RP);
            }
        }

        // Parse BASE keyword + base type
        let base_tok = self.eat()?;
        match base_tok {
            Some(tok) if tok.token_type == TK_ID => {
                let kw = from_bytes_as_str(tok.as_bytes());
                if !kw.eq_ignore_ascii_case("BASE") {
                    return Err(Error::ParseError(format!("expected BASE, got {kw}")));
                }
            }
            _ => return Err(Error::ParseError("expected BASE keyword".to_owned())),
        }

        let base_type_tok = self.eat()?;
        let base = match base_type_tok {
            Some(tok) if tok.token_type == TK_ID => from_bytes(tok.as_bytes()).to_lowercase(),
            _ => return Err(Error::ParseError("expected base type name".to_owned())),
        };

        // Validate base type
        match base.as_str() {
            "text" | "integer" | "real" | "blob" => {}
            _ => {
                return Err(Error::ParseError(format!(
                    "invalid base type '{base}', must be one of: text, integer, real, blob"
                )));
            }
        }

        let mut encode = None;
        let mut decode = None;
        let mut default = None;
        let mut operators = Vec::new();

        // Parse optional clauses: ENCODE, DECODE, OPERATOR
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_ID => {
                    let kw = from_bytes_as_str(tok.as_bytes()).to_ascii_uppercase();
                    match kw.as_str() {
                        "ENCODE" => {
                            eat_assert!(self, TK_ID);
                            let expr = self.parse_expr(0)?;
                            encode = Some(expr);
                        }
                        "DECODE" => {
                            eat_assert!(self, TK_ID);
                            let expr = self.parse_expr(0)?;
                            decode = Some(expr);
                        }
                        "OPERATOR" => {
                            eat_assert!(self, TK_ID);
                            // Parse operator symbol as a string literal: '+'
                            let op_tok = eat_expect!(self, TK_STRING);
                            let op_raw = from_bytes(op_tok.as_bytes());
                            // Strip quotes from string literal
                            let op = op_raw
                                .strip_prefix('\'')
                                .and_then(|s| s.strip_suffix('\''))
                                .unwrap_or(&op_raw)
                                .to_owned();
                            // Three syntaxes:
                            // 1. OPERATOR 'op' func_name          (named)
                            // 2. OPERATOR 'op' (type) -> func_name (old named)
                            // 3. OPERATOR 'op'                     (naked: use base type)
                            let func_name = if matches!(self.peek()?, Some(t) if t.token_type == TK_LP)
                            {
                                // Old syntax: skip (type) ->
                                eat_assert!(self, TK_LP);
                                self.eat()?; // consume type name
                                eat_expect!(self, TK_RP);
                                eat_expect!(self, TK_PTR);
                                let func_tok = self.eat()?;
                                match func_tok {
                                    Some(t) if t.token_type == TK_ID => {
                                        Some(from_bytes(t.as_bytes()))
                                    }
                                    _ => {
                                        return Err(Error::ParseError(
                                            "expected function name in OPERATOR clause".to_owned(),
                                        ))
                                    }
                                }
                            } else if matches!(self.peek()?, Some(t) if t.token_type == TK_ID) {
                                // Check if the next ID is a clause keyword (not a function name)
                                let next_kw = from_bytes_as_str(self.peek()?.unwrap().as_bytes())
                                    .to_ascii_uppercase();
                                match next_kw.as_str() {
                                    "ENCODE" | "DECODE" | "OPERATOR" | "DEFAULT" => {
                                        // Naked operator: no function name
                                        None
                                    }
                                    _ => {
                                        // Named operator: consume function name
                                        let func_tok = self.eat()?;
                                        match func_tok {
                                            Some(t) if t.token_type == TK_ID => {
                                                Some(from_bytes(t.as_bytes()))
                                            }
                                            _ => {
                                                return Err(Error::ParseError(
                                                    "expected function name in OPERATOR clause"
                                                        .to_owned(),
                                                ))
                                            }
                                        }
                                    }
                                }
                            } else {
                                // End of statement or semicolon: naked operator
                                None
                            };
                            operators.push(TypeOperator { op, func_name });
                        }
                        _ => break,
                    }
                }
                Some(tok) if tok.token_type == TK_DEFAULT => {
                    eat_assert!(self, TK_DEFAULT);
                    let expr = self.parse_expr(0)?;
                    default = Some(expr);
                }
                _ => break,
            }
        }

        Ok(Stmt::CreateType {
            if_not_exists,
            type_name,
            body: CreateTypeBody::CustomType {
                params,
                base,
                encode,
                decode,
                operators,
                default,
            },
        })
    }

    /// Parse `CREATE DOMAIN [IF NOT EXISTS] name AS base_type
    ///     [DEFAULT expr]
    ///     [NOT NULL | NULL]
    ///     [[CONSTRAINT name] CHECK (expr)]...`
    fn parse_create_domain(&mut self) -> Result<Stmt> {
        // Eat 'DOMAIN' (which is TK_ID)
        eat_assert!(self, TK_ID);
        let if_not_exists = self.parse_if_not_exists()?;

        // Parse domain name
        let name_tok = self.eat()?;
        let domain_name = match name_tok {
            Some(tok) if tok.token_type == TK_ID => from_bytes(tok.as_bytes()),
            _ => return Err(Error::ParseError("expected domain name".to_owned())),
        };

        // Eat AS keyword
        eat_expect!(self, TK_AS);

        // Parse base type name (any identifier — primitive, custom type, or another domain)
        let base_tok = self.eat()?;
        let base_type = match base_tok {
            Some(tok) if tok.token_type == TK_ID => from_bytes(tok.as_bytes()),
            _ => return Err(Error::ParseError("expected base type name".to_owned())),
        };

        let mut default = None;
        let mut not_null = false;
        let mut has_null = false;
        let mut has_default = false;
        let mut constraints = Vec::new();

        // Parse optional clauses: DEFAULT, NOT NULL, NULL, CONSTRAINT/CHECK
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_DEFAULT => {
                    eat_assert!(self, TK_DEFAULT);
                    if has_default {
                        return Err(Error::ParseError(
                            "multiple DEFAULT clauses in domain definition".to_owned(),
                        ));
                    }
                    let expr = self.parse_expr(0)?;
                    default = Some(expr);
                    has_default = true;
                }
                Some(tok) if tok.token_type == TK_NOT => {
                    eat_assert!(self, TK_NOT);
                    eat_expect!(self, TK_NULL);
                    if has_null {
                        return Err(Error::ParseError(
                            "conflicting NULL/NOT NULL clauses in domain definition".to_owned(),
                        ));
                    }
                    if not_null {
                        return Err(Error::ParseError(
                            "duplicate NOT NULL clause in domain definition".to_owned(),
                        ));
                    }
                    not_null = true;
                }
                Some(tok) if tok.token_type == TK_NULL => {
                    eat_assert!(self, TK_NULL);
                    if not_null {
                        return Err(Error::ParseError(
                            "conflicting NULL/NOT NULL clauses in domain definition".to_owned(),
                        ));
                    }
                    has_null = true;
                }
                Some(tok) if tok.token_type == TK_CONSTRAINT => {
                    eat_assert!(self, TK_CONSTRAINT);
                    let cname_tok = self.eat()?;
                    let cname = match cname_tok {
                        Some(t) if t.token_type == TK_ID => from_bytes(t.as_bytes()),
                        _ => return Err(Error::ParseError("expected constraint name".to_owned())),
                    };
                    eat_expect!(self, TK_CHECK);
                    eat_expect!(self, TK_LP);
                    let check_expr = self.parse_expr(0)?;
                    eat_expect!(self, TK_RP);
                    constraints.push(DomainConstraint {
                        name: Some(cname),
                        check: check_expr,
                    });
                }
                Some(tok) if tok.token_type == TK_CHECK => {
                    eat_assert!(self, TK_CHECK);
                    eat_expect!(self, TK_LP);
                    let check_expr = self.parse_expr(0)?;
                    eat_expect!(self, TK_RP);
                    constraints.push(DomainConstraint {
                        name: None,
                        check: check_expr,
                    });
                }
                _ => break,
            }
        }

        Ok(Stmt::CreateDomain {
            if_not_exists,
            domain_name,
            base_type,
            default,
            not_null,
            constraints,
        })
    }

    fn parse_create_sequence(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ID); // eat SEQUENCE
        let if_not_exists = self.parse_if_not_exists()?;
        let seq_name = self.parse_fullname(false)?;

        let mut start = None;
        let mut increment = None;
        let mut min_value = None;
        let mut max_value = None;
        let mut cycle = false;

        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == TK_ID => {
                    let kw = tok.to_utf8().to_uppercase();
                    match kw.as_str() {
                        "START" => {
                            eat_assert!(self, TK_ID);
                            // Optional WITH keyword
                            if matches!(self.peek()?, Some(t) if t.token_type == TK_WITH) {
                                eat_assert!(self, TK_WITH);
                            }
                            start = Some(self.parse_sequence_i64()?);
                        }
                        "INCREMENT" => {
                            eat_assert!(self, TK_ID);
                            // Optional BY keyword
                            if matches!(self.peek()?, Some(t) if t.token_type == TK_BY) {
                                eat_assert!(self, TK_BY);
                            }
                            increment = Some(self.parse_sequence_i64()?);
                        }
                        "MINVALUE" => {
                            eat_assert!(self, TK_ID);
                            min_value = Some(self.parse_sequence_i64()?);
                        }
                        "MAXVALUE" => {
                            eat_assert!(self, TK_ID);
                            max_value = Some(self.parse_sequence_i64()?);
                        }
                        "CYCLE" => {
                            eat_assert!(self, TK_ID);
                            cycle = true;
                        }
                        _ => break,
                    }
                }
                Some(tok) if tok.token_type == TK_NO => {
                    eat_assert!(self, TK_NO);
                    let next = self.peek()?;
                    match next {
                        Some(t)
                            if t.token_type == TK_ID
                                && t.to_utf8().eq_ignore_ascii_case("CYCLE") =>
                        {
                            eat_assert!(self, TK_ID);
                            cycle = false;
                        }
                        _ => return Err(Error::ParseError("expected CYCLE after NO".to_owned())),
                    }
                }
                _ => break,
            }
        }

        Ok(Stmt::CreateSequence {
            if_not_exists,
            seq_name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
        })
    }

    fn parse_sequence_i64(&mut self) -> Result<i64> {
        let tok = self.peek_no_eof()?;
        let sign: i64 = if tok.token_type == TK_MINUS {
            eat_assert!(self, TK_MINUS);
            -1
        } else if tok.token_type == TK_PLUS {
            eat_assert!(self, TK_PLUS);
            1
        } else {
            1
        };
        let num_tok = eat_expect!(self, TK_INTEGER);
        let s = from_bytes(num_tok.as_bytes());
        let val: i64 = s.parse().map_err(|e| {
            Error::ParseError(format!("invalid integer in sequence definition: {e}"))
        })?;
        Ok(sign * val)
    }

    fn parse_drop_stmt(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_DROP);
        let tok = peek_expect!(self, TK_TABLE, TK_INDEX, TK_TRIGGER, TK_VIEW, TK_TYPE, TK_ID);

        match tok.token_type {
            TK_TABLE => {
                eat_assert!(self, TK_TABLE);
                let if_exists = self.parse_if_exists()?;
                let tbl_name = self.parse_fullname(false)?;
                Ok(Stmt::DropTable {
                    if_exists,
                    tbl_name,
                })
            }
            TK_INDEX => {
                eat_assert!(self, TK_INDEX);
                let if_exists = self.parse_if_exists()?;
                let idx_name = self.parse_fullname(false)?;
                Ok(Stmt::DropIndex {
                    if_exists,
                    idx_name,
                })
            }
            TK_TRIGGER => {
                eat_assert!(self, TK_TRIGGER);
                let if_exists = self.parse_if_exists()?;
                let trigger_name = self.parse_fullname(false)?;
                Ok(Stmt::DropTrigger {
                    if_exists,
                    trigger_name,
                })
            }
            TK_VIEW => {
                eat_assert!(self, TK_VIEW);
                let if_exists = self.parse_if_exists()?;
                let view_name = self.parse_fullname(false)?;
                Ok(Stmt::DropView {
                    if_exists,
                    view_name,
                })
            }
            TK_TYPE => {
                eat_assert!(self, TK_TYPE);
                let if_exists = self.parse_if_exists()?;
                let name_tok = self.eat()?;
                let type_name = match name_tok {
                    Some(tok) if tok.token_type == TK_ID => from_bytes(tok.as_bytes()),
                    _ => return Err(Error::ParseError("expected type name".to_owned())),
                };
                Ok(Stmt::DropType {
                    if_exists,
                    type_name,
                })
            }
            TK_ID if tok.to_utf8().eq_ignore_ascii_case("DOMAIN") => {
                eat_assert!(self, TK_ID);
                let if_exists = self.parse_if_exists()?;
                let name_tok = self.eat()?;
                let domain_name = match name_tok {
                    Some(tok) if tok.token_type == TK_ID => from_bytes(tok.as_bytes()),
                    _ => return Err(Error::ParseError("expected domain name".to_owned())),
                };
                Ok(Stmt::DropDomain {
                    if_exists,
                    domain_name,
                })
            }
            TK_ID if tok.to_utf8().eq_ignore_ascii_case("SEQUENCE") => {
                eat_assert!(self, TK_ID);
                let if_exists = self.parse_if_exists()?;
                let seq_name = self.parse_fullname(false)?;
                Ok(Stmt::DropSequence {
                    if_exists,
                    seq_name,
                })
            }
            _ => Err(Error::ParseError(format!(
                "unexpected token: {}",
                tok.to_utf8()
            )))?,
        }
    }

    fn parse_insert_without_cte(&mut self, with: Option<With>) -> Result<Stmt> {
        let tok = eat_assert!(self, TK_INSERT, TK_REPLACE);
        let resolve_type = match tok.token_type {
            TK_INSERT => self.parse_or_conflict()?,
            TK_REPLACE => Some(ResolveType::Replace),
            _ => unreachable!(),
        };

        eat_expect!(self, TK_INTO);
        let tbl_name = self.parse_fullname(true)?;
        let columns = self.parse_nm_list_opt()?;
        let (body, returning) = match self.peek_no_eof()?.token_type {
            TK_DEFAULT => {
                if !columns.is_empty() {
                    return Err(Error::Custom(format!(
                        "0 values for {} columns",
                        columns.len()
                    )));
                }

                eat_assert!(self, TK_DEFAULT);
                eat_expect!(self, TK_VALUES);
                (InsertBody::DefaultValues, self.parse_returning()?)
            }
            _ => {
                let select = self.parse_select()?;
                if !columns.is_empty() {
                    if let ColumnCount::Fixed(n) = select.body.select.column_count() {
                        if n != columns.len() {
                            return Err(Error::Custom(format!(
                                "{} values for {} columns",
                                n,
                                columns.len()
                            )));
                        }
                    }
                }

                let (upsert, returning) = self.parse_upsert()?;
                (InsertBody::Select(select, upsert), returning)
            }
        };

        Ok(Stmt::Insert {
            with,
            or_conflict: resolve_type,
            tbl_name,
            columns,
            body,
            returning,
        })
    }

    fn parse_insert(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        self.parse_insert_without_cte(with)
    }

    fn parse_update_without_cte(&mut self, with: Option<With>) -> Result<Stmt> {
        eat_assert!(self, TK_UPDATE);
        let resolve_type = self.parse_or_conflict()?;
        let tbl_name = self.parse_fullname(true)?;
        let indexed = self.parse_indexed()?;
        eat_expect!(self, TK_SET);
        let sets = self.parse_set_list()?;
        let from = self.parse_from_clause_opt()?;
        let where_clause = self.parse_where()?;
        let returning = self.parse_returning()?;
        Ok(Stmt::Update(Update {
            with,
            or_conflict: resolve_type,
            tbl_name,
            indexed,
            sets,
            from,
            where_clause,
            returning,
        }))
    }

    fn parse_update(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        self.parse_update_without_cte(with)
    }

    fn parse_reindex(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_REINDEX);
        match self.peek()? {
            Some(tok) => match tok.token_type.fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_JOIN_KW | TK_INDEXED | TK_LBRACKET => Ok(Stmt::Reindex {
                    name: Some(self.parse_reindex_fullname()?),
                }),
                _ => Ok(Stmt::Reindex { name: None }),
            },
            _ => Ok(Stmt::Reindex { name: None }),
        }
    }

    /// Parse `OPTIMIZE INDEX [idx_name]`
    fn parse_optimize(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_OPTIMIZE);
        eat_expect!(self, TK_INDEX);
        match self.peek()? {
            Some(tok) => match tok.token_type.fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_JOIN_KW | TK_INDEXED => Ok(Stmt::Optimize {
                    idx_name: Some(self.parse_fullname(false)?),
                }),
                _ => Ok(Stmt::Optimize { idx_name: None }),
            },
            _ => Ok(Stmt::Optimize { idx_name: None }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset() {
        let s = "SELECT 1; SELECT 1";
        let mut p = Parser::new(s.as_bytes());
        p.next_cmd().unwrap();
        assert_eq!(&s[..p.offset()], "SELECT 1; ");
    }

    #[test]
    fn test_offset_multiple_statements_with_insert_columns() {
        let s = "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, col_a TEXT, col_b TEXT, col_c TEXT, col_d TEXT); INSERT INTO test (col_b, col_d, col_a, col_c) VALUES ('1', '2', '3', '4'); SELECT * FROM test;";
        let mut p = Parser::new(s.as_bytes());

        p.next_cmd().unwrap();
        let first = p.offset();
        assert_eq!(&s[..first], "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, col_a TEXT, col_b TEXT, col_c TEXT, col_d TEXT); ");

        p.next_cmd().unwrap();
        let second = p.offset();
        assert_eq!(
            &s[first..second],
            "INSERT INTO test (col_b, col_d, col_a, col_c) VALUES ('1', '2', '3', '4'); "
        );

        p.next_cmd().unwrap();
        let third = p.offset();
        assert_eq!(&s[second..third], "SELECT * FROM test;");
    }

    #[test]
    fn check_constraint_comments_survive_formatting() {
        for (sql, comment) in [
            (
                "CREATE TABLE t (x CHECK(x /* column comment */ > 0))",
                "/* column comment */",
            ),
            (
                "CREATE TABLE t (x, CHECK(x -- table comment\n > 0))",
                "-- table comment",
            ),
        ] {
            let command = Parser::new(sql.as_bytes()).next().unwrap().unwrap();
            let formatted = command.to_string();

            assert!(formatted.contains(comment), "formatted SQL: {formatted}");
            Parser::new(formatted.as_bytes()).next().unwrap().unwrap();
        }
    }

    #[test]
    fn test_variable_index_bounds() {
        for sql in ["SELECT ?0", "SELECT ?250001"] {
            let mut p = Parser::new(sql.as_bytes());
            let err = p.next_cmd().unwrap_err().to_string();
            assert!(
                err.contains("variable number must be between ?1 and ?250000"),
                "unexpected error for {sql}: {err}"
            );
        }

        let mut p = Parser::new("SELECT ?250000".as_bytes());
        assert!(p.next_cmd().is_ok());
    }

    #[test]
    fn test_namespace_qualified_parameter_names() {
        // TCL passes `$::g` and `$ns::var` into SQL; each spelling is one
        // parameter, keyed on its full text, and a repeat reuses its index.
        let sql = "SELECT $ns::var, $::g, $ns::var, :::g, @a::b::";
        let mut p = Parser::new(sql.as_bytes());
        let cmd = p.next_cmd().unwrap().unwrap();
        assert_eq!(cmd.to_string(), format!("{sql};"));
        let index = |name: &str| p.named_variables[name.as_bytes()].get();
        assert_eq!(index("$ns::var"), 1);
        assert_eq!(index("$::g"), 2);
        assert_eq!(index(":::g"), 3);
        assert_eq!(index("@a::b::"), 4);
        assert_eq!(p.named_variables.len(), 4);
    }

    #[test]
    fn test_array_element_parameter_names() {
        // TCL array elements, `$arr(elem)`, are one parameter including the
        // suffix; only one suffix is allowed, so `$a(b)(c)` is a call-like
        // syntax error, as in SQLite.
        let sql = "SELECT $arr(elem), $ns::arr(k), $arr(elem)";
        let mut p = Parser::new(sql.as_bytes());
        let cmd = p.next_cmd().unwrap().unwrap();
        assert_eq!(cmd.to_string(), format!("{sql};"));
        assert_eq!(p.named_variables[b"$arr(elem)".as_slice()].get(), 1);
        assert_eq!(p.named_variables[b"$ns::arr(k)".as_slice()].get(), 2);
        assert_eq!(p.named_variables.len(), 2);

        let mut p = Parser::new(b"SELECT $a(b)(c)");
        assert!(p.next_cmd().is_err());
        let mut p = Parser::new(b"SELECT $a(b c)");
        let err = p.next_cmd().unwrap_err().to_string();
        assert!(err.contains("unrecognized token: \"$a(b\""), "{err}");
    }

    #[test]
    fn test_expect_fail() {
        let testcases = vec![
            "ALTER TABLE my_table ADD COLUMN my_column PRIMARY KEY",
            "ALTER TABLE my_table ADD COLUMN my_column UNIQUE",
            // https://github.com/tursodatabase/turso/issues/7058
            "ALTER TABLE my_table ADD COLUMN my_column DEFAULT 5 AS (1)",
            "ALTER TABLE my_table ADD COLUMN my_column AS (1) DEFAULT 5",
            "CREATE TABLE foo(b DEFAULT 5 AS (a+1))",
            "CREATE TABLE foo(b AS (a+1) DEFAULT 5)",
            "CREATE TEMP TABLE baz.foo(bar)",
            "CREATE TABLE foo(d INT AS (a*abs(b)))",
            "CREATE TABLE foo(d INT AS (a*abs(b)))",
            "CREATE TABLE foo(bar) STRICT",
            "CREATE TABLE foo(bar) WITHOUT ROWID",
            "CREATE VIEW foo(bar, bar) AS SELECT 1, 1",
            "CREATE VIEW foo(bar) AS SELECT 1, 1",
            "CREATE VIEW v AS WITH cte AS (SELECT 1), cte AS (SELECT 1) SELECT 1",
            "DELETE FROM my_table ORDER BY col1",
            "INSERT INTO my_table(bar) DEFAULT VALUES",
            "INSERT INTO my_table(bar, baz, barr) VALUES (1, 1)",
            "UPDATE foo SET bar = 1 ORDER BY bar",
            "CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) RETURNING bar, baz; END",
            "CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT (bar, baz) WHERE 1 DO NOTHING RETURNING bar, baz; END",
            "CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT DO UPDATE SET (bar, baz) = 1 WHERE 1 RETURNING bar, baz; END",
            "CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo RETURNING *; END",
            "CREATE TRIGGER foo INSERT ON bar BEGIN UPDATE foo SET bar = 1 RETURNING *; END",
        ];

        for tc in testcases {
            let mut p = Parser::new(tc.as_bytes());
            let result = p.next_cmd();
            assert!(result.is_err(), "Expected error for: {tc}");
        }
    }

    #[expect(clippy::large_stack_frames)]
    #[test]
    fn test_parser() {
        let test_cases = vec![
            // begin
            (
                b"BEGIN".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"EXPLAIN BEGIN".as_slice(),
                vec![Cmd::Explain(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"EXPLAIN QUERY PLAN BEGIN".as_slice(),
                vec![Cmd::ExplainQueryPlan {
                    stmt: Stmt::Begin {
                        typ: None,
                        name: None,
                    },
                    format: EqpFormat::Text,
                }],
            ),
            (
                b"EXPLAIN QUERY PLAN FORMAT=JSON BEGIN".as_slice(),
                vec![Cmd::ExplainQueryPlan {
                    stmt: Stmt::Begin {
                        typ: None,
                        name: None,
                    },
                    format: EqpFormat::Json,
                }],
            ),
            (
                b"explain query plan format = json begin".as_slice(),
                vec![Cmd::ExplainQueryPlan {
                    stmt: Stmt::Begin {
                        typ: None,
                        name: None,
                    },
                    format: EqpFormat::Json,
                }],
            ),
            (
                b"EXPLAIN QUERY PLAN FORMAT=TEXT BEGIN".as_slice(),
                vec![Cmd::ExplainQueryPlan {
                    stmt: Stmt::Begin {
                        typ: None,
                        name: None,
                    },
                    format: EqpFormat::Text,
                }],
            ),
            (
                b"BEGIN TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"BEGIN DEFERRED TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Deferred),
                    name: None,
                })],
            ),
            (
                b"BEGIN IMMEDIATE TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Immediate),
                    name: None,
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: None,
                })],
            ),
            (
                b"BEGIN DEFERRED TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Deferred),
                    name: Some(Name::from_string("my_transaction")),
                })],
            ),
            (
                b"BEGIN IMMEDIATE TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Immediate),
                    name: Some(Name::from_string("my_transaction")),
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: Some(Name::from_string("my_transaction")),
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION 'my_transaction'".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: Some(Name::from_string("'my_transaction'")),
                })],
            ),
            (
                b"BEGIN CONCURRENT TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Concurrent),
                    name: None,
                })],
            ),
            (
                b"BEGIN CONCURRENT TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Concurrent),
                    name: Some(Name::from_string("my_transaction")),
                })],
            ),
            (
                b"BEGIN CONCURRENT TRANSACTION 'my_transaction'".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Concurrent),
                    name: Some(Name::from_string("'my_transaction'")),
                })],
            ),
            (
                ";;;BEGIN;BEGIN;;;;;;BEGIN".as_bytes(),
                vec![
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                ],
            ),
            // commit
            (
                b"COMMIT".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"END".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"COMMIT TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"END TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"COMMIT TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit {
                    name: Some(Name::from_string("my_transaction")),
                })],
            ),
            (
                b"END TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit {
                    name: Some(Name::from_string("my_transaction")),
                })],
            ),
            // Rollback
            (
                b"ROLLBACK".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: None,
                })],
            ),
            (
                b"ROLLBACK TO SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: Some(Name::from_string("my_savepoint")),
                })],
            ),
            (
                b"ROLLBACK TO my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: Some(Name::from_string("my_savepoint")),
                })],
            ),
            (
                b"ROLLBACK TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: Some(Name::from_string("my_transaction")),
                    savepoint_name: None,
                })],
            ),
            (
                b"ROLLBACK TRANSACTION my_transaction TO my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: Some(Name::from_string("my_transaction")),
                    savepoint_name: Some(Name::from_string("my_savepoint")),
                })],
            ),
            // savepoint
            (
                b"SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Savepoint {
                    name: Name::from_string("my_savepoint"),
                })],
            ),
            (
                b"SAVEPOINT 'my_savepoint'".as_slice(),
                vec![Cmd::Stmt(Stmt::Savepoint {
                    name: Name::from_string("'my_savepoint'"),
                })],
            ),
            // release
            (
                b"RELEASE my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::from_string("my_savepoint"),
                })],
            ),
            (
                b"RELEASE SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::from_string("my_savepoint"),
                })],
            ),
            (
                b"RELEASE SAVEPOINT 'my_savepoint'".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::from_string("'my_savepoint'"),
                })],
            ),
            (
                b"RELEASE SAVEPOINT ABORT".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::from_string("ABORT"),
                })],
            ),
            // test expr operand
            (
                b"SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Parenthesized(vec![Box::new(Expr::Literal(
                                    Literal::Numeric("1".to_owned()),
                                ))])),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Null)),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT X'ab'".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Blob("X'ab'".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 3.333".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("3.333".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT ?".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Variable(Variable::indexed(1u32.try_into().unwrap()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT ?, :named, ?".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![
                                ResultColumn::Expr(
                                    Box::new(Expr::Variable(Variable::indexed(1u32.try_into().unwrap()))),
                                    None,
                                ),
                                ResultColumn::Expr(
                                    Box::new(Expr::Variable(Variable::named(
                                        ":named".to_owned(),
                                        2u32.try_into().unwrap(),
                                    ))),
                                    None,
                                ),
                                ResultColumn::Expr(
                                    Box::new(Expr::Variable(Variable::indexed(3u32.try_into().unwrap()))),
                                    None,
                                ),
                            ],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT ?1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Variable(Variable::indexed(1u32.try_into().unwrap()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CAST(1 AS INTEGER)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Cast {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    type_name: Some(Type {
                                        name: "INTEGER".to_owned(),
                                        size: None,
                                        array_dimensions: 0,
                                    }),
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CAST(1 AS VARCHAR(255))".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Cast {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    type_name: Some(Type {
                                        name: "VARCHAR".to_owned(),
                                        size: Some(TypeSize::MaxSize(Box::new(Expr::Literal(
                                            Literal::Numeric("255".to_owned()),
                                        )))),
                                        array_dimensions: 0,
                                    }),
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CAST(1 AS DECIMAL(10, 5))".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Cast {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    type_name: Some(Type {
                                        name: "DECIMAL".to_owned(),
                                        size: Some(TypeSize::TypeSize(
                                            Box::new(Expr::Literal(Literal::Numeric(
                                                "10".to_owned(),
                                            ))),
                                            Box::new(Expr::Literal(Literal::Numeric(
                                                "5".to_owned(),
                                            ))),
                                        )),
                                        array_dimensions: 0,
                                    }),
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CURRENT_DATE".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::CurrentDate)),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CURRENT_TIME".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::CurrentTime)),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CURRENT_TIMESTAMP".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::CurrentTimestamp)),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT NOT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Not,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT NOT 1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Not,
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Add,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT ~1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::BitwiseNot,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::Add,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT +1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Positive,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::Add,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT -1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Negative,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::Add,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT EXISTS (SELECT 1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Exists(Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric(
                                                    "1".to_owned(),
                                                ))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                })),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CASE WHEN 1 THEN 2 ELSE 3 END".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Case {
                                    base: None,
                                    when_then_pairs: vec![(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )],
                                    else_expr: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "3".to_owned(),
                                    )))),
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CASE 4 WHEN 1 THEN 2 ELSE 3 END".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Case {
                                    base: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "4".to_owned(),
                                    )))),
                                    when_then_pairs: vec![(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )],
                                    else_expr: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "3".to_owned(),
                                    )))),
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT CASE 4 WHEN 1 THEN 2 END".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Case {
                                    base: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "4".to_owned(),
                                    )))),
                                    when_then_pairs: vec![(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )],
                                    else_expr: None,
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT (SELECT 1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Subquery(Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric(
                                                    "1".to_owned(),
                                                ))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                })),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT RAISE (Ignore)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(ResolveType::Ignore, None)),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT RAISE (FAIL, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Fail,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT RAISE (ROLLBACK, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Rollback,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT RAISE (ABORT, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Abort,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT RAISE (ABORT, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Abort,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT RAISE ('error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Abort,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT col_1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Id(Name::exact("col_1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'col_1'".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::String("'col_1'".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT tbl_name.col_1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Qualified(
                                    Name::exact("tbl_name".to_owned()),
                                    Name::exact("col_1".to_owned()),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT schema_name.tbl_name.col_1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::DoublyQualified(
                                    Name::exact("schema_name".to_owned()),
                                    Name::exact("tbl_name".to_owned()),
                                    Name::exact("col_1".to_owned()),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name()".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: None,
                                    args: vec![],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: None,
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) FILTER (WHERE x) OVER window_name".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: Some(Box::new(Expr::Id(Name::exact(
                                            "x".to_owned(),
                                        )))),
                                        over_clause: Some(Over::Name(Name::exact(
                                            "window_name".to_owned(),
                                        ))),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (PARTITION BY product)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: None,
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: None,
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: None,
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product ORDER BY test ASC NULLS LAST)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![
                                                SortedColumn {
                                                    expr: Box::new(Expr::Id(Name::exact("test".to_owned()))),
                                                    order: Some(SortOrder::Asc),
                                                    nulls: Some(NullsOrder::Last),
                                                }
                                            ],
                                            frame_clause: None,
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Rows,
                                                start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Range,
                                                start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN 2 FOLLOWING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::Following(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::UnboundedPreceding,
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::UnboundedFollowing),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND 1 PRECEDING)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::Preceding(
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                                )),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::Following(
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                                )),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE NO OTHERS)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::NoOthers)
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::CurrentRow)
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE GROUP)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::Group)
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE TIES)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::exact("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::exact(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::Ties)
                                            }),
                                        })),
                                    },
                                }),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            // parse expr
            (
                b"SELECT 1 NOT NULL AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::NotNull(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 IS 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Is,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 IS NOT 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::IsNot,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 IS NOT DISTINCT FROM 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Is,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 IS DISTINCT FROM 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::IsNot,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 + 2 * 3".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::Add,
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                Operator::Multiply,
                                Box::new(Expr::Literal(Literal::Numeric("3".to_owned())))
                            ))
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 AND 2 OR 3".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))
                            )),
                            Operator::Or,
                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 = 0 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Equals,
                                Box::new(Expr::Literal(Literal::Numeric("0".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 != 0 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::NotEquals,
                                Box::new(Expr::Literal(Literal::Numeric("0".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 BETWEEN 2 AND 3 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Between {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        start: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        end: Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 NOT BETWEEN 2 AND 3 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Between {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: true,
                                        start: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        end: Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 IN (SELECT 1) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InSelect {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        rhs: Select {
                                            with: None,
                                            body: SelectBody {
                                                select: OneSelect::Select {
                                                    distinctness: None,
                                                    columns: vec![ResultColumn::Expr(
                                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                        None,
                                                    )],
                                                    from: None,
                                                    where_clause: None,
                                                    group_by: None,
                                                    window_clause: vec![],
                                                },
                                                compounds: vec![],
                                            },
                                            order_by: vec![],
                                            limit: None
                                        },
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 NOT IN (SELECT 1) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InSelect {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: true,
                                        rhs: Select {
                                            with: None,
                                            body: SelectBody {
                                                select: OneSelect::Select {
                                                    distinctness: None,
                                                    columns: vec![ResultColumn::Expr(
                                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                        None,
                                                    )],
                                                    from: None,
                                                    where_clause: None,
                                                    group_by: None,
                                                    window_clause: vec![],
                                                },
                                                compounds: vec![],
                                            },
                                            order_by: vec![],
                                            limit: None
                                        },
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 IN (1, 2, 3) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InList {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        rhs: vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                        ],
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 IN test(1, 2, 3) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InTable {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        rhs: QualifiedName {
                                            db_name: None,
                                            name: Name::exact("test".to_owned()),
                                            alias: None,
                                        },
                                        args: vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                        ],
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'test' MATCH 'foo' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: false,
                                        op: LikeOperator::Match,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: None,
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'test' NOT MATCH 'foo' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Match,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: None,
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'test' NOT MATCH 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Match,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'test' NOT LIKE 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Like,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'test' NOT GLOB 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Glob,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'test' NOT REGEXP 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Regexp,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 ISNULL AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::IsNull (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 NOTNULL AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::NotNull(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 < 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Less,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 > 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Greater,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 <= 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::LessEquals,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 >= 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::GreaterEquals,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 & 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::BitwiseAnd,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 | 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::BitwiseOr,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 << 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::LeftShift,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 >> 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::RightShift,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 / 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Divide,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 % 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Modulus,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 || 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Concat,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 -> 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::ArrowRight,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 ->> 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::ArrowRightShift,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 'foo' COLLATE bar AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Collate (
                                        Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        Name::exact("bar".to_owned()),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            // test select
            (
                b"VALUES (1, 2), (3, 4), (5, 6)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Values(vec![
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                            ],
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("4".to_owned()))),
                            ],
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("5".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("6".to_owned()))),
                            ],
                        ]),
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT *".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT tbl_name.*".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::TableStar(
                                Name::exact("tbl_name".to_owned()),
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT col_1 OVER".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Id(Name::exact("col_1".to_owned()))),
                                Some(As::Elided(Name::exact("OVER".to_owned()))),
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT col_1 AS OVER".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Id(Name::exact("col_1".to_owned()))),
                                Some(As::As(Name::exact("OVER".to_owned()))),
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"WITH test AS (SELECT 1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                }
                            },
                        ]
                    }),
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"WITH test(col_1) AS MATERIALIZED (SELECT 1 AS col_1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![
                                    IndexedColumn {
                                        col_name: Name::exact("col_1".to_owned()),
                                        collation_name: None,
                                        order: None,
                                    },
                                ],
                                materialized: Materialized::Yes,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                Some(As::As(Name::exact("col_1".to_owned()))),
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                }
                            },
                        ]
                    }),
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"WITH test(col_1) AS NOT MATERIALIZED (SELECT 1 AS col_1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![
                                    IndexedColumn {
                                        col_name: Name::exact("col_1".to_owned()),
                                        collation_name: None,
                                        order: None,
                                    },
                                ],
                                materialized: Materialized::No,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                Some(As::As(Name::exact("col_1".to_owned()))),
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                }
                            },
                        ]
                    }),
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"WITH test AS (SELECT 1), test_2 AS (SELECT 1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                }
                            },
                            CommonTableExpr {
                                tbl_name: Name::exact("test_2".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                }
                            },
                        ]
                    }),
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 ORDER BY 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            order: None,
                            nulls: None,
                        },
                    ],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 ORDER BY 1 DESC NULLS FIRST".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            order: Some(SortOrder::Desc),
                            nulls: Some(NullsOrder::First),
                        },
                    ],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 ORDER BY 1 ASC NULLS LAST".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            order: Some(SortOrder::Asc),
                            nulls: Some(NullsOrder::Last),
                        },
                    ],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 LIMIT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        offset: None,
                    }),
                }))],
            ),
            (
                b"SELECT 1 LIMIT 1,2".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                        offset: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    }),
                }))],
            ),
            (
                b"SELECT 1 LIMIT 1 OFFSET 2".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        offset: Some(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                    }),
                }))],
            ),
            (
                b"SELECT 1 UNION SELECT 2".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::Union,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 UNION ALL SELECT 2".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::UnionAll,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 EXCEPT SELECT 2".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::Except,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 INTERSECT SELECT 2".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::Intersect,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo(1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::TableCall(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM (SELECT 1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Select(
                                    Select {
                                        with: None,
                                        body: SelectBody {
                                            select: OneSelect::Select {
                                                distinctness: None,
                                                columns: vec![ResultColumn::Expr(
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                    None,
                                                )],
                                                from: None,
                                                where_clause: None,
                                                group_by: None,
                                                window_clause: vec![],
                                            },
                                            compounds: vec![],
                                        },
                                        order_by: vec![],
                                        limit: None,
                                    },
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM (tbl_name)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Sub(
                                    FromClause {
                                        select: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("tbl_name".to_owned()), alias: None },
                                            None,
                                            None
                                        )),
                                        joins: vec![]
                                    },
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo INDEXED BY bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    Some(Indexed::IndexedBy(Name::exact("bar".to_owned()))),
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo NOT INDEXED".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    Some(Indexed::NotIndexed),
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo, bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::Comma,
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo, bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::Comma,
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo NATURAL JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo CROSS JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::INNER|JoinType::CROSS)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo LEFT JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::LEFT|JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo RIGHT JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::RIGHT|JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo FULL JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::LEFT | JoinType::RIGHT | JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo INNER JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::INNER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo NATURAL INNER JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL | JoinType::INNER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo NATURAL LEFT OUTER JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL | JoinType::LEFT | JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo JOIN bar ON 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: Some(JoinConstraint::On(Box::new(Expr::Binary(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Operator::Equals,
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        )))),
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo JOIN bar USING (col_1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: Some(JoinConstraint::Using(vec![
                                            Name::exact("col_1".to_owned()),
                                        ])),
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo JOIN bar bar_alias USING (col_1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            Some(As::Elided(Name::exact("bar_alias".to_owned()))),
                                            None,
                                        )),
                                        constraint: Some(JoinConstraint::Using(vec![
                                            Name::exact("col_1".to_owned()),
                                        ])),
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo JOIN bar(1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::TableCall(
                                            QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                            vec![
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                            ],
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo JOIN (VALUES (1,2), (3, 4))".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Select(
                                            Select {
                                                with: None,
                                                body: SelectBody {
                                                    select: OneSelect::Values(vec![
                                                    vec![
                                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))
                                                    ],
                                                    vec![
                                                        Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                                        Box::new(Expr::Literal(Literal::Numeric("4".to_owned())))
                                                    ],
                                                    ]),
                                                    compounds: vec![],
                                                },
                                                order_by: vec![],
                                                limit: None,
                                            },
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo JOIN (bar)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Sub(
                                            FromClause {
                                                select: Box::new(SelectTable::Table(
                                                    QualifiedName { db_name: None, name: Name::exact("bar".to_owned()), alias: None },
                                                    None,
                                                    None,
                                                )),
                                                joins: vec![]
                                            },
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo WHERE 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: Some(Box::new(Expr::Binary(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Equals,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            ))),
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo GROUP BY 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: Some(GroupBy {
                                exprs: vec![
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Equals,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                ],
                                having: None,
                            }),
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo GROUP BY 1 = 1 HAVING 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: Some(GroupBy {
                                exprs: vec![
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Equals,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                ],
                                having: Some(Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ))),
                            }),
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo GROUP BY 1 = 1 HAVING 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: Some(GroupBy {
                                exprs: vec![
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Equals,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                ],
                                having: Some(Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ))),
                            }),
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT sum(a) s FROM t HAVING s = 15".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::exact("sum".to_owned()),
                                    distinctness: None,
                                    args: vec![Box::new(Expr::Id(Name::exact("a".to_owned())))],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: None,
                                    },
                                }),
                                Some(As::Elided(Name::exact("s".to_owned()))),
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("t".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: Some(GroupBy {
                                exprs: vec![],
                                having: Some(Box::new(Expr::Binary(
                                    Box::new(Expr::Id(Name::exact("s".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("15".to_owned()))),
                                ))),
                            }),
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT * FROM t0 WINDOW JOIN t0;".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                    Some(As::Elided(Name::exact("WINDOW".to_owned()))),
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT * FROM t0 WINDOW window_1 AS (PARTITION BY product)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![
                                WindowDef {
                                    name: Name::exact("window_1".to_owned()),
                                    window: Window {
                                        base: None,
                                        partition_by: vec![
                                            Box::new(Expr::Id(Name::exact("product".to_owned()))),
                                        ],
                                        order_by: vec![],
                                        frame_clause: None,
                                    },
                                }
                            ],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT * FROM t0 WINDOW window_1 AS (PARTITION BY product), window_2 AS (PARTITION BY product_2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::exact("t0".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![
                                WindowDef {
                                    name: Name::exact("window_1".to_owned()),
                                    window: Window {
                                        base: None,
                                        partition_by: vec![
                                            Box::new(Expr::Id(Name::exact("product".to_owned()))),
                                        ],
                                        order_by: vec![],
                                        frame_clause: None,
                                    },
                                },
                                WindowDef {
                                    name: Name::exact("window_2".to_owned()),
                                    window: Window {
                                        base: None,
                                        partition_by: vec![
                                            Box::new(Expr::Id(Name::exact("product_2".to_owned()))),
                                        ],
                                        order_by: vec![],
                                        frame_clause: None,
                                    },
                                }
                            ],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            // parse Analyze
            (
                b"ANALYZE".as_slice(),
                vec![Cmd::Stmt(Stmt::Analyze {
                    name: None,
                })],
            ),
            (
                b"ANALYZE foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Analyze {
                    name: Some(QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None }),
                })],
            ),
            // parse attach
            (
                b"ATTACH DATABASE 'foo' AS bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Attach {
                    expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                    db_name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                    key: None,
                })],
            ),
            (
                b"ATTACH 'foo' AS bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Attach {
                    expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                    db_name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                    key: None,
                })],
            ),
            (
                b"ATTACH 'foo' AS bar key baz".as_slice(),
                vec![Cmd::Stmt(Stmt::Attach {
                    expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                    db_name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                    key: Some(Box::new(Expr::Id(Name::exact("baz".to_owned())))),
                })],
            ),
            // parse detach
            (
                b"DETACH DATABASE bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Detach {
                    name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                })],
            ),
            (
                b"DETACH bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Detach {
                    name: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                })],
            ),
            // parse pragma
            (
                b"PRAGMA foreign_keys = ON".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("ON".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys = DELETE".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("DELETE".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys = DEFAULT".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("DEFAULT".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                    body: None,
                })],
            ),
            (
                b"PRAGMA foreign_keys = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys = test".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Name(Name::exact("test".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::exact("foreign_keys".to_owned()),  alias: None },
                    body: None,
                })],
            ),
            // parse vacuum
            (
                b"VACUUM".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: None,
                    into: None,
                })],
            ),
            (
                b"VACUUM INTO 'foo'".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: None,
                    into: Some(Box::new(Expr::Literal(Literal::String("'foo'".to_owned())))),
                })],
            ),
            (
                b"VACUUM INTO foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: None,
                    into: Some(Box::new(Expr::Id(Name::exact("foo".to_owned())))),
                })],
            ),
            (
                b"VACUUM foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: Some(Name::exact("foo".to_owned())),
                    into: None,
                })],
            ),
            (
                b"VACUUM foo INTO 'bar'".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: Some(Name::exact("foo".to_owned())),
                    into: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                })],
            ),
            // parse alter
            (
                b"ALTER TABLE foo RENAME TO bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::RenameTo(Name::exact("bar".to_owned())),
                }))],
            ),
            (
                b"ALTER TABLE foo RENAME baz TO bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::RenameColumn {
                        old: Name::exact("baz".to_owned()),
                        new: Name::exact("bar".to_owned())
                    },
                }))],
            ),
            (
                b"ALTER TABLE foo RENAME COLUMN baz TO bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::RenameColumn {
                        old: Name::exact("baz".to_owned()),
                        new: Name::exact("bar".to_owned())
                    },
                }))],
            ),
            (
                b"ALTER TABLE foo DROP baz".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::DropColumn(Name::exact("baz".to_owned())),
                }))],
            ),
            (
                b"ALTER TABLE foo DROP COLUMN baz".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::DropColumn(Name::exact("baz".to_owned())),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD baz".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: None,
                        constraints: vec![],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Parenthesized(vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    ]))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT +1".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Positive,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    ))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT -1".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Negative,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    ))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT hello".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Id(Name::exact("hello".to_owned())))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: true,
                                    conflict_clause: None,
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT IGNORE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Ignore),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT REPLACE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Replace),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT ROLLBACK".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Rollback),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT ROLLBACK".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Rollback),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER CHECK (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Check {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    source: Some("1".to_owned()),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER CHECK (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Check {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    source: Some("1".to_owned()),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON INSERT SET NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::exact("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::exact("test_3".to_owned())),
                                            RefArg::OnInsert(RefAct::SetNull),
                                        ]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON UPDATE SET NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::exact("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::exact("test_3".to_owned())),
                                            RefArg::OnUpdate(RefAct::SetNull),
                                        ]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE SET NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::exact("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::exact("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::SetNull),
                                        ]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE SET DEFAULT".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::exact("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::exact("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::SetDefault),
                                        ]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE CASCADE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::exact("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::exact("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::Cascade),
                                        ]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE RESTRICT".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::exact("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::exact("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::Restrict),
                                        ]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE NO ACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::exact("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::exact("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::NoAction),
                                        ]
                                    },
                                    defer_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar DEFERRABLE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    defer_clause: Some(DeferSubclause {
                                        deferrable: true,
                                        init_deferred: None,
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY IMMEDIATE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    defer_clause: Some(DeferSubclause {
                                        deferrable: false,
                                        init_deferred: Some(InitDeferredPred::InitiallyImmediate),
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY DEFERRED".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    defer_clause: Some(DeferSubclause {
                                        deferrable: false,
                                        init_deferred: Some(InitDeferredPred::InitiallyDeferred),
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY DEFERRED".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    defer_clause: Some(DeferSubclause {
                                        deferrable: false,
                                        init_deferred: Some(InitDeferredPred::InitiallyDeferred),
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER COLLATE bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Collate {
                                    collation_name: Name::exact("bar".to_owned()),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER GENERATED ALWAYS AS (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Generated {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    typ: None,
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER AS (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Generated {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    typ: None,
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER AS (1) STORED".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::exact("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                            array_dimensions: 0,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Generated {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    typ: Some(GeneratedColumnType::Stored),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ALTER COLUMN bar TO baz INTEGER".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::exact("foo".to_owned()), alias: None },
                    body: AlterTableBody::AlterColumn {
                        old: Name::exact("bar".to_owned()),
                        new: ColumnDefinition {
                            col_name: Name::exact("baz".to_owned()),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                                array_dimensions: 0,
                            }),
                            constraints: vec![],
                        },

                    },
                }))],
            ),
            // parse create index
            (
                b"CREATE INDEX idx_foo ON foo (bar)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateIndex {
                    unique: false,
                    if_not_exists: false,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("idx_foo".to_owned()),
                        alias: None,
                    },
                    tbl_name: Name::exact("foo".to_owned()),
                    columns: vec![SortedColumn {
                        expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                        order: None,
                        nulls: None,
                    }],
                    where_clause: None,
                    using: None,
                    with_clause: Vec::new(),
                })],
            ),
            (
                b"CREATE UNIQUE INDEX IF NOT EXISTS idx_foo ON foo (bar) WHERE 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateIndex {
                    unique: true,
                    if_not_exists: true,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("idx_foo".to_owned()),
                        alias: None,
                    },
                    tbl_name: Name::exact("foo".to_owned()),
                    columns: vec![SortedColumn {
                        expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                        order: None,
                        nulls: None,
                    }],
                    where_clause: Some(Box::new(
                        Expr::Literal(Literal::Numeric("1".to_owned()))
                    )),
                    using: None,
                    with_clause: Vec::new(),
                })],
            ),
            // parse create table
            (
                b"CREATE TABLE foo (column)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::exact("column".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![],
                        options: TableOptions::empty(),
                    },
                })],
            ),
            (
                b"CREATE TABLE foo (a CONSTRAINT c PRIMARY KEY, b, d CONSTRAINT e)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::exact("a".to_owned()),
                                col_type: None,
                                constraints: vec![
                                    NamedColumnConstraint {
                                        name: Some(Name::exact("c".to_owned())),
                                        constraint: ColumnConstraint::PrimaryKey {
                                            order: None,
                                            conflict_clause: None,
                                            auto_increment: false,
                                        }
                                    }
                                ],
                            },
                            ColumnDefinition {
                                col_name: Name::exact("b".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                            ColumnDefinition {
                                col_name: Name::exact("d".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![],
                        options: TableOptions::empty(),
                    },
                })],
            ),
            (
                b"CREATE TABLE foo AS SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::AsSelect(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![
                                    ResultColumn::Expr(Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))), None)
                                ],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![]
                        },
                        order_by: vec![],
                        limit: None,
                    }),
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (baz INTEGER, CONSTRAINT tbl_cons PRIMARY KEY (bar AUTOINCREMENT) ON CONFLICT ROLLBACK) STRICT".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::exact("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                    array_dimensions: 0,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: Some(Name::exact("tbl_cons".to_owned())),
                                constraint: TableConstraint::PrimaryKey {
                                    columns: vec![
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                                            order: None,
                                            nulls: None,
                                        },
                                    ],
                                    auto_increment: true,
                                    conflict_clause:  Some(ResolveType::Rollback)
                                }
                            },
                        ],
                        options: TableOptions { without_rowid_text: None, strict_text: Some("STRICT".to_string()) },
                    },
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (bar INTEGER PRIMARY KEY, baz INTEGER, UNIQUE (bar) ON CONFLICT ROLLBACK) WITHOUT ROWID".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::exact("bar".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                    array_dimensions: 0,
                                }),
                                constraints: vec![
                                    NamedColumnConstraint {
                                        name: None,
                                        constraint: ColumnConstraint::PrimaryKey {
                                            order: None,
                                            conflict_clause: None,
                                            auto_increment: false,
                                        }
                                    }
                                ],
                            },
                            ColumnDefinition {
                                col_name: Name::exact("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                    array_dimensions: 0,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::Unique {
                                    columns: vec![
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                                            order: None,
                                            nulls: None,
                                        },
                                    ],
                                    conflict_clause:  Some(ResolveType::Rollback)
                                }
                            },
                        ],
                        options: TableOptions { without_rowid_text: Some("WITHOUT ROWID".to_string()), strict_text: None },
                    },
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (bar, baz INTEGER, CHECK (1))".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::exact("bar".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                            ColumnDefinition {
                                col_name: Name::exact("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                    array_dimensions: 0,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::Check {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    source: Some("1".to_owned()),
                                },
                            },
                        ],
                        options: TableOptions::empty(),
                    },
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (bar, baz INTEGER, FOREIGN KEY (bar) REFERENCES foo_2(bar_2), CHECK (1))".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::exact("bar".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                            ColumnDefinition {
                                col_name: Name::exact("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                    array_dimensions: 0,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::ForeignKey {
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::exact("bar".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::exact("foo_2".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::exact("bar_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![],
                                    },
                                    defer_clause: None,
                                },
                            },
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::Check {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    source: Some("1".to_owned()),
                                },
                            },
                        ],
                        options: TableOptions::empty(),
                    },
                })],
            ),
            // parse create trigger
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Select(Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                },
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        })
                    ],
                })],
            ),
            (
                b"CREATE TEMP TRIGGER IF NOT EXISTS foo AFTER UPDATE ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: true,
                    if_not_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: Some(TriggerTime::After),
                    event: TriggerEvent::Update,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: true,
                    when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    commands: vec![
                        TriggerCmd::Select(Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                },
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        })
                    ],
                })],
            ),
            (
                b"CREATE TEMP TRIGGER IF NOT EXISTS foo BEFORE DELETE ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: true,
                    if_not_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: Some(TriggerTime::Before),
                    event: TriggerEvent::Delete,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: true,
                    when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    commands: vec![
                        TriggerCmd::Select(Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                },
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        })
                    ],
                })],
            ),
            (
                b"CREATE TEMP TRIGGER IF NOT EXISTS foo INSTEAD OF UPDATE OF baz, bar ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: true,
                    if_not_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: Some(TriggerTime::InsteadOf),
                    event: TriggerEvent::UpdateOf(vec![
                        Name::exact("baz".to_owned()),
                        Name::exact("bar".to_owned()),
                    ]),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: true,
                    when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    commands: vec![
                        TriggerCmd::Select(Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                },
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        })
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2); END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: None,
                            tbl_name: Name::exact("foo".to_owned()),
                            col_names: vec![],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: None,
                            returning: vec![],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT OR ROLLBACK INTO foo(bar, baz) VALUES (1, 2); END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: Some(ResolveType::Rollback),
                            tbl_name: Name::exact("foo".to_owned()),
                            col_names: vec![
                                Name::exact("bar".to_owned()),
                                Name::exact("baz".to_owned()),
                            ],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: None,
                            returning: vec![],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT (bar, baz) DO NOTHING ON CONFLICT DO NOTHING; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: None,
                            tbl_name: Name::exact("foo".to_owned()),
                            col_names: vec![],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: Some(Box::new(Upsert {
                                index: Some(UpsertIndex {
                                    targets: vec![
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                                            order: None,
                                            nulls: None
                                        },
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::exact("baz".to_owned()))),
                                            order: None,
                                            nulls: None
                                        },
                                    ],
                                    where_clause: None,
                                }),
                                do_clause: UpsertDo::Nothing,
                                next: Some(Box::new(Upsert {
                                    index: None,
                                    do_clause: UpsertDo::Nothing,
                                    next: None,
                                })),
                            })),
                            returning: vec![],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN UPDATE foo SET bar = 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Update {
                            or_conflict: None,
                            tbl_name: Name::exact("foo".to_owned()),
                            sets: vec![
                                Set {
                                    col_names: vec![Name::exact("bar".to_owned())],

                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                },
                            ],
                            from: None,
                            where_clause: None,
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Delete {
                            tbl_name: Name::exact("foo".to_owned()),
                            where_clause: None,
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo WHERE 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Delete {
                            tbl_name: Name::exact("foo".to_owned()),
                            where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN SELECT 1; SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Select(Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                },
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        }),
                        TriggerCmd::Select(Select {
                            with: None,
                            body: SelectBody {
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                },
                                compounds: vec![],
                            },
                            order_by: vec![],
                            limit: None,
                        })
                    ],
                })],
            ),
            // parse create view
            (
                b"CREATE VIEW foo(bar) AS SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateView {
                    temporary: false,
                    if_not_exists: false,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![
                        IndexedColumn {
                            col_name: Name::exact("bar".to_owned()),
                            collation_name: None,
                            order: None,
                        }
                    ],
                    select: Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    },
                })],
            ),
            (
                b"CREATE TEMP VIEW IF NOT EXISTS foo(bar) AS SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateView {
                    temporary: true,
                    if_not_exists: true,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![
                        IndexedColumn {
                            col_name: Name::exact("bar".to_owned()),
                            collation_name: None,
                            order: None,
                        }
                    ],
                    select: Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![ResultColumn::Expr(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    None,
                                )],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    },
                })],
            ),
            // parse CREATE VIRTUAL TABLE
            (
                b"CREATE VIRTUAL TABLE foo USING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable {
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    module_name: Name::exact("bar".to_owned()),
                    args: vec![],
                }))],
            ),
            (
                b"CREATE VIRTUAL TABLE foo USING bar()".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable{
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    module_name: Name::exact("bar".to_owned()),
                    args: vec![],
                }))],
            ),
            (
                b"CREATE VIRTUAL TABLE IF NOT EXISTS foo USING bar(1, 2, ('hello', (3.333), 'world', (1, 2)))".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable{
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    module_name: Name::exact("bar".to_owned()),
                    args: vec![
                        "1".to_owned(),
                        "2".to_owned(),
                        "('hello', (3.333), 'world', (1, 2))".to_owned(),
                    ],
                }))],
            ),
            (
                b"CREATE VIRTUAL TABLE ft USING fts5(x, tokenize = '''porter'' ''ascii''')".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable {
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("ft".to_owned()),
                        alias: None,
                    },
                    module_name: Name::exact("fts5".to_owned()),
                    args: vec![
                        "x".to_owned(),
                        "tokenize = '''porter'' ''ascii'''".to_owned(),
                    ],
                }))],
            ),
            // parse delete
            (
                b"DELETE FROM foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Delete {
                    with: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None
                    },
                    indexed: None,
                    where_clause: None,
                    returning: vec![],
                })],
            ),
            (
                b"WITH test AS (SELECT 1) DELETE FROM foo NOT INDEXED WHERE 1 RETURNING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Delete {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                },
                            }
                        ],
                    }),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None
                    },
                    indexed: Some(Indexed::NotIndexed),
                    where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                            None,
                        ),
                    ],
                })],
            ),
            // parse drop index
            (
                b"DROP INDEX foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropIndex {
                    if_exists: false,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP INDEX IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropIndex {
                    if_exists: true,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse drop table
            (
                b"DROP TABLE foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTable {
                    if_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP TABLE IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTable {
                    if_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse drop trigger
            (
                b"DROP TRIGGER foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTrigger {
                    if_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP TRIGGER IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTrigger {
                    if_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse drop view
            (
                b"DROP VIEW foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropView {
                    if_exists: false,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP VIEW IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropView {
                    if_exists: true,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse insert
            (
                b"INSERT INTO foo VALUES (1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: None,
                    or_conflict: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Values(vec![
                                vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                            ]),
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    }, None),
                    returning: vec![],
                })],
            ),
            (
                b"REPLACE INTO foo VALUES (1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: None,
                    or_conflict: Some(ResolveType::Replace),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Values(vec![
                                vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                            ]),
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    }, None),
                    returning: vec![],
                })],
            ),
            (
                b"WITH test AS (SELECT 1) INSERT INTO foo DEFAULT VALUES RETURNING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                },
                            }
                        ],
                    }),
                    or_conflict: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::DefaultValues,
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                            None,
                        ),
                    ],
                })],
            ),
            (
                b"WITH test AS (SELECT 1) REPLACE INTO foo DEFAULT VALUES RETURNING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                },
                            }
                        ],
                    }),
                    or_conflict: Some(ResolveType::Replace),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::DefaultValues,
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                            None,
                        ),
                    ],
                })],
            ),
            // parse update
            (
                b"UPDATE foo SET bar = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Update(Update {
                    with: None,
                    or_conflict: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    indexed: None,
                    sets: vec![
                        Set {
                            col_names: vec![
                                Name::exact("bar".to_owned()),
                            ],

                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        }
                    ],
                    from: None,
                    where_clause: None,
                    returning: vec![],
                }))],
            ),
            (
                b"WITH test AS (SELECT 1) UPDATE OR REPLACE foo NOT INDEXED SET bar = 1 FROM foo_2 WHERE 1 RETURNING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Update(Update {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::exact("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None,
                                            )],
                                            from: None,
                                            where_clause: None,
                                            group_by: None,
                                            window_clause: vec![],
                                        },
                                        compounds: vec![],
                                    },
                                    order_by: vec![],
                                    limit: None,
                                },
                            }
                        ],
                    }),
                    or_conflict: Some(ResolveType::Replace),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None,
                    },
                    indexed: Some(Indexed::NotIndexed),
                    sets: vec![
                        Set {
                            col_names: vec![
                                Name::exact("bar".to_owned()),
                            ],

                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        }
                    ],
                    from: Some(FromClause {
                        select: Box::new(SelectTable::Table(
                            QualifiedName {
                                db_name: None,
                                name: Name::exact("foo_2".to_owned()),
                                alias: None,
                            },
                            None,
                            None,
                        )),
                        joins: vec![]
                    }),
                    where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::exact("bar".to_owned()))),
                            None,
                        ),
                    ],
                }))],
            ),
            // parse reindex
            (
                b"REINDEX".as_slice(),
                vec![Cmd::Stmt(Stmt::Reindex {
                    name: None,
                })],
            ),
            (
                b"REINDEX foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Reindex {
                    name: Some(QualifiedName {
                        db_name: None,
                        name: Name::exact("foo".to_owned()),
                        alias: None
                    }),
                })],
            ),
            // issue 2875
            (
                b"CREATE TABLE \"settings\" (\"enabled\" INTEGER DEFAULT CURRENT_TIMESTAMP NOT NULL)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::from_string("\"settings\""),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints{
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::from_string("\"enabled\""),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                    array_dimensions: 0,
                                }),
                                constraints: vec![
                                    NamedColumnConstraint {
                                        name: None,
                                        constraint: ColumnConstraint::Default(Box::new(Expr::Literal(Literal::CurrentTimestamp))),
                                    },
                                    NamedColumnConstraint {
                                        name: None,
                                        constraint: ColumnConstraint::NotNull { nullable: false, conflict_clause: None }
                                    },
                                ],
                            }
                        ],
                        constraints: vec![],
                        options: TableOptions::empty(),
                    },
                })],
            ),
            (
                b"CREATE INDEX t_idx ON t USING custom_index (x) WITH (a = 1, b = 'test', c = x'deadbeef', d = NULL)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateIndex {
                    unique: false,
                    if_not_exists: false,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::exact("t_idx".to_owned()),
                        alias: None,
                    },
                    tbl_name: Name::exact("t".to_owned()),
                    columns: vec![SortedColumn {
                        expr: Box::new(Expr::Id(Name::exact("x".to_owned()))),
                        order: None,
                        nulls: None,
                    }],
                    where_clause: None,
                    using: Some(Name::exact("custom_index".to_owned())),
                    with_clause: vec![
                        (Name::exact("a".to_string()), Box::new(Expr::Literal(Literal::Numeric("1".to_string())))),
                        (Name::exact("b".to_string()), Box::new(Expr::Literal(Literal::String("'test'".to_string())))),
                        (Name::exact("c".to_string()), Box::new(Expr::Literal(Literal::Blob("x'deadbeef'".to_string())))),
                        (Name::exact("d".to_string()), Box::new(Expr::Literal(Literal::Null))),
                    ],
                })],
            )
        ];

        for (input, expected) in test_cases {
            let input_str = from_bytes(input);
            let parser = Parser::new(input);
            let mut results = Vec::new();
            for cmd in parser {
                results.push(cmd.unwrap());
            }

            // Compare serialized forms since ImplicitColumnName (display-only
            // metadata) serializes to nothing, making comparison insensitive
            // to its presence.
            let results_str: Vec<String> = results.iter().map(|c| c.to_string()).collect();
            let expected_str: Vec<String> = expected.iter().map(|c| c.to_string()).collect();
            assert_eq!(results_str, expected_str, "Input: {input_str:?}");

            // to_string round-trip tests
            for (i, r) in results.iter().enumerate() {
                let rstring = r.to_string();
                // put new string into parser again
                let result = Parser::new(rstring.as_bytes()).next().unwrap().unwrap();
                let result_str = result.to_string();
                assert_eq!(result_str, expected_str[i], "Input: {rstring:?}");
            }
        }
    }

    #[test]
    fn test_inline_struct_rejected() {
        let sql = b"CREATE TABLE t(s STRUCT(x INT, y TEXT)) STRICT";
        let err = Parser::new(sql).next().unwrap().unwrap_err();
        assert!(err.to_string().contains("inline STRUCT/UNION"));
    }

    #[test]
    fn test_inline_union_rejected() {
        let sql = b"CREATE TABLE t(u UNION(i INT, t TEXT)) STRICT";
        let err = Parser::new(sql).next().unwrap().unwrap_err();
        assert!(err.to_string().contains("inline STRUCT/UNION"));
    }

    #[test]
    fn test_delete_and_update_reject_limit_and_order_by() {
        // Default SQLite builds (without SQLITE_ENABLE_UPDATE_DELETE_LIMIT)
        // reject LIMIT and ORDER BY on DELETE and UPDATE.
        for sql in [
            b"DELETE FROM t LIMIT 1".as_slice(),
            b"DELETE FROM t LIMIT 1 OFFSET 2".as_slice(),
            b"DELETE FROM t ORDER BY x LIMIT 1".as_slice(),
            b"UPDATE t SET x = 1 LIMIT 1".as_slice(),
            b"UPDATE t SET x = 1 ORDER BY x LIMIT 1".as_slice(),
        ] {
            let result = Parser::new(sql).next().unwrap();
            assert!(result.is_err(), "expected parse error for {sql:?}");
        }
    }

    #[test]
    fn test_create_type_as_struct() {
        let sql = b"CREATE TYPE point AS STRUCT(x INT, y INT)";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        if let Cmd::Stmt(Stmt::CreateType {
            type_name, body, ..
        }) = cmd
        {
            assert_eq!(type_name, "point");
            match body {
                CreateTypeBody::Struct(fields) => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[0].name.to_string(), "x");
                    assert_eq!(fields[1].name.to_string(), "y");
                }
                _ => panic!("expected Struct body"),
            }
        } else {
            panic!("expected CreateType");
        }
    }

    #[test]
    fn test_create_type_as_union() {
        let sql = b"CREATE TYPE platform AS UNION(telegram INT, slack TEXT)";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        if let Cmd::Stmt(Stmt::CreateType {
            type_name, body, ..
        }) = cmd
        {
            assert_eq!(type_name, "platform");
            match body {
                CreateTypeBody::Union(fields) => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[0].name.to_string(), "telegram");
                    assert_eq!(fields[1].name.to_string(), "slack");
                }
                _ => panic!("expected Union body"),
            }
        } else {
            panic!("expected CreateType");
        }
    }

    #[test]
    fn test_dot_notation_still_produces_qualified() {
        let sql = b"SELECT col.field FROM t";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
            if let OneSelect::Select { columns, .. } = &sel.body.select {
                if let ResultColumn::Expr(expr, _) = &columns[0] {
                    assert!(
                        matches!(expr.as_ref(), Expr::Qualified(_, _)),
                        "expected Qualified, got: {expr:?}",
                    );
                } else {
                    panic!("expected Expr");
                }
            } else {
                panic!("expected Select");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_struct_pack_positional() {
        let sql = b"SELECT struct_pack(1, 'hello')";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
            if let OneSelect::Select { columns, .. } = &sel.body.select {
                if let ResultColumn::Expr(expr, _) = &columns[0] {
                    if let Expr::FunctionCall { name, args, .. } = expr.as_ref() {
                        assert_eq!(name.to_string(), "struct_pack");
                        assert_eq!(args.len(), 2);
                    } else {
                        panic!("expected FunctionCall");
                    }
                } else {
                    panic!("expected Expr");
                }
            } else {
                panic!("expected Select");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_union_value_string_tag() {
        let sql = b"SELECT union_value('i', 42)";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
            if let OneSelect::Select { columns, .. } = &sel.body.select {
                if let ResultColumn::Expr(expr, _) = &columns[0] {
                    if let Expr::FunctionCall { name, args, .. } = expr.as_ref() {
                        assert_eq!(name.to_string(), "union_value");
                        assert_eq!(args.len(), 2);
                    } else {
                        panic!("expected FunctionCall");
                    }
                } else {
                    panic!("expected Expr");
                }
            } else {
                panic!("expected Select");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_parse_create_sequence_defaults() {
        let sql = b"CREATE SEQUENCE foo";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        match cmd {
            Cmd::Stmt(Stmt::CreateSequence {
                if_not_exists,
                seq_name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
            }) => {
                assert!(!if_not_exists);
                assert_eq!(seq_name.name.to_string(), "foo");
                assert_eq!(start, None);
                assert_eq!(increment, None);
                assert_eq!(min_value, None);
                assert_eq!(max_value, None);
                assert!(!cycle);
            }
            _ => panic!("expected CreateSequence"),
        }
    }

    #[test]
    fn test_parse_create_sequence_full() {
        let sql = b"CREATE SEQUENCE foo START WITH 10 INCREMENT BY 5 MINVALUE 0 MAXVALUE 100 CYCLE";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        match cmd {
            Cmd::Stmt(Stmt::CreateSequence {
                if_not_exists,
                start,
                increment,
                min_value,
                max_value,
                cycle,
                ..
            }) => {
                assert!(!if_not_exists);
                assert_eq!(start, Some(10));
                assert_eq!(increment, Some(5));
                assert_eq!(min_value, Some(0));
                assert_eq!(max_value, Some(100));
                assert!(cycle);
            }
            _ => panic!("expected CreateSequence"),
        }
    }

    #[test]
    fn test_parse_create_sequence_if_not_exists() {
        let sql = b"CREATE SEQUENCE IF NOT EXISTS foo";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        match cmd {
            Cmd::Stmt(Stmt::CreateSequence { if_not_exists, .. }) => {
                assert!(if_not_exists);
            }
            _ => panic!("expected CreateSequence"),
        }
    }

    #[test]
    fn test_parse_create_sequence_negative_values() {
        let sql = b"CREATE SEQUENCE foo INCREMENT BY -1 START WITH -1 MINVALUE -100 MAXVALUE -1";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        match cmd {
            Cmd::Stmt(Stmt::CreateSequence {
                start,
                increment,
                min_value,
                max_value,
                ..
            }) => {
                assert_eq!(start, Some(-1));
                assert_eq!(increment, Some(-1));
                assert_eq!(min_value, Some(-100));
                assert_eq!(max_value, Some(-1));
            }
            _ => panic!("expected CreateSequence"),
        }
    }

    #[test]
    fn test_parse_create_sequence_no_cycle() {
        let sql = b"CREATE SEQUENCE foo NO CYCLE";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        match cmd {
            Cmd::Stmt(Stmt::CreateSequence { cycle, .. }) => {
                assert!(!cycle);
            }
            _ => panic!("expected CreateSequence"),
        }
    }

    #[test]
    fn test_parse_drop_sequence() {
        let sql = b"DROP SEQUENCE foo";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        match cmd {
            Cmd::Stmt(Stmt::DropSequence {
                if_exists,
                seq_name,
            }) => {
                assert!(!if_exists);
                assert_eq!(seq_name.name.to_string(), "foo");
            }
            _ => panic!("expected DropSequence"),
        }
    }

    #[test]
    fn test_parse_drop_sequence_if_exists() {
        let sql = b"DROP SEQUENCE IF EXISTS foo";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        match cmd {
            Cmd::Stmt(Stmt::DropSequence { if_exists, .. }) => {
                assert!(if_exists);
            }
            _ => panic!("expected DropSequence"),
        }
    }

    #[test]
    fn test_parse_nextval() {
        let sql = b"SELECT nextval('foo')";
        let cmd = Parser::new(sql).next().unwrap().unwrap();
        if let Cmd::Stmt(Stmt::Select(sel)) = cmd {
            if let OneSelect::Select { columns, .. } = &sel.body.select {
                if let ResultColumn::Expr(expr, _) = &columns[0] {
                    if let Expr::FunctionCall { name, .. } = expr.as_ref() {
                        assert_eq!(name.to_string(), "nextval");
                    } else {
                        panic!("expected FunctionCall");
                    }
                } else {
                    panic!("expected Expr");
                }
            } else {
                panic!("expected Select");
            }
        } else {
            panic!("expected Select");
        }
    }
}
