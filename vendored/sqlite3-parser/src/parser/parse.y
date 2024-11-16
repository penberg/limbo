%include {
/*
** 2001-09-15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains SQLite's SQL parser.
**
** The canonical source code to this file ("parse.y") is a Lemon grammar
** file that specifies the input grammar and actions to take while parsing.
** That input file is processed by Lemon to generate a C-language
** implementation of a parser for the given grammar.  You might be reading
** this comment as part of the translated C-code.  Edits should be made
** to the original parse.y sources.
*/
}

// All token codes are small integers with #defines that begin with "TK_"
%token_prefix TK_

// The type of the data attached to each token is Token.  This is also the
// default type for non-terminals.
//
%token_type "Token<'i>"
%default_type "Token<'i>"

// An extra argument to the constructor for the parser, which is available
// to all actions.
%extra_context {ctx: Context}

// This code runs whenever there is a syntax error
//
%syntax_error {
  if TokenType::TK_EOF as YYCODETYPE == yymajor {
    error!(target: TARGET, "incomplete input");
    self.ctx.error = Some(ParserError::UnexpectedEof);
  } else {
    error!(target: TARGET, "near \"{:?}\": syntax error", yyminor);
    self.ctx.error = Some(ParserError::SyntaxError(from_bytes(yyminor.1)));
  }
}

// The name of the generated procedure that implements the parser
// is as follows:
%name sqlite3Parser

// The following text is included near the beginning of the C source
// code file that implements the parser.
//
%include {
use crate::custom_err;
use crate::parser::ast::*;
use crate::parser::{Context, ParserError};
use crate::dialect::{from_bytes, from_token, Token, TokenType};
use indexmap::IndexMap;
use log::error;

#[expect(non_camel_case_types)]
type sqlite3ParserError = crate::parser::ParserError;
} // end %include

// Input is a single SQL command
input ::= cmdlist.
cmdlist ::= cmdlist ecmd.
cmdlist ::= ecmd.
ecmd ::= SEMI.
ecmd ::= cmdx SEMI.
%ifndef SQLITE_OMIT_EXPLAIN
ecmd ::= explain cmdx SEMI.       {NEVER-REDUCE}
explain ::= EXPLAIN.              { self.ctx.explain = Some(ExplainKind::Explain); }
explain ::= EXPLAIN QUERY PLAN.   { self.ctx.explain = Some(ExplainKind::QueryPlan); }
%endif  SQLITE_OMIT_EXPLAIN
cmdx ::= cmd.           { self.ctx.sqlite3_finish_coding(); }

///////////////////// Begin and end transactions. ////////////////////////////
//

cmd ::= BEGIN transtype(Y) trans_opt(X).  {self.ctx.stmt = Some(Stmt::Begin(Y, X));}
%type trans_opt {Option<Name>}
trans_opt(A) ::= .               {A = None;}
trans_opt(A) ::= TRANSACTION.    {A = None;}
trans_opt(A) ::= TRANSACTION nm(X). {A = Some(X);}
%type transtype {Option<TransactionType>}
transtype(A) ::= .             {A = None;}
transtype(A) ::= DEFERRED.  {A = Some(TransactionType::Deferred);}
transtype(A) ::= IMMEDIATE. {A = Some(TransactionType::Immediate);}
transtype(A) ::= EXCLUSIVE. {A = Some(TransactionType::Exclusive);}
cmd ::= COMMIT|END trans_opt(X).   {self.ctx.stmt = Some(Stmt::Commit(X));}
cmd ::= ROLLBACK trans_opt(X).     {self.ctx.stmt = Some(Stmt::Rollback{tx_name: X, savepoint_name: None});}

savepoint_opt ::= SAVEPOINT.
savepoint_opt ::= .
cmd ::= SAVEPOINT nm(X). {
  self.ctx.stmt = Some(Stmt::Savepoint(X));
}
cmd ::= RELEASE savepoint_opt nm(X). {
  self.ctx.stmt = Some(Stmt::Release(X));
}
cmd ::= ROLLBACK trans_opt(Y) TO savepoint_opt nm(X). {
  self.ctx.stmt = Some(Stmt::Rollback{tx_name: Y, savepoint_name: Some(X)});
}

///////////////////// The CREATE TABLE statement ////////////////////////////
//
cmd ::= createkw temp(T) TABLE ifnotexists(E) fullname(Y) create_table_args(X). {
  self.ctx.stmt = Some(Stmt::CreateTable{ temporary: T, if_not_exists: E, tbl_name: Y, body: X });
}
createkw(A) ::= CREATE(A).

%type ifnotexists {bool}
ifnotexists(A) ::= .              {A = false;}
ifnotexists(A) ::= IF NOT EXISTS. {A = true;}
%type temp {bool}
%ifndef SQLITE_OMIT_TEMPDB
temp(A) ::= TEMP.  {A = true;}
%endif  SQLITE_OMIT_TEMPDB
temp(A) ::= .      {A = false;}

%type create_table_args {CreateTableBody}
create_table_args(A) ::= LP columnlist(C) conslist_opt(X) RP table_option_set(F). {
  A = CreateTableBody::columns_and_constraints(C, X, F)?;
}
create_table_args(A) ::= AS select(S). {
  A = CreateTableBody::AsSelect(S);
}
%type table_option_set {TableOptions}
%type table_option {TableOptions}
table_option_set(A) ::= .    {A = TableOptions::NONE;}
table_option_set(A) ::= table_option(A).
table_option_set(A) ::= table_option_set(X) COMMA table_option(Y). {A = X|Y;}
table_option(A) ::= WITHOUT nm(X). {
  let option = X;
  if option == "rowid" {
    A = TableOptions::WITHOUT_ROWID;
  }else{
    return Err(custom_err!("unknown table option: {}", option));
  }
}
table_option(A) ::= nm(X). {
  let option = X;
  if option == "strict" {
    A = TableOptions::STRICT;
  }else{
    return Err(custom_err!("unknown table option: {}", option));
  }
}
%type columnlist {IndexMap<Name,ColumnDefinition>}
columnlist(A) ::= columnlist(A) COMMA columnname(X) carglist(Y). {
  let col = X;
  let cd = ColumnDefinition{ col_name: col.0, col_type: col.1, constraints: Y };
  ColumnDefinition::add_column(A, cd)?;
}
columnlist(A) ::= columnname(X) carglist(Y). {
  let col = X;
  let cd = ColumnDefinition{ col_name: col.0, col_type: col.1, constraints: Y };
  let mut map = IndexMap::new();
  ColumnDefinition::add_column(&mut map, cd)?;
  A = map;
}
%type columnname {(Name, Option<Type>)}
columnname(A) ::= nm(X) typetoken(Y). {A = (X, Y);}

// Declare some tokens early in order to influence their values, to
// improve performance and reduce the executable size.  The goal here is
// to get the "jump" operations in ISNULL through ESCAPE to have numeric
// values that are early enough so that all jump operations are clustered
// at the beginning.  Also, operators like NE and EQ need to be adjacent,
// and all of the comparison operators need to be clustered together.
// Various assert() statements throughout the code enforce these restrictions.
//
%token ABORT ACTION AFTER ANALYZE ASC ATTACH BEFORE BEGIN BY CASCADE CAST.
%token CONFLICT DATABASE DEFERRED DESC DETACH EACH END EXCLUSIVE EXPLAIN FAIL.
%token OR AND NOT IS ISNOT MATCH LIKE_KW BETWEEN IN ISNULL NOTNULL NE EQ.
%token GT LE LT GE ESCAPE.

// The following directive causes tokens ABORT, AFTER, ASC, etc. to
// fallback to ID if they will not parse as their original value.
// This obviates the need for the "id" nonterminal.
//
%fallback ID
  ABORT ACTION AFTER ANALYZE ASC ATTACH BEFORE BEGIN BY CASCADE CAST COLUMNKW
  CONFLICT DATABASE DEFERRED DESC DETACH DO
  EACH END EXCLUSIVE EXPLAIN FAIL FOR
  IGNORE IMMEDIATE INITIALLY INSTEAD LIKE_KW MATCH NO PLAN
  QUERY KEY OF OFFSET PRAGMA RAISE RECURSIVE RELEASE REPLACE RESTRICT ROW ROWS
  ROLLBACK SAVEPOINT TEMP TRIGGER VACUUM VIEW VIRTUAL WITH WITHOUT
  NULLS FIRST LAST
%ifdef SQLITE_OMIT_COMPOUND_SELECT
  EXCEPT INTERSECT UNION
%endif SQLITE_OMIT_COMPOUND_SELECT
%ifndef SQLITE_OMIT_WINDOWFUNC
  CURRENT FOLLOWING PARTITION PRECEDING RANGE UNBOUNDED
  EXCLUDE GROUPS OTHERS TIES
%endif SQLITE_OMIT_WINDOWFUNC
%ifndef SQLITE_OMIT_GENERATED_COLUMNS
  GENERATED ALWAYS
%endif
  MATERIALIZED
  REINDEX RENAME CTIME_KW IF
  .
%wildcard ANY.

// Define operator precedence early so that this is the first occurrence
// of the operator tokens in the grammar.  Keeping the operators together
// causes them to be assigned integer values that are close together,
// which keeps parser tables smaller.
//
// The token values assigned to these symbols is determined by the order
// in which lemon first sees them.  It must be the case that ISNULL/NOTNULL,
// NE/EQ, GT/LE, and GE/LT are separated by only a single value.  See
// the sqlite3ExprIfFalse() routine for additional information on this
// constraint.
//
%left OR.
%left AND.
%right NOT.
%left IS MATCH LIKE_KW BETWEEN IN ISNULL NOTNULL NE EQ.
%left GT LE LT GE.
%right ESCAPE.
%left BITAND BITOR LSHIFT RSHIFT.
%left PLUS MINUS.
%left STAR SLASH REM.
%left CONCAT PTR.
%left COLLATE.
%right BITNOT.
%nonassoc ON.

// An IDENTIFIER can be a generic identifier, or one of several
// keywords.  Any non-standard keyword can also be an identifier.
//
%token_class id  ID|INDEXED.

// And "ids" is an identifier-or-string.
//
%token_class ids  ID|STRING.

// An identifier or a join-keyword
//
%token_class idj  ID|INDEXED|JOIN_KW.

// The name of a column or table can be any of the following:
//
%type nm {Name}
nm(A) ::= idj(X). { A = Name::from_token(@X, X); }
nm(A) ::= STRING(X). { A = Name::from_token(@X, X); }

// A typetoken is really zero or more tokens that form a type name such
// as can be found after the column name in a CREATE TABLE statement.
// Multiple tokens are concatenated to form the value of the typetoken.
//
%type typetoken {Option<Type>}
typetoken(A) ::= .   {A = None;}
typetoken(A) ::= typename(X). {A = Some(Type{ name: X, size: None });}
typetoken(A) ::= typename(X) LP signed(Y) RP. {
  A = Some(Type{ name: X, size: Some(TypeSize::MaxSize(Box::new(Y))) });
}
typetoken(A) ::= typename(X) LP signed(Y) COMMA signed(Z) RP. {
  A = Some(Type{ name: X, size: Some(TypeSize::TypeSize(Box::new(Y), Box::new(Z))) });
}
%type typename {String}
typename(A) ::= ids(X). {A=from_token(@X, X);}
typename(A) ::= typename(A) ids(Y). {let ids=from_token(@Y, Y); A.push(' '); A.push_str(&ids);}
%type signed {Expr}
signed ::= plus_num.
signed ::= minus_num.

// The scanpt non-terminal takes a value which is a pointer to the
// input text just past the last token that has been shifted into
// the parser.  By surrounding some phrase in the grammar with two
// scanpt non-terminals, we can capture the input text for that phrase.
// For example:
//
//      something ::= .... scanpt(A) phrase scanpt(Z).
//
// The text that is parsed as "phrase" is a string starting at A
// and containing (int)(Z-A) characters.  There might be some extra
// whitespace on either end of the text, but that can be removed in
// post-processing, if needed.
//

// "carglist" is a list of additional constraints that come after the
// column name and column type in a CREATE TABLE statement.
//
%type carglist {Vec<NamedColumnConstraint>}
carglist(A) ::= carglist(A) ccons(X). {if self.ctx.no_constraint_name() { let cc = X; A.push(cc); }}
carglist(A) ::= .                     {A = vec![];}
%type ccons {NamedColumnConstraint}
ccons ::= CONSTRAINT nm(X).           { self.ctx.constraint_name = Some(X);}
ccons(A) ::= DEFAULT term(X). {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Default(X);
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= DEFAULT LP expr(X) RP. {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Default(Expr::parenthesized(X));
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= DEFAULT PLUS term(X). {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Default(Expr::Unary(UnaryOperator::Positive, Box::new(X)));
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= DEFAULT MINUS term(X).      {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Default(Expr::Unary(UnaryOperator::Negative, Box::new(X)));
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= DEFAULT id(X).       {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Default(Expr::id(@X, X));
  A = NamedColumnConstraint{ name, constraint };
}

// In addition to the type name, we also care about the primary key and
// UNIQUE constraints.
//
ccons(A) ::= NULL onconf(R). {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::NotNull{ nullable: true, conflict_clause: R};
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= NOT NULL onconf(R).    {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::NotNull{ nullable: false, conflict_clause: R};
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= PRIMARY KEY sortorder(Z) onconf(R) autoinc(I). {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::PrimaryKey{ order: Z, conflict_clause: R, auto_increment: I };
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= UNIQUE onconf(R).      {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Unique(R);
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= CHECK LP expr(X) RP.   {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Check(X);
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= REFERENCES nm(T) eidlist_opt(TA) refargs(R). {
  let name = self.ctx.constraint_name();
  let clause = ForeignKeyClause{ tbl_name: T, columns: TA, args: R };
  let constraint = ColumnConstraint::ForeignKey{ clause, deref_clause: None }; // FIXME deref_clause
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= defer_subclause(D).    {
  let constraint = ColumnConstraint::Defer(D);
  A = NamedColumnConstraint{ name: None, constraint };
}
ccons(A) ::= COLLATE ids(C).        {
  let name = self.ctx.constraint_name();
  let constraint = ColumnConstraint::Collate{ collation_name: Name::from_token(@C, C) };
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= GENERATED ALWAYS AS generated(X). {
  let name = self.ctx.constraint_name();
  let constraint = X;
  A = NamedColumnConstraint{ name, constraint };
}
ccons(A) ::= AS generated(X). {
  let name = self.ctx.constraint_name();
  let constraint = X;
  A = NamedColumnConstraint{ name, constraint };
}
%type generated {ColumnConstraint}
generated(X) ::= LP expr(E) RP. {
  X = ColumnConstraint::Generated{ expr: E, typ: None };
}
generated(X) ::= LP expr(E) RP ID(TYPE). {
  X = ColumnConstraint::Generated{ expr: E, typ: Some(Id::from_token(@TYPE, TYPE)) };
}

// The optional AUTOINCREMENT keyword
%type autoinc {bool}
autoinc(X) ::= .          {X = false;}
autoinc(X) ::= AUTOINCR.  {X = true;}

// The next group of rules parses the arguments to a REFERENCES clause
// that determine if the referential integrity checking is deferred or
// or immediate and which determine what action to take if a ref-integ
// check fails.
//
%type refargs {Vec<RefArg>}
refargs(A) ::= .                  { A = vec![]; /* EV: R-19803-45884 */}
refargs(A) ::= refargs(A) refarg(Y). { let ra = Y; A.push(ra); }
%type refarg {RefArg}
refarg(A) ::= MATCH nm(X).              { A = RefArg::Match(X); }
refarg(A) ::= ON INSERT refact(X).      { A = RefArg::OnInsert(X); }
refarg(A) ::= ON DELETE refact(X).   { A = RefArg::OnDelete(X); }
refarg(A) ::= ON UPDATE refact(X).   { A = RefArg::OnUpdate(X); }
%type refact {RefAct}
refact(A) ::= SET NULL.              { A = RefAct::SetNull;  /* EV: R-33326-45252 */}
refact(A) ::= SET DEFAULT.           { A = RefAct::SetDefault;  /* EV: R-33326-45252 */}
refact(A) ::= CASCADE.               { A = RefAct::Cascade;  /* EV: R-33326-45252 */}
refact(A) ::= RESTRICT.              { A = RefAct::Restrict; /* EV: R-33326-45252 */}
refact(A) ::= NO ACTION.             { A = RefAct::NoAction;     /* EV: R-33326-45252 */}
%type defer_subclause {DeferSubclause}
defer_subclause(A) ::= NOT DEFERRABLE init_deferred_pred_opt(X).     {A = DeferSubclause{ deferrable: false, init_deferred: X };}
defer_subclause(A) ::= DEFERRABLE init_deferred_pred_opt(X).      {A = DeferSubclause{ deferrable: true, init_deferred: X };}
%type init_deferred_pred_opt {Option<InitDeferredPred>}
init_deferred_pred_opt(A) ::= .                       {A = None;}
init_deferred_pred_opt(A) ::= INITIALLY DEFERRED.     {A = Some(InitDeferredPred::InitiallyDeferred);}
init_deferred_pred_opt(A) ::= INITIALLY IMMEDIATE.    {A = Some(InitDeferredPred::InitiallyImmediate);}

%type conslist_opt {Option<Vec<NamedTableConstraint>>}
conslist_opt(A) ::= .                         {A = None;}
conslist_opt(A) ::= COMMA conslist(X).        {A = Some(X);}
%type conslist {Vec<NamedTableConstraint>}
conslist(A) ::= conslist(A) tconscomma tcons(X). {if self.ctx.no_constraint_name() { let tc = X; A.push(tc); }}
conslist(A) ::= tcons(X).                        {if self.ctx.no_constraint_name() { let tc = X; A = vec![tc]; } else { A = vec![]; }}
tconscomma ::= COMMA.            { self.ctx.constraint_name = None;} // TODO Validate: useful ?
tconscomma ::= .
%type tcons {NamedTableConstraint}
tcons ::= CONSTRAINT nm(X).      { self.ctx.constraint_name = Some(X)}
tcons(A) ::= PRIMARY KEY LP sortlist(X) autoinc(I) RP onconf(R). {
  let name = self.ctx.constraint_name();
  let constraint = TableConstraint::PrimaryKey{ columns: X, auto_increment: I, conflict_clause: R };
  A = NamedTableConstraint{ name, constraint };
}
tcons(A) ::= UNIQUE LP sortlist(X) RP onconf(R). {
  let name = self.ctx.constraint_name();
  let constraint = TableConstraint::Unique{ columns: X, conflict_clause: R };
  A = NamedTableConstraint{ name, constraint };
}
tcons(A) ::= CHECK LP expr(E) RP onconf. {
  let name = self.ctx.constraint_name();
  let constraint = TableConstraint::Check(E);
  A = NamedTableConstraint{ name, constraint };
}
tcons(A) ::= FOREIGN KEY LP eidlist(FA) RP
          REFERENCES nm(T) eidlist_opt(TA) refargs(R) defer_subclause_opt(D). {
  let name = self.ctx.constraint_name();
  let clause = ForeignKeyClause{ tbl_name: T, columns: TA, args: R };
  let constraint = TableConstraint::ForeignKey{ columns: FA, clause, deref_clause: D };
  A = NamedTableConstraint{ name, constraint };
}
%type defer_subclause_opt {Option<DeferSubclause>}
defer_subclause_opt(A) ::= .                    {A = None;}
defer_subclause_opt(A) ::= defer_subclause(X).  {A = Some(X);}

// The following is a non-standard extension that allows us to declare the
// default behavior when there is a constraint conflict.
//
%type onconf {Option<ResolveType>}
%type orconf {Option<ResolveType>}
%type resolvetype {ResolveType}
onconf(A) ::= .                              {A = None;}
onconf(A) ::= ON CONFLICT resolvetype(X).    {A = Some(X);}
orconf(A) ::= .                              {A = None;}
orconf(A) ::= OR resolvetype(X).             {A = Some(X);}
resolvetype(A) ::= raisetype(A).
resolvetype(A) ::= IGNORE.                   {A = ResolveType::Ignore;}
resolvetype(A) ::= REPLACE.                  {A = ResolveType::Replace;}

////////////////////////// The DROP TABLE /////////////////////////////////////
//
cmd ::= DROP TABLE ifexists(E) fullname(X). {
  self.ctx.stmt = Some(Stmt::DropTable{ if_exists: E, tbl_name: X});
}
%type ifexists {bool}
ifexists(A) ::= IF EXISTS.   {A = true;}
ifexists(A) ::= .            {A = false;}

///////////////////// The CREATE VIEW statement /////////////////////////////
//
%ifndef SQLITE_OMIT_VIEW
cmd ::= createkw temp(T) VIEW ifnotexists(E) fullname(Y) eidlist_opt(C)
          AS select(S). {
  self.ctx.stmt = Some(Stmt::CreateView{ temporary: T, if_not_exists: E, view_name: Y, columns: C,
                                         select: S });
}
cmd ::= DROP VIEW ifexists(E) fullname(X). {
  self.ctx.stmt = Some(Stmt::DropView{ if_exists: E, view_name: X });
}
%endif  SQLITE_OMIT_VIEW

//////////////////////// The SELECT statement /////////////////////////////////
//
cmd ::= select(X).  {
  self.ctx.stmt = Some(Stmt::Select(X));
}

%type select {Select}
%type selectnowith {SelectBody}
%type oneselect {OneSelect}

%include {
}

%ifndef SQLITE_OMIT_CTE
select(A) ::= WITH wqlist(W) selectnowith(X) orderby_opt(Z) limit_opt(L). {
  A = Select{ with: Some(With { recursive: false, ctes: W }), body: X, order_by: Z, limit: L };
}
select(A) ::= WITH RECURSIVE wqlist(W) selectnowith(X) orderby_opt(Z) limit_opt(L). {
  A = Select{ with: Some(With { recursive: true, ctes: W }), body: X, order_by: Z, limit: L };
}
%endif /* SQLITE_OMIT_CTE */
select(A) ::= selectnowith(X) orderby_opt(Z) limit_opt(L). {
  A = Select{ with: None, body: X, order_by: Z, limit: L }; /*A-overwrites-X*/
}

selectnowith(A) ::= oneselect(X). {
  A = SelectBody{ select: X, compounds: None };
}
%ifndef SQLITE_OMIT_COMPOUND_SELECT
selectnowith(A) ::= selectnowith(A) multiselect_op(Y) oneselect(Z).  {
  let cs = CompoundSelect{ operator: Y, select: Z };
  A.push(cs)?;
}
%type multiselect_op {CompoundOperator}
multiselect_op(A) ::= UNION.             {A = CompoundOperator::Union;}
multiselect_op(A) ::= UNION ALL.         {A = CompoundOperator::UnionAll;}
multiselect_op(A) ::= EXCEPT.            {A = CompoundOperator::Except;}
multiselect_op(A) ::= INTERSECT.         {A = CompoundOperator::Intersect;}
%endif SQLITE_OMIT_COMPOUND_SELECT

oneselect(A) ::= SELECT distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P). {
  A = OneSelect::Select{ distinctness: D, columns: W, from: X, where_clause: Y,
                         group_by: P, window_clause: None };
    }
%ifndef SQLITE_OMIT_WINDOWFUNC
oneselect(A) ::= SELECT distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) window_clause(R). {
  A = OneSelect::Select{ distinctness: D, columns: W, from: X, where_clause: Y,
                         group_by: P, window_clause: Some(R) };
}
%endif


oneselect(A) ::= values(X). { A = OneSelect::Values(X); }

%type values {Vec<Vec<Expr>>}
values(A) ::= VALUES LP nexprlist(X) RP. {
  A = vec![X];
}
values(A) ::= values(A) COMMA LP nexprlist(Y) RP. {
  let exprs = Y;
  OneSelect::push(A, exprs)?;
}

// The "distinct" nonterminal is true (1) if the DISTINCT keyword is
// present and false (0) if it is not.
//
%type distinct {Option<Distinctness>}
distinct(A) ::= DISTINCT.   {A = Some(Distinctness::Distinct);}
distinct(A) ::= ALL.        {A = Some(Distinctness::All);}
distinct(A) ::= .           {A = None;}

// selcollist is a list of expressions that are to become the return
// values of the SELECT statement.  The "*" in statements like
// "SELECT * FROM ..." is encoded as a special expression with an
// opcode of TK_ASTERISK.
//
%type selcollist {Vec<ResultColumn>}
%type sclp {Vec<ResultColumn>}
sclp(A) ::= selcollist(A) COMMA.
sclp(A) ::= .                                {A = Vec::<ResultColumn>::new();}
selcollist(A) ::= sclp(A) expr(X) as(Y).     {
  let rc = ResultColumn::Expr(X, Y);
  A.push(rc);
}
selcollist(A) ::= sclp(A) STAR. {
  let rc = ResultColumn::Star;
  A.push(rc);
}
selcollist(A) ::= sclp(A) nm(X) DOT STAR. {
  let rc = ResultColumn::TableStar(X);
  A.push(rc);
}

// An option "AS <id>" phrase that can follow one of the expressions that
// define the result set, or one of the tables in the FROM clause.
//
%type as {Option<As>}
as(X) ::= AS nm(Y).    {X = Some(As::As(Y));}
as(X) ::= ids(Y).      {X = Some(As::Elided(Name::from_token(@Y, Y)));}
as(X) ::= .            {X = None;}


%type seltablist {FromClause}
%type stl_prefix {FromClause}
%type from {Option<FromClause>}

// A complete FROM clause.
//
from(A) ::= .                {A = None;}
from(A) ::= FROM seltablist(X). {
  A = Some(X);
}

// "seltablist" is a "Select Table List" - the content of the FROM clause
// in a SELECT statement.  "stl_prefix" is a prefix of this list.
//
stl_prefix(A) ::= seltablist(A) joinop(Y).    {
   let op = Y;
   A.push_op(op);
}
stl_prefix(A) ::= .                           {A = FromClause::empty();}
seltablist(A) ::= stl_prefix(A) fullname(Y) as(Z) indexed_opt(I)
                  on_using(N). {
    let st = SelectTable::Table(Y, Z, I);
    let jc = N;
    A.push(st, jc)?;
}
seltablist(A) ::= stl_prefix(A) fullname(Y) LP exprlist(E) RP as(Z)
                  on_using(N). {
    let st = SelectTable::TableCall(Y, E, Z);
    let jc = N;
    A.push(st, jc)?;
}
%ifndef SQLITE_OMIT_SUBQUERY
  seltablist(A) ::= stl_prefix(A) LP select(S) RP
                    as(Z) on_using(N). {
    let st = SelectTable::Select(S, Z);
    let jc = N;
    A.push(st, jc)?;
  }
  seltablist(A) ::= stl_prefix(A) LP seltablist(F) RP
                    as(Z) on_using(N). {
    let st = SelectTable::Sub(F, Z);
    let jc = N;
    A.push(st, jc)?;
  }
%endif  SQLITE_OMIT_SUBQUERY

%type fullname {QualifiedName}
fullname(A) ::= nm(X).  {
  A = QualifiedName::single(X);
}
fullname(A) ::= nm(X) DOT nm(Y). {
  A = QualifiedName::fullname(X, Y);
}

%type xfullname {QualifiedName}
xfullname(A) ::= nm(X).
   {A = QualifiedName::single(X); /*A-overwrites-X*/}
xfullname(A) ::= nm(X) DOT nm(Y).
   {A = QualifiedName::fullname(X, Y); /*A-overwrites-X*/}
xfullname(A) ::= nm(X) DOT nm(Y) AS nm(Z).  {
   A = QualifiedName::xfullname(X, Y, Z); /*A-overwrites-X*/
}
xfullname(A) ::= nm(X) AS nm(Z). {
   A = QualifiedName::alias(X, Z); /*A-overwrites-X*/
}

%type joinop {JoinOperator}
joinop(X) ::= COMMA.              { X = JoinOperator::Comma; }
joinop(X) ::= JOIN.              { X = JoinOperator::TypedJoin(None); }
joinop(X) ::= JOIN_KW(A) JOIN.
                  {X = JoinOperator::from(A, None, None)?;  /*X-overwrites-A*/}
joinop(X) ::= JOIN_KW(A) nm(B) JOIN.
                  {X = JoinOperator::from(A, Some(B), None)?; /*X-overwrites-A*/}
joinop(X) ::= JOIN_KW(A) nm(B) nm(C) JOIN.
                  {X = JoinOperator::from(A, Some(B), Some(C))?;/*X-overwrites-A*/}

// There is a parsing ambiguity in an upsert statement that uses a
// SELECT on the RHS of a the INSERT:
//
//      INSERT INTO tab SELECT * FROM aaa JOIN bbb ON CONFLICT ...
//                                        here ----^^
//
// When the ON token is encountered, the parser does not know if it is
// the beginning of an ON CONFLICT clause, or the beginning of an ON
// clause associated with the JOIN.  The conflict is resolved in favor
// of the JOIN.  If an ON CONFLICT clause is intended, insert a dummy
// WHERE clause in between, like this:
//
//      INSERT INTO tab SELECT * FROM aaa JOIN bbb WHERE true ON CONFLICT ...
//
// The [AND] and [OR] precedence marks in the rules for on_using cause the
// ON in this context to always be interpreted as belonging to the JOIN.
//
%type on_using {Option<JoinConstraint>}
on_using(N) ::= ON expr(E).            {N = Some(JoinConstraint::On(E));}
on_using(N) ::= USING LP idlist(L) RP. {N = Some(JoinConstraint::Using(L));}
on_using(N) ::= .                 [OR] {N = None;}

// Note that this block abuses the Token type just a little. If there is
// no "INDEXED BY" clause, the returned token is empty (z==0 && n==0). If
// there is an INDEXED BY clause, then the token is populated as per normal,
// with z pointing to the token data and n containing the number of bytes
// in the token.
//
// If there is a "NOT INDEXED" clause, then (z==0 && n==1), which is
// normally illegal. The sqlite3SrcListIndexedBy() function
// recognizes and interprets this as a special case.
//
%type indexed_opt {Option<Indexed>}
indexed_opt(A) ::= .                 {A = None;}
indexed_opt(A) ::= INDEXED BY nm(X). {A = Some(Indexed::IndexedBy(X));}
indexed_opt(A) ::= NOT INDEXED.      {A = Some(Indexed::NotIndexed);}

%type orderby_opt {Option<Vec<SortedColumn>>}

// the sortlist non-terminal stores a list of expression where each
// expression is optionally followed by ASC or DESC to indicate the
// sort order.
//
%type sortlist {Vec<SortedColumn>}

orderby_opt(A) ::= .                          {A = None;}
orderby_opt(A) ::= ORDER BY sortlist(X).      {A = Some(X);}
sortlist(A) ::= sortlist(A) COMMA expr(Y) sortorder(Z) nulls(X). {
  let sc = SortedColumn { expr: Y, order: Z, nulls: X };
  A.push(sc);
}
sortlist(A) ::= expr(Y) sortorder(Z) nulls(X). {
  A = vec![SortedColumn { expr: Y, order: Z, nulls: X }]; /*A-overwrites-Y*/
}

%type sortorder {Option<SortOrder>}

sortorder(A) ::= ASC.           {A = Some(SortOrder::Asc);}
sortorder(A) ::= DESC.          {A = Some(SortOrder::Desc);}
sortorder(A) ::= .              {A = None;}

%type nulls {Option<NullsOrder>}
nulls(A) ::= NULLS FIRST.       {A = Some(NullsOrder::First);}
nulls(A) ::= NULLS LAST.        {A = Some(NullsOrder::Last);}
nulls(A) ::= .                  {A = None;}

%type groupby_opt {Option<GroupBy>}
groupby_opt(A) ::= .                      {A = None;}
groupby_opt(A) ::= GROUP BY nexprlist(X) having_opt(Y). {A = Some(GroupBy{ exprs: X, having: Y });}

%type having_opt {Option<Expr>}
having_opt(A) ::= .                {A = None;}
having_opt(A) ::= HAVING expr(X).  {A = Some(X);}

%type limit_opt {Option<Limit>}

// The destructor for limit_opt will never fire in the current grammar.
// The limit_opt non-terminal only occurs at the end of a single production
// rule for SELECT statements.  As soon as the rule that create the
// limit_opt non-terminal reduces, the SELECT statement rule will also
// reduce.  So there is never a limit_opt non-terminal on the stack
// except as a transient.  So there is never anything to destroy.
//
//%destructor limit_opt {sqlite3ExprDelete(pParse->db, $$);}
limit_opt(A) ::= .       {A = None;}
limit_opt(A) ::= LIMIT expr(X).
                         {A = Some(Limit{ expr: X, offset: None });}
limit_opt(A) ::= LIMIT expr(X) OFFSET expr(Y).
                         {A = Some(Limit{ expr: X, offset: Some(Y) });}
limit_opt(A) ::= LIMIT expr(X) COMMA expr(Y).
                         {A = Some(Limit{ expr: X, offset: Some(Y) });}

/////////////////////////// The DELETE statement /////////////////////////////
//
%if SQLITE_ENABLE_UPDATE_DELETE_LIMIT || SQLITE_UDL_CAPABLE_PARSER
cmd ::= with(C) DELETE FROM xfullname(X) indexed_opt(I) where_opt_ret(W)
        orderby_opt(O) limit_opt(L). {
  let (where_clause, returning) = W;
  self.ctx.stmt = Some(Stmt::Delete{ with: C, tbl_name: X, indexed: I, where_clause, returning,
                                     order_by: O, limit: L });
}
%else
cmd ::= with(C) DELETE FROM xfullname(X) indexed_opt(I) where_opt_ret(W). {
  let (where_clause, returning) = W;
  self.ctx.stmt = Some(Stmt::Delete{ with: C, tbl_name: X, indexed: I, where_clause, returning,
                                     order_by: None, limit: None });
}
%endif

%type where_opt {Option<Expr>}
%type where_opt_ret {(Option<Expr>, Option<Vec<ResultColumn>>)}

where_opt(A) ::= .                    {A = None;}
where_opt(A) ::= WHERE expr(X).       {A = Some(X);}
where_opt_ret(A) ::= .                                      {A = (None, None);}
where_opt_ret(A) ::= WHERE expr(X).                         {A = (Some(X), None);}
where_opt_ret(A) ::= RETURNING selcollist(X).
       {A = (None, Some(X));}
where_opt_ret(A) ::= WHERE expr(X) RETURNING selcollist(Y).
       {A = (Some(X), Some(Y));}

////////////////////////// The UPDATE command ////////////////////////////////
//
%if SQLITE_ENABLE_UPDATE_DELETE_LIMIT || SQLITE_UDL_CAPABLE_PARSER
cmd ::= with(C) UPDATE orconf(R) xfullname(X) indexed_opt(I) SET setlist(Y) from(F)
        where_opt_ret(W) orderby_opt(O) limit_opt(L).  {
  let (where_clause, returning) = W;
  self.ctx.stmt = Some(Stmt::Update { with: C, or_conflict: R, tbl_name: X, indexed: I, sets: Y, from: F,
                                      where_clause, returning, order_by: O, limit: L });
}
%else
cmd ::= with(C) UPDATE orconf(R) xfullname(X) indexed_opt(I) SET setlist(Y) from(F)
        where_opt_ret(W). {
  let (where_clause, returning) = W;
  self.ctx.stmt = Some(Stmt::Update { with: C, or_conflict: R, tbl_name: X, indexed: I, sets: Y, from: F,
                                      where_clause, returning, order_by: None, limit: None });
}
%endif



%type setlist {Vec<Set>}

setlist(A) ::= setlist(A) COMMA nm(X) EQ expr(Y). {
  let s = Set{ col_names: DistinctNames::single(X), expr: Y };
  A.push(s);
}
setlist(A) ::= setlist(A) COMMA LP idlist(X) RP EQ expr(Y). {
  let s = Set{ col_names: X, expr: Y };
  A.push(s);
}
setlist(A) ::= nm(X) EQ expr(Y). {
  A = vec![Set{ col_names: DistinctNames::single(X), expr: Y }];
}
setlist(A) ::= LP idlist(X) RP EQ expr(Y). {
  A = vec![Set{ col_names: X, expr: Y }];
}

////////////////////////// The INSERT command /////////////////////////////////
//
cmd ::= with(W) insert_cmd(R) INTO xfullname(X) idlist_opt(F) select(S)
        upsert(U). {
  let (upsert, returning) = U;
  let body = InsertBody::Select(S, upsert);
  self.ctx.stmt = Some(Stmt::Insert{ with: W, or_conflict: R, tbl_name: X, columns: F,
                                     body, returning });
}
cmd ::= with(W) insert_cmd(R) INTO xfullname(X) idlist_opt(F) DEFAULT VALUES returning(Y).
{
  let body = InsertBody::DefaultValues;
  self.ctx.stmt = Some(Stmt::Insert{ with: W, or_conflict: R, tbl_name: X, columns: F,
                                     body, returning: Y });
}

%type upsert {(Option<Upsert>, Option<Vec<ResultColumn>>)}

// Because upsert only occurs at the tip end of the INSERT rule for cmd,
// there is never a case where the value of the upsert pointer will not
// be destroyed by the cmd action.  So comment-out the destructor to
// avoid unreachable code.
//%destructor upsert {sqlite3UpsertDelete(pParse->db,$$);}
upsert(A) ::= . { A = (None, None); }
upsert(A) ::= RETURNING selcollist(X).  { A = (None, Some(X)); }
upsert(A) ::= ON CONFLICT LP sortlist(T) RP where_opt(TW)
              DO UPDATE SET setlist(Z) where_opt(W) upsert(N).
              { let index = UpsertIndex{ targets: T, where_clause: TW };
                let do_clause = UpsertDo::Set{ sets: Z, where_clause: W };
                let (next, returning) = N;
                A = (Some(Upsert{ index: Some(index), do_clause, next: next.map(Box::new) }), returning);}
upsert(A) ::= ON CONFLICT LP sortlist(T) RP where_opt(TW) DO NOTHING upsert(N).
              { let index = UpsertIndex{ targets: T, where_clause: TW };
                let (next, returning) = N;
                A = (Some(Upsert{ index: Some(index), do_clause: UpsertDo::Nothing, next: next.map(Box::new) }), returning); }
upsert(A) ::= ON CONFLICT DO NOTHING returning(R).
              { A = (Some(Upsert{ index: None, do_clause: UpsertDo::Nothing, next: None }), R); }
upsert(A) ::= ON CONFLICT DO UPDATE SET setlist(Z) where_opt(W) returning(R).
              { let do_clause = UpsertDo::Set{ sets: Z, where_clause: W };
                A = (Some(Upsert{ index: None, do_clause, next: None }), R);}

%type returning {Option<Vec<ResultColumn>>}
returning(A) ::= RETURNING selcollist(X).  {A = Some(X);}
returning(A) ::= . {A = None;}

%type insert_cmd {Option<ResolveType>}
insert_cmd(A) ::= INSERT orconf(R).   {A = R;}
insert_cmd(A) ::= REPLACE.            {A = Some(ResolveType::Replace);}

%type idlist_opt {Option<DistinctNames>}
%type idlist {DistinctNames}
idlist_opt(A) ::= .                       {A = None;}
idlist_opt(A) ::= LP idlist(X) RP.    {A = Some(X);}
idlist(A) ::= idlist(A) COMMA nm(Y).
    {let id = Y; A.insert(id)?;}
idlist(A) ::= nm(Y).
    { A = DistinctNames::new(Y); /*A-overwrites-Y*/}

/////////////////////////// Expression Processing /////////////////////////////
//

%type expr {Expr}
%type term {Expr}

%include {
}

expr(A) ::= term(A).
expr(A) ::= LP expr(X) RP. {A = Expr::parenthesized(X);}
expr(A) ::= idj(X).          {A= Expr::id(@X, X); /*A-overwrites-X*/}
expr(A) ::= nm(X) DOT nm(Y). {
  A = Expr::Qualified(X, Y); /*A-overwrites-X*/
}
expr(A) ::= nm(X) DOT nm(Y) DOT nm(Z). {
  A = Expr::DoublyQualified(X, Y, Z); /*A-overwrites-X*/
}
term(A) ::= NULL. {A=Expr::Literal(Literal::Null);}
term(A) ::= BLOB(X). {A=Expr::Literal(Literal::Blob(X.unwrap())); /*A-overwrites-X*/}
term(A) ::= STRING(X).          {A=Expr::Literal(Literal::String(X.unwrap())); /*A-overwrites-X*/}
term(A) ::= FLOAT|INTEGER(X). {
  A = Expr::Literal(Literal::Numeric(X.unwrap())); /*A-overwrites-X*/
}
expr(A) ::= VARIABLE(X).     {
  A = Expr::Variable(X.unwrap()); /*A-overwrites-X*/
}
expr(A) ::= expr(X) COLLATE ids(C). {
  A = Expr::collate(X, @C, C); /*A-overwrites-X*/
}
%ifndef SQLITE_OMIT_CAST
expr(A) ::= CAST LP expr(E) AS typetoken(T) RP. {
  A = Expr::cast(E, T);
}
%endif  SQLITE_OMIT_CAST

expr(A) ::= idj(X) LP distinct(D) exprlist(Y) RP. {
  A = Expr::FunctionCall{ name: Id::from_token(@X, X), distinctness: D, args: Y, order_by: None, filter_over: None }; /*A-overwrites-X*/
}
expr(A) ::= idj(X) LP distinct(D) exprlist(Y) ORDER BY sortlist(O) RP. {
  A = Expr::FunctionCall{ name: Id::from_token(@X, X), distinctness: D, args: Y, order_by: Some(O), filter_over: None }; /*A-overwrites-X*/
}
expr(A) ::= idj(X) LP STAR RP. {
  A = Expr::FunctionCallStar{ name: Id::from_token(@X, X), filter_over: None }; /*A-overwrites-X*/
}

%ifndef SQLITE_OMIT_WINDOWFUNC
expr(A) ::= idj(X) LP distinct(D) exprlist(Y) RP filter_over(Z). {
  A = Expr::FunctionCall{ name: Id::from_token(@X, X), distinctness: D, args: Y, order_by: None, filter_over: Some(Z) }; /*A-overwrites-X*/
}
expr(A) ::= idj(X) LP distinct(D) exprlist(Y) ORDER BY sortlist(O) RP filter_over(Z). {
  A = Expr::FunctionCall{ name: Id::from_token(@X, X), distinctness: D, args: Y, order_by: Some(O), filter_over: Some(Z) }; /*A-overwrites-X*/
}
expr(A) ::= idj(X) LP STAR RP filter_over(Z). {
  A = Expr::FunctionCallStar{ name: Id::from_token(@X, X), filter_over: Some(Z) }; /*A-overwrites-X*/
}
%endif

term(A) ::= CTIME_KW(OP). {
  A = Expr::Literal(Literal::from_ctime_kw(OP));
}

expr(A) ::= LP nexprlist(X) COMMA expr(Y) RP. {
  let mut x = X;
  x.push(Y);
  A = Expr::Parenthesized(x);
}

expr(A) ::= expr(X) AND(OP) expr(Y).    {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
expr(A) ::= expr(X) OR(OP) expr(Y).     {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
expr(A) ::= expr(X) LT|GT|GE|LE(OP) expr(Y).
                                        {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
expr(A) ::= expr(X) EQ|NE(OP) expr(Y).  {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
expr(A) ::= expr(X) BITAND|BITOR|LSHIFT|RSHIFT(OP) expr(Y).
                                        {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
expr(A) ::= expr(X) PLUS|MINUS(OP) expr(Y).
                                        {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
expr(A) ::= expr(X) STAR|SLASH|REM(OP) expr(Y).
                                        {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
expr(A) ::= expr(X) CONCAT(OP) expr(Y). {A=Expr::binary(X,@OP,Y); /*A-overwrites-X*/}
%type likeop {(bool, LikeOperator)}
likeop(A) ::= LIKE_KW|MATCH(X). {A=(false, LikeOperator::from_token(@X, X)); /*A-overwrite-X*/}
likeop(A) ::= NOT LIKE_KW|MATCH(X). {A=(true, LikeOperator::from_token(@X, X)); /*A-overwrite-X*/}
expr(A) ::= expr(X) likeop(OP) expr(Y).  [LIKE_KW]  {
  let op = OP;
  A=Expr::like(X,op.0,op.1,Y,None); /*A-overwrites-X*/
}
expr(A) ::= expr(X) likeop(OP) expr(Y) ESCAPE expr(E).  [LIKE_KW]  {
  let op = OP;
  A=Expr::like(X,op.0,op.1,Y,Some(E)); /*A-overwrites-X*/
}

expr(A) ::= expr(X) ISNULL|NOTNULL(E).   {A = Expr::not_null(X, @E); /*A-overwrites-X*/}
expr(A) ::= expr(X) NOT NULL.    {A = Expr::not_null(X, TokenType::TK_NOTNULL as YYCODETYPE); /*A-overwrites-X*/}

%include {
}

//    expr1 IS expr2
//    expr1 IS NOT expr2
//
// If expr2 is NULL then code as TK_ISNULL or TK_NOTNULL.  If expr2
// is any other expression, code as TK_IS or TK_ISNOT.
//
expr(A) ::= expr(X) IS(OP) expr(Y).     {
  A = Expr::binary(X, @OP, Y); /*A-overwrites-X*/
}
expr(A) ::= expr(X) IS NOT expr(Y). {
  A = Expr::binary(X, TokenType::TK_NOT as YYCODETYPE, Y); /*A-overwrites-X*/
}
expr(A) ::= expr(X) IS NOT DISTINCT FROM expr(Y).     {
  A = Expr::binary(X, TokenType::TK_IS as YYCODETYPE, Y); /*A-overwrites-X*/
}
expr(A) ::= expr(X) IS DISTINCT FROM expr(Y). {
  A = Expr::binary(X, TokenType::TK_NOT as YYCODETYPE, Y); /*A-overwrites-X*/
}

expr(A) ::= NOT(B) expr(X).
              {A = Expr::unary(UnaryOperator::from(@B), X);/*A-overwrites-B*/}
expr(A) ::= BITNOT(B) expr(X).
              {A = Expr::unary(UnaryOperator::from(@B), X);/*A-overwrites-B*/}
expr(A) ::= PLUS|MINUS(B) expr(X). [BITNOT] {
  A = Expr::unary(UnaryOperator::from(@B), X);/*A-overwrites-B*/
}

expr(A) ::= expr(B) PTR(C) expr(D). {
  A = Expr::ptr(B, C, D);
}

%type between_op {bool}
between_op(A) ::= BETWEEN.     {A = false;}
between_op(A) ::= NOT BETWEEN. {A = true;}
expr(A) ::= expr(B) between_op(N) expr(X) AND expr(Y). [BETWEEN] {
  A = Expr::between(B, N, X, Y);/*A-overwrites-B*/
}
%ifndef SQLITE_OMIT_SUBQUERY
  %type in_op {bool}
  in_op(A) ::= IN.      {A = false;}
  in_op(A) ::= NOT IN.  {A = true;}
  expr(A) ::= expr(X) in_op(N) LP exprlist(Y) RP. [IN] {
    A = Expr::in_list(X, N, Y);/*A-overwrites-X*/
  }
  expr(A) ::= LP select(X) RP. {
    A = Expr::sub_query(X);
  }
  expr(A) ::= expr(X) in_op(N) LP select(Y) RP.  [IN] {
    A = Expr::in_select(X, N, Y);/*A-overwrites-X*/
  }
  expr(A) ::= expr(X) in_op(N) fullname(Y) paren_exprlist(E). [IN] {
    A = Expr::in_table(X, N, Y, E);/*A-overwrites-X*/
  }
  expr(A) ::= EXISTS LP select(Y) RP. {
    A = Expr::Exists(Box::new(Y));
  }
%endif SQLITE_OMIT_SUBQUERY

/* CASE expressions */
expr(A) ::= CASE case_operand(X) case_exprlist(Y) case_else(Z) END. {
  A = Expr::Case{ base: X.map(Box::new), when_then_pairs: Y, else_expr: Z.map(Box::new)};
}
%type case_exprlist {Vec<(Expr, Expr)>}
case_exprlist(A) ::= case_exprlist(A) WHEN expr(Y) THEN expr(Z). {
  let pair = (Y, Z);
  A.push(pair);
}
case_exprlist(A) ::= WHEN expr(Y) THEN expr(Z). {
  A = vec![(Y, Z)];
}
%type case_else {Option<Expr>}
case_else(A) ::=  ELSE expr(X).         {A = Some(X);}
case_else(A) ::=  .                     {A = None;}
%type case_operand {Option<Expr>}
case_operand(A) ::= expr(X).            {A = Some(X); /*A-overwrites-X*/}
case_operand(A) ::= .                   {A = None;}

%type exprlist {Option<Vec<Expr>>}
%type nexprlist {Vec<Expr>}

exprlist(A) ::= nexprlist(X).                {A = Some(X);}
exprlist(A) ::= .                            {A = None;}
nexprlist(A) ::= nexprlist(A) COMMA expr(Y).
    { let expr = Y; A.push(expr);}
nexprlist(A) ::= expr(Y).
    {A = vec![Y]; /*A-overwrites-Y*/}

%ifndef SQLITE_OMIT_SUBQUERY
/* A paren_exprlist is an optional expression list contained inside
** of parenthesis */
%type paren_exprlist {Option<Vec<Expr>>}
paren_exprlist(A) ::= .   {A = None;}
paren_exprlist(A) ::= LP exprlist(X) RP.  {A = X;}
%endif SQLITE_OMIT_SUBQUERY


///////////////////////////// The CREATE INDEX command ///////////////////////
//
cmd ::= createkw uniqueflag(U) INDEX ifnotexists(NE) fullname(X)
        ON nm(Y) LP sortlist(Z) RP where_opt(W). {
  self.ctx.stmt = Some(Stmt::CreateIndex { unique: U, if_not_exists: NE, idx_name: X,
                                            tbl_name: Y, columns: Z, where_clause: W });
}

%type uniqueflag {bool}
uniqueflag(A) ::= UNIQUE.  {A = true;}
uniqueflag(A) ::= .        {A = false;}


// The eidlist non-terminal (Expression Id List) generates an ExprList
// from a list of identifiers.  The identifier names are in ExprList.a[].zName.
// This list is stored in an ExprList rather than an IdList so that it
// can be easily sent to sqlite3ColumnsExprList().
//
// eidlist is grouped with CREATE INDEX because it used to be the non-terminal
// used for the arguments to an index.  That is just an historical accident.
//
// IMPORTANT COMPATIBILITY NOTE:  Some prior versions of SQLite accepted
// COLLATE clauses and ASC or DESC keywords on ID lists in inappropriate
// places - places that might have been stored in the sqlite_schema table.
// Those extra features were ignored.  But because they might be in some
// (busted) old databases, we need to continue parsing them when loading
// historical schemas.
//
%type eidlist {Vec<IndexedColumn>}
%type eidlist_opt {Option<Vec<IndexedColumn>>}

%include {
} // end %include

eidlist_opt(A) ::= .                         {A = None;}
eidlist_opt(A) ::= LP eidlist(X) RP.         {A = Some(X);}
eidlist(A) ::= eidlist(A) COMMA nm(Y) collate(C) sortorder(Z).  {
  let ic = IndexedColumn{ col_name: Y, collation_name: C, order: Z };
  A.push(ic);
}
eidlist(A) ::= nm(Y) collate(C) sortorder(Z). {
  A = vec![IndexedColumn{ col_name: Y, collation_name: C, order: Z }]; /*A-overwrites-Y*/
}

%type collate {Option<Name>}
collate(C) ::= .              {C = None;}
collate(C) ::= COLLATE ids(X).   {C = Some(Name::from_token(@X, X));}


///////////////////////////// The DROP INDEX command /////////////////////////
//
cmd ::= DROP INDEX ifexists(E) fullname(X).   {self.ctx.stmt = Some(Stmt::DropIndex{if_exists: E, idx_name: X});}

///////////////////////////// The VACUUM command /////////////////////////////
//
%if !SQLITE_OMIT_VACUUM && !SQLITE_OMIT_ATTACH
%type vinto {Option<Expr>}
cmd ::= VACUUM vinto(Y).                {self.ctx.stmt = Some(Stmt::Vacuum(None, Y));}
cmd ::= VACUUM nm(X) vinto(Y).          {self.ctx.stmt = Some(Stmt::Vacuum(Some(X), Y));}
vinto(A) ::= INTO expr(X).              {A = Some(X);}
vinto(A) ::= .                          {A = None;}
%endif

///////////////////////////// The PRAGMA command /////////////////////////////
//
%ifndef SQLITE_OMIT_PRAGMA
cmd ::= PRAGMA fullname(X).                {self.ctx.stmt = Some(Stmt::Pragma(X, None));}
cmd ::= PRAGMA fullname(X) EQ nmnum(Y).    {self.ctx.stmt = Some(Stmt::Pragma(X, Some(PragmaBody::Equals(Y))));}
cmd ::= PRAGMA fullname(X) LP nmnum(Y) RP. {self.ctx.stmt = Some(Stmt::Pragma(X, Some(PragmaBody::Call(Y))));}
cmd ::= PRAGMA fullname(X) EQ minus_num(Y).
                                             {self.ctx.stmt = Some(Stmt::Pragma(X, Some(PragmaBody::Equals(Y))));}
cmd ::= PRAGMA fullname(X) LP minus_num(Y) RP.
                                             {self.ctx.stmt = Some(Stmt::Pragma(X, Some(PragmaBody::Call(Y))));}

%type nmnum {Expr}
nmnum(A) ::= plus_num(A).
nmnum(A) ::= nm(X). {A = Expr::Name(X);}
nmnum(A) ::= ON(X). {A = Expr::Literal(Literal::Keyword(from_token(@X, X)));}
nmnum(A) ::= DELETE(X). {A = Expr::Literal(Literal::Keyword(from_token(@X, X)));}
nmnum(A) ::= DEFAULT(X). {A = Expr::Literal(Literal::Keyword(from_token(@X, X)));}
%endif SQLITE_OMIT_PRAGMA
%token_class number INTEGER|FLOAT.
%type plus_num {Expr}
plus_num(A) ::= PLUS number(X).       {A = Expr::unary(UnaryOperator::Positive, Expr::Literal(Literal::Numeric(X.unwrap())));}
plus_num(A) ::= number(X).            {A = Expr::Literal(Literal::Numeric(X.unwrap()));}
%type minus_num {Expr}
minus_num(A) ::= MINUS number(X).     {A = Expr::unary(UnaryOperator::Negative, Expr::Literal(Literal::Numeric(X.unwrap())));}
//////////////////////////// The CREATE TRIGGER command /////////////////////

%ifndef SQLITE_OMIT_TRIGGER

cmd ::= createkw temp(T) TRIGGER ifnotexists(NOERR) fullname(B) trigger_time(C) trigger_event(D)
        ON fullname(E) foreach_clause(X) when_clause(G) BEGIN trigger_cmd_list(S) END. {
  self.ctx.stmt = Some(Stmt::CreateTrigger{
    temporary: T, if_not_exists: NOERR, trigger_name: B, time: C, event: D, tbl_name: E,
    for_each_row: X, when_clause: G, commands: S
  });
}

%type trigger_time {Option<TriggerTime>}
trigger_time(A) ::= BEFORE.  { A = Some(TriggerTime::Before); }
trigger_time(A) ::= AFTER.  { A = Some(TriggerTime::After); }
trigger_time(A) ::= INSTEAD OF.  { A = Some(TriggerTime::InsteadOf);}
trigger_time(A) ::= .            { A = None; }

%type trigger_event {TriggerEvent}
trigger_event(A) ::= DELETE.   {A = TriggerEvent::Delete;}
trigger_event(A) ::= INSERT.   {A = TriggerEvent::Insert;}
trigger_event(A) ::= UPDATE.          {A = TriggerEvent::Update;}
trigger_event(A) ::= UPDATE OF idlist(X).{A = TriggerEvent::UpdateOf(X);}

%type foreach_clause {bool}
foreach_clause(A) ::= .             { A = false; }
foreach_clause(A) ::= FOR EACH ROW. { A = true;  }

%type when_clause {Option<Expr>}
when_clause(A) ::= .             { A = None; }
when_clause(A) ::= WHEN expr(X). { A = Some(X); }

%type trigger_cmd_list {Vec<TriggerCmd>}
trigger_cmd_list(A) ::= trigger_cmd_list(A) trigger_cmd(X) SEMI. {
  let tc = X;
  A.push(tc);
}
trigger_cmd_list(A) ::= trigger_cmd(X) SEMI. {
  A = vec![X];
}

// Disallow qualified table names on INSERT, UPDATE, and DELETE statements
// within a trigger.  The table to INSERT, UPDATE, or DELETE is always in
// the same database as the table that the trigger fires on.
//
%type trnm {Name}
trnm(A) ::= nm(A).
trnm(A) ::= nm DOT nm(X). {
  A = X;
  return Err(custom_err!("qualified table names are not allowed on INSERT, UPDATE, and DELETE \
         statements within triggers"));
}

// Disallow the INDEX BY and NOT INDEXED clauses on UPDATE and DELETE
// statements within triggers.  We make a specific error message for this
// since it is an exception to the default grammar rules.
//
tridxby ::= .
tridxby ::= INDEXED BY nm. {
  return Err(custom_err!(
        "the INDEXED BY clause is not allowed on UPDATE or DELETE statements \
         within triggers"));
}
tridxby ::= NOT INDEXED. {
  return Err(custom_err!(
        "the NOT INDEXED clause is not allowed on UPDATE or DELETE statements \
         within triggers"));
}



%type trigger_cmd {TriggerCmd}
// UPDATE
trigger_cmd(A) ::=
   UPDATE orconf(R) trnm(X) tridxby SET setlist(Y) from(F) where_opt(Z).
   {A = TriggerCmd::Update{ or_conflict: R, tbl_name: X, sets: Y, from: F, where_clause: Z };}

// INSERT
trigger_cmd(A) ::= insert_cmd(R) INTO
                      trnm(X) idlist_opt(F) select(S) upsert(U). {
  let (upsert, returning) = U;
   A = TriggerCmd::Insert{ or_conflict: R, tbl_name: X, col_names: F, select: S, upsert, returning };/*A-overwrites-R*/
}
// DELETE
trigger_cmd(A) ::= DELETE FROM trnm(X) tridxby where_opt(Y).
   {A = TriggerCmd::Delete{ tbl_name: X, where_clause: Y };}

// SELECT
trigger_cmd(A) ::= select(X).
   {A = TriggerCmd::Select(X); /*A-overwrites-X*/}

// The special RAISE expression that may occur in trigger programs
expr(A) ::= RAISE LP IGNORE RP.  {
  A = Expr::Raise(ResolveType::Ignore, None);
}
expr(A) ::= RAISE LP raisetype(T) COMMA expr(Z) RP.  {
  A = Expr::Raise(T, Some(Box::new(Z)));
}
%endif  !SQLITE_OMIT_TRIGGER

%type raisetype {ResolveType}
raisetype(A) ::= ROLLBACK.  {A = ResolveType::Rollback;}
raisetype(A) ::= ABORT.     {A = ResolveType::Abort;}
raisetype(A) ::= FAIL.      {A = ResolveType::Fail;}


////////////////////////  DROP TRIGGER statement //////////////////////////////
%ifndef SQLITE_OMIT_TRIGGER
cmd ::= DROP TRIGGER ifexists(NOERR) fullname(X). {
  self.ctx.stmt = Some(Stmt::DropTrigger{ if_exists: NOERR, trigger_name: X});
}
%endif  !SQLITE_OMIT_TRIGGER

//////////////////////// ATTACH DATABASE file AS name /////////////////////////
%ifndef SQLITE_OMIT_ATTACH
cmd ::= ATTACH database_kw_opt expr(F) AS expr(D) key_opt(K). {
  self.ctx.stmt = Some(Stmt::Attach{ expr: F, db_name: D, key: K });
}
cmd ::= DETACH database_kw_opt expr(D). {
  self.ctx.stmt = Some(Stmt::Detach(D));
}

%type key_opt {Option<Expr>}
key_opt(A) ::= .                     { A = None; }
key_opt(A) ::= KEY expr(X).          { A = Some(X); }

database_kw_opt ::= DATABASE.
database_kw_opt ::= .
%endif SQLITE_OMIT_ATTACH

////////////////////////// REINDEX collation //////////////////////////////////
%ifndef SQLITE_OMIT_REINDEX
cmd ::= REINDEX.                {self.ctx.stmt = Some(Stmt::Reindex { obj_name: None });}
cmd ::= REINDEX fullname(X).  {self.ctx.stmt = Some(Stmt::Reindex { obj_name: Some(X) });}
%endif  SQLITE_OMIT_REINDEX

/////////////////////////////////// ANALYZE ///////////////////////////////////
%ifndef SQLITE_OMIT_ANALYZE
cmd ::= ANALYZE.                {self.ctx.stmt = Some(Stmt::Analyze(None));}
cmd ::= ANALYZE fullname(X).  {self.ctx.stmt = Some(Stmt::Analyze(Some(X)));}
%endif

//////////////////////// ALTER TABLE table ... ////////////////////////////////
%ifndef SQLITE_OMIT_ALTERTABLE
cmd ::= ALTER TABLE fullname(X) RENAME TO nm(Z). {
  self.ctx.stmt = Some(Stmt::AlterTable(X, AlterTableBody::RenameTo(Z)));
}
cmd ::= ALTER TABLE fullname(X)
        ADD kwcolumn_opt columnname(Y) carglist(C). {
  let (col_name, col_type) = Y;
  let cd = ColumnDefinition{ col_name, col_type, constraints: C };
  self.ctx.stmt = Some(Stmt::AlterTable(X, AlterTableBody::AddColumn(cd)));
}
cmd ::= ALTER TABLE fullname(X) RENAME kwcolumn_opt nm(Y) TO nm(Z). {
  self.ctx.stmt = Some(Stmt::AlterTable(X, AlterTableBody::RenameColumn{ old: Y, new: Z }));
}
cmd ::= ALTER TABLE fullname(X) DROP kwcolumn_opt nm(Y). {
  self.ctx.stmt = Some(Stmt::AlterTable(X, AlterTableBody::DropColumn(Y)));
}

kwcolumn_opt ::= .
kwcolumn_opt ::= COLUMNKW.
%endif  SQLITE_OMIT_ALTERTABLE

//////////////////////// CREATE VIRTUAL TABLE ... /////////////////////////////
%ifndef SQLITE_OMIT_VIRTUALTABLE
cmd ::= create_vtab(X).                       {self.ctx.stmt = Some(X);}
cmd ::= create_vtab(X) LP vtabarglist RP.  {
  let mut stmt = X;
  if let Stmt::CreateVirtualTable{ ref mut args, .. } = stmt {
    *args = self.ctx.module_args();
  }
  self.ctx.stmt = Some(stmt);
}
%type create_vtab {Stmt}
create_vtab(A) ::= createkw VIRTUAL TABLE ifnotexists(E)
                fullname(X) USING nm(Z). {
    A = Stmt::CreateVirtualTable{ if_not_exists: E, tbl_name: X, module_name: Z, args: None };
}
vtabarglist ::= vtabarg.
vtabarglist ::= vtabarglist COMMA vtabarg.
vtabarg ::= .                       {self.ctx.vtab_arg_init();}
vtabarg ::= vtabarg vtabargtoken.
vtabargtoken ::= ANY(X).            { let x = X; self.ctx.vtab_arg_extend(x);}
vtabargtoken ::= lp anylist RP(X).  {let x = X; self.ctx.vtab_arg_extend(x);}
lp ::= LP(X).                       {let x = X; self.ctx.vtab_arg_extend(x);}
anylist ::= .
anylist ::= anylist LP anylist RP.
anylist ::= anylist ANY.
%endif  SQLITE_OMIT_VIRTUALTABLE


//////////////////////// COMMON TABLE EXPRESSIONS ////////////////////////////
%type with {Option<With>}
%type wqlist {Vec<CommonTableExpr>}
%type wqitem {CommonTableExpr}
// %destructor wqitem {sqlite3CteDelete(pParse->db, $$);} // not reachable

with(A) ::= . { A = None; }
%ifndef SQLITE_OMIT_CTE
with(A) ::= WITH wqlist(W).              { A = Some(With{ recursive: false, ctes: W }); }
with(A) ::= WITH RECURSIVE wqlist(W).    { A = Some(With{ recursive: true, ctes: W }); }

%type wqas {Materialized}
wqas(A)   ::= AS.                  {A = Materialized::Any;}
wqas(A)   ::= AS MATERIALIZED.     {A = Materialized::Yes;}
wqas(A)   ::= AS NOT MATERIALIZED. {A = Materialized::No;}
wqitem(A) ::= nm(X) eidlist_opt(Y) wqas(M) LP select(Z) RP. {
  A = CommonTableExpr{ tbl_name: X, columns: Y, materialized: M, select: Z }; /*A-overwrites-X*/
}
wqlist(A) ::= wqitem(X). {
  A = vec![X]; /*A-overwrites-X*/
}
wqlist(A) ::= wqlist(A) COMMA wqitem(X). {
  let cte = X;
  CommonTableExpr::add_cte(A, cte)?;
}
%endif  SQLITE_OMIT_CTE

//////////////////////// WINDOW FUNCTION EXPRESSIONS /////////////////////////
// These must be at the end of this file. Specifically, the rules that
// introduce tokens WINDOW, OVER and FILTER must appear last. This causes
// the integer values assigned to these tokens to be larger than all other
// tokens that may be output by the tokenizer except TK_SPACE and TK_ILLEGAL.
//
%ifndef SQLITE_OMIT_WINDOWFUNC
%type windowdefn_list {Vec<WindowDef>}
windowdefn_list(A) ::= windowdefn(Z). { A = vec![Z]; }
windowdefn_list(A) ::= windowdefn_list(A) COMMA windowdefn(Z). {
  let w = Z;
  A.push(w);
}

%type windowdefn {WindowDef}
windowdefn(A) ::= nm(X) AS LP window(Y) RP. {
  A = WindowDef { name: X, window: Y};
}

%type window {Window}

%type frame_opt {Option<FrameClause>}

%type filter_clause {Expr}

%type over_clause {Over}

%type filter_over {FunctionTail}

%type range_or_rows {FrameMode}

%type frame_bound {FrameBound}
%type frame_bound_s {FrameBound}
%type frame_bound_e {FrameBound}

window(A) ::= PARTITION BY nexprlist(X) orderby_opt(Y) frame_opt(Z). {
  A = Window{ base: None,  partition_by: Some(X), order_by: Y, frame_clause: Z};
}
window(A) ::= nm(W) PARTITION BY nexprlist(X) orderby_opt(Y) frame_opt(Z). {
  A = Window{ base: Some(W),  partition_by: Some(X), order_by: Y, frame_clause: Z};
}
window(A) ::= ORDER BY sortlist(Y) frame_opt(Z). {
  A = Window{ base: None,  partition_by: None, order_by: Some(Y), frame_clause: Z};
}
window(A) ::= nm(W) ORDER BY sortlist(Y) frame_opt(Z). {
  A = Window{ base: Some(W),  partition_by: None, order_by: Some(Y), frame_clause: Z};
}
window(A) ::= frame_opt(Z). {
  A = Window{ base: None,  partition_by: None, order_by: None, frame_clause: Z};
}
window(A) ::= nm(W) frame_opt(Z). {
  A = Window{ base: Some(W),  partition_by: None, order_by: None, frame_clause: Z};
}

frame_opt(A) ::= .                             {
  A = None;
}
frame_opt(A) ::= range_or_rows(X) frame_bound_s(Y) frame_exclude_opt(Z). {
  A = Some(FrameClause{ mode: X, start: Y, end: None, exclude: Z });
}
frame_opt(A) ::= range_or_rows(X) BETWEEN frame_bound_s(Y) AND
                          frame_bound_e(Z) frame_exclude_opt(W). {
  A = Some(FrameClause{ mode: X, start: Y, end: Some(Z), exclude: W });
}

range_or_rows(A) ::= RANGE.   { A = FrameMode::Range; }
range_or_rows(A) ::= ROWS.    { A = FrameMode::Rows; }
range_or_rows(A) ::= GROUPS.  { A = FrameMode::Groups; }


frame_bound_s(A) ::= frame_bound(X).      {A = X;}
frame_bound_s(A) ::= UNBOUNDED PRECEDING. {A = FrameBound::UnboundedPreceding;}
frame_bound_e(A) ::= frame_bound(X).      {A = X;}
frame_bound_e(A) ::= UNBOUNDED FOLLOWING. {A = FrameBound::UnboundedFollowing;}

frame_bound(A) ::= expr(X) PRECEDING.   { A = FrameBound::Preceding(X); }
frame_bound(A) ::= CURRENT ROW.         { A = FrameBound::CurrentRow; }
frame_bound(A) ::= expr(X) FOLLOWING.   { A = FrameBound::Following(X); }

%type frame_exclude_opt {Option<FrameExclude>}
frame_exclude_opt(A) ::= . {A = None;}
frame_exclude_opt(A) ::= EXCLUDE frame_exclude(X). {A = Some(X);}

%type frame_exclude {FrameExclude}
frame_exclude(A) ::= NO OTHERS.   { A = FrameExclude::NoOthers; }
frame_exclude(A) ::= CURRENT ROW. { A = FrameExclude::CurrentRow; }
frame_exclude(A) ::= GROUP.       { A = FrameExclude::Group; }
frame_exclude(A) ::= TIES.        { A = FrameExclude::Ties; }

%type window_clause {Vec<WindowDef>}
window_clause(A) ::= WINDOW windowdefn_list(B). { A = B; }

filter_over(A) ::= filter_clause(F) over_clause(O). {
  A = FunctionTail{ filter_clause: Some(Box::new(F)), over_clause: Some(Box::new(O)) };
}
filter_over(A) ::= over_clause(O). {
  A = FunctionTail{ filter_clause: None, over_clause: Some(Box::new(O)) };
}
filter_over(A) ::= filter_clause(F). {
  A = FunctionTail{ filter_clause: Some(Box::new(F)), over_clause: None };
}

over_clause(A) ::= OVER LP window(Z) RP. {
  A = Over::Window(Z);
}
over_clause(A) ::= OVER nm(Z). {
  A = Over::Name(Z);
}

filter_clause(A) ::= FILTER LP WHERE expr(X) RP.  { A = X; }
%endif /* SQLITE_OMIT_WINDOWFUNC */
