use crate::sync::Arc;
use crate::HashMap;
use crate::LimboError;

use crate::ext::VTabImpl;
use crate::function::{Deterministic, Func, MathFunc, ScalarFunc};
use crate::schema::{
    create_table, translate_ident_to_string_literal, BTreeCharacteristics, BTreeTable, ColDef,
    Column, SchemaObjectType, Table, Type, RESERVED_TABLE_PREFIXES, SQLITE_SEQUENCE_TABLE_NAME,
    TURSO_TYPES_TABLE_NAME,
};
use crate::stats::STATS_TABLE;
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::{
    emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns, prepare_cdc_if_necessary,
    OperationMode, Resolver,
};
use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::fkeys::emit_fk_drop_table_check;
use crate::translate::plan::{compound_column_affinity, Plan, QueryDestination};
use crate::translate::planner::ROWID_STRS;
use crate::translate::select::{emit_select_plan, prepare_select_plan};
use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::util::{
    escape_sql_string_literal, normalize_ident, quote_identifier,
    PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX,
};
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{
    to_u32, {CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral},
};
use crate::{bail_parse_error, turso_assert, turso_assert_eq, CaptureDataChangesExt, Result};
use crate::{Connection, MAIN_DB_ID};

use turso_ext::VTabKind;
use turso_parser::ast;
use turso_parser::ast::ColumnDefinition;

/// Validate a CHECK constraint expression at CREATE TABLE / ALTER TABLE ADD COLUMN time.
/// Rejects non-existent columns, non-existent functions, aggregates, window functions,
/// bind parameters, and subqueries.
pub(crate) fn validate_check_expr(
    expr: &ast::Expr,
    table_name: &str,
    column_names: &[&str],
    resolver: &Resolver,
) -> Result<()> {
    let normalized_table = normalize_ident(table_name);
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                let n = normalize_ident(name.as_str());
                if !column_names.iter().any(|c| normalize_ident(c) == n)
                    && !ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&n))
                {
                    bail_parse_error!("no such column: {}", name.as_str());
                }
            }
            ast::Expr::Qualified(tbl, col) => {
                if normalize_ident(tbl.as_str()) != normalized_table {
                    bail_parse_error!("no such column: {}.{}", tbl.as_str(), col.as_str());
                }
                let cn = normalize_ident(col.as_str());
                if !column_names.iter().any(|c| normalize_ident(c) == cn)
                    && !ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&cn))
                {
                    bail_parse_error!("no such column: {}", col.as_str());
                }
            }
            ast::Expr::DoublyQualified(db, tbl, col) => {
                bail_parse_error!(
                    "no such column: {}.{}.{}",
                    db.as_str(),
                    tbl.as_str(),
                    col.as_str()
                );
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), args.len())? {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                    }
                    if matches!(func, Func::Window(..)) {
                        bail_parse_error!("misuse of window function {}()", name.as_str());
                    }
                } else {
                    bail_parse_error!("no such function: {}", name.as_str());
                }
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), 0)? {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                    }
                    if matches!(func, Func::Window(..)) {
                        bail_parse_error!("misuse of window function {}()", name.as_str());
                    }
                } else {
                    bail_parse_error!("no such function: {}", name.as_str());
                }
            }
            ast::Expr::Variable(_) => {
                bail_parse_error!("parameters prohibited in CHECK constraints");
            }
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries prohibited in CHECK constraints");
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn validate_default_expr(expr: &ast::Expr, col: &ColumnDefinition) -> Result<()> {
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Column { .. }
            | ast::Expr::RowId { .. }
            | ast::Expr::Name(_)
            | ast::Expr::Qualified(_, _)
            | ast::Expr::DoublyQualified(_, _, _)
            | ast::Expr::Variable(_)
            | ast::Expr::Raise(_, _)
            | ast::Expr::Exists(_)
            | ast::Expr::InSelect { .. }
            | ast::Expr::InTable { .. }
            | ast::Expr::Subquery(_)
            | ast::Expr::SubqueryResult { .. }
            | ast::Expr::Id(_) => {
                bail_parse_error!(
                    "default value of column [{}] is not constant",
                    col.col_name.as_str()
                );
            }
            _ => Ok(WalkControl::Continue),
        }
    })?;
    Ok(())
}

/// Resolved type of an expression node for strict type checking of CHECK constraints.
/// In STRICT tables, every comparison operand must have a determinable, compatible type.
/// If a type cannot be determined (e.g. function calls), the user must use an explicit CAST.
#[derive(Debug, Clone, PartialEq)]
enum CheckExprType {
    Integer,
    Real,
    Text,
    Blob,
    Any,
    Null,
    CustomType(String),
}

impl CheckExprType {
    fn is_numeric(&self) -> bool {
        matches!(self, Self::Integer | Self::Real)
    }

    fn is_compatible_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => true,
            (Self::Any, _) | (_, Self::Any) => true,
            (a, b) if a == b => true,
            (a, b) if a.is_numeric() && b.is_numeric() => true,
            _ => false,
        }
    }

    fn display_name(&self) -> &str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
            Self::Any => "ANY",
            Self::Null => "NULL",
            Self::CustomType(name) => name.as_str(),
        }
    }
}

/// Resolve the type of an expression node in a CHECK constraint.
/// Returns an error if the type cannot be determined — the user must use CAST.
fn resolve_check_expr_type(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    use ast::{Literal, Operator, UnaryOperator};
    match expr {
        ast::Expr::Id(name) | ast::Expr::Name(name) => {
            let n = normalize_ident(name.as_str());
            // rowid/oid/_rowid_ are INTEGER
            if ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&n)) {
                return Ok(CheckExprType::Integer);
            }
            for col in columns {
                if normalize_ident(col.col_name.as_str()) == n {
                    return resolve_column_type(col, resolver);
                }
            }
            bail_parse_error!("no such column: {}", name.as_str());
        }
        ast::Expr::Qualified(_tbl, col) => {
            let cn = normalize_ident(col.as_str());
            if ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&cn)) {
                return Ok(CheckExprType::Integer);
            }
            for c in columns {
                if normalize_ident(c.col_name.as_str()) == cn {
                    return resolve_column_type(c, resolver);
                }
            }
            bail_parse_error!("no such column: {}", col.as_str());
        }
        ast::Expr::Literal(lit) => match lit {
            Literal::Numeric(s) => {
                if s.contains('.') || s.contains('e') || s.contains('E') {
                    Ok(CheckExprType::Real)
                } else {
                    Ok(CheckExprType::Integer)
                }
            }
            Literal::String(_) => Ok(CheckExprType::Text),
            Literal::Blob(_) => Ok(CheckExprType::Blob),
            Literal::Null => Ok(CheckExprType::Null),
            Literal::True | Literal::False => Ok(CheckExprType::Integer),
            Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp => {
                Ok(CheckExprType::Text)
            }
            Literal::Keyword(s) => {
                bail_parse_error!(
                    "cannot determine type of '{}' in CHECK constraint; use CAST",
                    s
                );
            }
        },
        ast::Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                resolve_check_expr_type(&exprs[0], columns, resolver)
            } else {
                bail_parse_error!(
                    "cannot determine type of expression in CHECK constraint; use CAST"
                );
            }
        }
        ast::Expr::Cast { type_name, .. } => {
            if let Some(ref tn) = type_name {
                resolve_type_name(&tn.name, resolver)
            } else {
                bail_parse_error!(
                    "cannot determine type of CAST in CHECK constraint; use CAST with explicit type"
                );
            }
        }
        ast::Expr::Unary(op, inner) => match op {
            UnaryOperator::Negative | UnaryOperator::Positive => {
                let inner_ty = resolve_check_expr_type(inner, columns, resolver)?;
                if !inner_ty.is_numeric() && inner_ty != CheckExprType::Null {
                    bail_parse_error!(
                        "unary minus/plus requires a numeric type, got {}",
                        inner_ty.display_name()
                    );
                }
                Ok(inner_ty)
            }
            UnaryOperator::BitwiseNot => Ok(CheckExprType::Integer),
            UnaryOperator::Not => Ok(CheckExprType::Integer),
        },
        ast::Expr::Binary(lhs, op, rhs) => {
            match op {
                // Arithmetic: both must be numeric, result follows promotion rules
                Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                    let lty = resolve_check_expr_type(lhs, columns, resolver)?;
                    let rty = resolve_check_expr_type(rhs, columns, resolver)?;
                    if lty == CheckExprType::Null || rty == CheckExprType::Null {
                        return Ok(CheckExprType::Null);
                    }
                    if !lty.is_numeric() || !rty.is_numeric() {
                        bail_parse_error!(
                            "arithmetic requires numeric types, got {} and {}",
                            lty.display_name(),
                            rty.display_name()
                        );
                    }
                    if lty == CheckExprType::Real || rty == CheckExprType::Real {
                        Ok(CheckExprType::Real)
                    } else {
                        Ok(CheckExprType::Integer)
                    }
                }
                Operator::Modulus => Ok(CheckExprType::Integer),
                Operator::Concat => Ok(CheckExprType::Text),
                Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::LeftShift
                | Operator::RightShift => Ok(CheckExprType::Integer),
                // Logical: recurse to find nested comparisons
                Operator::And | Operator::Or => {
                    // The result of AND/OR is boolean (integer), but we need to
                    // recurse to validate any comparisons inside.
                    validate_check_types_in_expr(lhs, columns, resolver)?;
                    validate_check_types_in_expr(rhs, columns, resolver)?;
                    Ok(CheckExprType::Integer)
                }
                // Comparison operators: validate type compatibility and return Integer (boolean)
                Operator::Equals
                | Operator::NotEquals
                | Operator::Less
                | Operator::LessEquals
                | Operator::Greater
                | Operator::GreaterEquals => {
                    let lty = resolve_check_expr_type(lhs, columns, resolver)?;
                    let rty = resolve_check_expr_type(rhs, columns, resolver)?;
                    if !lty.is_compatible_with(&rty) {
                        bail_parse_error!(
                            "type mismatch in CHECK constraint: cannot compare {} with {}",
                            lty.display_name(),
                            rty.display_name()
                        );
                    }
                    Ok(CheckExprType::Integer)
                }
                // IS/IS NOT are NULL-checking operators, skip type validation
                Operator::Is | Operator::IsNot => Ok(CheckExprType::Integer),
                _ => {
                    bail_parse_error!(
                        "cannot determine type of expression in CHECK constraint; use CAST"
                    );
                }
            }
        }
        ast::Expr::NotNull(_) | ast::Expr::IsNull(_) => Ok(CheckExprType::Integer),
        ast::Expr::FunctionCall { name, args, .. } => {
            if let Some(func) = resolver.resolve_function(name.as_str(), args.len())? {
                resolve_func_return_type(&func, name.as_str(), args, columns, resolver)
            } else {
                bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            }
        }
        ast::Expr::FunctionCallStar { name, .. } => {
            if let Some(func) = resolver.resolve_function(name.as_str(), 0)? {
                resolve_func_return_type(&func, name.as_str(), &[], columns, resolver)
            } else {
                bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            }
        }
        _ => {
            bail_parse_error!("cannot determine type of expression in CHECK constraint; use CAST");
        }
    }
}

/// Resolve the return type of a built-in function for CHECK constraint type checking.
fn resolve_func_return_type(
    func: &Func,
    name: &str,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match func {
        Func::Scalar(sf) => resolve_scalar_func_return_type(sf, args, columns, resolver),
        Func::Math(mf) => resolve_math_func_return_type(mf),
        #[cfg(feature = "json")]
        Func::Json(jf) => resolve_json_func_return_type(jf),
        Func::Agg(_) => bail_parse_error!("misuse of aggregate function {}()", name),
        Func::External(_) => {
            bail_parse_error!(
                "cannot determine return type of function {}() in CHECK constraint; \
                 wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                name,
                name
            );
        }
        _ => Ok(CheckExprType::Any),
    }
}

/// Resolve the return type of a scalar function.
fn resolve_scalar_func_return_type(
    func: &ScalarFunc,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match func {
        // Functions that always return INTEGER
        ScalarFunc::Length
        | ScalarFunc::OctetLength
        | ScalarFunc::Instr
        | ScalarFunc::Unicode
        | ScalarFunc::Sign
        | ScalarFunc::Random
        | ScalarFunc::Changes
        | ScalarFunc::TotalChanges
        | ScalarFunc::LastInsertRowid
        | ScalarFunc::Glob
        | ScalarFunc::Like
        | ScalarFunc::Likely
        | ScalarFunc::Unlikely
        | ScalarFunc::Likelihood
        | ScalarFunc::BooleanToInt
        | ScalarFunc::IntToBoolean
        | ScalarFunc::IsAutocommit
        | ScalarFunc::ConnTxnId
        | ScalarFunc::TestUintLt
        | ScalarFunc::TestUintEq
        | ScalarFunc::NumericLt
        | ScalarFunc::NumericEq
        | ScalarFunc::ValidateIpAddr
        | ScalarFunc::GetByte
        | ScalarFunc::UnixEpoch => Ok(CheckExprType::Integer),

        // Functions that always return TEXT
        ScalarFunc::Upper
        | ScalarFunc::Lower
        | ScalarFunc::Trim
        | ScalarFunc::LTrim
        | ScalarFunc::RTrim
        | ScalarFunc::Hex
        | ScalarFunc::Soundex
        | ScalarFunc::Quote
        | ScalarFunc::Replace
        | ScalarFunc::Substr
        | ScalarFunc::Substring
        | ScalarFunc::Char
        | ScalarFunc::Concat
        | ScalarFunc::ConcatWs
        | ScalarFunc::Typeof
        | ScalarFunc::SqliteVersion
        | ScalarFunc::TursoVersion
        | ScalarFunc::SqliteSourceId
        | ScalarFunc::Date
        | ScalarFunc::Time
        | ScalarFunc::DateTime
        | ScalarFunc::StrfTime
        | ScalarFunc::TimeDiff
        | ScalarFunc::Printf
        | ScalarFunc::StringReverse => Ok(CheckExprType::Text),

        // Functions that always return REAL
        ScalarFunc::Round | ScalarFunc::JulianDay => Ok(CheckExprType::Real),

        // Functions that always return BLOB
        ScalarFunc::RandomBlob | ScalarFunc::ZeroBlob | ScalarFunc::Unhex | ScalarFunc::SetByte => {
            Ok(CheckExprType::Blob)
        }

        // Functions whose return type depends on arguments
        ScalarFunc::Abs | ScalarFunc::Nullif => {
            if let Some(arg) = args.first() {
                resolve_check_expr_type(arg, columns, resolver)
            } else {
                Ok(CheckExprType::Any)
            }
        }

        ScalarFunc::Coalesce | ScalarFunc::IfNull => {
            for arg in args {
                let ty = resolve_check_expr_type(arg, columns, resolver)?;
                if ty != CheckExprType::Null {
                    return Ok(ty);
                }
            }
            Ok(CheckExprType::Null)
        }

        ScalarFunc::Min | ScalarFunc::Max => {
            if let Some(first) = args.first() {
                resolve_check_expr_type(first, columns, resolver)
            } else {
                Ok(CheckExprType::Any)
            }
        }

        ScalarFunc::Iif => {
            // iif(cond, then_val, else_val) — return type of then_val
            if args.len() >= 2 {
                resolve_check_expr_type(&args[1], columns, resolver)
            } else {
                Ok(CheckExprType::Any)
            }
        }

        // Internal/custom type functions
        ScalarFunc::TestUintEncode
        | ScalarFunc::TestUintDecode
        | ScalarFunc::TestUintAdd
        | ScalarFunc::TestUintSub
        | ScalarFunc::TestUintMul
        | ScalarFunc::TestUintDiv
        | ScalarFunc::NumericEncode
        | ScalarFunc::NumericDecode
        | ScalarFunc::NumericAdd
        | ScalarFunc::NumericSub
        | ScalarFunc::NumericMul
        | ScalarFunc::NumericDiv => Ok(CheckExprType::Blob),

        // Remaining functions — treat as ANY
        _ => Ok(CheckExprType::Any),
    }
}

/// Resolve the return type of a math function.
fn resolve_math_func_return_type(func: &MathFunc) -> Result<CheckExprType> {
    match func {
        // Floor/ceil/trunc return INTEGER for integer inputs, but always produce numeric results
        MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc => {
            Ok(CheckExprType::Integer)
        }
        // All other math functions return REAL
        _ => Ok(CheckExprType::Real),
    }
}

/// Resolve the return type of a JSON function.
#[cfg(feature = "json")]
fn resolve_json_func_return_type(func: &crate::function::JsonFunc) -> Result<CheckExprType> {
    use crate::function::JsonFunc;
    match func {
        // Functions that return TEXT (JSON text)
        JsonFunc::Json
        | JsonFunc::JsonArray
        | JsonFunc::JsonObject
        | JsonFunc::JsonPatch
        | JsonFunc::JsonRemove
        | JsonFunc::JsonReplace
        | JsonFunc::JsonInsert
        | JsonFunc::JsonSet
        | JsonFunc::JsonPretty
        | JsonFunc::JsonQuote
        | JsonFunc::JsonType => Ok(CheckExprType::Text),

        // Functions that return BLOB (JSONB binary)
        JsonFunc::Jsonb
        | JsonFunc::JsonbArray
        | JsonFunc::JsonbObject
        | JsonFunc::JsonbPatch
        | JsonFunc::JsonbRemove
        | JsonFunc::JsonbReplace
        | JsonFunc::JsonbInsert
        | JsonFunc::JsonbSet => Ok(CheckExprType::Blob),

        // Functions that return INTEGER
        JsonFunc::JsonArrayLength | JsonFunc::JsonErrorPosition | JsonFunc::JsonValid => {
            Ok(CheckExprType::Integer)
        }

        // Extract functions can return any type
        JsonFunc::JsonExtract
        | JsonFunc::JsonbExtract
        | JsonFunc::JsonArrowExtract
        | JsonFunc::JsonArrowShiftExtract => Ok(CheckExprType::Any),
    }
}

/// Resolve a column's type from its definition.
fn resolve_column_type(col: &ast::ColumnDefinition, resolver: &Resolver) -> Result<CheckExprType> {
    if let Some(ref col_type) = col.col_type {
        resolve_type_name(&col_type.name, resolver)
    } else {
        // No type specified — in STRICT tables this would be caught elsewhere,
        // but treat as ANY for CHECK validation purposes.
        Ok(CheckExprType::Any)
    }
}

/// Resolve a type name string to a CheckExprType.
fn resolve_type_name(type_name: &str, resolver: &Resolver) -> Result<CheckExprType> {
    let name_bytes = type_name.as_bytes();
    let result = turso_macros::match_ignore_ascii_case!(match name_bytes {
        b"INT" | b"INTEGER" => Some(CheckExprType::Integer),
        b"REAL" | b"FLOAT" | b"DOUBLE" => Some(CheckExprType::Real),
        b"TEXT" => Some(CheckExprType::Text),
        b"BLOB" => Some(CheckExprType::Blob),
        b"ANY" => Some(CheckExprType::Any),
        _ => None,
    });
    if let Some(ty) = result {
        return Ok(ty);
    }
    // Check if it's a known custom type
    if let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) {
        // Domains are transparent wrappers — resolve to the base primitive type
        // so CHECK constraint type checking compares primitives, not domain names.
        if resolved.is_domain() {
            return resolve_type_name(&resolved.primitive, resolver);
        }
        return Ok(CheckExprType::CustomType(type_name.to_lowercase()));
    }
    bail_parse_error!("unknown type '{}' in CHECK constraint", type_name);
}

/// Walk a CHECK expression and validate that all comparisons have compatible types.
/// Only called for STRICT tables.
fn validate_check_types_in_expr(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<()> {
    use ast::Operator;
    match expr {
        ast::Expr::Binary(lhs, op, rhs) => {
            match op {
                Operator::Equals
                | Operator::NotEquals
                | Operator::Less
                | Operator::LessEquals
                | Operator::Greater
                | Operator::GreaterEquals => {
                    let lty = resolve_check_expr_type(lhs, columns, resolver)?;
                    let rty = resolve_check_expr_type(rhs, columns, resolver)?;
                    if !lty.is_compatible_with(&rty) {
                        bail_parse_error!(
                            "type mismatch in CHECK constraint: cannot compare {} with {}",
                            lty.display_name(),
                            rty.display_name()
                        );
                    }
                }
                Operator::And | Operator::Or => {
                    validate_check_types_in_expr(lhs, columns, resolver)?;
                    validate_check_types_in_expr(rhs, columns, resolver)?;
                }
                // Arithmetic, concat, bitwise — recurse to find nested comparisons
                _ => {
                    validate_check_types_in_expr(lhs, columns, resolver)?;
                    validate_check_types_in_expr(rhs, columns, resolver)?;
                }
            }
        }
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            let lty = resolve_check_expr_type(lhs, columns, resolver)?;
            let sty = resolve_check_expr_type(start, columns, resolver)?;
            let ety = resolve_check_expr_type(end, columns, resolver)?;
            if !lty.is_compatible_with(&sty) {
                bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lty.display_name(),
                    sty.display_name()
                );
            }
            if !lty.is_compatible_with(&ety) {
                bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lty.display_name(),
                    ety.display_name()
                );
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            let lty = resolve_check_expr_type(lhs, columns, resolver)?;
            for item in rhs {
                let ity = resolve_check_expr_type(item, columns, resolver)?;
                if !lty.is_compatible_with(&ity) {
                    bail_parse_error!(
                        "type mismatch in CHECK IN list: cannot compare {} with {}",
                        lty.display_name(),
                        ity.display_name()
                    );
                }
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            for e in exprs {
                validate_check_types_in_expr(e, columns, resolver)?;
            }
        }
        ast::Expr::Unary(_, inner) => {
            validate_check_types_in_expr(inner, columns, resolver)?;
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(op) = base {
                validate_check_types_in_expr(op, columns, resolver)?;
            }
            for (when_expr, then_expr) in when_then_pairs {
                validate_check_types_in_expr(when_expr, columns, resolver)?;
                validate_check_types_in_expr(then_expr, columns, resolver)?;
            }
            if let Some(else_e) = else_expr {
                validate_check_types_in_expr(else_e, columns, resolver)?;
            }
        }
        ast::Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_check_types_in_expr(arg, columns, resolver)?;
            }
        }
        // Leaf nodes and other expressions: no nested comparisons to validate
        _ => {}
    }
    Ok(())
}

fn validate(
    body: &ast::CreateTableBody,
    table_name: &str,
    resolver: &Resolver,
    conn: &Connection,
) -> Result<()> {
    if let ast::CreateTableBody::ColumnsAndConstraints {
        options,
        columns,
        constraints,
    } = &body
    {
        if columns.len() > crate::types::MAX_COLUMN {
            return Err(LimboError::ParseError(format!(
                "too many columns on {table_name}"
            )));
        }
        let column_names: Vec<&str> = columns.iter().map(|c| c.col_name.as_str()).collect();
        for i in 0..columns.len() {
            let col_i = &columns[i];
            for constraint in &col_i.constraints {
                match &constraint.constraint {
                    ast::ColumnConstraint::Check { expr, .. } => {
                        validate_check_expr(expr, table_name, &column_names, resolver)?;
                    }
                    ast::ColumnConstraint::Generated { .. }
                        if !conn.experimental_generated_columns_enabled() =>
                    {
                        bail_parse_error!(
                            "Generated columns require --experimental-generated-columns flag"
                        );
                    }
                    ast::ColumnConstraint::Default(expr) => {
                        let expr =
                            translate_ident_to_string_literal(expr).unwrap_or_else(|| expr.clone());
                        validate_default_expr(&expr, col_i)?
                    }
                    ast::ColumnConstraint::Collate { collation_name } => {
                        let collation = resolver.resolve_collation(collation_name.as_str())?;
                        if collation.is_custom() {
                            bail_parse_error!(
                                "custom collations are not supported in schema definitions"
                            );
                        }
                    }
                    _ => {}
                }
            }
            for j in &columns[(i + 1)..] {
                if col_i
                    .col_name
                    .as_str()
                    .eq_ignore_ascii_case(j.col_name.as_str())
                {
                    bail_parse_error!("duplicate column name: {}", j.col_name.as_str());
                }
            }
        }
        for constraint in constraints {
            if let ast::TableConstraint::Check { ref expr, .. } = constraint.constraint {
                validate_check_expr(expr, table_name, &column_names, resolver)?;
            }
        }

        let is_strict = options.contains_strict();

        for c in columns {
            if let Some(ref col_type) = c.col_type {
                let type_name = &col_type.name;
                let name_bytes = type_name.as_bytes();
                let is_builtin = turso_macros::match_ignore_ascii_case!(match name_bytes {
                    b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" | b"ANY" => true,
                    _ => false,
                });

                // Array columns require STRICT tables because the encode/decode
                // pipeline is only emitted for STRICT tables.
                if col_type.is_array() && !is_strict {
                    bail_parse_error!(
                        "array type columns require STRICT tables: {}.{}",
                        table_name,
                        c.col_name
                    );
                }

                // Domain types require STRICT tables because domain constraints
                // (CHECK, NOT NULL, DEFAULT) are only enforced on STRICT tables.
                if !is_builtin && !is_strict {
                    let type_def = resolver.schema().get_type_def_unchecked(type_name);
                    if let Some(td) = type_def {
                        if td.is_domain {
                            bail_parse_error!(
                                "domain type columns require STRICT tables: {}.{}",
                                table_name,
                                c.col_name
                            );
                        }
                    }
                }

                if !is_builtin && is_strict {
                    let type_def = resolver.schema().get_type_def_unchecked(type_name);
                    {
                        match type_def {
                            None => {
                                bail_parse_error!(
                                    "unknown datatype for {}.{}: \"{}\"",
                                    table_name,
                                    c.col_name,
                                    type_name
                                );
                            }
                            Some(td) if td.user_params().next().is_some() => {
                                // Parametric type: verify the column provides the right
                                // number of user parameters (excluding `value`).
                                let provided = match &col_type.size {
                                    Some(ast::TypeSize::TypeSize(_, _)) => 2,
                                    Some(ast::TypeSize::MaxSize(_)) => 1,
                                    None => 0,
                                };
                                let expected = td.user_params().count();
                                if provided != expected {
                                    bail_parse_error!(
                                        "type \"{}\" requires {} parameter(s), got {}",
                                        type_name,
                                        expected,
                                        provided
                                    );
                                }
                            }
                            Some(_) => {}
                        }
                    }
                }
            }
        }

        // In STRICT tables, validate that CHECK constraint comparisons have
        // compatible types. This catches type mismatches at CREATE TABLE time
        // rather than producing wrong results at INSERT/UPDATE time.
        if is_strict {
            let col_refs: Vec<&ast::ColumnDefinition> = columns.iter().collect();
            for col in columns {
                for constraint in &col.constraints {
                    if let ast::ColumnConstraint::Check { expr, .. } = &constraint.constraint {
                        validate_check_types_in_expr(expr, &col_refs, resolver)?;
                    }
                }
            }
            for constraint in constraints {
                if let ast::TableConstraint::Check { ref expr, .. } = constraint.constraint {
                    validate_check_types_in_expr(expr, &col_refs, resolver)?;
                }
            }
        }

        let table = create_table(table_name, body, 0)?;
        if !table.has_rowid {
            if table.has_autoincrement {
                bail_parse_error!("AUTOINCREMENT is not allowed on WITHOUT ROWID tables");
            }
            if table.primary_key_columns.is_empty() {
                bail_parse_error!("PRIMARY KEY missing on table {}", table_name);
            }
            if table.unique_sets.iter().any(|us| !us.is_primary_key) {
                bail_parse_error!(
                    "secondary UNIQUE constraints on WITHOUT ROWID tables are not supported"
                );
            }
        }
    }
    Ok(())
}

/// Schema information derived from a CTAS SELECT.
struct CtasInfo {
    plan: Plan,
    schema_sql: String,
}

/// Pre-plan the SELECT to derive the schema for a CTAS table.
/// Returns the plan (for reuse in emission) along with the complete schema SQL and column definitions.
fn derive_ctas_schema(
    select: ast::Select,
    table_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<(CtasInfo, Vec<ColumnDefinition>)> {
    let plan = prepare_select_plan(
        select,
        resolver,
        program,
        &[],
        QueryDestination::ResultRows,
        connection,
    )?;

    // For compound selects, use the leftmost select's columns for naming (matching SQLite).
    // The planner guarantees `left` is always non-empty in a CompoundSelect.
    let (result_columns, table_refs) = match &plan {
        Plan::Select(sp) => (&sp.result_columns, &sp.table_references),
        Plan::CompoundSelect { left, .. } => {
            (&left[0].0.result_columns, &left[0].0.table_references)
        }
        _ => bail_parse_error!("unexpected plan type for CTAS"),
    };
    // SQLite derives a compound output affinity from all arms, not only the leftmost arm.
    let compound_arms = match &plan {
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut arms = Vec::with_capacity(left.len() + 1);
            arms.extend(left.iter().map(|(select, _)| select));
            arms.push(right_most);
            Some(arms)
        }
        _ => None,
    };

    // Collect names first, then deduplicate using SQLite's :N suffix convention.
    let mut names: Vec<String> = result_columns
        .iter()
        .map(|col| col.name_or_expr(table_refs))
        .collect();

    let mut seen: HashMap<String, usize> = HashMap::default();
    for name in &mut names {
        let lower = name.to_lowercase();
        let count = seen.entry(lower).or_insert(0);
        if *count > 0 {
            *name = format!("{name}:{count}");
        }
        *count += 1;
    }

    let mut sql_parts = Vec::with_capacity(result_columns.len());
    let mut col_defs = Vec::with_capacity(result_columns.len());

    for (column_index, (col, name)) in result_columns.iter().zip(names).enumerate() {
        // Names come from the leftmost arm, but compound affinity can weaken its type.
        let ty = compound_arms
            .as_ref()
            .map(|arms| compound_column_affinity(arms, column_index).short_type_name())
            .unwrap_or_else(|| col.declared_type(table_refs));

        let quoted = quote_identifier(&name);
        if ty.is_empty() {
            sql_parts.push(quoted);
        } else {
            sql_parts.push(format!("{quoted} {ty}"));
        }

        let col_type = if ty.is_empty() {
            None
        } else {
            Some(ast::Type {
                name: ty.to_string(),
                size: None,
                array_dimensions: 0,
            })
        };
        col_defs.push(ColumnDefinition {
            col_name: ast::Name::exact(name),
            col_type,
            constraints: vec![],
        });
    }

    let info = CtasInfo {
        plan,
        schema_sql: format!("CREATE TABLE {table_name}({})", sql_parts.join(",")),
    };
    Ok((info, col_defs))
}

/// Emit bytecode to populate a newly-created CTAS table from the SELECT.
/// Uses a coroutine to run the SELECT and insert each result row.
/// Takes an already-prepared plan (from `derive_ctas_schema`) to avoid double planning.
#[allow(clippy::too_many_arguments)]
fn emit_ctas_insert(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut plan: Plan,
    body: &ast::CreateTableBody,
    table_root_reg: usize,
    col_count: usize,
    database_id: usize,
    table_name: &str,
    connection: &Arc<Connection>,
) -> Result<()> {
    let opts = ProgramBuilderOpts {
        num_cursors: 2,
        approx_num_insns: 20,
        approx_num_labels: 3,
    };
    program.extend(&opts);

    // Set up coroutine for the SELECT
    let yield_reg = program.alloc_register();
    let jump_on_definition_label = program.allocate_label();
    let start_offset_label = program.allocate_label();
    let halt_label = program.allocate_label();

    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: jump_on_definition_label,
        start_offset: start_offset_label,
    });
    program.preassign_label_to_next_insn(start_offset_label);

    // Switch the plan's destination to coroutine yield mode.
    let dest = plan.select_query_destination_mut().ok_or_else(|| {
        crate::LimboError::InternalError("CTAS plan must be a SELECT or CompoundSelect".into())
    })?;
    *dest = QueryDestination::CoroutineYield {
        yield_reg,
        coroutine_implementation_start: halt_label,
    };

    let num_result_cols =
        program.nested(|program| emit_select_plan(plan, resolver, program, connection))?;

    if num_result_cols != col_count {
        bail_parse_error!(
            "CTAS internal error: expected {} columns from SELECT but got {}",
            col_count,
            num_result_cols
        );
    }

    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(jump_on_definition_label);

    // Open the new table for writing using the root page from CreateBtree.
    let ctas_btree = Arc::new(create_table(table_name, body, 0)?);
    // SQLite applies the derived CTAS affinity in MakeRecord. This keeps each
    // stored value consistent with the declared type of the CTAS column.
    let affinity_str = ctas_btree
        .columns()
        .iter()
        .map(|column| column.affinity().aff_mask())
        .collect();
    let new_table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ctas_btree));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: new_table_cursor_id,
        root_page: RegisterOrLiteral::Register(table_root_reg),
        db: database_id,
    });

    // Main insert loop: yield from coroutine, make record, insert
    let loop_start = program.allocate_label();
    let loop_end = program.allocate_label();

    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::Yield {
        yield_reg,
        end_offset: loop_end,
        subtype_clear_start_reg: 0,
        subtype_clear_count: 0,
    });

    let result_start_reg = program.reg_result_cols_start.ok_or_else(|| {
        crate::LimboError::InternalError(
            "CTAS internal error: result column start register not set".into(),
        )
    })?;
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(result_start_reg),
        count: to_u32(col_count),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: new_table_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    program.emit_insn(Insn::Insert {
        cursor: new_table_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: table_name.to_string(),
    });

    program.emit_insn(Insn::Goto {
        target_pc: loop_start,
    });

    program.preassign_label_to_next_insn(loop_end);
    program.preassign_label_to_next_insn(halt_label);

    program.result_columns.clear();

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn translate_create_table(
    tbl_name: ast::QualifiedName,
    resolver: &Resolver,
    temporary: bool,
    if_not_exists: bool,
    body: ast::CreateTableBody,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    input: &str,
) -> Result<()> {
    // For CTAS, extract the SELECT, determine column info, and convert to a
    // regular ColumnsAndConstraints body + separate SELECT for data insertion.
    let (body, ctas_info) = match body {
        ast::CreateTableBody::AsSelect(select) => {
            let (info, col_defs) = derive_ctas_schema(
                select,
                &tbl_name.name.as_ident(),
                resolver,
                program,
                connection,
            )?;
            let body = ast::CreateTableBody::ColumnsAndConstraints {
                columns: col_defs,
                constraints: vec![],
                options: ast::TableOptions::empty(),
            };
            (body, Some(info))
        }
        other => (other, None),
    };

    let database_id = if temporary {
        crate::TEMP_DB_ID
    } else {
        resolver.resolve_database_id(&tbl_name)?
    };
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    let normalized_tbl_name = normalize_ident(tbl_name.name.as_str());
    validate(&body, &normalized_tbl_name, resolver, connection)?;

    // Gate array column types behind the experimental custom types flag.
    if !connection.experimental_custom_types_enabled() {
        if let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = &body {
            for col in columns {
                if col.col_type.as_ref().is_some_and(|t| t.is_array()) {
                    bail_parse_error!(
                        "Array column types require --experimental-custom-types flag"
                    );
                }
            }
        }
    }

    if !connection.experimental_without_rowid_enabled() {
        if let ast::CreateTableBody::ColumnsAndConstraints { options, .. } = &body {
            if options.contains_without_rowid() {
                bail_parse_error!(
                    "WITHOUT ROWID tables are an experimental feature. Enable with --experimental-without-rowid flag"
                );
            }
        }
    }

    let opts = ProgramBuilderOpts::new(1, 30, 1);
    program.extend(&opts);

    if !connection.is_mvcc_bootstrap_connection()
        && RESERVED_TABLE_PREFIXES
            .iter()
            .any(|prefix| normalized_tbl_name.starts_with(prefix))
        && !connection.is_nested_stmt()
    {
        bail_parse_error!(
            "Object name reserved for internal use: {}",
            tbl_name.name.as_str()
        );
    }

    // Check for name conflicts with existing schema objects
    if let Some(object_type) =
        resolver.with_schema(database_id, |s| s.get_object_type(&normalized_tbl_name))
    {
        match object_type {
            // IF NOT EXISTS suppresses errors for table/view conflicts
            SchemaObjectType::Table | SchemaObjectType::View if if_not_exists => {
                return Ok(());
            }
            _ => {
                // SQLite echoes the new table's name token as written
                // (`table "t" already exists`), except when the name clashes
                // with an index, which gets its own message shape.
                let token = crate::util::identifier_token_for_error(&tbl_name.name);
                match object_type {
                    SchemaObjectType::Table => {
                        bail_parse_error!("table {} already exists", token)
                    }
                    SchemaObjectType::View => {
                        bail_parse_error!("view {} already exists", token)
                    }
                    SchemaObjectType::Index => bail_parse_error!(
                        "there is already an index named {}",
                        tbl_name.name.as_str()
                    ),
                }
            }
        }
    }

    let mut has_autoincrement = false;
    if let ast::CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        ..
    } = &body
    {
        for col in columns {
            for constraint in &col.constraints {
                if let ast::ColumnConstraint::PrimaryKey { auto_increment, .. } =
                    constraint.constraint
                {
                    if auto_increment {
                        has_autoincrement = true;
                        break;
                    }
                }
            }
            if has_autoincrement {
                break;
            }
        }
        if !has_autoincrement {
            for constraint in constraints {
                if let ast::TableConstraint::PrimaryKey { auto_increment, .. } =
                    constraint.constraint
                {
                    if auto_increment {
                        has_autoincrement = true;
                        break;
                    }
                }
            }
        }
    }

    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), Some(SQLITE_TABLEID))?;

    let create_btree_label = program.allocate_label();
    let database_format_reg = program.alloc_register();
    program.emit_insn(Insn::ReadCookie {
        db: database_id,
        dest: database_format_reg,
        cookie: Cookie::DatabaseFormat,
    });
    program.emit_insn(Insn::If {
        reg: database_format_reg,
        target_pc: create_btree_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::DatabaseFormat,
        value: 4,
        p5: 0,
    });
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::DatabaseTextEncoding,
        value: 1,
        p5: 0,
    });
    program.preassign_label_to_next_insn(create_btree_label);

    let created_sequence_table = if has_autoincrement
        && resolver.with_schema(database_id, |s| {
            s.get_table(SQLITE_SEQUENCE_TABLE_NAME).is_none()
        }) {
        let schema_master_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
        let sqlite_schema_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeTable(schema_master_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id,
            root_page: 1i64.into(),
            db: database_id,
        });
        let seq_table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: database_id,
            root: seq_table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });

        let seq_sql = "CREATE TABLE sqlite_sequence(name,seq)";
        emit_schema_entry(
            program,
            resolver,
            sqlite_schema_cursor_id,
            cdc_table.as_ref().map(|x| x.0),
            SchemaEntryType::Table,
            SQLITE_SEQUENCE_TABLE_NAME,
            SQLITE_SEQUENCE_TABLE_NAME,
            seq_table_root_reg,
            Some(seq_sql.to_string()),
        )?;
        true
    } else {
        false
    };

    // For CTAS, use the pre-built SQL string; for regular CREATE TABLE, let
    // the schema dialect format the SQL to store (the SQLite dialect renders
    // canonical text from the AST, a frontend dialect preserves its own
    // input text).
    let sql = if let Some(ref info) = ctas_info {
        info.schema_sql.clone()
    } else {
        connection
            .dialect()
            .format_table_sql(input, &tbl_name, &body)?
    };

    let parse_schema_label = program.allocate_label();

    let table_root_reg = program.alloc_register();
    let btree_flags = match &body {
        ast::CreateTableBody::ColumnsAndConstraints { options, .. }
            if options.contains_without_rowid() =>
        {
            CreateBTreeFlags::new_index()
        }
        _ => CreateBTreeFlags::new_table(),
    };
    program.emit_insn(Insn::CreateBtree {
        db: database_id,
        root: table_root_reg,
        flags: btree_flags,
    });

    // Create an automatic index B-tree if needed
    //
    // NOTE: we are deviating from SQLite bytecode here. For some reason, SQLite first creates a placeholder entry
    // for the table in sqlite_schema, then writes the index to sqlite_schema, then UPDATEs the table placeholder entry
    // in sqlite_schema with actual data.
    //
    // What we do instead is:
    // 1. Create the table B-tree
    // 2. Create the index B-tree
    // 3. Add the table entry to sqlite_schema
    // 4. Add the index entry to sqlite_schema
    //
    // I.e. we skip the weird song and dance with the placeholder entry. Unclear why sqlite does this.
    // The sqlite code has this comment:
    //
    // "This just creates a place-holder record in the sqlite_schema table.
    // The record created does not contain anything yet.  It will be replaced
    // by the real entry in code generated at sqlite3EndTable()."
    //
    // References:
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1355
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L2856-L2871
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1334C5-L1336C65

    let index_regs = collect_autoindexes(&body, program, &normalized_tbl_name)?;
    if let Some(index_regs) = index_regs.as_ref() {
        for index_reg in index_regs.iter() {
            program.emit_insn(Insn::CreateBtree {
                db: database_id,
                root: *index_reg,
                flags: CreateBTreeFlags::new_index(),
            });
        }
    }

    let table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), Some(SQLITE_TABLEID))?;

    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.as_ref().map(|x| x.0),
        SchemaEntryType::Table,
        &normalized_tbl_name,
        &normalized_tbl_name,
        table_root_reg,
        Some(sql),
    )?;

    if let Some(index_regs) = index_regs {
        for (idx, index_reg) in index_regs.into_iter().enumerate() {
            let index_name = format!(
                "{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{}_{}",
                normalized_tbl_name,
                idx + 1
            );
            emit_schema_entry(
                program,
                resolver,
                sqlite_schema_cursor_id,
                None,
                SchemaEntryType::Index,
                &index_name,
                &normalized_tbl_name,
                index_reg,
                None,
            )?;
        }
    }

    // Create the backing table for the hidden AUTOINCREMENT sequence. The
    // `__turso_internal_autoincrement_` prefix identifies the sequence object;
    // `sequence_backing_table_name` then maps it to the physical
    // `__turso_internal_seq_<sequence-name>` table.
    // Skip if it already exists (e.g. VACUUM INTO copies all tables first).
    if has_autoincrement {
        let autoinc_seq_name = crate::schema::autoincrement_sequence_name(&normalized_tbl_name);
        let backing_table_name =
            crate::translate::sequence::sequence_backing_table_name(&autoinc_seq_name);
        let already_exists = resolver.with_schema(database_id, |s| {
            s.get_btree_table(&backing_table_name).is_some()
        });
        if !already_exists {
            crate::translate::sequence::emit_sequence_backing_table(
                program,
                resolver,
                database_id,
                sqlite_schema_cursor_id,
                &autoinc_seq_name,
                1,        // start
                1,        // increment
                1,        // min
                i64::MAX, // max
                false,    // cycle
            )?;
        }
    }

    program.preassign_label_to_next_insn(parse_schema_label);
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });

    let escaped_tbl_name = escape_sql_string_literal(&normalized_tbl_name);
    let mut parse_schema_where_clause = String::with_capacity(
        "tbl_name = '' AND type != 'trigger'".len()
            + escaped_tbl_name.len()
            + if created_sequence_table {
                " OR tbl_name = 'sqlite_sequence'".len()
            } else {
                0
            },
    );
    parse_schema_where_clause.push_str("tbl_name = '");
    parse_schema_where_clause.push_str(&escaped_tbl_name);
    parse_schema_where_clause.push_str("' AND type != 'trigger'");
    if created_sequence_table {
        parse_schema_where_clause.push_str(" OR tbl_name = 'sqlite_sequence'");
    }

    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(parse_schema_where_clause),
        trigger_target_database_id: None,
    });

    // For CTAS, emit bytecode to populate the new table from the SELECT
    if let Some(info) = ctas_info {
        let col_count = match &body {
            ast::CreateTableBody::ColumnsAndConstraints { columns, .. } => columns.len(),
            _ => unreachable!("CTAS body was converted to ColumnsAndConstraints above"),
        };
        emit_ctas_insert(
            program,
            resolver,
            info.plan,
            &body,
            table_root_reg,
            col_count,
            database_id,
            &normalized_tbl_name,
            connection,
        )?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaEntryType {
    Table,
    Index,
    View,
    Trigger,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
            SchemaEntryType::View => "view",
            SchemaEntryType::Trigger => "trigger",
        }
    }
}
pub const SQLITE_TABLEID: &str = "sqlite_schema";

#[allow(clippy::too_many_arguments)]
pub fn emit_schema_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    sqlite_schema_cursor_id: usize,
    cdc_table_cursor_id: Option<usize>,
    entry_type: SchemaEntryType,
    name: &str,
    tbl_name: &str,
    root_page_reg: usize,
    sql: Option<String>,
) -> Result<()> {
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let type_reg = program.emit_string8_new_reg(entry_type.as_str().to_string());
    program.emit_string8_new_reg(name.to_string());
    program.emit_string8_new_reg(tbl_name.to_string());

    let table_root_reg = program.alloc_register();
    if root_page_reg == 0 {
        program.emit_insn(Insn::Integer {
            dest: table_root_reg,
            value: 0, // virtual tables in sqlite always have rootpage=0
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: root_page_reg,
            dst_reg: table_root_reg,
            extra_amount: 0,
        });
    }

    let sql_reg = program.alloc_register();
    if let Some(sql) = sql {
        program.emit_string8(sql, sql_reg);
    } else {
        program.emit_null(sql_reg, None);
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(type_reg),
        count: to_u32(5),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: tbl_name.to_string(),
    });

    if let Some(cdc_table_cursor_id) = cdc_table_cursor_id {
        let after_record_reg = if program.capture_data_changes_info().has_after() {
            Some(record_reg)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::INSERT,
            cdc_table_cursor_id,
            rowid_reg,
            None,
            after_record_reg,
            None,
            SQLITE_TABLEID,
        )?;
        emit_cdc_autocommit_commit(program, resolver, cdc_table_cursor_id)?;
    }
    Ok(())
}

/// Check if an automatic PRIMARY KEY index is required for the table.
/// If so, create a register for the index root page and return it.
///
/// An automatic PRIMARY KEY index is not required if:
/// - The table has no PRIMARY KEY
/// - The table has a single-column PRIMARY KEY whose typename is _exactly_ "INTEGER" e.g. not "INT".
///   In this case, the PRIMARY KEY column becomes an alias for the rowid.
///
/// Otherwise, an automatic PRIMARY KEY index is required.
fn collect_autoindexes(
    body: &ast::CreateTableBody,
    program: &mut ProgramBuilder,
    tbl_name: &str,
) -> Result<Option<Vec<usize>>> {
    let table = create_table(tbl_name, body, 0)?;

    let mut regs: Vec<usize> = Vec::new();

    // include UNIQUE singles, include PK single only if not rowid alias
    for us in table.unique_sets.iter().filter(|us| us.columns.len() == 1) {
        if us.is_primary_key && !table.has_rowid {
            continue;
        }
        let col_name = &us.columns.first().unwrap().name;
        let Some((_pos, col)) = table.get_column(col_name) else {
            bail_parse_error!("Column {col_name} not found in table {}", table.name);
        };

        let needs_index = if us.is_primary_key {
            !(col.primary_key() && col.is_rowid_alias())
        } else {
            // UNIQUE single needs an index
            true
        };

        if needs_index {
            regs.push(program.alloc_register());
        }
    }

    for _us in table.unique_sets.iter().filter(|us| us.columns.len() > 1) {
        if !table.has_rowid && _us.is_primary_key {
            continue;
        }
        regs.push(program.alloc_register());
    }
    if regs.is_empty() {
        Ok(None)
    } else {
        Ok(Some(regs))
    }
}

fn create_vtable_body_to_str(vtab: &ast::CreateVirtualTable, module: Arc<VTabImpl>) -> String {
    let args = vtab
        .args
        .iter()
        .map(|arg| arg.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    let if_not_exists = if vtab.if_not_exists {
        "IF NOT EXISTS "
    } else {
        ""
    };
    let ext_args = vtab
        .args
        .iter()
        .map(|a| turso_ext::Value::from_text(a.to_string()))
        .collect::<Vec<_>>();
    let schema = module
        .implementation
        .create_schema(ext_args)
        .unwrap_or_default();
    let vtab_args = if let Some(first_paren) = schema.find('(') {
        let closing_paren = schema.rfind(')').unwrap_or_default();
        &schema[first_paren..=closing_paren]
    } else {
        "()"
    };
    format!(
        "CREATE VIRTUAL TABLE {} {} USING {}{}\n /*{}{}*/",
        vtab.tbl_name.name.as_ident(),
        if_not_exists,
        vtab.module_name.as_ident(),
        if args.is_empty() {
            String::new()
        } else {
            format!("({args})")
        },
        vtab.tbl_name.name.as_ident(),
        vtab_args
    )
}

pub fn translate_create_virtual_table(
    vtab: ast::CreateVirtualTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    if connection.mvcc_enabled() {
        bail_parse_error!("Virtual tables are not supported in MVCC mode");
    }
    let ast::CreateVirtualTable {
        if_not_exists,
        tbl_name,
        module_name,
        args,
    } = &vtab;

    let table_name = tbl_name.name.as_str().to_string();
    let module_name_str = module_name.as_str().to_string();
    let args_vec = args.clone();
    let Some(vtab_module) = resolver.symbol_table.vtab_modules.get(&module_name_str) else {
        bail_parse_error!("no such module: {}", module_name_str);
    };
    if !vtab_module.module_kind.eq(&VTabKind::VirtualTable) {
        bail_parse_error!("module {} is not a virtual table", module_name_str);
    };
    if resolver.schema().get_table(&table_name).is_some() {
        if *if_not_exists {
            return Ok(());
        }
        bail_parse_error!(
            "table {} already exists",
            crate::util::identifier_token_for_error(&tbl_name.name)
        );
    }

    let opts = ProgramBuilderOpts::new(2, 40, 2);
    program.extend(&opts);
    let module_name_reg = program.emit_string8_new_reg(module_name_str.clone());
    let table_name_reg = program.emit_string8_new_reg(table_name.clone());
    let args_reg = if !args_vec.is_empty() {
        let args_start = program.alloc_registers(args_vec.len());

        // Emit string8 instructions for each arg
        for (i, arg) in args_vec.iter().enumerate() {
            program.emit_string8(arg.clone(), args_start + i);
        }
        let args_record_reg = program.alloc_register();

        // VCreate expects an array of args as a record
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(args_start),
            count: to_u32(args_vec.len()),
            dest_reg: to_u32(args_record_reg),
            index_name: None,
            affinity_str: None,
        });
        Some(args_record_reg)
    } else {
        None
    };

    program.emit_insn(Insn::VCreate {
        module_name: module_name_reg,
        table_name: table_name_reg,
        args_reg,
    });
    let table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: crate::MAIN_DB_ID,
    });

    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), Some(SQLITE_TABLEID))?;
    let sql = create_vtable_body_to_str(&vtab, vtab_module.clone());
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Table,
        tbl_name.name.as_str(),
        tbl_name.name.as_str(),
        0, // virtual tables dont have a root page
        Some(sql),
    )?;

    program.emit_insn(Insn::SetCookie {
        db: crate::MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: resolver.schema().schema_version as i32 + 1,
        p5: 0,
    });
    let escaped_table_name = escape_sql_string_literal(&table_name);
    let parse_schema_where_clause =
        format!("tbl_name = '{escaped_table_name}' AND type != 'trigger'");
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
        trigger_target_database_id: None,
    });

    Ok(())
}

/// Validates whether a DROP TABLE operation is allowed on the given table name.
fn validate_drop_table(
    resolver: &Resolver,
    database_id: usize,
    tbl_name: &str,
    connection: &Arc<Connection>,
) -> Result<()> {
    if !connection.is_nested_stmt()
        && crate::schema::is_system_table(tbl_name)
        // special case, allow dropping `sqlite_stat1`
        && !tbl_name.eq_ignore_ascii_case(STATS_TABLE)
    {
        bail_parse_error!("Cannot drop system table {}", tbl_name);
    }
    // Check if this is a materialized view - if so, refuse to drop it with DROP TABLE
    if resolver.with_schema(database_id, |schema| schema.is_materialized_view(tbl_name)) {
        bail_parse_error!(
            "Cannot DROP TABLE on materialized view {tbl_name}. Use DROP VIEW instead.",
        );
    }
    Ok(())
}

pub fn translate_drop_table(
    tbl_name: ast::QualifiedName,
    resolver: &mut Resolver,
    if_exists: bool,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(&tbl_name)?;
    let name = tbl_name.name.as_str();
    let opts = ProgramBuilderOpts::new(4, 40, 4);
    program.extend(&opts);

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let Some(table) = resolver.with_schema(database_id, |s| s.get_table(name)) else {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!(
            "no such table: {}",
            crate::util::table_name_for_error(&tbl_name)
        );
    };
    validate_drop_table(resolver, database_id, name, connection)?;
    // Check if foreign keys are enabled and if this table is referenced by foreign keys
    // Fire FK actions (CASCADE, SET NULL, SET DEFAULT) or check for violations (RESTRICT, NO ACTION)
    if connection.foreign_keys_enabled()
        && resolver.with_schema(database_id, |s| s.any_resolved_fks_referencing(name))
    {
        emit_fk_drop_table_check(program, resolver, name, connection, database_id)?;
    }
    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), Some(SQLITE_TABLEID))?;

    let null_reg = program.alloc_register(); //  r1
    program.emit_null(null_reg, None);
    let table_name_and_root_page_register = program.alloc_register(); //  r2, this register is special because it's first used to track table name and then moved root page
    let table_reg = program.emit_string8_new_reg(normalize_ident(tbl_name.name.as_str())); //  r3
    program.mark_last_insn_constant();
    let _table_type = program.emit_string8_new_reg("trigger".to_string()); //  r4
    program.mark_last_insn_constant();
    let row_id_reg = program.alloc_register(); //  r5

    let schema_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id_0 = program.alloc_cursor_id(
        //  cursor 0
        CursorType::BTreeTable(schema_table.clone()),
    );
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id_0,
        root_page: 1i64.into(),
        db: database_id,
    });

    //  1. Remove all entries from the schema table related to the table we are dropping (including triggers)
    //  loop to beginning of schema table
    let end_metadata_label = program.allocate_label();
    let metadata_loop = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_empty: end_metadata_label,
    });
    program.preassign_label_to_next_insn(metadata_loop);

    // start loop on schema table
    program.emit_column_or_rowid(
        sqlite_schema_cursor_id_0,
        2,
        table_name_and_root_page_register,
    );
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: table_name_and_root_page_register,
        rhs: table_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id_0,
        dest: row_id_reg,
    });
    if let Some((cdc_cursor_id, _)) = cdc_table {
        let table_type = program.emit_string8_new_reg("table".to_string()); // r4
        program.mark_last_insn_constant();

        let skip_cdc_label = program.allocate_label();

        let entry_type_reg = program.alloc_register();
        program.emit_column_or_rowid(sqlite_schema_cursor_id_0, 0, entry_type_reg);
        program.emit_insn(Insn::Ne {
            lhs: entry_type_reg,
            rhs: table_type,
            target_pc: skip_cdc_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        let before_record_reg = if program.capture_data_changes_info().has_before() {
            Some(emit_cdc_full_record(
                program,
                schema_table.columns(),
                sqlite_schema_cursor_id_0,
                row_id_reg,
                schema_table.is_strict,
            ))
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::DELETE,
            cdc_cursor_id,
            row_id_reg,
            before_record_reg,
            None,
            None,
            SQLITE_TABLEID,
        )?;
        program.preassign_label_to_next_insn(skip_cdc_label);
    }
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id_0,
        table_name: SQLITE_TABLEID.to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_next: metadata_loop,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(end_metadata_label);
    // end of loop on schema table
    if let Some((cdc_cursor_id, _)) = cdc_table {
        emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
    }

    // SQLite removes temp triggers targeting the dropped table.
    // Enumerate the temp schema triggers at translate time (which is
    // safe because a concurrent DDL would bump the schema cookie and
    // force a reprepare) and emit per-trigger bytecode to delete only
    // the rows that should be removed. Filtering in the bytecode by
    // `tbl_name` alone is not enough: two triggers with the same
    // unqualified table name can live in the temp schema but point to
    // different databases (e.g. one on `main.t`, one on `temp.t` when a
    // shadow table exists). We must key on trigger name.
    if database_id != crate::TEMP_DB_ID && resolver.has_temp_database() {
        // A temp schema trigger targets the dropped db iff:
        //   - it explicitly qualifies with the dropped db, or
        //   - it is unqualified AND dropping from main AND temp has no
        //     shadow table of the same name (in which case the
        //     unqualified reference resolves to main).
        let temp_has_shadow = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_table(tbl_name.name.as_str()).is_some()
        });
        let trigger_names_to_drop: Vec<String> = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_triggers_for_table(tbl_name.name.as_str())
                .filter(|trigger| match trigger.target_database_id {
                    Some(db_id) => db_id == database_id,
                    None => !temp_has_shadow && database_id == crate::MAIN_DB_ID,
                })
                .map(|trigger| trigger.name.clone())
                .collect()
        });

        if !trigger_names_to_drop.is_empty() {
            let temp_schema_cookie = resolver.with_schema(crate::TEMP_DB_ID, |s| s.schema_version);
            program.begin_write_on_database(crate::TEMP_DB_ID, temp_schema_cookie)?;
            let temp_schema_table =
                resolver.with_schema(crate::TEMP_DB_ID, |s| s.get_btree_table(SQLITE_TABLEID));
            if let Some(temp_schema_table) = temp_schema_table {
                let temp_cursor =
                    program.alloc_cursor_id(CursorType::BTreeTable(temp_schema_table));
                program.emit_insn(Insn::OpenWrite {
                    cursor_id: temp_cursor,
                    root_page: 1i64.into(),
                    db: crate::TEMP_DB_ID,
                });
                // Hoist the literal trigger names + `"trigger"` type
                // string into constant registers before the loop.
                let trigger_type_reg = program.emit_string8_new_reg("trigger".to_string());
                program.mark_last_insn_constant();
                let name_regs: Vec<usize> = trigger_names_to_drop
                    .iter()
                    .map(|name| {
                        let reg = program.emit_string8_new_reg(name.clone());
                        program.mark_last_insn_constant();
                        reg
                    })
                    .collect();

                let temp_end_label = program.allocate_label();
                let temp_loop_label = program.allocate_label();
                program.emit_insn(Insn::Rewind {
                    cursor_id: temp_cursor,
                    pc_if_empty: temp_end_label,
                });
                program.preassign_label_to_next_insn(temp_loop_label);
                let temp_next_label = program.allocate_label();
                let temp_delete_label = program.allocate_label();

                // Skip non-trigger rows (column 0 = type).
                let temp_type_reg = program.alloc_register();
                program.emit_column_or_rowid(temp_cursor, 0, temp_type_reg);
                program.emit_insn(Insn::Ne {
                    lhs: temp_type_reg,
                    rhs: trigger_type_reg,
                    target_pc: temp_next_label,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });

                // Cascade-check name (column 1) against each trigger we
                // want to drop. First match jumps to the delete label.
                let temp_name_reg = program.alloc_register();
                program.emit_column_or_rowid(temp_cursor, 1, temp_name_reg);
                for name_reg in &name_regs {
                    program.emit_insn(Insn::Eq {
                        lhs: temp_name_reg,
                        rhs: *name_reg,
                        target_pc: temp_delete_label,
                        flags: CmpInsFlags::default(),
                        collation: None,
                    });
                }
                // No name matched — skip the delete.
                program.emit_insn(Insn::Goto {
                    target_pc: temp_next_label,
                });
                program.preassign_label_to_next_insn(temp_delete_label);
                program.emit_insn(Insn::Delete {
                    cursor_id: temp_cursor,
                    table_name: SQLITE_TABLEID.to_string(),
                    is_part_of_update: false,
                });
                program.preassign_label_to_next_insn(temp_next_label);
                program.emit_insn(Insn::Next {
                    cursor_id: temp_cursor,
                    pc_if_next: temp_loop_label,
                    fullscan: false,
                });
                program.preassign_label_to_next_insn(temp_end_label);
            }
        }
    }

    //  2. Destroy the indices within a loop
    let indices: Vec<_> = resolver.with_schema(database_id, |s| {
        s.get_indices(tbl_name.name.as_str()).cloned().collect()
    });
    for index in &indices {
        if index.index_method.is_some() && !index.is_backing_btree_index() {
            // Index methods without backing btree need special destroy handling
            let cursor_id = program.alloc_cursor_index(None, index)?;
            program.emit_insn(Insn::IndexMethodDestroy {
                db: database_id,
                cursor_id,
            });
        } else {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: index.root_page,
                former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
                is_temp: 0,
            });
        }

        //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
        //  Requires support via https://github.com/tursodatabase/turso/pull/768

        //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
        //  Requires support via https://github.com/tursodatabase/turso/pull/768
    }

    //  3. Destroy the table structure
    match table.as_ref() {
        Table::BTree(table) => {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: table.root_page,
                former_root_reg: table_name_and_root_page_register,
                is_temp: 0,
            });
        }
        Table::Virtual(vtab) => {
            if !vtab.is_droppable {
                return Err(crate::LimboError::ParseError(format!(
                    "table {} may not be dropped",
                    vtab.name
                )));
            }
            program.emit_insn(Insn::VDestroy {
                table_name: vtab.name.clone(),
                db: database_id,
            });
        }
        Table::FromClauseSubquery(..) => panic!("FromClauseSubquery can't be dropped"),
        Table::RecursiveCteInput(..) => panic!("recursive CTE inputs cannot be dropped"),
    };

    let schema_data_register = program.alloc_register();
    let schema_row_id_register = program.alloc_register();
    program.emit_null(schema_data_register, Some(schema_row_id_register));

    // All of the following processing needs to be done only if the table is not a virtual table
    if table.btree().is_some() {
        // 4. Open an ephemeral table, and read over the entry from the schema table whose root page was moved in the destroy operation

        // cursor id 1
        let sqlite_schema_cursor_id_1 =
            program.alloc_cursor_id(CursorType::BTreeTable(schema_table.clone()));
        let columns = crate::alloc::try_vec![Column::new(
            Some("rowid".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef::default(),
        )]?;
        let simple_table_rc = Arc::new(BTreeTable::new(
            0, // root_page, not relevant for ephemeral table definition
            "ephemeral_scratch".to_string(),
            crate::alloc::vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ));
        // cursor id 2
        let ephemeral_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(simple_table_rc));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: ephemeral_cursor_id,
            is_table: true,
        });
        let if_not_label = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: table_name_and_root_page_register,
            target_pc: if_not_label,
            jump_if_null: true, //  jump anyway
        });
        program.emit_insn(Insn::OpenRead {
            cursor_id: sqlite_schema_cursor_id_1,
            root_page: 1i64,
            db: database_id,
        });

        let schema_column_0_register = program.alloc_register();
        let schema_column_1_register = program.alloc_register();
        let schema_column_2_register = program.alloc_register();
        let moved_to_root_page_register = program.alloc_register(); //  the register that will contain the root page number the last root page is moved to
        let schema_column_4_register = program.alloc_register();
        let prev_root_page_register = program.alloc_register(); //  the register that will contain the root page number that the last root page was on before VACUUM
        let _r14 = program.alloc_register(); //  Unsure why this register is allocated but putting it in here to make comparison with SQLite easier
        let new_record_register = program.alloc_register();

        //  Loop to copy over row id's from the schema table for rows that have the same root page as the one that was moved
        let copy_schema_to_temp_table_loop_end_label = program.allocate_label();
        let copy_schema_to_temp_table_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: sqlite_schema_cursor_id_1,
            pc_if_empty: copy_schema_to_temp_table_loop_end_label,
        });
        program.preassign_label_to_next_insn(copy_schema_to_temp_table_loop);
        // start loop on schema table
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 3, prev_root_page_register);
        // The label and Insn::Ne are used to skip over any rows in the schema table that don't have the root page that was moved
        let next_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: prev_root_page_register,
            rhs: table_name_and_root_page_register,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::RowId {
            cursor_id: sqlite_schema_cursor_id_1,
            dest: schema_row_id_register,
        });
        program.emit_insn(Insn::Insert {
            cursor: ephemeral_cursor_id,
            key_reg: schema_row_id_register,
            record_reg: schema_data_register,
            flag: InsertFlags::new(),
            table_name: "scratch_table".to_string(),
        });

        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: sqlite_schema_cursor_id_1,
            pc_if_next: copy_schema_to_temp_table_loop,
            fullscan: false,
        });
        program.preassign_label_to_next_insn(copy_schema_to_temp_table_loop_end_label);
        // End loop to copy over row id's from the schema table for rows that have the same root page as the one that was moved

        program.preassign_label_to_next_insn(if_not_label);

        // 5. Open a write cursor to the schema table and re-insert the records placed in the ephemeral table but insert the correct root page now
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id_1,
            root_page: 1i64.into(),
            db: database_id,
        });

        // Loop to copy over row id's from the ephemeral table and then re-insert into the schema table with the correct root page
        let copy_temp_table_to_schema_loop_end_label = program.allocate_label();
        let copy_temp_table_to_schema_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: ephemeral_cursor_id,
            pc_if_empty: copy_temp_table_to_schema_loop_end_label,
        });
        program.preassign_label_to_next_insn(copy_temp_table_to_schema_loop);
        //  start loop on schema table
        program.emit_insn(Insn::RowId {
            cursor_id: ephemeral_cursor_id,
            dest: schema_row_id_register,
        });
        //  the next_label and Insn::NotExists are used to skip patching any rows in the schema table that don't have the row id that was written to the ephemeral table
        let next_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: sqlite_schema_cursor_id_1,
            rowid_reg: schema_row_id_register,
            target_pc: next_label,
        });
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 0, schema_column_0_register);
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 1, schema_column_1_register);
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 2, schema_column_2_register);
        let root_page = table.get_root_page()?;
        program.emit_insn(Insn::Integer {
            value: root_page,
            dest: moved_to_root_page_register,
        });
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 4, schema_column_4_register);
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(schema_column_0_register),
            count: to_u32(5),
            dest_reg: to_u32(new_record_register),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: sqlite_schema_cursor_id_1,
            table_name: SQLITE_TABLEID.to_string(),
            is_part_of_update: false,
        });
        program.emit_insn(Insn::Insert {
            cursor: sqlite_schema_cursor_id_1,
            key_reg: schema_row_id_register,
            record_reg: new_record_register,
            flag: InsertFlags::new(),
            table_name: SQLITE_TABLEID.to_string(),
        });

        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: ephemeral_cursor_id,
            pc_if_next: copy_temp_table_to_schema_loop,
            fullscan: false,
        });
        program.preassign_label_to_next_insn(copy_temp_table_to_schema_loop_end_label);
        // End loop to copy over row id's from the ephemeral table and then re-insert into the schema table with the correct root page
    }

    // If the dropped table had AUTOINCREMENT, clear its `sqlite_sequence` row.
    // Look up the table in the TARGET database's schema (not the main one).
    // `resolver.schema()` returns the MAIN schema, so for `DROP TABLE aux.t`
    // it would either (a) skip the cleanup entirely when main lacks
    // `sqlite_sequence`, leaving a stale high-water-mark in `aux.sqlite_sequence`
    // that the next AUTOINCREMENT INSERT into a same-named table would
    // resume from, or (b) — worse — open the cursor against the attached
    // database using main's root page, corrupting whatever lived there.
    // The matching `OpenWrite` below passes `db: database_id`, so the root
    // page must come from the same schema.
    if let Some(seq_table) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(SQLITE_SEQUENCE_TABLE_NAME)
    }) {
        let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));
        let seq_table_name_reg = program.alloc_register();
        let dropped_table_name_reg =
            program.emit_string8_new_reg(normalize_ident(tbl_name.name.as_str()));
        program.mark_last_insn_constant();

        program.emit_insn(Insn::OpenWrite {
            cursor_id: seq_cursor_id,
            root_page: seq_table.root_page.into(),
            db: database_id,
        });

        let end_loop_label = program.allocate_label();
        let loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: seq_cursor_id,
            pc_if_empty: end_loop_label,
        });

        program.preassign_label_to_next_insn(loop_start_label);

        program.emit_column_or_rowid(seq_cursor_id, 0, seq_table_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: seq_table_name_reg,
            rhs: dropped_table_name_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        program.emit_insn(Insn::Delete {
            cursor_id: seq_cursor_id,
            table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });

        program.preassign_label_to_next_insn(continue_loop_label);
        program.emit_insn(Insn::Next {
            cursor_id: seq_cursor_id,
            pc_if_next: loop_start_label,
            fullscan: false,
        });

        program.preassign_label_to_next_insn(end_loop_label);
    }

    // Clean up turso_cdc_version entry for the dropped table (if version table exists)
    if let Some(version_table) = resolver
        .schema()
        .get_table(crate::cdc::TURSO_CDC_VERSION_TABLE_NAME)
        .and_then(|t| t.btree())
    {
        let version_index_name = format!(
            "{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{}_1",
            crate::cdc::TURSO_CDC_VERSION_TABLE_NAME
        );
        let version_index = resolver
            .schema()
            .get_index(
                crate::cdc::TURSO_CDC_VERSION_TABLE_NAME,
                &version_index_name,
            )
            .cloned();
        let ver_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(version_table.clone()));
        let ver_index_cursor_id = version_index
            .as_ref()
            .map(|index| program.alloc_cursor_index(None, index))
            .transpose()?;
        let ver_table_name_reg = program.alloc_register();
        let dropped_name_reg =
            program.emit_string8_new_reg(normalize_ident(tbl_name.name.as_str()));
        program.mark_last_insn_constant();

        program.emit_insn(Insn::OpenWrite {
            cursor_id: ver_cursor_id,
            root_page: version_table.root_page.into(),
            db: crate::MAIN_DB_ID,
        });
        if let (Some(index), Some(cursor_id)) = (&version_index, ver_index_cursor_id) {
            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: index.root_page.into(),
                db: crate::MAIN_DB_ID,
            });
        }

        let end_ver_loop_label = program.allocate_label();
        let ver_loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: ver_cursor_id,
            pc_if_empty: end_ver_loop_label,
        });

        program.preassign_label_to_next_insn(ver_loop_start_label);

        program.emit_column_or_rowid(ver_cursor_id, 0, ver_table_name_reg);

        let continue_ver_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: ver_table_name_reg,
            rhs: dropped_name_reg,
            target_pc: continue_ver_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        if let (Some(index), Some(cursor_id)) = (&version_index, ver_index_cursor_id) {
            turso_assert_eq!(index.columns.len(), 1);
            turso_assert!(index.has_rowid);
            turso_assert!(index.where_clause.is_none());
            turso_assert!(index.columns[0].expr.is_none());

            let index_key_reg = program.alloc_registers(2);
            program.emit_column_or_rowid(
                ver_cursor_id,
                index.columns[0].pos_in_table,
                index_key_reg,
            );
            program.emit_insn(Insn::RowId {
                cursor_id: ver_cursor_id,
                dest: index_key_reg + 1,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: index_key_reg,
                num_regs: 2,
                cursor_id,
                raise_error_if_no_matching_entry: true,
            });
        }

        program.emit_insn(Insn::Delete {
            cursor_id: ver_cursor_id,
            table_name: crate::cdc::TURSO_CDC_VERSION_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });

        program.preassign_label_to_next_insn(continue_ver_label);
        program.emit_insn(Insn::Next {
            cursor_id: ver_cursor_id,
            pc_if_next: ver_loop_start_label,
            fullscan: false,
        });

        program.preassign_label_to_next_insn(end_ver_loop_label);
    }

    // Drop the in-memory structures for the table
    program.emit_insn(Insn::DropTable {
        db: database_id,
        _p2: 0,
        _p3: 0,
        table_name: tbl_name.name.as_str().to_string(),
    });

    // If the dropped table owned an implicit AUTOINCREMENT sequence, tear
    // down its backing table too: destroy the B-tree root, remove the
    // sqlite_schema entry, and drop the in-memory `Sequence`. Without this
    // the `__turso_internal_seq_<…>` backing table outlives its parent,
    // and a subsequent `CREATE TABLE <name>` with the same name and
    // AUTOINCREMENT under MVCC would resume nextval at the old watermark
    // (and the orphan table bloats the DB indefinitely in WAL mode).
    // Schema-cookie bump is shared with the table-drop below; the helper
    // never touches it. No-op when has_autoincrement is false or the
    // sequence/backing-table is somehow already missing — same idempotency
    // contract as DROP SEQUENCE IF EXISTS.
    if table.btree().is_some_and(|bt| bt.has_autoincrement) {
        let seq_name =
            crate::schema::autoincrement_sequence_name(&normalize_ident(tbl_name.name.as_str()));
        crate::translate::sequence::emit_drop_sequence_cleanup(
            program,
            resolver,
            database_id,
            &seq_name,
        )?;
    }

    let current_schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: current_schema_version as i32 + 1,
        p5: 0,
    });

    Ok(())
}

/// Validate an encode or decode expression for safety.
/// Rejects subqueries, aggregates, and window functions.
fn validate_type_expr(expr: &ast::Expr, kind: &str, resolver: &Resolver) -> Result<()> {
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries prohibited in {kind} expressions");
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("window functions prohibited in {kind} expressions");
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), args.len())? {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!(
                            "aggregate functions prohibited in {kind} expressions: {}",
                            name.as_str()
                        );
                    }
                    // Reject known non-deterministic built-in functions.
                    // External functions are excluded from this check since
                    // they default to non-deterministic but may actually be
                    // deterministic (e.g. uuid_blob).
                    if !matches!(func, Func::External(_)) && !func.is_deterministic() {
                        bail_parse_error!(
                            "non-deterministic functions prohibited in {kind} expressions: {}",
                            name.as_str()
                        );
                    }
                }
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                bail_parse_error!(
                    "aggregate functions prohibited in {kind} expressions: {}",
                    name.as_str()
                );
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Shared persistence logic for CREATE TYPE / CREATE DOMAIN.
/// Persists the type SQL into __turso_internal_types and registers it in memory.
fn persist_type_definition(
    normalized_name: String,
    sql: String,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    // Ensure sqlite_turso_types table exists (lazy creation)
    let types_table: Arc<BTreeTable>;
    let types_root_page: RegisterOrLiteral<i64>;

    if let Some(existing) = resolver.schema().get_btree_table(TURSO_TYPES_TABLE_NAME) {
        types_table = existing.clone();
        types_root_page = RegisterOrLiteral::Literal(existing.root_page);
    } else {
        // Create the sqlite_turso_types btree
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: MAIN_DB_ID,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let create_sql =
            format!("CREATE TABLE {TURSO_TYPES_TABLE_NAME}(name TEXT PRIMARY KEY, sql TEXT)");
        types_table = Arc::new(BTreeTable::from_sql(&create_sql, 0)?);
        types_root_page = RegisterOrLiteral::Register(table_root_reg);

        // Register it in sqlite_schema so it persists
        let schema_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
        let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: schema_cursor_id,
            root_page: 1i64.into(),
            db: MAIN_DB_ID,
        });
        emit_schema_entry(
            program,
            resolver,
            schema_cursor_id,
            None,
            SchemaEntryType::Table,
            TURSO_TYPES_TABLE_NAME,
            TURSO_TYPES_TABLE_NAME,
            table_root_reg,
            Some(create_sql),
        )?;

        // Parse schema to register the new table in-memory
        program.emit_insn(Insn::ParseSchema {
            db: schema_cursor_id,
            where_clause: Some(format!(
                "tbl_name = '{TURSO_TYPES_TABLE_NAME}' AND type != 'trigger'"
            )),
            trigger_target_database_id: None,
        });
    }

    // Open sqlite_turso_types for writing
    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_root_page,
        db: MAIN_DB_ID,
    });

    // Insert (name, sql) record
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: types_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    let name_reg = program.emit_string8_new_reg(normalized_name);
    program.emit_string8_new_reg(sql.clone());
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(name_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: types_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
    });

    // Add the type to the in-memory registry
    program.emit_insn(Insn::AddType {
        db: MAIN_DB_ID,
        sql,
    });

    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });

    Ok(())
}

pub fn translate_create_type(
    type_name: &str,
    body: &ast::CreateTypeBody,
    if_not_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let normalized_name = normalize_ident(type_name);

    // Reject names that shadow SQLite base types
    let is_base_type = turso_macros::match_ignore_ascii_case!(match normalized_name.as_bytes() {
        b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" | b"ANY" => true,
        _ => false,
    });
    if is_base_type {
        bail_parse_error!("cannot create type \"{normalized_name}\": name is a built-in type");
    }

    // Check if type already exists
    if resolver
        .schema()
        .get_type_def_unchecked(&normalized_name)
        .is_some()
    {
        if if_not_exists {
            return Ok(());
        }
        bail_parse_error!("type {normalized_name} already exists");
    }

    // Validate encode/decode expressions for safety (only for custom types)
    if let ast::CreateTypeBody::CustomType {
        ref encode,
        ref decode,
        ..
    } = body
    {
        if let Some(ref encode) = encode {
            validate_type_expr(encode, "ENCODE", resolver)?;
        }
        if let Some(ref decode) = decode {
            validate_type_expr(decode, "DECODE", resolver)?;
        }
    }

    // Build canonical SQL (without IF NOT EXISTS) for persistence
    let sql = build_create_type_sql(&normalized_name, body);

    persist_type_definition(normalized_name, sql, resolver, program)
}

/// Build canonical CREATE TYPE SQL from a normalized name and parsed body.
fn build_create_type_sql(name: &str, body: &ast::CreateTypeBody) -> String {
    use crate::util::quote_identifier as quote_ident;
    fn quote_string_literal(s: &str) -> String {
        s.replace('\'', "''")
    }

    match body {
        ast::CreateTypeBody::CustomType {
            params,
            base,
            encode,
            decode,
            default,
            operators,
        } => {
            let mut sql = if params.is_empty() {
                format!(
                    "CREATE TYPE {} BASE {}",
                    quote_ident(name),
                    quote_ident(base)
                )
            } else {
                let param_strs: Vec<String> = params
                    .iter()
                    .map(|p| match &p.ty {
                        Some(ty) => format!("{} {}", quote_ident(&p.name), ty),
                        None => quote_ident(&p.name),
                    })
                    .collect();
                format!(
                    "CREATE TYPE {}({}) BASE {}",
                    quote_ident(name),
                    param_strs.join(", "),
                    quote_ident(base)
                )
            };
            if let Some(ref encode) = encode {
                sql.push_str(&format!(" ENCODE {encode}"));
            }
            if let Some(ref decode) = decode {
                sql.push_str(&format!(" DECODE {decode}"));
            }
            if let Some(ref default) = default {
                sql.push_str(&format!(" DEFAULT {default}"));
            }
            for op in operators {
                match &op.func_name {
                    Some(func_name) => sql.push_str(&format!(
                        " OPERATOR '{}' {}",
                        quote_string_literal(&op.op),
                        quote_ident(func_name)
                    )),
                    None => sql.push_str(&format!(" OPERATOR '{}'", quote_string_literal(&op.op))),
                }
            }
            sql
        }
        ast::CreateTypeBody::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "{} {}",
                        quote_ident(f.name.as_str()),
                        quote_ident(&f.field_type.name)
                    )
                })
                .collect();
            format!(
                "CREATE TYPE {} AS STRUCT({})",
                quote_ident(name),
                field_strs.join(", ")
            )
        }
        ast::CreateTypeBody::Union(fields) => {
            let variant_strs: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "{} {}",
                        quote_ident(f.name.as_str()),
                        quote_ident(&f.field_type.name)
                    )
                })
                .collect();
            format!(
                "CREATE TYPE {} AS UNION({})",
                quote_ident(name),
                variant_strs.join(", ")
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn translate_create_domain(
    domain_name: &str,
    base_type: &str,
    not_null: bool,
    constraints: &[ast::DomainConstraint],
    default: Option<Box<ast::Expr>>,
    if_not_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let normalized_name = normalize_ident(domain_name);

    // Reject names that shadow SQLite base types
    let is_base_type = turso_macros::match_ignore_ascii_case!(match normalized_name.as_bytes() {
        b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" | b"ANY" => true,
        _ => false,
    });
    if is_base_type {
        bail_parse_error!("cannot create domain \"{normalized_name}\": name is a built-in type");
    }

    // Check if type/domain already exists
    if resolver
        .schema()
        .get_type_def_unchecked(&normalized_name)
        .is_some()
    {
        if if_not_exists {
            return Ok(());
        }
        bail_parse_error!("type {normalized_name} already exists");
    }

    // Validate base type exists — must be a primitive or a registered type
    let base_normalized = normalize_ident(base_type);
    let is_primitive = turso_macros::match_ignore_ascii_case!(match base_normalized.as_bytes() {
        b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" => true,
        _ => false,
    });
    if !is_primitive
        && resolver
            .schema()
            .get_type_def_unchecked(&base_normalized)
            .is_none()
    {
        bail_parse_error!("base type \"{base_type}\" does not exist");
    }

    // Validate no cycles — check if base type chain is acyclic
    if !is_primitive {
        resolver
            .schema()
            .resolve_base_type_chain(&base_normalized)?;
    }

    // Validate CHECK and DEFAULT expressions (reject subqueries, aggregates, etc.)
    for c in constraints {
        validate_type_expr(&c.check, "domain CHECK", resolver)?;
    }
    if let Some(ref def) = default {
        validate_type_expr(def, "domain DEFAULT", resolver)?;
    }

    // Build the CREATE DOMAIN SQL for persistence
    let sql = {
        let mut s = format!("CREATE DOMAIN {normalized_name} AS {base_type}");
        if let Some(ref def) = default {
            s.push_str(&format!(" DEFAULT {def}"));
        }
        if not_null {
            s.push_str(" NOT NULL");
        }
        for c in constraints {
            if let Some(ref name) = c.name {
                s.push_str(&format!(" CONSTRAINT {name}"));
            }
            s.push_str(&format!(" CHECK ({})", c.check));
        }
        s
    };

    persist_type_definition(normalized_name, sql, resolver, program)
}

pub fn translate_drop_type(
    type_name: &str,
    if_exists: bool,
    is_domain_drop: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let normalized_name = normalize_ident(type_name);
    let kind = if is_domain_drop { "domain" } else { "type" };

    // Check if type exists
    let type_def = resolver.schema().get_type_def_unchecked(&normalized_name);
    if type_def.is_none() {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("no such {kind}: {normalized_name}");
    }

    let type_def = type_def.unwrap();

    // Validate that DROP TYPE targets a type and DROP DOMAIN targets a domain
    let target_is_domain = type_def.is_domain;
    if is_domain_drop && !target_is_domain {
        bail_parse_error!("{normalized_name} is a type, not a domain. Use DROP TYPE instead");
    }
    if !is_domain_drop && target_is_domain {
        bail_parse_error!("{normalized_name} is a domain, not a type. Use DROP DOMAIN instead");
    }

    // Check if built-in type
    if type_def.is_builtin {
        bail_parse_error!("cannot drop built-in type: {normalized_name}");
    }

    // Check if any table uses this type
    for (_, table) in resolver.schema().tables.iter() {
        for col in table.columns() {
            if normalize_ident(&col.ty_str) == normalized_name {
                bail_parse_error!(
                    "cannot drop type {normalized_name}: used by column {} in table {}",
                    col.name.as_deref().unwrap_or("?"),
                    table.get_name()
                );
            }
        }
    }

    // Check if any other type/domain depends on this type
    for (name, td) in resolver.schema().type_registry.iter() {
        if normalize_ident(td.base()) == normalized_name {
            bail_parse_error!(
                "cannot drop type {}: type {} depends on it",
                normalized_name,
                name
            );
        }
    }

    // Open cursor to sqlite_turso_types table
    let types_table = resolver
        .schema()
        .get_btree_table(TURSO_TYPES_TABLE_NAME)
        .ok_or_else(|| crate::LimboError::ParseError(format!("no such type: {normalized_name}")))?;
    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_table.root_page.into(),
        db: MAIN_DB_ID,
    });

    // Search for matching row: name=type_name (col 0)
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        dest: name_reg,
        value: normalized_name.clone(),
    });

    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: types_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    // Read name (col 0)
    let col0_reg = program.alloc_register();
    program.emit_column_or_rowid(types_cursor_id, 0, col0_reg);

    let skip_delete_label = program.allocate_label();

    // Check name=type_name
    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // Delete matching row
    program.emit_insn(Insn::Delete {
        cursor_id: types_cursor_id,
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(skip_delete_label);

    program.emit_insn(Insn::Next {
        cursor_id: types_cursor_id,
        pc_if_next: loop_start_label,
        fullscan: false,
    });

    program.preassign_label_to_next_insn(end_loop_label);

    // Remove from in-memory schema
    program.emit_insn(Insn::DropType {
        db: MAIN_DB_ID,
        type_name: normalized_name,
    });

    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });

    Ok(())
}
