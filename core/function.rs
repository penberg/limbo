use crate::sync::Arc;
use std::fmt;
use std::fmt::{Debug, Display};
use strum::IntoEnumIterator;
use turso_ext::{
    ContextDestructor, FinalizeFunction, InitAggFunction, ScalarFunction, StepFunction,
    ValueDestructor,
};

use crate::LimboError;

pub type ContextCollationFunction = unsafe extern "C" fn(
    context: usize,
    left_ptr: *const u8,
    left_len: usize,
    right_ptr: *const u8,
    right_len: usize,
) -> i32;

pub trait Deterministic: std::fmt::Display {
    fn is_deterministic(&self) -> bool;
}

pub struct ExternalFunc {
    pub name: String,
    pub func: ExtFunc,
}

pub struct ExternalCollation {
    pub name: String,
    pub context: usize,
    pub callback: ContextCollationFunction,
    pub context_destructor: Option<ContextDestructor>,
}

impl ExternalCollation {
    pub fn new(
        name: String,
        context: usize,
        callback: ContextCollationFunction,
        context_destructor: Option<ContextDestructor>,
    ) -> Self {
        Self {
            name,
            context,
            callback,
            context_destructor,
        }
    }
}

impl Drop for ExternalCollation {
    fn drop(&mut self) {
        if let Some(destructor) = self.context_destructor {
            unsafe { destructor(self.context) };
        }
    }
}

impl Debug for ExternalCollation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalCollation")
            .field("name", &self.name)
            .finish()
    }
}

impl Deterministic for ExternalFunc {
    fn is_deterministic(&self) -> bool {
        match self.func {
            ExtFunc::Scalar { deterministic, .. } => deterministic,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExtFunc {
    Scalar {
        context: usize,
        argc: i32,
        deterministic: bool,
        callback: ScalarFunction,
        context_destructor: Option<ContextDestructor>,
        value_destructor: Option<ValueDestructor>,
    },
    Aggregate {
        context: usize,
        argc: i32,
        init: InitAggFunction,
        step: StepFunction,
        finalize: FinalizeFunction,
        context_destructor: Option<ContextDestructor>,
        aggregate_destructor: Option<ContextDestructor>,
        value_destructor: Option<ValueDestructor>,
    },
}

impl ExtFunc {
    pub fn agg_args(&self) -> Result<i32, ()> {
        if let ExtFunc::Aggregate { argc, .. } = self {
            return Ok(*argc);
        }
        Err(())
    }

    pub fn matches_arg_count(&self, arg_count: usize) -> bool {
        match self {
            Self::Scalar { argc, .. } => *argc < 0 || *argc as usize == arg_count,
            Self::Aggregate { argc, .. } => *argc < 0 || *argc as usize == arg_count,
        }
    }

    pub fn is_aggregate(&self) -> bool {
        matches!(self, Self::Aggregate { .. })
    }

    pub fn with_aggregate_arg_count(&self, arg_count: usize) -> Self {
        match self {
            Self::Aggregate {
                context,
                init,
                step,
                finalize,
                aggregate_destructor,
                value_destructor,
                ..
            } => Self::Aggregate {
                context: *context,
                argc: arg_count as i32,
                init: *init,
                step: *step,
                finalize: *finalize,
                context_destructor: None,
                aggregate_destructor: *aggregate_destructor,
                value_destructor: *value_destructor,
            },
            _ => self.clone(),
        }
    }
}

impl ExternalFunc {
    pub fn new_scalar(
        name: String,
        argc: i32,
        deterministic: bool,
        context: usize,
        callback: ScalarFunction,
        context_destructor: Option<ContextDestructor>,
        value_destructor: Option<ValueDestructor>,
    ) -> Self {
        Self {
            name,
            func: ExtFunc::Scalar {
                context,
                argc,
                deterministic,
                callback,
                context_destructor,
                value_destructor,
            },
        }
    }

    pub fn new_aggregate(
        name: String,
        argc: i32,
        context: usize,
        func: (InitAggFunction, StepFunction, FinalizeFunction),
        context_destructor: Option<ContextDestructor>,
        aggregate_destructor: Option<ContextDestructor>,
        value_destructor: Option<ValueDestructor>,
    ) -> Self {
        Self {
            name,
            func: ExtFunc::Aggregate {
                context,
                argc,
                init: func.0,
                step: func.1,
                finalize: func.2,
                context_destructor,
                aggregate_destructor,
                value_destructor,
            },
        }
    }
}

impl Drop for ExternalFunc {
    fn drop(&mut self) {
        match self.func {
            ExtFunc::Scalar {
                context,
                context_destructor: Some(context_destructor),
                ..
            }
            | ExtFunc::Aggregate {
                context,
                context_destructor: Some(context_destructor),
                ..
            } => unsafe { context_destructor(context) },
            _ => {}
        }
    }
}

impl Debug for ExternalFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Display for ExternalFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
pub enum JsonFunc {
    Json,
    Jsonb,
    JsonArray,
    JsonbArray,
    JsonArrayLength,
    JsonArrowExtract,
    JsonArrowShiftExtract,
    JsonExtract,
    JsonbExtract,
    JsonObject,
    JsonbObject,
    JsonType,
    JsonErrorPosition,
    JsonValid,
    JsonPatch,
    JsonbPatch,
    JsonRemove,
    JsonbRemove,
    JsonReplace,
    JsonbReplace,
    JsonInsert,
    JsonbInsert,
    JsonPretty,
    JsonSet,
    JsonbSet,
    JsonQuote,
}

#[cfg(feature = "json")]
impl Deterministic for JsonFunc {
    fn is_deterministic(&self) -> bool {
        true
    }
}

#[cfg(feature = "json")]
impl Display for JsonFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Json => "json",
                Self::Jsonb => "jsonb",
                Self::JsonArray => "json_array",
                Self::JsonbArray => "jsonb_array",
                Self::JsonExtract => "json_extract",
                Self::JsonbExtract => "jsonb_extract",
                Self::JsonArrayLength => "json_array_length",
                Self::JsonArrowExtract => "->",
                Self::JsonArrowShiftExtract => "->>",
                Self::JsonObject => "json_object",
                Self::JsonbObject => "jsonb_object",
                Self::JsonType => "json_type",
                Self::JsonErrorPosition => "json_error_position",
                Self::JsonValid => "json_valid",
                Self::JsonPatch => "json_patch",
                Self::JsonbPatch => "jsonb_patch",
                Self::JsonRemove => "json_remove",
                Self::JsonbRemove => "jsonb_remove",
                Self::JsonReplace => "json_replace",
                Self::JsonbReplace => "jsonb_replace",
                Self::JsonInsert => "json_insert",
                Self::JsonbInsert => "jsonb_insert",
                Self::JsonPretty => "json_pretty",
                Self::JsonSet => "json_set",
                Self::JsonbSet => "jsonb_set",
                Self::JsonQuote => "json_quote",
            }
        )
    }
}

#[cfg(feature = "json")]
impl JsonFunc {
    /// Returns true for operator-style entries that should not appear in PRAGMA function_list.
    pub fn is_internal(&self) -> bool {
        matches!(self, Self::JsonArrowExtract | Self::JsonArrowShiftExtract)
    }

    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Json | Self::Jsonb | Self::JsonQuote | Self::JsonErrorPosition => &[1],
            Self::JsonPatch | Self::JsonbPatch => &[2],
            Self::JsonArrayLength | Self::JsonType | Self::JsonValid => &[1, 2],
            // Operators — filtered out, arity doesn't matter
            Self::JsonArrowExtract | Self::JsonArrowShiftExtract => &[2],
            // Variable-arg
            _ => &[-1],
        }
    }
}

#[derive(Debug, Clone, strum::EnumIter)]
pub enum VectorFunc {
    Vector,
    Vector32,
    Vector32Sparse,
    Vector64,
    Vector8,
    Vector1Bit,
    VectorExtract,
    VectorDistanceCos,
    VectorDistanceL2,
    VectorDistanceJaccard,
    VectorDistanceDot,
    VectorConcat,
    VectorSlice,
}

impl Deterministic for VectorFunc {
    fn is_deterministic(&self) -> bool {
        true
    }
}

impl Display for VectorFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Vector => "vector",
            Self::Vector32 => "vector32",
            Self::Vector32Sparse => "vector32_sparse",
            Self::Vector64 => "vector64",
            Self::Vector8 => "vector8",
            Self::Vector1Bit => "vector1bit",
            Self::VectorExtract => "vector_extract",
            Self::VectorDistanceCos => "vector_distance_cos",
            Self::VectorDistanceL2 => "vector_distance_l2",
            Self::VectorDistanceJaccard => "vector_distance_jaccard",
            Self::VectorDistanceDot => "vector_distance_dot",
            Self::VectorConcat => "vector_concat",
            Self::VectorSlice => "vector_slice",
        };
        write!(f, "{str}")
    }
}

impl VectorFunc {
    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Vector
            | Self::Vector32
            | Self::Vector32Sparse
            | Self::Vector64
            | Self::Vector8
            | Self::Vector1Bit
            | Self::VectorExtract => &[1],
            Self::VectorDistanceCos
            | Self::VectorDistanceL2
            | Self::VectorDistanceJaccard
            | Self::VectorDistanceDot => &[2],
            Self::VectorSlice => &[3],
            Self::VectorConcat => &[-1],
        }
    }
}

/// Full-text search functions
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
pub enum FtsFunc {
    /// fts_score(col1, col2, ..., query): computes FTS relevance score
    /// When used with an FTS index, the optimizer routes through the index method
    Score,
    /// fts_match(col1, col2, ..., query): returns true if document matches query
    /// Used in WHERE clause for filtering rows by FTS match
    Match,
    /// fts_highlight(text, query, before_tag, after_tag): returns text with matching terms highlighted
    /// Wraps matching query terms in the text with before_tag and after_tag markers
    Highlight,
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
impl FtsFunc {
    pub fn is_deterministic(&self) -> bool {
        true
    }

    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Highlight => &[4],
            // Score and Match take variable columns + query
            Self::Score | Self::Match => &[-1],
        }
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
impl Display for FtsFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Score => "fts_score",
            Self::Match => "fts_match",
            Self::Highlight => "fts_highlight",
        };
        write!(f, "{str}")
    }
}

#[derive(Debug, Clone, strum::EnumIter)]
pub enum AggFunc {
    Avg,
    /// COUNT(expr)
    Count,
    /// COUNT(*) or COUNT()
    Count0,
    GroupConcat,
    Max,
    Min,
    StringAgg,
    Sum,
    Total,
    #[cfg(feature = "json")]
    JsonbGroupArray,
    #[cfg(feature = "json")]
    JsonGroupArray,
    #[cfg(feature = "json")]
    JsonbGroupObject,
    #[cfg(feature = "json")]
    JsonGroupObject,
    ArrayAgg,
    /// `mode() WITHIN GROUP (ORDER BY x)` — most frequent value of `x`.
    /// Stored args (post-planning): `[value]`.
    #[strum(disabled)]
    Mode,
    /// `percentile_cont(fraction) WITHIN GROUP (ORDER BY x)` — interpolated percentile.
    /// Stored args (post-planning): `[value, fraction]`.
    #[strum(disabled)]
    PercentileCont,
    /// `percentile_disc(fraction) WITHIN GROUP (ORDER BY x)` — discrete percentile.
    /// Stored args (post-planning): `[value, fraction]`.
    #[strum(disabled)]
    PercentileDisc,
    #[strum(disabled)]
    External(Arc<ExtFunc>),
}

#[derive(Debug, Clone, strum::EnumIter)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    #[strum(disabled)]
    External(Arc<ExtFunc>),
}

impl WindowFunc {
    /// SQL name of this window function. Matches the strings used by
    /// `Display` so EXPLAIN output and error messages agree.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RowNumber => "row_number",
            Self::Rank => "rank",
            Self::DenseRank => "dense_rank",
            Self::PercentRank => "percent_rank",
            Self::CumeDist => "cume_dist",
            Self::Ntile => "ntile",
            Self::Lag => "lag",
            Self::Lead => "lead",
            Self::FirstValue => "first_value",
            Self::LastValue => "last_value",
            Self::NthValue => "nth_value",
            Self::External(_) => unreachable!(
                "WindowFunc::External is not constructible: ExtFunc has no Window variant"
            ),
        }
    }

    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::RowNumber | Self::Rank | Self::DenseRank | Self::PercentRank | Self::CumeDist => {
                &[0]
            }
            Self::Ntile | Self::FirstValue | Self::LastValue => &[1],
            Self::NthValue => &[2],
            Self::Lag | Self::Lead => &[1, 2, 3],
            Self::External(_) => unreachable!(
                "WindowFunc::External is not constructible: ExtFunc has no Window variant"
            ),
        }
    }

    /// Whether name resolution + runtime dispatch are wired up. Stub variants
    /// must not be advertised via `pragma_function_list`, or introspection
    /// drifts ahead of the resolver and users get "no such function" when
    /// they try to call them.
    pub fn is_implemented(&self) -> bool {
        matches!(
            self,
            Self::RowNumber
                | Self::Rank
                | Self::DenseRank
                | Self::FirstValue
                | Self::LastValue
                | Self::NthValue
                | Self::Lag
                | Self::Lead
                | Self::Ntile
                | Self::PercentRank
                | Self::CumeDist
        )
    }

    /// The hardcoded frame this built-in evaluates over, overriding any
    /// user-written FRAME clause.
    /// - `Some(frame)` = even if the user provides an explicit frame, it's ignored in favor of this hardcoded frame.
    /// - `None` = the function honors the user's frame, falling back to Frame::default() when user hasn't specified one.
    ///
    /// This is taken from SQLite's `sqlite3WindowUpdate` table at `window.c:699-708`.
    pub fn coerced_frame(&self) -> Option<crate::translate::plan::Frame> {
        use crate::translate::plan::{Frame, FrameBoundary};
        use turso_parser::ast::{Expr, FrameMode, Literal};
        match self {
            // Lag shares row_number's streaming frame even though its lookup
            // can point forward (negative offset): SQLite emits a row as soon
            // as the row after it is buffered, so a forward lookup past that
            // one row misses and yields the default — behavior we match by
            // using the same frame rather than caching the whole partition.
            Self::RowNumber | Self::Lag => Some(Frame {
                mode: FrameMode::Rows,
                start: FrameBoundary::UnboundedPreceding,
                end: FrameBoundary::CurrentRow,
                exclude: None,
            }),
            Self::Rank | Self::DenseRank => Some(Frame {
                mode: FrameMode::Range,
                start: FrameBoundary::UnboundedPreceding,
                end: FrameBoundary::CurrentRow,
                exclude: None,
            }),
            Self::PercentRank => Some(Frame {
                mode: FrameMode::Groups,
                start: FrameBoundary::CurrentRow,
                end: FrameBoundary::UnboundedFollowing,
                exclude: None,
            }),
            Self::CumeDist => Some(Frame {
                mode: FrameMode::Groups,
                start: FrameBoundary::Following(Box::new(Expr::Literal(Literal::Numeric(
                    "1".to_string(),
                )))),
                end: FrameBoundary::UnboundedFollowing,
                exclude: None,
            }),
            Self::Ntile => Some(Frame {
                mode: FrameMode::Rows,
                start: FrameBoundary::CurrentRow,
                end: FrameBoundary::UnboundedFollowing,
                exclude: None,
            }),
            Self::Lead => Some(Frame {
                mode: FrameMode::Rows,
                start: FrameBoundary::UnboundedPreceding,
                end: FrameBoundary::UnboundedFollowing,
                exclude: None,
            }),
            Self::FirstValue | Self::LastValue | Self::NthValue => None,
            Self::External(_) => unreachable!(
                "WindowFunc::External is not constructible: ExtFunc has no Window variant"
            ),
        }
    }
}

impl PartialEq for WindowFunc {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::RowNumber, Self::RowNumber)
            | (Self::Rank, Self::Rank)
            | (Self::DenseRank, Self::DenseRank)
            | (Self::PercentRank, Self::PercentRank)
            | (Self::CumeDist, Self::CumeDist)
            | (Self::Ntile, Self::Ntile)
            | (Self::Lag, Self::Lag)
            | (Self::Lead, Self::Lead)
            | (Self::FirstValue, Self::FirstValue)
            | (Self::LastValue, Self::LastValue)
            | (Self::NthValue, Self::NthValue) => true,
            (Self::External(a), Self::External(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for WindowFunc {}

impl Deterministic for WindowFunc {
    fn is_deterministic(&self) -> bool {
        match self {
            Self::RowNumber
            | Self::Rank
            | Self::DenseRank
            | Self::PercentRank
            | Self::CumeDist
            | Self::Ntile
            | Self::Lag
            | Self::Lead
            | Self::FirstValue
            | Self::LastValue
            | Self::NthValue => true,
            Self::External(_) => unreachable!(
                "WindowFunc::External is not constructible: ExtFunc has no Window variant"
            ),
        }
    }
}

impl std::fmt::Display for WindowFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Function reference used by AggStep / AggValue / AggFinal opcodes.
/// Aggregates used in window context and pure window functions share the same
/// step/value dispatch path; this enum carries which side of that split a
/// particular call belongs to.
#[derive(Debug, Clone)]
pub enum AccumulatorFunc {
    Agg(AggFunc),
    Window(WindowFunc),
}

impl AccumulatorFunc {
    /// Extract the inner `AggFunc` when this kind is known to be an
    /// aggregate. `unreachable!`s on `Window(...)` — the only opcodes
    /// that carry an `AccumulatorFunc` are the AggStep / AggValue /
    /// AggFinal trio, and the call sites that emit those wrap aggregates
    /// only. A `Window` value reaching here is a planner bug.
    pub fn expect_agg(&self) -> &AggFunc {
        match self {
            Self::Agg(f) => f,
            Self::Window(f) => {
                unreachable!("window function {f} reached an aggregate-only dispatch path")
            }
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agg(f) => f.as_str(),
            Self::Window(f) => f.as_str(),
        }
    }
}

impl PartialEq for AggFunc {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Avg, Self::Avg)
            | (Self::Count, Self::Count)
            | (Self::GroupConcat, Self::GroupConcat)
            | (Self::Max, Self::Max)
            | (Self::Min, Self::Min)
            | (Self::StringAgg, Self::StringAgg)
            | (Self::Sum, Self::Sum)
            | (Self::Total, Self::Total)
            | (Self::ArrayAgg, Self::ArrayAgg)
            | (Self::Mode, Self::Mode)
            | (Self::PercentileCont, Self::PercentileCont)
            | (Self::PercentileDisc, Self::PercentileDisc) => true,
            (Self::External(a), Self::External(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Deterministic for AggFunc {
    fn is_deterministic(&self) -> bool {
        false // consider aggregate functions nondeterministic since they depend on the number of rows, not only the input arguments
    }
}
impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl AggFunc {
    pub fn num_args(&self) -> usize {
        match self {
            Self::Avg => 1,
            Self::Count0 => 0,
            Self::Count => 1,
            Self::GroupConcat => 1,
            Self::Max => 1,
            Self::Min => 1,
            Self::StringAgg => 2,
            Self::Sum => 1,
            Self::Total => 1,
            Self::ArrayAgg => 1,
            // Ordered-set aggregates: args are rewritten by the planner to
            // `[value]` (mode) or `[value, fraction]` (percentiles).
            Self::Mode => 1,
            Self::PercentileCont | Self::PercentileDisc => 2,
            #[cfg(feature = "json")]
            Self::JsonGroupArray | Self::JsonbGroupArray => 1,
            #[cfg(feature = "json")]
            Self::JsonGroupObject | Self::JsonbGroupObject => 2,
            Self::External(func) => func
                .agg_args()
                .map(|argc| argc.max(0) as usize)
                .unwrap_or(0),
        }
    }

    /// Returns all valid arities for this aggregate function.
    /// Most aggregates have a single arity, but group_concat accepts 1 or 2 args.
    pub fn arities(&self) -> &'static [i32] {
        match self {
            Self::Avg => &[1],
            Self::Count0 => &[0],
            Self::Count => &[1],
            Self::GroupConcat => &[1, 2],
            Self::Max => &[1],
            Self::Min => &[1],
            Self::StringAgg => &[2],
            Self::Sum => &[1],
            Self::Total => &[1],
            Self::ArrayAgg => &[1],
            Self::Mode => &[1],
            Self::PercentileCont | Self::PercentileDisc => &[2],
            #[cfg(feature = "json")]
            Self::JsonGroupArray | Self::JsonbGroupArray => &[1],
            #[cfg(feature = "json")]
            Self::JsonGroupObject | Self::JsonbGroupObject => &[2],
            Self::External(_) => &[-1],
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Count0 => "count",
            Self::Count => "count",
            Self::GroupConcat => "group_concat",
            Self::Max => "max",
            Self::Min => "min",
            Self::StringAgg => "string_agg",
            Self::Sum => "sum",
            Self::Total => "total",
            Self::ArrayAgg => "array_agg",
            Self::Mode => "mode",
            Self::PercentileCont => "percentile_cont",
            Self::PercentileDisc => "percentile_disc",
            #[cfg(feature = "json")]
            Self::JsonbGroupArray => "jsonb_group_array",
            #[cfg(feature = "json")]
            Self::JsonGroupArray => "json_group_array",
            #[cfg(feature = "json")]
            Self::JsonbGroupObject => "jsonb_group_object",
            #[cfg(feature = "json")]
            Self::JsonGroupObject => "json_group_object",
            Self::External(_) => "extension function",
        }
    }
}

#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
pub enum ScalarFunc {
    Cast,
    Changes,
    Char,
    Coalesce,
    Concat,
    ConcatWs,
    Glob,
    IfNull,
    Iif,
    Instr,
    Like,
    Abs,
    Upper,
    Lower,
    Random,
    RandomBlob,
    Trim,
    LTrim,
    RTrim,
    Round,
    Length,
    OctetLength,
    Min,
    Max,
    Nullif,
    Sign,
    Substr,
    Substring,
    Soundex,
    Date,
    Time,
    TotalChanges,
    DateTime,
    Subtype,
    Typeof,
    Unicode,
    Unistr,
    UnistrQuote,
    Quote,
    SqliteVersion,
    TursoVersion,
    SqliteSourceId,
    UnixEpoch,
    JulianDay,
    Hex,
    Unhex,
    GetByte,
    SetByte,
    ZeroBlob,
    LastInsertRowid,
    Replace,
    #[cfg(feature = "fs")]
    #[cfg(not(target_family = "wasm"))]
    LoadExtension,
    StrfTime,
    Printf,
    Likely,
    TimeDiff,
    Likelihood,
    TableColumnsJsonArray,
    BinRecordJsonObject,
    Attach,
    Detach,
    Unlikely,
    StatInit,
    StatPush,
    StatGet,
    ConnTxnId,
    IsAutocommit,
    SequenceWatermark,
    // Test type functions (for custom type system testing)
    TestUintEncode,
    TestUintDecode,
    TestUintAdd,
    TestUintSub,
    TestUintMul,
    TestUintDiv,
    TestUintLt,
    TestUintEq,
    /// Test-only: returns a monotonically increasing 64-bit integer on every
    /// evaluation. Used to verify that the planner does not deduplicate
    /// equivalent SQL calls that contain nondeterministic functions.
    #[cfg(feature = "test_helper")]
    TestNondetCounter,
    StringReverse,
    // SQL-standard string and math extensions (PG/MySQL/Oracle compatible)
    Gcd,
    Lcm,
    Repeat,
    Lpad,
    Rpad,
    // Built-in type support functions
    BooleanToInt,
    IntToBoolean,
    ValidateIpAddr,
    // Numeric type functions
    NumericEncode,
    NumericDecode,
    NumericAdd,
    NumericSub,
    NumericMul,
    NumericDiv,
    NumericLt,
    NumericEq,
    // Array construction / element access (desugared from ARRAY[…] and expr[n] syntax)
    Array,
    ArrayElement,
    ArraySetElement,
    // Array utility functions
    ArrayLength,
    ArrayAppend,
    ArrayPrepend,
    ArrayCat,
    ArrayRemove,
    ArrayContains,
    ArrayPosition,
    ArraySlice,
    StringToArray,
    ArrayToString,
    ArrayOverlap,
    ArrayContainsAll,
    // Struct/Union construction and access
    StructPack,
    StructExtractFunc,
    UnionValueFunc,
    UnionTagFunc,
    UnionExtractFunc,
    // Sequence functions
    NextVal,
    CurrVal,
    SetVal,
}

impl Deterministic for ScalarFunc {
    fn is_deterministic(&self) -> bool {
        match self {
            ScalarFunc::Cast => true,
            ScalarFunc::Changes => false, // depends on DB state
            ScalarFunc::Char => true,
            ScalarFunc::Coalesce => true,
            ScalarFunc::Concat => true,
            ScalarFunc::ConcatWs => true,
            ScalarFunc::Glob => true,
            ScalarFunc::IfNull => true,
            ScalarFunc::Iif => true,
            ScalarFunc::Instr => true,
            ScalarFunc::Like => true,
            ScalarFunc::Abs => true,
            ScalarFunc::Upper => true,
            ScalarFunc::Lower => true,
            ScalarFunc::Random => false,     // duh
            ScalarFunc::RandomBlob => false, // duh
            ScalarFunc::Trim => true,
            ScalarFunc::LTrim => true,
            ScalarFunc::RTrim => true,
            ScalarFunc::Round => true,
            ScalarFunc::Length => true,
            ScalarFunc::OctetLength => true,
            ScalarFunc::Min => true,
            ScalarFunc::Max => true,
            ScalarFunc::Nullif => true,
            ScalarFunc::Sign => true,
            ScalarFunc::Substr => true,
            ScalarFunc::Substring => true,
            ScalarFunc::Soundex => true,
            ScalarFunc::Date => false,
            ScalarFunc::Time => false,
            ScalarFunc::TotalChanges => false,
            ScalarFunc::DateTime => false,
            ScalarFunc::Subtype => true,
            ScalarFunc::Typeof => true,
            ScalarFunc::Unicode => true,
            ScalarFunc::Unistr => true,
            ScalarFunc::UnistrQuote => true,
            ScalarFunc::Quote => true,
            ScalarFunc::SqliteVersion => false,
            ScalarFunc::TursoVersion => false,
            ScalarFunc::SqliteSourceId => false,
            ScalarFunc::UnixEpoch => false,
            ScalarFunc::JulianDay => false,
            ScalarFunc::Hex => true,
            ScalarFunc::Unhex => true,
            ScalarFunc::GetByte => true,
            ScalarFunc::SetByte => true,
            ScalarFunc::ZeroBlob => true,
            ScalarFunc::LastInsertRowid => false,
            ScalarFunc::Replace => true,
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            ScalarFunc::LoadExtension => false,
            ScalarFunc::StrfTime => false,
            ScalarFunc::Printf => true,
            ScalarFunc::Likely => true,
            ScalarFunc::TimeDiff => false,
            ScalarFunc::Likelihood => true,
            ScalarFunc::TableColumnsJsonArray => true, // while columns of the table can change with DDL statements, within single query plan it's static
            ScalarFunc::BinRecordJsonObject => true,
            ScalarFunc::Attach => false, // changes database state
            ScalarFunc::Detach => false, // changes database state
            ScalarFunc::Unlikely => true,
            ScalarFunc::StatInit => false, // internal ANALYZE function
            ScalarFunc::StatPush => false, // internal ANALYZE function
            ScalarFunc::StatGet => false,  // internal ANALYZE function
            ScalarFunc::ConnTxnId => false, // depends on connection state
            ScalarFunc::IsAutocommit => false, // depends on connection state
            ScalarFunc::SequenceWatermark => false, // depends on active MVCC transactions
            ScalarFunc::TestUintEncode
            | ScalarFunc::TestUintDecode
            | ScalarFunc::TestUintAdd
            | ScalarFunc::TestUintSub
            | ScalarFunc::TestUintMul
            | ScalarFunc::TestUintDiv
            | ScalarFunc::TestUintLt
            | ScalarFunc::TestUintEq
            | ScalarFunc::StringReverse => true,
            ScalarFunc::Gcd
            | ScalarFunc::Lcm
            | ScalarFunc::Repeat
            | ScalarFunc::Lpad
            | ScalarFunc::Rpad => true,
            #[cfg(feature = "test_helper")]
            ScalarFunc::TestNondetCounter => false,
            ScalarFunc::BooleanToInt
            | ScalarFunc::IntToBoolean
            | ScalarFunc::ValidateIpAddr
            | ScalarFunc::NumericEncode
            | ScalarFunc::NumericDecode
            | ScalarFunc::NumericAdd
            | ScalarFunc::NumericSub
            | ScalarFunc::NumericMul
            | ScalarFunc::NumericDiv
            | ScalarFunc::NumericLt
            | ScalarFunc::NumericEq => true,
            ScalarFunc::Array
            | ScalarFunc::ArrayElement
            | ScalarFunc::ArraySetElement
            | ScalarFunc::ArrayLength
            | ScalarFunc::ArrayAppend
            | ScalarFunc::ArrayPrepend
            | ScalarFunc::ArrayCat
            | ScalarFunc::ArrayRemove
            | ScalarFunc::ArrayContains
            | ScalarFunc::ArrayPosition
            | ScalarFunc::ArraySlice
            | ScalarFunc::StringToArray
            | ScalarFunc::ArrayToString
            | ScalarFunc::ArrayOverlap
            | ScalarFunc::ArrayContainsAll => true,
            ScalarFunc::StructPack
            | ScalarFunc::StructExtractFunc
            | ScalarFunc::UnionValueFunc
            | ScalarFunc::UnionTagFunc
            | ScalarFunc::UnionExtractFunc => true,
            ScalarFunc::NextVal | ScalarFunc::CurrVal | ScalarFunc::SetVal => false,
        }
    }
}

impl ScalarFunc {
    /// Returns true if this function returns a record-format array blob
    /// that needs ArrayDecode for display.
    ///
    /// FIXME: ideally every function would declare its return type via a
    /// `return_type()` method, and this whitelist would be replaced by a
    /// generic check. Postponed for now — the set of array-returning
    /// functions is small and controlled by us.
    pub fn returns_array_blob(&self) -> bool {
        matches!(
            self,
            Self::Array
                | Self::ArraySetElement
                | Self::ArrayAppend
                | Self::ArrayPrepend
                | Self::ArrayCat
                | Self::ArrayRemove
                | Self::ArraySlice
                | Self::StringToArray
        )
    }
}

impl Display for ScalarFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Cast => "cast",
            Self::Changes => "changes",
            Self::Char => "char",
            Self::Coalesce => "coalesce",
            Self::Concat => "concat",
            Self::ConcatWs => "concat_ws",
            Self::Glob => "glob",
            Self::IfNull => "ifnull",
            Self::Iif => "iif",
            Self::Instr => "instr",
            Self::Like => "like",
            Self::Abs => "abs",
            Self::Upper => "upper",
            Self::Lower => "lower",
            Self::Random => "random",
            Self::RandomBlob => "randomblob",
            Self::Trim => "trim",
            Self::LTrim => "ltrim",
            Self::RTrim => "rtrim",
            Self::Round => "round",
            Self::Length => "length",
            Self::OctetLength => "octet_length",
            Self::Min => "min",
            Self::Max => "max",
            Self::Nullif => "nullif",
            Self::Sign => "sign",
            Self::Substr => "substr",
            Self::Substring => "substring",
            Self::Soundex => "soundex",
            Self::Date => "date",
            Self::Time => "time",
            Self::TotalChanges => "total_changes",
            Self::Subtype => "subtype",
            Self::Typeof => "typeof",
            Self::Unicode => "unicode",
            Self::Unistr => "unistr",
            Self::UnistrQuote => "unistr_quote",
            Self::Quote => "quote",
            Self::SqliteVersion => "sqlite_version",
            Self::TursoVersion => "turso_version",
            Self::SqliteSourceId => "sqlite_source_id",
            Self::JulianDay => "julianday",
            Self::UnixEpoch => "unixepoch",
            Self::Hex => "hex",
            Self::Unhex => "unhex",
            Self::GetByte => "get_byte",
            Self::SetByte => "set_byte",
            Self::ZeroBlob => "zeroblob",
            Self::LastInsertRowid => "last_insert_rowid",
            Self::Replace => "replace",
            Self::DateTime => "datetime",
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            Self::LoadExtension => "load_extension",
            Self::StrfTime => "strftime",
            Self::Printf => "printf",
            Self::Likely => "likely",
            Self::TimeDiff => "timediff",
            Self::Likelihood => "likelihood",
            Self::TableColumnsJsonArray => "table_columns_json_array",
            Self::BinRecordJsonObject => "bin_record_json_object",
            Self::Attach => "attach",
            Self::Detach => "detach",
            Self::Unlikely => "unlikely",
            Self::StatInit => "stat_init",
            Self::StatPush => "stat_push",
            Self::StatGet => "stat_get",
            Self::ConnTxnId => "conn_txn_id",
            Self::IsAutocommit => "is_autocommit",
            Self::SequenceWatermark => "sequence_watermark_experimental",
            Self::TestUintEncode => "test_uint_encode",
            Self::TestUintDecode => "test_uint_decode",
            Self::TestUintAdd => "test_uint_add",
            Self::TestUintSub => "test_uint_sub",
            Self::TestUintMul => "test_uint_mul",
            Self::TestUintDiv => "test_uint_div",
            Self::TestUintLt => "test_uint_lt",
            Self::TestUintEq => "test_uint_eq",
            #[cfg(feature = "test_helper")]
            Self::TestNondetCounter => "test_nondet_counter",
            Self::StringReverse => "string_reverse",
            Self::Gcd => "gcd",
            Self::Lcm => "lcm",
            Self::Repeat => "repeat",
            Self::Lpad => "lpad",
            Self::Rpad => "rpad",
            Self::BooleanToInt => "boolean_to_int",
            Self::IntToBoolean => "int_to_boolean",
            Self::ValidateIpAddr => "validate_ipaddr",
            Self::NumericEncode => "numeric_encode",
            Self::NumericDecode => "numeric_decode",
            Self::NumericAdd => "numeric_add",
            Self::NumericSub => "numeric_sub",
            Self::NumericMul => "numeric_mul",
            Self::NumericDiv => "numeric_div",
            Self::NumericLt => "numeric_lt",
            Self::NumericEq => "numeric_eq",
            Self::Array => "array",
            Self::ArrayElement => "array_element",
            Self::ArraySetElement => "array_set_element",
            Self::ArrayLength => "array_length",
            Self::ArrayAppend => "array_append",
            Self::ArrayPrepend => "array_prepend",
            Self::ArrayCat => "array_cat",
            Self::ArrayRemove => "array_remove",
            Self::ArrayContains => "array_contains",
            Self::ArrayPosition => "array_position",
            Self::ArraySlice => "array_slice",
            Self::StringToArray => "string_to_array",
            Self::ArrayToString => "array_to_string",
            Self::ArrayOverlap => "array_overlap",
            Self::ArrayContainsAll => "array_contains_all",
            Self::StructPack => "struct_pack",
            Self::StructExtractFunc => "struct_extract",
            Self::UnionValueFunc => "union_value",
            Self::UnionTagFunc => "union_tag",
            Self::UnionExtractFunc => "union_extract",
            Self::NextVal => "nextval",
            Self::CurrVal => "currval",
            Self::SetVal => "setval",
        };
        write!(f, "{str}")
    }
}

impl ScalarFunc {
    /// Returns true for internal functions that should not appear in PRAGMA function_list.
    pub fn is_internal(&self) -> bool {
        matches!(
            self,
            Self::Cast
                | Self::Array
                | Self::ArrayElement
                | Self::ArraySetElement
                | Self::StatInit
                | Self::StatPush
                | Self::StatGet
                | Self::Attach
                | Self::Detach
                | Self::TableColumnsJsonArray
                | Self::BinRecordJsonObject
                | Self::ConnTxnId
                | Self::IsAutocommit
        )
    }

    /// Returns the valid arities for this function.
    /// Each value becomes a separate row in PRAGMA function_list.
    /// -1 means truly variable arguments (e.g. coalesce, printf).
    pub fn arities(&self) -> &'static [i32] {
        match self {
            // 0-arg
            Self::Changes
            | Self::LastInsertRowid
            | Self::Random
            | Self::SqliteVersion
            | Self::TursoVersion
            | Self::SqliteSourceId
            | Self::TotalChanges => &[0],
            #[cfg(feature = "test_helper")]
            Self::TestNondetCounter => &[0],
            // 1-arg
            Self::Abs
            | Self::Hex
            | Self::Length
            | Self::Lower
            | Self::OctetLength
            | Self::Quote
            | Self::UnistrQuote
            | Self::RandomBlob
            | Self::Sign
            | Self::Soundex
            | Self::Subtype
            | Self::Typeof
            | Self::Unicode
            | Self::Unistr
            | Self::Upper
            | Self::ZeroBlob
            | Self::Likely
            | Self::Unlikely
            | Self::SequenceWatermark => &[1],
            // 2-arg
            Self::Glob
            | Self::Instr
            | Self::Nullif
            | Self::IfNull
            | Self::Likelihood
            | Self::GetByte
            | Self::TimeDiff => &[2],
            // 3-arg
            Self::Iif | Self::Replace | Self::SetByte => &[3],
            // Multi-arity (one row per valid arity)
            Self::Like => &[2, 3],
            Self::Trim | Self::LTrim | Self::RTrim | Self::Round | Self::Unhex => &[1, 2],
            Self::Substr | Self::Substring => &[2, 3],
            // Truly variable-arg
            Self::Char
            | Self::Coalesce
            | Self::Concat
            | Self::ConcatWs
            | Self::Date
            | Self::Time
            | Self::DateTime
            | Self::UnixEpoch
            | Self::JulianDay
            | Self::StrfTime
            | Self::Printf => &[-1],
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            Self::LoadExtension => &[-1],
            // Internal functions — arity doesn't matter since they're filtered out
            Self::Cast
            | Self::StatInit
            | Self::StatPush
            | Self::StatGet
            | Self::Attach
            | Self::Detach
            | Self::TableColumnsJsonArray
            | Self::BinRecordJsonObject
            | Self::ConnTxnId
            | Self::IsAutocommit => &[0],
            // Scalar max/min (multi-arg)
            Self::Max | Self::Min => &[-1],
            // SQL-standard string and math extensions
            Self::Gcd | Self::Lcm | Self::Repeat => &[2],
            Self::Lpad | Self::Rpad => &[2, 3],
            // Test functions for custom types (1-arg encode/decode, 2-arg operators)
            Self::TestUintEncode | Self::TestUintDecode | Self::StringReverse => &[1],
            Self::TestUintAdd
            | Self::TestUintSub
            | Self::TestUintMul
            | Self::TestUintDiv
            | Self::TestUintLt
            | Self::TestUintEq => &[2],
            // Built-in type functions
            Self::BooleanToInt
            | Self::IntToBoolean
            | Self::ValidateIpAddr
            | Self::NumericDecode => &[1],
            Self::NumericAdd
            | Self::NumericSub
            | Self::NumericMul
            | Self::NumericDiv
            | Self::NumericLt
            | Self::NumericEq => &[2],
            Self::NumericEncode => &[3],
            // Array construction / element access
            Self::Array => &[-1], // variable arity
            Self::ArrayElement => &[2],
            Self::ArraySetElement => &[3],
            // Array functions
            Self::ArrayLength => &[1, 2],
            Self::ArrayAppend
            | Self::ArrayPrepend
            | Self::ArrayCat
            | Self::ArrayRemove
            | Self::ArrayContains
            | Self::ArrayPosition
            | Self::ArrayOverlap
            | Self::ArrayContainsAll => &[2],
            Self::ArraySlice => &[3],
            Self::StringToArray => &[2, 3],
            Self::ArrayToString => &[2, 3],
            // Struct/Union functions
            // struct_pack is intentionally variable-arity: field count validation
            // happens at INSERT time when the value is stored into a typed column.
            // Standalone calls produce a generic record blob.
            Self::StructPack => &[-1],
            Self::StructExtractFunc => &[2], // struct_extract(col, 'field')
            Self::UnionValueFunc => &[2],    // union_value('tag', value)
            Self::UnionTagFunc => &[1],      // union_tag(col)
            Self::UnionExtractFunc => &[2],  // union_extract(col, 'tag')
            // Sequence functions
            Self::NextVal | Self::CurrVal => &[1],
            Self::SetVal => &[2, 3],
        }
    }

    /// Returns true for functions that can turn NULL arguments into a non-NULL result.
    ///
    /// This is used by planner/optimizer logic that needs to reason about whether
    /// predicates are null-rejecting for outer-join simplification.
    pub fn can_mask_nulls(&self) -> bool {
        matches!(self, Self::Coalesce | Self::IfNull)
    }
}

#[derive(Debug, Clone, PartialEq, strum::EnumIter)]
pub enum MathFunc {
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Ceil,
    Ceiling,
    Cos,
    Cosh,
    Degrees,
    Exp,
    Floor,
    Ln,
    Log,
    Log10,
    Log2,
    Mod,
    Pi,
    Pow,
    Power,
    Radians,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
    Trunc,
}

pub enum MathFuncArity {
    Nullary,
    Unary,
    Binary,
    UnaryOrBinary,
}

impl Deterministic for MathFunc {
    fn is_deterministic(&self) -> bool {
        true
    }
}

impl MathFunc {
    pub fn arity(&self) -> MathFuncArity {
        match self {
            Self::Pi => MathFuncArity::Nullary,
            Self::Acos
            | Self::Acosh
            | Self::Asin
            | Self::Asinh
            | Self::Atan
            | Self::Atanh
            | Self::Ceil
            | Self::Ceiling
            | Self::Cos
            | Self::Cosh
            | Self::Degrees
            | Self::Exp
            | Self::Floor
            | Self::Ln
            | Self::Log10
            | Self::Log2
            | Self::Radians
            | Self::Sin
            | Self::Sinh
            | Self::Sqrt
            | Self::Tan
            | Self::Tanh
            | Self::Trunc => MathFuncArity::Unary,

            Self::Atan2 | Self::Mod | Self::Pow | Self::Power => MathFuncArity::Binary,

            Self::Log => MathFuncArity::UnaryOrBinary,
        }
    }

    pub fn arities(&self) -> &'static [i32] {
        match self.arity() {
            MathFuncArity::Nullary => &[0],
            MathFuncArity::Unary => &[1],
            MathFuncArity::Binary => &[2],
            MathFuncArity::UnaryOrBinary => &[1, 2],
        }
    }
}

impl Display for MathFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Acos => "acos",
            Self::Acosh => "acosh",
            Self::Asin => "asin",
            Self::Asinh => "asinh",
            Self::Atan => "atan",
            Self::Atan2 => "atan2",
            Self::Atanh => "atanh",
            Self::Ceil => "ceil",
            Self::Ceiling => "ceiling",
            Self::Cos => "cos",
            Self::Cosh => "cosh",
            Self::Degrees => "degrees",
            Self::Exp => "exp",
            Self::Floor => "floor",
            Self::Ln => "ln",
            Self::Log => "log",
            Self::Log10 => "log10",
            Self::Log2 => "log2",
            Self::Mod => "mod",
            Self::Pi => "pi",
            Self::Pow => "pow",
            Self::Power => "power",
            Self::Radians => "radians",
            Self::Sin => "sin",
            Self::Sinh => "sinh",
            Self::Sqrt => "sqrt",
            Self::Tan => "tan",
            Self::Tanh => "tanh",
            Self::Trunc => "trunc",
        };
        write!(f, "{str}")
    }
}

#[derive(Debug, Clone)]
pub enum AlterTableFunc {
    RenameTable,
    AlterColumn,
    RenameColumn,
}

impl Display for AlterTableFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterTableFunc::RenameTable => write!(f, "limbo_rename_table"),
            AlterTableFunc::RenameColumn => write!(f, "limbo_rename_column"),
            AlterTableFunc::AlterColumn => write!(f, "limbo_alter_column"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Func {
    Agg(AggFunc),
    Window(WindowFunc),
    Scalar(ScalarFunc),
    Math(MathFunc),
    Vector(VectorFunc),
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    Fts(FtsFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
    AlterTable(AlterTableFunc),
    External(Arc<ExternalFunc>),
    /// Scalar function provided by the database's schema dialect (e.g. a
    /// PostgreSQL catalog function). Resolved and executed through
    /// [`crate::dialect::Dialect`]; the engine only carries the name.
    Dialect(String),
}

impl Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg(agg_func) => write!(f, "{}", agg_func.as_str()),
            Self::Window(window_func) => write!(f, "{window_func}"),
            Self::Scalar(scalar_func) => write!(f, "{scalar_func}"),
            Self::Math(math_func) => write!(f, "{math_func}"),
            Self::Vector(vector_func) => write!(f, "{vector_func}"),
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            Self::Fts(fts_func) => write!(f, "{fts_func}"),
            #[cfg(feature = "json")]
            Self::Json(json_func) => write!(f, "{json_func}"),
            Self::External(generic_func) => write!(f, "{generic_func}"),
            Self::AlterTable(alter_func) => write!(f, "{alter_func}"),
            Self::Dialect(name) => write!(f, "{name}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FuncCtx {
    pub func: Func,
    pub arg_count: usize,
}

impl Deterministic for Func {
    fn is_deterministic(&self) -> bool {
        match self {
            Self::Agg(agg_func) => agg_func.is_deterministic(),
            Self::Window(window_func) => window_func.is_deterministic(),
            Self::Scalar(scalar_func) => scalar_func.is_deterministic(),
            Self::Math(math_func) => math_func.is_deterministic(),
            Self::Vector(vector_func) => vector_func.is_deterministic(),
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            Self::Fts(fts_func) => fts_func.is_deterministic(),
            #[cfg(feature = "json")]
            Self::Json(json_func) => json_func.is_deterministic(),
            Self::External(external_func) => external_func.is_deterministic(),
            Self::AlterTable(_) => true,
            // Dialect scalars are catalog readers (stable within a
            // statement); a dialect that adds a nondeterministic function
            // should register it as an extension function instead.
            Self::Dialect(_) => true,
        }
    }
}

impl Func {
    pub fn supports_star_syntax(&self) -> bool {
        // Functions that need star expansion also support star syntax
        if self.needs_star_expansion() {
            return true;
        }
        match self {
            Self::Scalar(scalar_func) => {
                let basic = matches!(
                    scalar_func,
                    ScalarFunc::Changes
                        | ScalarFunc::Random
                        | ScalarFunc::TotalChanges
                        | ScalarFunc::SqliteVersion
                        | ScalarFunc::TursoVersion
                        | ScalarFunc::SqliteSourceId
                        | ScalarFunc::LastInsertRowid
                );
                #[cfg(feature = "test_helper")]
                let basic = basic || matches!(scalar_func, ScalarFunc::TestNondetCounter);
                basic
            }
            Self::Math(math_func) => {
                matches!(math_func.arity(), MathFuncArity::Nullary)
            }
            // Aggregate functions with (*) syntax are handled separately in the planner
            Self::Agg(_) => false,
            Self::Window(_) => false,
            _ => false,
        }
    }

    /// Returns true for functions that can turn NULL arguments into a non-NULL result.
    ///
    /// This metadata is currently used by optimizer null-rejection analysis.
    pub fn can_mask_nulls(&self) -> bool {
        match self {
            Self::Scalar(scalar_func) => scalar_func.can_mask_nulls(),
            _ => false,
        }
    }

    /// Returns true if the function needs the `*` to be expanded to all columns
    /// from the referenced tables. This is used for functions like `json_object(*)`
    /// and `jsonb_object(*)` which create a JSON object with column names as keys
    /// and column values as values.
    #[cfg(feature = "json")]
    pub fn needs_star_expansion(&self) -> bool {
        matches!(
            self,
            Self::Json(JsonFunc::JsonObject) | Self::Json(JsonFunc::JsonbObject)
        )
    }

    #[cfg(not(feature = "json"))]
    pub fn needs_star_expansion(&self) -> bool {
        false
    }
    /// Resolve a built-in function name. Thin wrapper over
    /// [`crate::dialect::sqlite::resolve_builtin_function`], where the
    /// SQLite name table lives; kept on `Func` for the engine call sites
    /// that classify translated AST.
    pub fn resolve_function(name: &str, arg_count: usize) -> Result<Option<Self>, LimboError> {
        crate::dialect::sqlite::resolve_builtin_function(name, arg_count)
    }

    /// Returns a list of all built-in functions for PRAGMA function_list.
    /// Derives the list from enum iteration so it stays in sync automatically.
    /// Functions with multiple valid arities get one row per arity.
    pub fn builtin_function_list() -> Vec<FunctionListEntry> {
        let mut funcs = Vec::new();

        // Helper: push one entry per arity for a function
        let mut push = |name: String, func_type: &'static str, arities: &[i32], det: bool| {
            for &narg in arities {
                funcs.push(FunctionListEntry {
                    name: name.clone(),
                    func_type,
                    narg,
                    deterministic: det,
                });
            }
        };

        // Scalar functions (filter out internal-only variants)
        for f in ScalarFunc::iter() {
            if f.is_internal() {
                continue;
            }
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // Aggregate functions (External is #[strum(disabled)], skipped automatically).
        // SQLite reports built-in aggregates as "w" (window-capable) since they
        // can all be used with OVER clauses.
        for f in AggFunc::iter() {
            push(f.to_string(), "w", f.arities(), f.is_deterministic());
        }

        // Window functions (skip stub variants until they're wired up).
        for f in WindowFunc::iter() {
            if !f.is_implemented() {
                continue;
            }
            push(f.to_string(), "w", f.arities(), f.is_deterministic());
        }

        // Math functions (all scalar)
        for f in MathFunc::iter() {
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // Vector functions (all scalar)
        for f in VectorFunc::iter() {
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // JSON functions (feature-gated, filter out operator-style entries)
        #[cfg(feature = "json")]
        for f in JsonFunc::iter() {
            if f.is_internal() {
                continue;
            }
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // FTS functions (feature-gated)
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        for f in FtsFunc::iter() {
            push(f.to_string(), "s", f.arities(), f.is_deterministic());
        }

        // Aliases: functions callable under multiple names.
        // These are additional names that resolve_function() accepts
        // but that map to existing enum variants.
        funcs.push(FunctionListEntry {
            name: "format".into(),
            func_type: "s",
            narg: -1,
            deterministic: true,
        });
        funcs.push(FunctionListEntry {
            name: "if".into(),
            func_type: "s",
            narg: 3,
            deterministic: true,
        });

        funcs
    }
}

pub struct FunctionListEntry {
    pub name: String,
    pub func_type: &'static str, // "s" = scalar, "a" = aggregate, "w" = window
    pub narg: i32,               // -1 = variable
    pub deterministic: bool,
}
