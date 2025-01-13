use crate::ext::ExtFunc;
use std::fmt;
use std::fmt::{Debug, Display};
use std::rc::Rc;

pub struct ExternalFunc {
    pub name: String,
    pub func: Box<dyn Fn(&[crate::types::Value]) -> crate::Result<crate::types::OwnedValue>>,
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
#[derive(Debug, Clone, PartialEq)]
pub enum JsonFunc {
    Json,
    JsonArray,
    JsonArrayLength,
    JsonArrowExtract,
    JsonArrowShiftExtract,
    JsonExtract,
    JsonType,
    JsonErrorPosition,
}

#[cfg(feature = "json")]
impl Display for JsonFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Json => "json".to_string(),
                Self::JsonArray => "json_array".to_string(),
                Self::JsonExtract => "json_extract".to_string(),
                Self::JsonArrayLength => "json_array_length".to_string(),
                Self::JsonArrowExtract => "->".to_string(),
                Self::JsonArrowShiftExtract => "->>".to_string(),
                Self::JsonType => "json_type".to_string(),
                Self::JsonErrorPosition => "json_error_position".to_string(),
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Avg,
    Count,
    GroupConcat,
    Max,
    Min,
    StringAgg,
    Sum,
    Total,
}

impl AggFunc {
    pub fn to_string(&self) -> &str {
        match self {
            Self::Avg => "avg",
            Self::Count => "count",
            Self::GroupConcat => "group_concat",
            Self::Max => "max",
            Self::Min => "min",
            Self::StringAgg => "string_agg",
            Self::Sum => "sum",
            Self::Total => "total",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
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
    Typeof,
    Unicode,
    Quote,
    SqliteVersion,
    UnixEpoch,
    JulianDay,
    Hex,
    Unhex,
    ZeroBlob,
    LastInsertRowid,
    Replace,
}

impl Display for ScalarFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Cast => "cast".to_string(),
            Self::Changes => "changes".to_string(),
            Self::Char => "char".to_string(),
            Self::Coalesce => "coalesce".to_string(),
            Self::Concat => "concat".to_string(),
            Self::ConcatWs => "concat_ws".to_string(),
            Self::Glob => "glob".to_string(),
            Self::IfNull => "ifnull".to_string(),
            Self::Iif => "iif".to_string(),
            Self::Instr => "instr".to_string(),
            Self::Like => "like(2)".to_string(),
            Self::Abs => "abs".to_string(),
            Self::Upper => "upper".to_string(),
            Self::Lower => "lower".to_string(),
            Self::Random => "random".to_string(),
            Self::RandomBlob => "randomblob".to_string(),
            Self::Trim => "trim".to_string(),
            Self::LTrim => "ltrim".to_string(),
            Self::RTrim => "rtrim".to_string(),
            Self::Round => "round".to_string(),
            Self::Length => "length".to_string(),
            Self::OctetLength => "octet_length".to_string(),
            Self::Min => "min".to_string(),
            Self::Max => "max".to_string(),
            Self::Nullif => "nullif".to_string(),
            Self::Sign => "sign".to_string(),
            Self::Substr => "substr".to_string(),
            Self::Substring => "substring".to_string(),
            Self::Soundex => "soundex".to_string(),
            Self::Date => "date".to_string(),
            Self::Time => "time".to_string(),
            Self::TotalChanges => "total_changes".to_string(),
            Self::Typeof => "typeof".to_string(),
            Self::Unicode => "unicode".to_string(),
            Self::Quote => "quote".to_string(),
            Self::SqliteVersion => "sqlite_version".to_string(),
            Self::JulianDay => "julianday".to_string(),
            Self::UnixEpoch => "unixepoch".to_string(),
            Self::Hex => "hex".to_string(),
            Self::Unhex => "unhex".to_string(),
            Self::ZeroBlob => "zeroblob".to_string(),
            Self::LastInsertRowid => "last_insert_rowid".to_string(),
            Self::Replace => "replace".to_string(),
            Self::DateTime => "datetime".to_string(),
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
}

impl Display for MathFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Acos => "acos".to_string(),
            Self::Acosh => "acosh".to_string(),
            Self::Asin => "asin".to_string(),
            Self::Asinh => "asinh".to_string(),
            Self::Atan => "atan".to_string(),
            Self::Atan2 => "atan2".to_string(),
            Self::Atanh => "atanh".to_string(),
            Self::Ceil => "ceil".to_string(),
            Self::Ceiling => "ceiling".to_string(),
            Self::Cos => "cos".to_string(),
            Self::Cosh => "cosh".to_string(),
            Self::Degrees => "degrees".to_string(),
            Self::Exp => "exp".to_string(),
            Self::Floor => "floor".to_string(),
            Self::Ln => "ln".to_string(),
            Self::Log => "log".to_string(),
            Self::Log10 => "log10".to_string(),
            Self::Log2 => "log2".to_string(),
            Self::Mod => "mod".to_string(),
            Self::Pi => "pi".to_string(),
            Self::Pow => "pow".to_string(),
            Self::Power => "power".to_string(),
            Self::Radians => "radians".to_string(),
            Self::Sin => "sin".to_string(),
            Self::Sinh => "sinh".to_string(),
            Self::Sqrt => "sqrt".to_string(),
            Self::Tan => "tan".to_string(),
            Self::Tanh => "tanh".to_string(),
            Self::Trunc => "trunc".to_string(),
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug)]
pub enum Func {
    Agg(AggFunc),
    Scalar(ScalarFunc),
    Math(MathFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
    Extension(ExtFunc),
    External(Rc<ExternalFunc>),
}

impl Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg(agg_func) => write!(f, "{}", agg_func.to_string()),
            Self::Scalar(scalar_func) => write!(f, "{}", scalar_func),
            Self::Math(math_func) => write!(f, "{}", math_func),
            #[cfg(feature = "json")]
            Self::Json(json_func) => write!(f, "{}", json_func),
            Self::Extension(ext_func) => write!(f, "{}", ext_func),
            Self::External(generic_func) => write!(f, "{}", generic_func),
        }
    }
}

#[derive(Debug)]
pub struct FuncCtx {
    pub func: Func,
    pub arg_count: usize,
}

impl Func {
    pub fn resolve_function(name: &str, arg_count: usize) -> Result<Self, ()> {
        match name {
            "avg" => Ok(Self::Agg(AggFunc::Avg)),
            "count" => Ok(Self::Agg(AggFunc::Count)),
            "group_concat" => Ok(Self::Agg(AggFunc::GroupConcat)),
            "max" if arg_count == 0 || arg_count == 1 => Ok(Self::Agg(AggFunc::Max)),
            "max" if arg_count > 1 => Ok(Self::Scalar(ScalarFunc::Max)),
            "min" if arg_count == 0 || arg_count == 1 => Ok(Self::Agg(AggFunc::Min)),
            "min" if arg_count > 1 => Ok(Self::Scalar(ScalarFunc::Min)),
            "nullif" if arg_count == 2 => Ok(Self::Scalar(ScalarFunc::Nullif)),
            "string_agg" => Ok(Self::Agg(AggFunc::StringAgg)),
            "sum" => Ok(Self::Agg(AggFunc::Sum)),
            "total" => Ok(Self::Agg(AggFunc::Total)),
            "char" => Ok(Self::Scalar(ScalarFunc::Char)),
            "coalesce" => Ok(Self::Scalar(ScalarFunc::Coalesce)),
            "concat" => Ok(Self::Scalar(ScalarFunc::Concat)),
            "concat_ws" => Ok(Self::Scalar(ScalarFunc::ConcatWs)),
            "changes" => Ok(Self::Scalar(ScalarFunc::Changes)),
            "total_changes" => Ok(Self::Scalar(ScalarFunc::TotalChanges)),
            "glob" => Ok(Self::Scalar(ScalarFunc::Glob)),
            "ifnull" => Ok(Self::Scalar(ScalarFunc::IfNull)),
            "iif" => Ok(Self::Scalar(ScalarFunc::Iif)),
            "instr" => Ok(Self::Scalar(ScalarFunc::Instr)),
            "like" => Ok(Self::Scalar(ScalarFunc::Like)),
            "abs" => Ok(Self::Scalar(ScalarFunc::Abs)),
            "upper" => Ok(Self::Scalar(ScalarFunc::Upper)),
            "lower" => Ok(Self::Scalar(ScalarFunc::Lower)),
            "random" => Ok(Self::Scalar(ScalarFunc::Random)),
            "randomblob" => Ok(Self::Scalar(ScalarFunc::RandomBlob)),
            "trim" => Ok(Self::Scalar(ScalarFunc::Trim)),
            "ltrim" => Ok(Self::Scalar(ScalarFunc::LTrim)),
            "rtrim" => Ok(Self::Scalar(ScalarFunc::RTrim)),
            "round" => Ok(Self::Scalar(ScalarFunc::Round)),
            "length" => Ok(Self::Scalar(ScalarFunc::Length)),
            "octet_length" => Ok(Self::Scalar(ScalarFunc::OctetLength)),
            "sign" => Ok(Self::Scalar(ScalarFunc::Sign)),
            "substr" => Ok(Self::Scalar(ScalarFunc::Substr)),
            "substring" => Ok(Self::Scalar(ScalarFunc::Substring)),
            "date" => Ok(Self::Scalar(ScalarFunc::Date)),
            "time" => Ok(Self::Scalar(ScalarFunc::Time)),
            "datetime" => Ok(Self::Scalar(ScalarFunc::DateTime)),
            "typeof" => Ok(Self::Scalar(ScalarFunc::Typeof)),
            "last_insert_rowid" => Ok(Self::Scalar(ScalarFunc::LastInsertRowid)),
            "unicode" => Ok(Self::Scalar(ScalarFunc::Unicode)),
            "quote" => Ok(Self::Scalar(ScalarFunc::Quote)),
            "sqlite_version" => Ok(Self::Scalar(ScalarFunc::SqliteVersion)),
            "replace" => Ok(Self::Scalar(ScalarFunc::Replace)),
            #[cfg(feature = "json")]
            "json" => Ok(Self::Json(JsonFunc::Json)),
            #[cfg(feature = "json")]
            "json_array_length" => Ok(Self::Json(JsonFunc::JsonArrayLength)),
            #[cfg(feature = "json")]
            "json_array" => Ok(Self::Json(JsonFunc::JsonArray)),
            #[cfg(feature = "json")]
            "json_extract" => Ok(Func::Json(JsonFunc::JsonExtract)),
            #[cfg(feature = "json")]
            "json_type" => Ok(Func::Json(JsonFunc::JsonType)),
            #[cfg(feature = "json")]
            "json_error_position" => Ok(Self::Json(JsonFunc::JsonErrorPosition)),
            "unixepoch" => Ok(Self::Scalar(ScalarFunc::UnixEpoch)),
            "julianday" => Ok(Self::Scalar(ScalarFunc::JulianDay)),
            "hex" => Ok(Self::Scalar(ScalarFunc::Hex)),
            "unhex" => Ok(Self::Scalar(ScalarFunc::Unhex)),
            "zeroblob" => Ok(Self::Scalar(ScalarFunc::ZeroBlob)),
            "soundex" => Ok(Self::Scalar(ScalarFunc::Soundex)),
            "acos" => Ok(Self::Math(MathFunc::Acos)),
            "acosh" => Ok(Self::Math(MathFunc::Acosh)),
            "asin" => Ok(Self::Math(MathFunc::Asin)),
            "asinh" => Ok(Self::Math(MathFunc::Asinh)),
            "atan" => Ok(Self::Math(MathFunc::Atan)),
            "atan2" => Ok(Self::Math(MathFunc::Atan2)),
            "atanh" => Ok(Self::Math(MathFunc::Atanh)),
            "ceil" => Ok(Self::Math(MathFunc::Ceil)),
            "ceiling" => Ok(Self::Math(MathFunc::Ceiling)),
            "cos" => Ok(Self::Math(MathFunc::Cos)),
            "cosh" => Ok(Self::Math(MathFunc::Cosh)),
            "degrees" => Ok(Self::Math(MathFunc::Degrees)),
            "exp" => Ok(Self::Math(MathFunc::Exp)),
            "floor" => Ok(Self::Math(MathFunc::Floor)),
            "ln" => Ok(Self::Math(MathFunc::Ln)),
            "log" => Ok(Self::Math(MathFunc::Log)),
            "log10" => Ok(Self::Math(MathFunc::Log10)),
            "log2" => Ok(Self::Math(MathFunc::Log2)),
            "mod" => Ok(Self::Math(MathFunc::Mod)),
            "pi" => Ok(Self::Math(MathFunc::Pi)),
            "pow" => Ok(Self::Math(MathFunc::Pow)),
            "power" => Ok(Self::Math(MathFunc::Power)),
            "radians" => Ok(Self::Math(MathFunc::Radians)),
            "sin" => Ok(Self::Math(MathFunc::Sin)),
            "sinh" => Ok(Self::Math(MathFunc::Sinh)),
            "sqrt" => Ok(Self::Math(MathFunc::Sqrt)),
            "tan" => Ok(Self::Math(MathFunc::Tan)),
            "tanh" => Ok(Self::Math(MathFunc::Tanh)),
            "trunc" => Ok(Self::Math(MathFunc::Trunc)),
            _ => match ExtFunc::resolve_function(name, arg_count) {
                Some(ext_func) => Ok(Self::Extension(ext_func)),
                None => Err(()),
            },
        }
    }
}
