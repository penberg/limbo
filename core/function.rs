use std::fmt;
use std::fmt::Display;

#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq)]
pub enum JsonFunc {
    Json,
}

#[cfg(feature = "json")]
impl Display for JsonFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Json => "json".to_string(),
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
    Char,
    Coalesce,
    Concat,
    ConcatWs,
    Glob,
    IfNull,
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
    Min,
    Max,
    Nullif,
    Sign,
    Substr,
    Substring,
    Date,
    Time,
    Typeof,
    Unicode,
    Quote,
    SqliteVersion,
    UnixEpoch,
    Hex,
    Unhex,
    ZeroBlob,
}

impl Display for ScalarFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Cast => "cast",
            Self::Char => "char.",
            Self::Coalesce => "coalesce",
            Self::Concat => "concat",
            Self::ConcatWs => "concat_ws",
            Self::Glob => "glob",
            Self::IfNull => "ifnull",
            Self::Instr => "instr",
            Self::Like => "like(2)",
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
            Self::Min => "min",
            Self::Max => "max",
            Self::Nullif => "nullif",
            Self::Sign => "sign",
            Self::Substr => "substr",
            Self::Substring => "substring",
            Self::Date => "date",
            Self::Time => "time",
            Self::Typeof => "typeof",
            Self::Unicode => "unicode",
            Self::Quote => "quote",
            Self::SqliteVersion => "sqlite_version",
            Self::UnixEpoch => "unixepoch",
            Self::Hex => "hex",
            Self::Unhex => "unhex",
            Self::ZeroBlob => "zeroblob",
        };
        write!(f, "{}", str.to_string())
    }
}

#[derive(Debug)]
pub enum Func {
    Agg(AggFunc),
    Scalar(ScalarFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
}

impl Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Agg(agg_func) => write!(f, "{}", agg_func.to_string()),
            Self::Scalar(scalar_func) => write!(f, "{}", scalar_func),
            #[cfg(feature = "json")]
            Self::Json(json_func) => write!(f, "{}", json_func),
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
        Ok(match name {
            "avg" => Self::Agg(AggFunc::Avg),
            "count" => Self::Agg(AggFunc::Count),
            "group_concat" => Self::Agg(AggFunc::GroupConcat),
            "max" if arg_count == 0 || arg_count == 1 => Self::Agg(AggFunc::Max),
            "max" if arg_count > 1 => Self::Scalar(ScalarFunc::Max),
            "min" if arg_count == 0 || arg_count == 1 => Self::Agg(AggFunc::Min),
            "min" if arg_count > 1 => Self::Scalar(ScalarFunc::Min),
            "nullif" if arg_count == 2 => Self::Scalar(ScalarFunc::Nullif),
            "string_agg" => Self::Agg(AggFunc::StringAgg),
            "sum" => Self::Agg(AggFunc::Sum),
            "total" => Self::Agg(AggFunc::Total),
            "char" => Self::Scalar(ScalarFunc::Char),
            "coalesce" => Self::Scalar(ScalarFunc::Coalesce),
            "concat" => Self::Scalar(ScalarFunc::Concat),
            "concat_ws" => Self::Scalar(ScalarFunc::ConcatWs),
            "glob" => Self::Scalar(ScalarFunc::Glob),
            "ifnull" => Self::Scalar(ScalarFunc::IfNull),
            "instr" => Self::Scalar(ScalarFunc::Instr),
            "like" => Self::Scalar(ScalarFunc::Like),
            "abs" => Self::Scalar(ScalarFunc::Abs),
            "upper" => Self::Scalar(ScalarFunc::Upper),
            "lower" => Self::Scalar(ScalarFunc::Lower),
            "random" => Self::Scalar(ScalarFunc::Random),
            "randomblob" => Self::Scalar(ScalarFunc::RandomBlob),
            "trim" => Self::Scalar(ScalarFunc::Trim),
            "ltrim" => Self::Scalar(ScalarFunc::LTrim),
            "rtrim" => Self::Scalar(ScalarFunc::RTrim),
            "round" => Self::Scalar(ScalarFunc::Round),
            "length" => Self::Scalar(ScalarFunc::Length),
            "sign" => Self::Scalar(ScalarFunc::Sign),
            "substr" => Self::Scalar(ScalarFunc::Substr),
            "substring" => Self::Scalar(ScalarFunc::Substring),
            "date" => Self::Scalar(ScalarFunc::Date),
            "time" => Self::Scalar(ScalarFunc::Time),
            "typeof" => Self::Scalar(ScalarFunc::Typeof),
            "unicode" => Self::Scalar(ScalarFunc::Unicode),
            "quote" => Self::Scalar(ScalarFunc::Quote),
            "sqlite_version" => Self::Scalar(ScalarFunc::SqliteVersion),
            #[cfg(feature = "json")]
            "json" => Self::Json(JsonFunc::Json),
            "unixepoch" => Self::Scalar(ScalarFunc::UnixEpoch),
            "hex" => Self::Scalar(ScalarFunc::Hex),
            "unhex" => Self::Scalar(ScalarFunc::Unhex),
            "zeroblob" => Self::Scalar(ScalarFunc::ZeroBlob),
            _ => return Err(()),
        })
    }
}
