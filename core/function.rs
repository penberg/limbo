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
                JsonFunc::Json => "json".to_string(),
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
            AggFunc::Avg => "avg",
            AggFunc::Count => "count",
            AggFunc::GroupConcat => "group_concat",
            AggFunc::Max => "max",
            AggFunc::Min => "min",
            AggFunc::StringAgg => "string_agg",
            AggFunc::Sum => "sum",
            AggFunc::Total => "total",
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
    Typeof,
    Unicode,
    Quote,
    SqliteVersion,
    UnixEpoch,
    Hex,
    Unhex,
    ZeroBlob,
    LastInsertRowid,
    Replace,
}

impl Display for ScalarFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            ScalarFunc::Cast => "cast".to_string(),
            ScalarFunc::Char => "char".to_string(),
            ScalarFunc::Coalesce => "coalesce".to_string(),
            ScalarFunc::Concat => "concat".to_string(),
            ScalarFunc::ConcatWs => "concat_ws".to_string(),
            ScalarFunc::Glob => "glob".to_string(),
            ScalarFunc::IfNull => "ifnull".to_string(),
            ScalarFunc::Iif => "iif".to_string(),
            ScalarFunc::Instr => "instr".to_string(),
            ScalarFunc::Like => "like(2)".to_string(),
            ScalarFunc::Abs => "abs".to_string(),
            ScalarFunc::Upper => "upper".to_string(),
            ScalarFunc::Lower => "lower".to_string(),
            ScalarFunc::Random => "random".to_string(),
            ScalarFunc::RandomBlob => "randomblob".to_string(),
            ScalarFunc::Trim => "trim".to_string(),
            ScalarFunc::LTrim => "ltrim".to_string(),
            ScalarFunc::RTrim => "rtrim".to_string(),
            ScalarFunc::Round => "round".to_string(),
            ScalarFunc::Length => "length".to_string(),
            ScalarFunc::OctetLength => "octet_length".to_string(),
            ScalarFunc::Min => "min".to_string(),
            ScalarFunc::Max => "max".to_string(),
            ScalarFunc::Nullif => "nullif".to_string(),
            ScalarFunc::Sign => "sign".to_string(),
            ScalarFunc::Substr => "substr".to_string(),
            ScalarFunc::Substring => "substring".to_string(),
            ScalarFunc::Soundex => "soundex".to_string(),
            ScalarFunc::Date => "date".to_string(),
            ScalarFunc::Time => "time".to_string(),
            ScalarFunc::Typeof => "typeof".to_string(),
            ScalarFunc::Unicode => "unicode".to_string(),
            ScalarFunc::Quote => "quote".to_string(),
            ScalarFunc::SqliteVersion => "sqlite_version".to_string(),
            ScalarFunc::UnixEpoch => "unixepoch".to_string(),
            ScalarFunc::Hex => "hex".to_string(),
            ScalarFunc::Unhex => "unhex".to_string(),
            ScalarFunc::ZeroBlob => "zeroblob".to_string(),
            ScalarFunc::LastInsertRowid => "last_insert_rowid".to_string(),
            ScalarFunc::Replace => "replace".to_string(),
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
            MathFunc::Pi => MathFuncArity::Nullary,

            MathFunc::Acos
            | MathFunc::Acosh
            | MathFunc::Asin
            | MathFunc::Asinh
            | MathFunc::Atan
            | MathFunc::Atanh
            | MathFunc::Ceil
            | MathFunc::Ceiling
            | MathFunc::Cos
            | MathFunc::Cosh
            | MathFunc::Degrees
            | MathFunc::Exp
            | MathFunc::Floor
            | MathFunc::Ln
            | MathFunc::Log10
            | MathFunc::Log2
            | MathFunc::Radians
            | MathFunc::Sin
            | MathFunc::Sinh
            | MathFunc::Sqrt
            | MathFunc::Tan
            | MathFunc::Tanh
            | MathFunc::Trunc => MathFuncArity::Unary,

            MathFunc::Atan2 | MathFunc::Mod | MathFunc::Pow | MathFunc::Power => {
                MathFuncArity::Binary
            }

            MathFunc::Log => MathFuncArity::UnaryOrBinary,
        }
    }
}

impl Display for MathFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            MathFunc::Acos => "acos".to_string(),
            MathFunc::Acosh => "acosh".to_string(),
            MathFunc::Asin => "asin".to_string(),
            MathFunc::Asinh => "asinh".to_string(),
            MathFunc::Atan => "atan".to_string(),
            MathFunc::Atan2 => "atan2".to_string(),
            MathFunc::Atanh => "atanh".to_string(),
            MathFunc::Ceil => "ceil".to_string(),
            MathFunc::Ceiling => "ceiling".to_string(),
            MathFunc::Cos => "cos".to_string(),
            MathFunc::Cosh => "cosh".to_string(),
            MathFunc::Degrees => "degrees".to_string(),
            MathFunc::Exp => "exp".to_string(),
            MathFunc::Floor => "floor".to_string(),
            MathFunc::Ln => "ln".to_string(),
            MathFunc::Log => "log".to_string(),
            MathFunc::Log10 => "log10".to_string(),
            MathFunc::Log2 => "log2".to_string(),
            MathFunc::Mod => "mod".to_string(),
            MathFunc::Pi => "pi".to_string(),
            MathFunc::Pow => "pow".to_string(),
            MathFunc::Power => "power".to_string(),
            MathFunc::Radians => "radians".to_string(),
            MathFunc::Sin => "sin".to_string(),
            MathFunc::Sinh => "sinh".to_string(),
            MathFunc::Sqrt => "sqrt".to_string(),
            MathFunc::Tan => "tan".to_string(),
            MathFunc::Tanh => "tanh".to_string(),
            MathFunc::Trunc => "trunc".to_string(),
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
}

impl Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Func::Agg(agg_func) => write!(f, "{}", agg_func.to_string()),
            Func::Scalar(scalar_func) => write!(f, "{}", scalar_func),
            Func::Math(math_func) => write!(f, "{}", math_func),
            #[cfg(feature = "json")]
            Func::Json(json_func) => write!(f, "{}", json_func),
        }
    }
}

#[derive(Debug)]
pub struct FuncCtx {
    pub func: Func,
    pub arg_count: usize,
}

impl Func {
    pub fn resolve_function(name: &str, arg_count: usize) -> Result<Func, ()> {
        match name {
            "avg" => Ok(Func::Agg(AggFunc::Avg)),
            "count" => Ok(Func::Agg(AggFunc::Count)),
            "group_concat" => Ok(Func::Agg(AggFunc::GroupConcat)),
            "max" if arg_count == 0 || arg_count == 1 => Ok(Func::Agg(AggFunc::Max)),
            "max" if arg_count > 1 => Ok(Func::Scalar(ScalarFunc::Max)),
            "min" if arg_count == 0 || arg_count == 1 => Ok(Func::Agg(AggFunc::Min)),
            "min" if arg_count > 1 => Ok(Func::Scalar(ScalarFunc::Min)),
            "nullif" if arg_count == 2 => Ok(Func::Scalar(ScalarFunc::Nullif)),
            "string_agg" => Ok(Func::Agg(AggFunc::StringAgg)),
            "sum" => Ok(Func::Agg(AggFunc::Sum)),
            "total" => Ok(Func::Agg(AggFunc::Total)),
            "char" => Ok(Func::Scalar(ScalarFunc::Char)),
            "coalesce" => Ok(Func::Scalar(ScalarFunc::Coalesce)),
            "concat" => Ok(Func::Scalar(ScalarFunc::Concat)),
            "concat_ws" => Ok(Func::Scalar(ScalarFunc::ConcatWs)),
            "glob" => Ok(Func::Scalar(ScalarFunc::Glob)),
            "ifnull" => Ok(Func::Scalar(ScalarFunc::IfNull)),
            "iif" => Ok(Func::Scalar(ScalarFunc::Iif)),
            "instr" => Ok(Func::Scalar(ScalarFunc::Instr)),
            "like" => Ok(Func::Scalar(ScalarFunc::Like)),
            "abs" => Ok(Func::Scalar(ScalarFunc::Abs)),
            "upper" => Ok(Func::Scalar(ScalarFunc::Upper)),
            "lower" => Ok(Func::Scalar(ScalarFunc::Lower)),
            "random" => Ok(Func::Scalar(ScalarFunc::Random)),
            "randomblob" => Ok(Func::Scalar(ScalarFunc::RandomBlob)),
            "trim" => Ok(Func::Scalar(ScalarFunc::Trim)),
            "ltrim" => Ok(Func::Scalar(ScalarFunc::LTrim)),
            "rtrim" => Ok(Func::Scalar(ScalarFunc::RTrim)),
            "round" => Ok(Func::Scalar(ScalarFunc::Round)),
            "length" => Ok(Func::Scalar(ScalarFunc::Length)),
            "octet_length" => Ok(Func::Scalar(ScalarFunc::OctetLength)),
            "sign" => Ok(Func::Scalar(ScalarFunc::Sign)),
            "substr" => Ok(Func::Scalar(ScalarFunc::Substr)),
            "substring" => Ok(Func::Scalar(ScalarFunc::Substring)),
            "date" => Ok(Func::Scalar(ScalarFunc::Date)),
            "time" => Ok(Func::Scalar(ScalarFunc::Time)),
            "typeof" => Ok(Func::Scalar(ScalarFunc::Typeof)),
            "last_insert_rowid" => Ok(Func::Scalar(ScalarFunc::LastInsertRowid)),
            "unicode" => Ok(Func::Scalar(ScalarFunc::Unicode)),
            "quote" => Ok(Func::Scalar(ScalarFunc::Quote)),
            "sqlite_version" => Ok(Func::Scalar(ScalarFunc::SqliteVersion)),
            "replace" => Ok(Func::Scalar(ScalarFunc::Replace)),
            #[cfg(feature = "json")]
            "json" => Ok(Func::Json(JsonFunc::Json)),
            "unixepoch" => Ok(Func::Scalar(ScalarFunc::UnixEpoch)),
            "hex" => Ok(Func::Scalar(ScalarFunc::Hex)),
            "unhex" => Ok(Func::Scalar(ScalarFunc::Unhex)),
            "zeroblob" => Ok(Func::Scalar(ScalarFunc::ZeroBlob)),
            "soundex" => Ok(Func::Scalar(ScalarFunc::Soundex)),
            "acos" => Ok(Func::Math(MathFunc::Acos)),
            "acosh" => Ok(Func::Math(MathFunc::Acosh)),
            "asin" => Ok(Func::Math(MathFunc::Asin)),
            "asinh" => Ok(Func::Math(MathFunc::Asinh)),
            "atan" => Ok(Func::Math(MathFunc::Atan)),
            "atan2" => Ok(Func::Math(MathFunc::Atan2)),
            "atanh" => Ok(Func::Math(MathFunc::Atanh)),
            "ceil" => Ok(Func::Math(MathFunc::Ceil)),
            "ceiling" => Ok(Func::Math(MathFunc::Ceiling)),
            "cos" => Ok(Func::Math(MathFunc::Cos)),
            "cosh" => Ok(Func::Math(MathFunc::Cosh)),
            "degrees" => Ok(Func::Math(MathFunc::Degrees)),
            "exp" => Ok(Func::Math(MathFunc::Exp)),
            "floor" => Ok(Func::Math(MathFunc::Floor)),
            "ln" => Ok(Func::Math(MathFunc::Ln)),
            "log" => Ok(Func::Math(MathFunc::Log)),
            "log10" => Ok(Func::Math(MathFunc::Log10)),
            "log2" => Ok(Func::Math(MathFunc::Log2)),
            "mod" => Ok(Func::Math(MathFunc::Mod)),
            "pi" => Ok(Func::Math(MathFunc::Pi)),
            "pow" => Ok(Func::Math(MathFunc::Pow)),
            "power" => Ok(Func::Math(MathFunc::Power)),
            "radians" => Ok(Func::Math(MathFunc::Radians)),
            "sin" => Ok(Func::Math(MathFunc::Sin)),
            "sinh" => Ok(Func::Math(MathFunc::Sinh)),
            "sqrt" => Ok(Func::Math(MathFunc::Sqrt)),
            "tan" => Ok(Func::Math(MathFunc::Tan)),
            "tanh" => Ok(Func::Math(MathFunc::Tanh)),
            "trunc" => Ok(Func::Math(MathFunc::Trunc)),
            _ => Err(()),
        }
    }
}
