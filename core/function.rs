#[derive(Debug, Clone, PartialEq)]
pub enum JsonFunc {
    JSON,
}

impl ToString for JsonFunc {
    fn to_string(&self) -> String {
        match self {
            JsonFunc::JSON => "json".to_string(),
        }
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
    Char,
    Coalesce,
    Concat,
    ConcatWs,
    IfNull,
    Like,
    Abs,
    Upper,
    Lower,
    Random,
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
    Unicode,
    Quote,
    SqliteVersion,
    UnixEpoch,
}

impl ToString for ScalarFunc {
    fn to_string(&self) -> String {
        match self {
            ScalarFunc::Char => "char".to_string(),
            ScalarFunc::Coalesce => "coalesce".to_string(),
            ScalarFunc::Concat => "concat".to_string(),
            ScalarFunc::ConcatWs => "concat_ws".to_string(),
            ScalarFunc::IfNull => "ifnull".to_string(),
            ScalarFunc::Like => "like(2)".to_string(),
            ScalarFunc::Abs => "abs".to_string(),
            ScalarFunc::Upper => "upper".to_string(),
            ScalarFunc::Lower => "lower".to_string(),
            ScalarFunc::Random => "random".to_string(),
            ScalarFunc::Trim => "trim".to_string(),
            ScalarFunc::LTrim => "ltrim".to_string(),
            ScalarFunc::RTrim => "rtrim".to_string(),
            ScalarFunc::Round => "round".to_string(),
            ScalarFunc::Length => "length".to_string(),
            ScalarFunc::Min => "min".to_string(),
            ScalarFunc::Max => "max".to_string(),
            ScalarFunc::Nullif => "nullif".to_string(),
            ScalarFunc::Sign => "sign".to_string(),
            ScalarFunc::Substr => "substr".to_string(),
            ScalarFunc::Substring => "substring".to_string(),
            ScalarFunc::Date => "date".to_string(),
            ScalarFunc::Time => "time".to_string(),
            ScalarFunc::Unicode => "unicode".to_string(),
            ScalarFunc::Quote => "quote".to_string(),
            ScalarFunc::SqliteVersion => "sqlite_version".to_string(),
            ScalarFunc::UnixEpoch => "unixepoch".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum Func {
    Agg(AggFunc),
    Scalar(ScalarFunc),
    Json(JsonFunc),
}

impl Func {
    pub fn to_string(&self) -> String {
        match self {
            Func::Agg(agg_func) => agg_func.to_string().to_string(),
            Func::Scalar(scalar_func) => scalar_func.to_string(),
            Func::Json(json_func) => json_func.to_string(),
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
            "ifnull" => Ok(Func::Scalar(ScalarFunc::IfNull)),
            "like" => Ok(Func::Scalar(ScalarFunc::Like)),
            "abs" => Ok(Func::Scalar(ScalarFunc::Abs)),
            "upper" => Ok(Func::Scalar(ScalarFunc::Upper)),
            "lower" => Ok(Func::Scalar(ScalarFunc::Lower)),
            "random" => Ok(Func::Scalar(ScalarFunc::Random)),
            "trim" => Ok(Func::Scalar(ScalarFunc::Trim)),
            "ltrim" => Ok(Func::Scalar(ScalarFunc::LTrim)),
            "rtrim" => Ok(Func::Scalar(ScalarFunc::RTrim)),
            "round" => Ok(Func::Scalar(ScalarFunc::Round)),
            "length" => Ok(Func::Scalar(ScalarFunc::Length)),
            "sign" => Ok(Func::Scalar(ScalarFunc::Sign)),
            "substr" => Ok(Func::Scalar(ScalarFunc::Substr)),
            "substring" => Ok(Func::Scalar(ScalarFunc::Substring)),
            "date" => Ok(Func::Scalar(ScalarFunc::Date)),
            "time" => Ok(Func::Scalar(ScalarFunc::Time)),
            "unicode" => Ok(Func::Scalar(ScalarFunc::Unicode)),
            "quote" => Ok(Func::Scalar(ScalarFunc::Quote)),
            "sqlite_version" => Ok(Func::Scalar(ScalarFunc::SqliteVersion)),
            "json" => Ok(Func::Json(JsonFunc::JSON)),
            "unixepoch" => Ok(Func::Scalar(ScalarFunc::UnixEpoch)),
            _ => Err(()),
        }
    }
}
