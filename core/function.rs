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
pub enum SingleRowFunc {
    Coalesce,
    Like,
    Abs,
    Upper,
    Lower,
    Random,
    Trim,
    Round,
    Length,
    Min,
    Max,
    Date,
}

impl ToString for SingleRowFunc {
    fn to_string(&self) -> String {
        match self {
            SingleRowFunc::Coalesce => "coalesce".to_string(),
            SingleRowFunc::Like => "like(2)".to_string(),
            SingleRowFunc::Abs => "abs".to_string(),
            SingleRowFunc::Upper => "upper".to_string(),
            SingleRowFunc::Lower => "lower".to_string(),
            SingleRowFunc::Random => "random".to_string(),
            SingleRowFunc::Trim => "trim".to_string(),
            SingleRowFunc::Round => "round".to_string(),
            SingleRowFunc::Length => "length".to_string(),
            SingleRowFunc::Min => "min".to_string(),
            SingleRowFunc::Max => "max".to_string(),
            SingleRowFunc::Date => "date".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum Func {
    Agg(AggFunc),
    SingleRow(SingleRowFunc),
}

impl Func {
    pub fn resolve_function(name: &str, arg_count: usize) -> Result<Func, ()> {
        match name {
            "avg" => Ok(Func::Agg(AggFunc::Avg)),
            "count" => Ok(Func::Agg(AggFunc::Count)),
            "group_concat" => Ok(Func::Agg(AggFunc::GroupConcat)),
            "max" if arg_count == 0 || arg_count == 1 => Ok(Func::Agg(AggFunc::Max)),
            "max" if arg_count > 1 => Ok(Func::SingleRow(SingleRowFunc::Max)),
            "min" if arg_count == 0 || arg_count == 1 => Ok(Func::Agg(AggFunc::Min)),
            "min" if arg_count > 1 => Ok(Func::SingleRow(SingleRowFunc::Min)),
            "string_agg" => Ok(Func::Agg(AggFunc::StringAgg)),
            "sum" => Ok(Func::Agg(AggFunc::Sum)),
            "total" => Ok(Func::Agg(AggFunc::Total)),
            "coalesce" => Ok(Func::SingleRow(SingleRowFunc::Coalesce)),
            "like" => Ok(Func::SingleRow(SingleRowFunc::Like)),
            "abs" => Ok(Func::SingleRow(SingleRowFunc::Abs)),
            "upper" => Ok(Func::SingleRow(SingleRowFunc::Upper)),
            "lower" => Ok(Func::SingleRow(SingleRowFunc::Lower)),
            "random" => Ok(Func::SingleRow(SingleRowFunc::Random)),
            "trim" => Ok(Func::SingleRow(SingleRowFunc::Trim)),
            "round" => Ok(Func::SingleRow(SingleRowFunc::Round)),
            "length" => Ok(Func::SingleRow(SingleRowFunc::Length)),
            "date" => Ok(Func::SingleRow(SingleRowFunc::Date)),
            _ => Err(()),
        }
    }
}
