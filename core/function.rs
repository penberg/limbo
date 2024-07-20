use std::str::FromStr;

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
        }
    }
}

pub enum Func {
    Agg(AggFunc),
    SingleRow(SingleRowFunc),
}

impl FromStr for Func {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "avg" => Ok(Func::Agg(AggFunc::Avg)),
            "count" => Ok(Func::Agg(AggFunc::Count)),
            "group_concat" => Ok(Func::Agg(AggFunc::GroupConcat)),
            "max" => Ok(Func::Agg(AggFunc::Max)),
            "min" => Ok(Func::Agg(AggFunc::Min)),
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
            _ => Err(()),
        }
    }
}
