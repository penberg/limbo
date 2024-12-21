use rand::Rng;

use crate::generation::{
    gen_random_text, pick, pick_index, readable_name_custom, Arbitrary, ArbitraryFrom,
};
use crate::model::table::{Column, ColumnType, Name, Table, Value};

impl Arbitrary for Name {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = readable_name_custom("_", rng);
        Name(name.replace("-", "_"))
    }
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let columns = (1..=rng.gen_range(1..5))
            .map(|_| Column::arbitrary(rng))
            .collect();
        Table {
            rows: Vec::new(),
            name,
            columns,
        }
    }
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let column_type = ColumnType::arbitrary(rng);
        Column {
            name,
            column_type,
            primary: false,
            unique: false,
        }
    }
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        pick(
            &vec![
                ColumnType::Integer,
                ColumnType::Float,
                ColumnType::Text,
                ColumnType::Blob,
            ],
            rng,
        )
        .to_owned()
    }
}

impl ArbitraryFrom<Vec<&Value>> for Value {
    fn arbitrary_from<R: Rng>(rng: &mut R, values: &Vec<&Value>) -> Self {
        if values.is_empty() {
            return Value::Null;
        }

        pick(values, rng).to_owned().clone()
    }
}

impl ArbitraryFrom<ColumnType> for Value {
    fn arbitrary_from<R: Rng>(rng: &mut R, column_type: &ColumnType) -> Self {
        match column_type {
            ColumnType::Integer => Value::Integer(rng.gen_range(i64::MIN..i64::MAX)),
            ColumnType::Float => Value::Float(rng.gen_range(-1e10..1e10)),
            ColumnType::Text => Value::Text(gen_random_text(rng)),
            ColumnType::Blob => Value::Blob(gen_random_text(rng).as_bytes().to_vec()),
        }
    }
}

pub(crate) struct LTValue(pub(crate) Value);

impl ArbitraryFrom<Vec<&Value>> for LTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, values: &Vec<&Value>) -> Self {
        if values.is_empty() {
            return LTValue(Value::Null);
        }

        let index = pick_index(values.len(), rng);
        LTValue::arbitrary_from(rng, values[index])
    }
}

impl ArbitraryFrom<Value> for LTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, value: &Value) -> Self {
        match value {
            Value::Integer(i) => LTValue(Value::Integer(rng.gen_range(i64::MIN..*i - 1))),
            Value::Float(f) => LTValue(Value::Float(rng.gen_range(-1e10..*f - 1.0))),
            Value::Text(t) => {
                // Either shorten the string, or make at least one character smaller and mutate the rest
                let mut t = t.clone();
                if rng.gen_bool(0.01) {
                    t.pop();
                    LTValue(Value::Text(t))
                } else {
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    let index = rng.gen_range(0..t.len());
                    t[index] -= 1;
                    // Mutate the rest of the string
                    for i in (index + 1)..t.len() {
                        t[i] = rng.gen_range('a' as u32..='z' as u32);
                    }
                    let t = t
                        .into_iter()
                        .map(|c| char::from_u32(c).unwrap_or('z'))
                        .collect::<String>();
                    LTValue(Value::Text(t))
                }
            }
            Value::Blob(b) => {
                // Either shorten the blob, or make at least one byte smaller and mutate the rest
                let mut b = b.clone();
                if rng.gen_bool(0.01) {
                    b.pop();
                    LTValue(Value::Blob(b))
                } else {
                    let index = rng.gen_range(0..b.len());
                    b[index] -= 1;
                    // Mutate the rest of the blob
                    for i in (index + 1)..b.len() {
                        b[i] = rng.gen_range(0..=255);
                    }
                    LTValue(Value::Blob(b))
                }
            }
            _ => unreachable!(),
        }
    }
}

pub(crate) struct GTValue(pub(crate) Value);

impl ArbitraryFrom<Vec<&Value>> for GTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, values: &Vec<&Value>) -> Self {
        if values.is_empty() {
            return GTValue(Value::Null);
        }

        let index = pick_index(values.len(), rng);
        GTValue::arbitrary_from(rng, values[index])
    }
}

impl ArbitraryFrom<Value> for GTValue {
    fn arbitrary_from<R: Rng>(rng: &mut R, value: &Value) -> Self {
        match value {
            Value::Integer(i) => GTValue(Value::Integer(rng.gen_range(*i..i64::MAX))),
            Value::Float(f) => GTValue(Value::Float(rng.gen_range(*f..1e10))),
            Value::Text(t) => {
                // Either lengthen the string, or make at least one character smaller and mutate the rest
                let mut t = t.clone();
                if rng.gen_bool(0.01) {
                    t.push(rng.gen_range(0..=255) as u8 as char);
                    GTValue(Value::Text(t))
                } else {
                    let mut t = t.chars().map(|c| c as u32).collect::<Vec<_>>();
                    let index = rng.gen_range(0..t.len());
                    t[index] += 1;
                    // Mutate the rest of the string
                    for i in (index + 1)..t.len() {
                        t[i] = rng.gen_range('a' as u32..='z' as u32);
                    }
                    let t = t
                        .into_iter()
                        .map(|c| char::from_u32(c).unwrap_or('a'))
                        .collect::<String>();
                    GTValue(Value::Text(t))
                }
            }
            Value::Blob(b) => {
                // Either lengthen the blob, or make at least one byte smaller and mutate the rest
                let mut b = b.clone();
                if rng.gen_bool(0.01) {
                    b.push(rng.gen_range(0..=255));
                    GTValue(Value::Blob(b))
                } else {
                    let index = rng.gen_range(0..b.len());
                    b[index] += 1;
                    // Mutate the rest of the blob
                    for i in (index + 1)..b.len() {
                        b[i] = rng.gen_range(0..=255);
                    }
                    GTValue(Value::Blob(b))
                }
            }
            _ => unreachable!(),
        }
    }
}
