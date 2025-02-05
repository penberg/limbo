use pest::iterators::Pair;
use pest::Parser as P;
use pest_derive::Parser;
use serde::de;
use serde::forward_to_deserialize_any;
use std::collections::VecDeque;

use crate::json::error::{self, Error, Result};

#[derive(Parser)]
#[grammar = "json/json.pest"]
struct Parser;

/// Deserialize an instance of type `T` from a string of JSON5 text. Can fail if the input is
/// invalid JSON5, or doesn&rsquo;t match the structure of the target type.
pub fn from_str<'a, T>(s: &'a str) -> Result<T>
where
    T: de::Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_str(s)?;
    T::deserialize(&mut deserializer)
}

/// A Deserializes JSON data into a Rust value.
pub struct Deserializer<'de> {
    pair: Option<Pair<'de, Rule>>,
}

impl<'de> Deserializer<'de> {
    /// Creates a JSON5 deserializer from a `&str`. This parses the input at construction time, so
    /// can fail if the input is not valid JSON5.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(input: &'de str) -> Result<Self> {
        let pair = Parser::parse(Rule::text, input)?.next().unwrap();
        Ok(Deserializer::from_pair(pair))
    }

    fn from_pair(pair: Pair<'de, Rule>) -> Self {
        Self { pair: Some(pair) }
    }
}

impl<'de> de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || match pair.as_rule() {
            Rule::null => visitor.visit_unit(),
            Rule::boolean => visitor.visit_bool(parse_bool(&pair)),
            Rule::string | Rule::identifier => visitor.visit_string(parse_string(pair)?),
            Rule::number => match pair.as_str() {
                "Infinity" | "+Infinity" => visitor.visit_f64(f64::INFINITY),
                "-Infinity" => visitor.visit_f64(f64::NEG_INFINITY),
                "NaN" | "-NaN" => visitor.visit_f64(f64::NAN),
                _ => {
                    if is_int(pair.as_str()) {
                        visitor.visit_i64(parse_integer(&pair)?)
                    } else {
                        visitor.visit_f64(parse_number(&pair)?)
                    }
                }
            },
            Rule::array => visitor.visit_seq(Seq::new(pair)),
            Rule::object => visitor.visit_map(Map::new(pair)),
            _ => unreachable!(),
        })();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = visitor.visit_enum(Enum { pair });
        error::set_location(&mut res, &span);
        res
    }

    // The below will get us the right types, but won't necessarily give
    // meaningful results if the source is out of the range of the target type.
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_i8(parse_number(&pair)? as i8))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_i16(parse_number(&pair)? as i16))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_i32(parse_number(&pair)? as i32))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_i64(parse_number(&pair)? as i64))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_i128(parse_number(&pair)? as i128))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_u8(parse_number(&pair)? as u8))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_u16(parse_number(&pair)? as u16))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_u32(parse_number(&pair)? as u32))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_u64(parse_number(&pair)? as u64))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_u128(parse_number(&pair)? as u128))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_f32(parse_number(&pair)? as f32))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = (move || visitor.visit_f64(parse_number(&pair)?))();
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let pair = self.pair.take().unwrap();
        let span = pair.as_span();
        let mut res = match pair.as_rule() {
            Rule::null => visitor.visit_none(),
            _ => visitor.visit_some(&mut Deserializer::from_pair(pair)),
        };
        error::set_location(&mut res, &span);
        res
    }

    fn deserialize_newtype_struct<V>(self, _name: &str, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let span = self.pair.as_ref().unwrap().as_span();
        let mut res = visitor.visit_newtype_struct(self);
        error::set_location(&mut res, &span);
        res
    }

    forward_to_deserialize_any! {
        bool char str string bytes byte_buf unit unit_struct seq
        tuple tuple_struct map struct identifier ignored_any
    }
}

fn parse_bool(pair: &Pair<'_, Rule>) -> bool {
    match pair.as_str() {
        "true" => true,
        "false" => false,
        _ => unreachable!(),
    }
}

fn parse_string(pair: Pair<'_, Rule>) -> Result<String> {
    let span = pair.as_span();
    let mut res = pair
        .into_inner()
        .map(|component| match component.as_rule() {
            Rule::char_literal => Ok(String::from(component.as_str())),
            Rule::char_escape_sequence => Ok(parse_char_escape_sequence(&component)),
            Rule::nul_escape_sequence => Ok(String::from("\u{0000}")),
            Rule::hex_escape_sequence => u8::from_str_radix(component.as_str(), 16)
                .map(|value| format!("\\u{:04X}", value))
                .map_err(|_| de::Error::custom("error hex sequence")), // TODO: FIX HEX SEQUENCE TO MATCH SQLITE
            Rule::unicode_escape_sequence => {
                let hex_escape = parse_hex(component.as_str())?;
                Ok(hex_escape.to_string())
            }
            _ => unreachable!(),
        })
        .collect();
    error::set_location(&mut res, &span);
    res
}

fn parse_char_escape_sequence(pair: &Pair<'_, Rule>) -> String {
    String::from(match pair.as_str() {
        "b" => "\u{0008}",
        "f" => "\u{000C}",
        "n" => "\n",
        "r" => "\r",
        "t" => "\t",
        "v" => "\u{000B}",
        "0" => "\u{0000}",
        c => c,
    })
}

fn parse_number(pair: &Pair<'_, Rule>) -> Result<f64> {
    match pair.as_str() {
        "Infinity" | "+Infinity" => Ok(f64::INFINITY),
        "-Infinity" => Ok(f64::NEG_INFINITY),
        "NaN" | "-NaN" => Ok(f64::NAN),
        s if is_hex_literal(s) => parse_hex(s).map(f64::from),
        s => {
            if let Ok(r) = s.parse::<f64>() {
                if r.is_finite() {
                    Ok(r)
                } else {
                    Err(de::Error::custom("error parsing number: too large"))
                }
            } else {
                Err(de::Error::custom("error parsing number"))
            }
        }
    }
}

fn parse_integer(pair: &Pair<'_, Rule>) -> Result<i64> {
    match pair.as_str() {
        s if is_hex_literal(s) => {
            let parsed = parse_hex(s)? as i64;
            Ok(parsed)
        }
        s => s
            .parse()
            .map_err(|_| de::Error::custom("error parsing integer")),
    }
}

fn is_int(s: &str) -> bool {
    !s.contains('.') && (is_hex_literal(s) || (!s.contains('e') && !s.contains('E')))
}

fn parse_hex(s: &str) -> Result<i32> {
    let (sign, trimmed) = match s.chars().next() {
        Some('-') => (-1, &s[3..]), // skip "-0x" or "-0X"
        Some('+') => (1, &s[3..]),  // skip "+0x" or "+0X"
        _ => (1, &s[2..]),          // skip "0x" or "0X"
    };
    i32::from_str_radix(trimmed, 16)
        .map(|v| v * sign)
        .map_err(|_| de::Error::custom("error parsing hex"))
}

fn is_hex_literal(s: &str) -> bool {
    let trimmed = s.trim_start_matches(['+', '-']);
    trimmed.len() > 2 && (&trimmed[..2] == "0x" || &trimmed[..2] == "0X")
}

struct Seq<'de> {
    pairs: VecDeque<Pair<'de, Rule>>,
}

impl<'de> Seq<'de> {
    pub fn new(pair: Pair<'de, Rule>) -> Self {
        Self {
            pairs: pair.into_inner().collect(),
        }
    }
}

impl<'de> de::SeqAccess<'de> for Seq<'de> {
    type Error = Error;

    fn size_hint(&self) -> Option<usize> {
        Some(self.pairs.len())
    }

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        if let Some(pair) = self.pairs.pop_front() {
            seed.deserialize(&mut Deserializer::from_pair(pair))
                .map(Some)
        } else {
            Ok(None)
        }
    }
}

struct Map<'de> {
    pairs: VecDeque<Pair<'de, Rule>>,
}

impl<'de> Map<'de> {
    pub fn new(pair: Pair<'de, Rule>) -> Self {
        Self {
            pairs: pair.into_inner().collect(),
        }
    }
}

impl<'de> de::MapAccess<'de> for Map<'de> {
    type Error = Error;

    fn size_hint(&self) -> Option<usize> {
        Some(self.pairs.len() / 2)
    }

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: de::DeserializeSeed<'de>,
    {
        if let Some(pair) = self.pairs.pop_front() {
            seed.deserialize(&mut Deserializer::from_pair(pair))
                .map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut Deserializer::from_pair(
            self.pairs.pop_front().unwrap(),
        ))
    }
}

struct Enum<'de> {
    pair: Pair<'de, Rule>,
}

impl<'de> de::EnumAccess<'de> for Enum<'de> {
    type Error = Error;
    type Variant = Variant<'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let span = self.pair.as_span();
        let mut res = (move || match self.pair.as_rule() {
            Rule::string => {
                let tag = seed.deserialize(&mut Deserializer::from_pair(self.pair))?;
                Ok((tag, Variant { pair: None }))
            }
            Rule::object => {
                let mut pairs = self.pair.into_inner();

                if let Some(tag_pair) = pairs.next() {
                    let tag = seed.deserialize(&mut Deserializer::from_pair(tag_pair))?;
                    Ok((tag, Variant { pair: pairs.next() }))
                } else {
                    Err(de::Error::custom("expected a nonempty object"))
                }
            }
            _ => Err(de::Error::custom("expected a string or an object")),
        })();
        error::set_location(&mut res, &span);
        res
    }
}

struct Variant<'de> {
    pair: Option<Pair<'de, Rule>>,
}

impl<'de> de::VariantAccess<'de> for Variant<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        if let Some(pair) = self.pair {
            serde::Deserialize::deserialize(&mut Deserializer::from_pair(pair))
        } else {
            Ok(())
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut Deserializer::from_pair(self.pair.unwrap()))
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        match self.pair {
            Some(pair) => match pair.as_rule() {
                Rule::array => visitor.visit_seq(Seq::new(pair)),
                _ => Err(de::Error::custom("expected an array")),
            },
            None => Err(de::Error::custom("expected an array")),
        }
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        match self.pair {
            Some(pair) => match pair.as_rule() {
                Rule::object => visitor.visit_map(Map::new(pair)),
                _ => Err(de::Error::custom("expected an object")),
            },
            None => Err(de::Error::custom("expected an object")),
        }
    }
}

pub mod ordered_object {

    use crate::json::Val;
    use serde::de::{MapAccess, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(pairs: &Vec<(String, Val)>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(pairs.len()))?;
        for (k, v) in pairs {
            if let Val::Removed = v {
                continue;
            }
            map.serialize_entry(k, v)?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<(String, Val)>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OrderedMapVisitor;

        impl<'de> Visitor<'de> for OrderedMapVisitor {
            type Value = Vec<(String, Val)>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map")
            }

            fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut pairs = match access.size_hint() {
                    Some(size) => Vec::with_capacity(size),
                    None => Vec::new(),
                };

                while let Some((key, value)) = access.next_entry()? {
                    pairs.push((key, value));
                }

                Ok(pairs)
            }
        }

        deserializer.deserialize_map(OrderedMapVisitor)
    }
}
