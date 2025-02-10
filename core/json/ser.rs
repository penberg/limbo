use serde::ser::{self, Serialize};
use std::{f32, f64, io, num::FpCategory};

use crate::json::error::{Error, Result};

#[derive(Eq, PartialEq)]
pub enum State {
    Empty,
    First,
    Rest,
}

struct Map<'a, W: 'a, F: 'a> {
    ser: &'a mut Serializer<W, F>,
    state: State,
}

/// Attempts to serialize the input as a JSON5 string (actually a JSON string).
pub fn to_string<T>(value: &T) -> Result<String>
where
    T: Serialize,
{
    let vec = to_vec(value)?;
    let string = String::from_utf8(vec).map_err(|err| Error::from(err.utf8_error()))?;
    Ok(string)
}

/// Attempts to serialize the input as a JSON5 string (actually a JSON string).
pub fn to_string_pretty<T>(value: &T, indent: &str) -> Result<String>
where
    T: Serialize,
{
    let vec = to_vec_pretty(value, indent)?;
    let string = String::from_utf8(vec).map_err(|err| Error::from(err.utf8_error()))?;
    Ok(string)
}

struct Serializer<W, F = CompactFormatter> {
    writer: W,
    formatter: F,
}

impl<W> Serializer<W>
where
    W: io::Write,
{
    pub fn new(writer: W) -> Self {
        Serializer::with_formatter(writer, CompactFormatter)
    }
}

impl<'a, W> Serializer<W, PrettyFormatter<'a>>
where
    W: io::Write,
{
    /// Creates a new JSON pretty print serializer.
    #[inline]
    pub fn pretty(writer: W, indent: &'a str) -> Self {
        Serializer::with_formatter(writer, PrettyFormatter::with_indent(indent.as_bytes()))
    }
}

impl<W, F> Serializer<W, F>
where
    W: io::Write,
    F: Formatter,
{
    /// Creates a new JSON visitor whose output will be written to the writer
    /// specified.
    pub fn with_formatter(writer: W, formatter: F) -> Self {
        Serializer { writer, formatter }
    }
}

impl<'a, W, F> ser::Serializer for &'a mut Serializer<W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Map<'a, W, F>;
    type SerializeTuple = Map<'a, W, F>;
    type SerializeTupleStruct = Map<'a, W, F>;
    type SerializeTupleVariant = Map<'a, W, F>;
    type SerializeMap = Map<'a, W, F>;
    type SerializeStruct = Map<'a, W, F>;
    type SerializeStructVariant = Map<'a, W, F>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.formatter
            .write_bool(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.formatter
            .write_i8(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.formatter
            .write_i16(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.formatter
            .write_i32(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.formatter
            .write_i64(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.formatter
            .write_u8(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.formatter
            .write_u16(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.formatter
            .write_u32(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.formatter
            .write_u64(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.formatter
            .write_f32(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.formatter
            .write_f64(&mut self.writer, v)
            .map_err(Error::from)
    }

    fn serialize_char(self, v: char) -> Result<()> {
        // A char encoded as UTF-8 takes 4 bytes at most.
        let mut buf = [0; 4];
        self.serialize_str(v.encode_utf8(&mut buf))
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        format_escaped_str(&mut self.writer, &mut self.formatter, v).map_err(Error::from)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        unimplemented!() // TODO
    }

    fn serialize_none(self) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        self.formatter
            .write_null(&mut self.writer)
            .map_err(Error::from)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.formatter
            .begin_object(&mut self.writer)
            .map_err(Error::from)?;
        self.formatter
            .begin_object_key(&mut self.writer, true)
            .map_err(Error::from)?;
        self.serialize_str(variant)?;
        self.formatter
            .end_object_key(&mut self.writer)
            .map_err(Error::from)?;
        self.formatter
            .begin_object_value(&mut self.writer)
            .map_err(Error::from)?;
        value.serialize(&mut *self)?;
        self.formatter
            .end_object_value(&mut self.writer)
            .map_err(Error::from)?;
        self.formatter
            .end_object(&mut self.writer)
            .map_err(Error::from)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.formatter
            .begin_array(&mut self.writer)
            .map_err(Error::from)?;

        if len == Some(0) {
            self.formatter
                .end_array(&mut self.writer)
                .map_err(Error::from)?;
            Ok(Map {
                ser: self,
                state: State::Empty,
            })
        } else {
            Ok(Map {
                ser: self,
                state: State::First,
            })
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.formatter.begin_object(&mut self.writer)?;
        self.formatter.begin_object_key(&mut self.writer, true)?;
        self.serialize_str(variant)?;
        self.formatter.end_object_key(&mut self.writer)?;
        self.formatter.begin_object_value(&mut self.writer)?;
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        self.formatter
            .begin_object(&mut self.writer)
            .map_err(Error::from)?;
        if len == Some(0) {
            self.formatter
                .end_object(&mut self.writer)
                .map_err(Error::from)?;
            Ok(Map {
                ser: self,
                state: State::Empty,
            })
        } else {
            Ok(Map {
                ser: self,
                state: State::First,
            })
        }
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.formatter
            .begin_object(&mut self.writer)
            .map_err(Error::from)?;
        self.formatter
            .begin_object_key(&mut self.writer, true)
            .map_err(Error::from)?;
        self.serialize_str(variant).map_err(Error::from)?;
        self.formatter
            .end_object_key(&mut self.writer)
            .map_err(Error::from)?;
        self.formatter
            .begin_object_value(&mut self.writer)
            .map_err(Error::from)?;
        self.serialize_map(Some(len))
    }
}

impl<W, F> ser::SerializeSeq for Map<'_, W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.ser
            .formatter
            .begin_array_value(&mut self.ser.writer, self.state == State::First)
            .map_err(Error::from)?;

        self.state = State::Rest;
        value.serialize(&mut *self.ser).map_err(Error::from)?;
        self.ser
            .formatter
            .end_array_value(&mut self.ser.writer)
            .map_err(Error::from)
    }

    fn end(self) -> Result<()> {
        match self.state {
            State::Empty => Ok(()),
            _ => self
                .ser
                .formatter
                .end_array(&mut self.ser.writer)
                .map_err(Error::from),
        }
    }
}

impl<W, F> ser::SerializeTuple for Map<'_, W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<()> {
        ser::SerializeSeq::end(self)
    }
}

impl<W, F> ser::SerializeTupleStruct for Map<'_, W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<()> {
        ser::SerializeSeq::end(self)
    }
}

impl<W, F> ser::SerializeTupleVariant for Map<'_, W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<()> {
        match self.state {
            State::Empty => {}
            _ => self
                .ser
                .formatter
                .end_array(&mut self.ser.writer)
                .map_err(Error::from)?,
        };
        self.ser
            .formatter
            .end_object_value(&mut self.ser.writer)
            .map_err(Error::from)?;
        self.ser
            .formatter
            .end_object(&mut self.ser.writer)
            .map_err(Error::from)
    }
}

impl<W, F> ser::SerializeMap for Map<'_, W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.ser
            .formatter
            .begin_object_key(&mut self.ser.writer, self.state == State::First)
            .map_err(Error::from)?;
        self.state = State::Rest;

        key.serialize(&mut *self.ser)?;

        self.ser
            .formatter
            .end_object_key(&mut self.ser.writer)
            .map_err(Error::from)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.ser
            .formatter
            .begin_object_value(&mut self.ser.writer)
            .map_err(Error::from)?;

        value.serialize(&mut *self.ser)?;

        self.ser
            .formatter
            .end_object_value(&mut self.ser.writer)
            .map_err(Error::from)
    }

    fn end(self) -> Result<()> {
        match self.state {
            State::Empty => Ok(()),
            _ => self
                .ser
                .formatter
                .end_object(&mut self.ser.writer)
                .map_err(Error::from),
        }
    }
}

impl<W, F> ser::SerializeStruct for Map<'_, W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeMap::serialize_key(self, key)?;
        ser::SerializeMap::serialize_value(self, value)
    }

    fn end(self) -> Result<()> {
        ser::SerializeMap::end(self)
    }
}

impl<W, F> ser::SerializeStructVariant for Map<'_, W, F>
where
    W: io::Write,
    F: Formatter,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeStruct::serialize_field(self, key, value)
    }

    fn end(self) -> Result<()> {
        match self.state {
            State::Empty => {}
            _ => self
                .ser
                .formatter
                .end_object(&mut self.ser.writer)
                .map_err(Error::from)?,
        };
        self.ser
            .formatter
            .end_object_value(&mut self.ser.writer)
            .map_err(Error::from)?;
        self.ser
            .formatter
            .end_object(&mut self.ser.writer)
            .map_err(Error::from)
    }
}

pub fn to_writer<W, T>(writer: W, value: &T) -> Result<()>
where
    W: io::Write,
    T: ?Sized + Serialize,
{
    let mut ser = Serializer::new(writer);
    value.serialize(&mut ser)
}

pub fn to_vec<T>(value: &T) -> Result<Vec<u8>>
where
    T: ?Sized + Serialize,
{
    let mut writer = Vec::with_capacity(128);
    to_writer(&mut writer, value)?;
    Ok(writer)
}

pub fn to_writer_pretty<W, T>(writer: W, value: &T, indent: &str) -> Result<()>
where
    W: io::Write,
    T: ?Sized + Serialize,
{
    let mut ser = Serializer::pretty(writer, indent);
    value.serialize(&mut ser)
}

pub fn to_vec_pretty<T>(value: &T, indent: &str) -> Result<Vec<u8>>
where
    T: ?Sized + Serialize,
{
    let mut writer = Vec::with_capacity(128);
    to_writer_pretty(&mut writer, value, indent)?;
    Ok(writer)
}

/// Represents a character escape code in a type-safe manner.
pub enum CharEscape {
    /// An escaped quote `"`
    Quote,
    /// An escaped reverse solidus `\`
    ReverseSolidus,
    /// An escaped solidus `/`
    Solidus,
    /// An escaped backspace character (usually escaped as `\b`)
    Backspace,
    /// An escaped form feed character (usually escaped as `\f`)
    FormFeed,
    /// An escaped line feed character (usually escaped as `\n`)
    LineFeed,
    /// An escaped carriage return character (usually escaped as `\r`)
    CarriageReturn,
    /// An escaped tab character (usually escaped as `\t`)
    Tab,
    /// An escaped ASCII plane control character (usually escaped as
    /// `\u00XX` where `XX` are two hex characters)
    AsciiControl(u8),
}

impl CharEscape {
    fn from_escape_table(escape: u8, byte: u8) -> CharEscape {
        match escape {
            self::BB => CharEscape::Backspace,
            self::TT => CharEscape::Tab,
            self::NN => CharEscape::LineFeed,
            self::FF => CharEscape::FormFeed,
            self::RR => CharEscape::CarriageReturn,
            self::QU => CharEscape::Quote,
            self::BS => CharEscape::ReverseSolidus,
            self::UU => CharEscape::AsciiControl(byte),
            _ => unreachable!(),
        }
    }
}

const BB: u8 = b'b'; // \x08
const TT: u8 = b't'; // \x09
const NN: u8 = b'n'; // \x0A
const FF: u8 = b'f'; // \x0C
const RR: u8 = b'r'; // \x0D
const QU: u8 = b'"'; // \x22
const BS: u8 = b'\\'; // \x5C
const UU: u8 = b'u'; // \x00...\x1F except the ones above
const __: u8 = 0;

pub trait Formatter {
    /// Writes a `null` value to the specified writer.
    fn write_null<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b"null")
    }

    /// Writes a `true` or `false` value to the specified writer.
    fn write_bool<W>(&mut self, writer: &mut W, value: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        let s = if value {
            b"true" as &[u8]
        } else {
            b"false" as &[u8]
        };
        writer.write_all(s)
    }

    /// Writes an integer value like `-123` to the specified writer.
    fn write_i8<W>(&mut self, writer: &mut W, value: i8) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `-123` to the specified writer.
    fn write_i16<W>(&mut self, writer: &mut W, value: i16) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `-123` to the specified writer.
    fn write_i32<W>(&mut self, writer: &mut W, value: i32) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `-123` to the specified writer.
    fn write_i64<W>(&mut self, writer: &mut W, value: i64) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `-123` to the specified writer.
    fn write_i128<W>(&mut self, writer: &mut W, value: i128) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `123` to the specified writer.
    fn write_u8<W>(&mut self, writer: &mut W, value: u8) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `123` to the specified writer.
    fn write_u16<W>(&mut self, writer: &mut W, value: u16) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `123` to the specified writer.
    fn write_u32<W>(&mut self, writer: &mut W, value: u32) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `123` to the specified writer.
    fn write_u64<W>(&mut self, writer: &mut W, value: u64) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes an integer value like `123` to the specified writer.
    fn write_u128<W>(&mut self, writer: &mut W, value: u128) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.to_string().as_bytes())
    }

    /// Writes a floating point value like `-31.26e+12` to the specified writer.
    fn write_f32<W>(&mut self, writer: &mut W, value: f32) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        match value.classify() {
            FpCategory::Nan => {
                self.write_null(writer)?;
            }
            FpCategory::Infinite => {
                let infinity = if value.is_sign_negative() {
                    "-9e999"
                } else {
                    "9e999"
                };
                writer.write_all(infinity.as_bytes())?;
            }
            _ => {
                writer.write_all(value.to_string().as_bytes())?;
            }
        }
        Ok(())
    }

    /// Writes a floating point value like `-31.26e+12` to the specified writer.
    fn write_f64<W>(&mut self, writer: &mut W, value: f64) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        match value.classify() {
            FpCategory::Nan => {
                self.write_null(writer)?;
            }
            FpCategory::Infinite => {
                let infinity = if value.is_sign_negative() {
                    "-9e999"
                } else {
                    "9e999"
                };
                writer.write_all(infinity.as_bytes())?;
            }
            _ => {
                let mut buffer = ryu::Buffer::new();
                let s = buffer.format_finite(value);
                writer.write_all(s.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Writes a number that has already been rendered to a string.
    fn write_number_str<W>(&mut self, writer: &mut W, value: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(value.as_bytes())
    }

    /// Called before each series of `write_string_fragment` and
    /// `write_char_escape`.  Writes a `"` to the specified writer.
    fn begin_string<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b"\"")
    }

    /// Called after each series of `write_string_fragment` and
    /// `write_char_escape`.  Writes a `"` to the specified writer.
    fn end_string<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b"\"")
    }

    /// Writes a string fragment that doesn't need any escaping to the
    /// specified writer.
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(fragment.as_bytes())
    }

    /// Writes a character escape code to the specified writer.
    fn write_char_escape<W>(&mut self, writer: &mut W, char_escape: CharEscape) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        use self::CharEscape::*;

        let s = match char_escape {
            Quote => b"\\\"",
            ReverseSolidus => b"\\\\",
            Solidus => b"\\/",
            Backspace => b"\\b",
            FormFeed => b"\\f",
            LineFeed => b"\\n",
            CarriageReturn => b"\\r",
            Tab => b"\\t",
            AsciiControl(byte) => {
                static HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";
                let bytes = &[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX_DIGITS[(byte >> 4) as usize],
                    HEX_DIGITS[(byte & 0xF) as usize],
                ];
                return writer.write_all(bytes);
            }
        };

        writer.write_all(s)
    }

    /// Writes the representation of a byte array. Formatters can choose whether
    /// to represent bytes as a JSON array of integers (the default), or some
    /// JSON string encoding like hex or base64.
    fn write_byte_array<W>(&mut self, writer: &mut W, value: &[u8]) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        self.begin_array(writer)?;
        let mut first = true;
        for byte in value {
            self.begin_array_value(writer, first)?;
            self.write_u8(writer, *byte)?;
            self.end_array_value(writer)?;
            first = false;
        }
        self.end_array(writer)
    }

    /// Called before every array.  Writes a `[` to the specified
    /// writer.
    fn begin_array<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b"[")
    }

    /// Called after every array.  Writes a `]` to the specified
    /// writer.
    fn end_array<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b"]")
    }

    /// Called before every array value.  Writes a `,` if needed to
    /// the specified writer.
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b",")
        }
    }

    /// Called after every array value.
    fn end_array_value<W>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        Ok(())
    }

    /// Called before every object.  Writes a `{` to the specified
    /// writer.
    fn begin_object<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b"{")
    }

    /// Called after every object.  Writes a `}` to the specified
    /// writer.
    fn end_object<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b"}")
    }

    /// Called before every object key.
    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b",")
        }
    }

    /// Called after every object key.  A `:` should be written to the
    /// specified writer by either this method or
    /// `begin_object_value`.
    fn end_object_key<W>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        Ok(())
    }

    /// Called before every object value.  A `:` should be written to
    /// the specified writer by either this method or
    /// `end_object_key`.
    fn begin_object_value<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b":")
    }

    /// Called after every object value.
    fn end_object_value<W>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        Ok(())
    }

    /// Writes a raw JSON fragment that doesn't need any escaping to the
    /// specified writer.
    fn write_raw_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(fragment.as_bytes())
    }
}

fn format_escaped_str<W, F>(writer: &mut W, formatter: &mut F, value: &str) -> io::Result<()>
where
    W: ?Sized + io::Write,
    F: ?Sized + Formatter,
{
    formatter.begin_string(writer)?;
    format_escaped_str_contents(writer, formatter, value)?;
    formatter.end_string(writer)
}

fn format_escaped_str_contents<W, F>(
    writer: &mut W,
    formatter: &mut F,
    value: &str,
) -> io::Result<()>
where
    W: ?Sized + io::Write,
    F: ?Sized + Formatter,
{
    let bytes = value.as_bytes();

    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        let escape = self::ESCAPE[byte as usize];
        if escape == 0 {
            continue;
        }

        if start < i {
            formatter.write_string_fragment(writer, &value[start..i])?;
        }

        let char_escape = CharEscape::from_escape_table(escape, byte);
        formatter.write_char_escape(writer, char_escape)?;

        start = i + 1;
    }

    if start == bytes.len() {
        return Ok(());
    }

    formatter.write_string_fragment(writer, &value[start..])
}

// Lookup table of escape sequences. A value of b'x' at index i means that byte
// i is escaped as "\x" in JSON. A value of 0 means that byte i is not escaped.
static ESCAPE: [u8; 256] = [
    //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    UU, UU, UU, UU, UU, UU, UU, UU, BB, TT, NN, UU, FF, RR, UU, UU, // 0
    UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, // 1
    __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 3
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 4
    __, __, __, __, __, __, __, __, __, __, __, __, BS, __, __, __, // 5
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 6
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
];

/// This structure compacts a JSON value with no extra whitespace.
#[derive(Clone, Debug)]
pub struct CompactFormatter;

impl Formatter for CompactFormatter {}

/// This structure pretty prints a JSON value to make it human readable.
#[derive(Clone, Debug)]
pub struct PrettyFormatter<'a> {
    current_indent: usize,
    has_value: bool,
    indent: &'a [u8],
}

impl<'a> PrettyFormatter<'a> {
    /// Construct a pretty printer formatter that defaults to using two spaces for indentation.
    pub fn new() -> Self {
        PrettyFormatter::with_indent(b"  ")
    }

    /// Construct a pretty printer formatter that uses the `indent` string for indentation.
    pub fn with_indent(indent: &'a [u8]) -> Self {
        PrettyFormatter {
            current_indent: 0,
            has_value: false,
            indent,
        }
    }
}

impl Default for PrettyFormatter<'_> {
    fn default() -> Self {
        PrettyFormatter::new()
    }
}

impl Formatter for PrettyFormatter<'_> {
    #[inline]
    fn begin_array<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        self.current_indent += 1;
        self.has_value = false;
        writer.write_all(b"[")
    }

    #[inline]
    fn end_array<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        self.current_indent -= 1;

        if self.has_value {
            writer.write_all(b"\n")?;
            indent(writer, self.current_indent, self.indent)?;
        }

        writer.write_all(b"]")
    }

    #[inline]
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(if first { b"\n" } else { b",\n" })?;
        indent(writer, self.current_indent, self.indent)
    }

    #[inline]
    fn end_array_value<W>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        self.has_value = true;
        Ok(())
    }

    #[inline]
    fn begin_object<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        self.current_indent += 1;
        self.has_value = false;
        writer.write_all(b"{")
    }

    #[inline]
    fn end_object<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        self.current_indent -= 1;

        if self.has_value {
            writer.write_all(b"\n")?;
            indent(writer, self.current_indent, self.indent)?;
        }

        writer.write_all(b"}")
    }

    #[inline]
    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(if first { b"\n" } else { b",\n" })?;
        indent(writer, self.current_indent, self.indent)
    }

    #[inline]
    fn begin_object_value<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(b": ")
    }

    #[inline]
    fn end_object_value<W>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        self.has_value = true;
        Ok(())
    }
}

fn indent<W>(wr: &mut W, n: usize, s: &[u8]) -> io::Result<()>
where
    W: ?Sized + io::Write,
{
    for _ in 0..n {
        wr.write_all(s)?;
    }

    Ok(())
}
