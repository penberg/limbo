use crate::turso_debug_assert;
use branches::{mark_unlikely, unlikely};
use either::Either;
use turso_ext::{AggCtx, ContextDestructor, FinalizeFunction, StepFunction, ValueDestructor};
use turso_parser::ast::SortOrder;

use crate::alloc::*;
use crate::error::LimboError;
use crate::ext::{ExtValue, ExtValueType};
use crate::index_method::IndexMethodCursor;
use crate::numeric::format_float;
use crate::numeric::nonnan::NonNan;
use crate::numeric::Numeric;
use crate::pseudo::PseudoCursor;
use crate::schema::Index;
use crate::storage::btree::CursorTrait;
use crate::storage::sqlite3_ondisk::{
    read_integer, read_value, read_varint, varint_len, write_varint,
};
use crate::translate::collate::CollationSeq;
use crate::translate::plan::IterationDirection;
use crate::vdbe::sorter::Sorter;
use crate::vdbe::Register;
use crate::vtab::VirtualTableCursor;
use crate::{Completion, CompletionError, Result, IO};
use std::borrow::{Borrow, Cow};
use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::iter::{FusedIterator, Peekable};
use std::ops::Deref;
use std::task::{Poll, Waker};

/// SQLite by default uses 2000 as maximum numbers in a row.
/// It controlld by the constant called SQLITE_MAX_COLUMN
/// But the hard limit of number of columns is 32,767 columns i16::MAX
pub const MAX_COLUMN: usize = 2000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueType {
    Null,
    Integer,
    Float,
    Text,
    Blob,
    Error,
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Null => "NULL",
            Self::Integer => "INT",
            Self::Float => "REAL",
            Self::Blob => "BLOB",
            Self::Text => "TEXT",
            Self::Error => "ERROR",
        };
        write!(f, "{value}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TextSubtype {
    Text,
    #[cfg(feature = "json")]
    Json,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Text {
    pub value: Cow<'static, str>,
    pub subtype: TextSubtype,
}

impl Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Text {
    pub fn new(value: impl Into<Cow<'static, str>>) -> Self {
        Self {
            value: value.into(),
            subtype: TextSubtype::Text,
        }
    }
    #[cfg(feature = "json")]
    pub fn json(value: String) -> Self {
        Self {
            value: value.into(),
            subtype: TextSubtype::Json,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

/// UTF-8 validation tuned for record decoding. TEXT values are usually short
/// ASCII read at arbitrary offsets inside a b-tree page: simdutf8 only uses
/// SIMD from 64 bytes up, and core's `from_utf8` word-at-a-time path is
/// alignment-sensitive, so both are slow here. OR-ing every byte together is
/// alignment-independent and branch-light; if no byte had the high bit set
/// the value is pure ASCII and needs no further validation. Non-ASCII and
/// values longer than the cutoff fall back to full simdutf8 validation —
/// above the cutoff the scalar OR loop loses to real SIMD.
///
/// Measured by `core/benches/text_validate_benchmark.rs` (varying slice
/// alignment, ASCII content) on an Apple M2, macOS 15.7, vs
/// `simdutf8::basic::from_utf8` alone:
///
///   1-128 B:  1.4-4x faster (peak 4.1x at 16 B)
///   256-512 B: 1.1-1.2x faster
///   1-2 KB:   parity
///   4 KB:     ~25% slower without the cutoff; equal with it
///   multibyte fallback: pays the wasted OR scan (~15% at 64 B)
///   length branch: ~+0.1ns/call, visible only on 1-2 B values
#[inline]
pub(crate) fn validate_utf8(data: &[u8]) -> Option<&str> {
    const ASCII_SCAN_CUTOFF: usize = 512;
    if data.len() <= ASCII_SCAN_CUTOFF && is_ascii(data) {
        // SAFETY: all bytes are ASCII, which is valid UTF-8.
        return Some(unsafe { core::str::from_utf8_unchecked(data) });
    }
    simdutf8::basic::from_utf8(data).ok()
}

/// ORs the bytes together a word at a time: eight, then four, two and one
/// for the rest, so a value of any length takes at most `len / 8 + 3`
/// loads. The loads are unaligned, so the slice's position on the page
/// does not matter.
#[inline]
pub(crate) fn is_ascii(data: &[u8]) -> bool {
    let mut acc = 0u64;
    let mut rest = data;
    while let Some((word, tail)) = rest.split_first_chunk::<8>() {
        acc |= u64::from_ne_bytes(*word);
        rest = tail;
    }
    if let Some((word, tail)) = rest.split_first_chunk::<4>() {
        acc |= u64::from(u32::from_ne_bytes(*word));
        rest = tail;
    }
    if let Some((word, tail)) = rest.split_first_chunk::<2>() {
        acc |= u64::from(u16::from_ne_bytes(*word));
        rest = tail;
    }
    if let Some(&byte) = rest.first() {
        acc |= u64::from(byte);
    }
    acc & 0x8080_8080_8080_8080 == 0
}

#[derive(Debug, Clone, Copy)]
pub struct TextRef<'a> {
    pub value: &'a str,
    pub subtype: TextSubtype,
}

impl<'a> TextRef<'a> {
    pub fn new(value: &'a str, subtype: TextSubtype) -> Self {
        Self { value, subtype }
    }

    #[inline]
    pub fn as_str(&self) -> &'a str {
        self.value
    }
}

impl<'a> Borrow<str> for TextRef<'a> {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<'a> Deref for TextRef<'a> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

pub trait Extendable<T> {
    fn do_extend(&mut self, other: &T) -> Result<()>;
}

/// Copies non-overlapping bytes while keeping common small lengths visible to the optimizer.
///
/// # Safety
///
/// `src` and `dst` must be valid for `len` bytes and must not overlap.
#[inline(always)]
unsafe fn copy_nonoverlapping_inline(src: *const u8, dst: *mut u8, len: usize) {
    // Record decoding frequently reuses registers for short values. Fixed-size
    // copies compile inline instead of calling the platform memcpy routine.
    unsafe {
        match len {
            0 => {}
            1 => std::ptr::copy_nonoverlapping(src, dst, 1),
            2 => std::ptr::copy_nonoverlapping(src, dst, 2),
            3 => std::ptr::copy_nonoverlapping(src, dst, 3),
            4 => std::ptr::copy_nonoverlapping(src, dst, 4),
            5 => std::ptr::copy_nonoverlapping(src, dst, 5),
            6 => std::ptr::copy_nonoverlapping(src, dst, 6),
            7 => std::ptr::copy_nonoverlapping(src, dst, 7),
            8 => std::ptr::copy_nonoverlapping(src, dst, 8),
            9 => std::ptr::copy_nonoverlapping(src, dst, 9),
            10 => std::ptr::copy_nonoverlapping(src, dst, 10),
            11 => std::ptr::copy_nonoverlapping(src, dst, 11),
            12 => std::ptr::copy_nonoverlapping(src, dst, 12),
            13 => std::ptr::copy_nonoverlapping(src, dst, 13),
            14 => std::ptr::copy_nonoverlapping(src, dst, 14),
            15 => std::ptr::copy_nonoverlapping(src, dst, 15),
            16 => std::ptr::copy_nonoverlapping(src, dst, 16),
            _ => std::ptr::copy_nonoverlapping(src, dst, len),
        }
    }
}

impl<T: AnyText> Extendable<T> for Text {
    #[inline(always)]
    fn do_extend(&mut self, other: &T) -> Result<()> {
        let other_str = other.as_ref();
        match &mut self.value {
            Cow::Owned(s) => {
                let needed = other_str.len();
                if s.capacity() >= needed {
                    // SAFETY: capacity >= needed, source is valid UTF-8
                    turso_debug_assert!(
                        s.as_ptr().wrapping_add(s.len()) <= other_str.as_ptr()
                            || other_str.as_ptr().wrapping_add(other_str.len()) <= s.as_ptr(),
                        "source and destination ranges must not overlap"
                    );
                    unsafe {
                        copy_nonoverlapping_inline(other_str.as_ptr(), s.as_mut_ptr(), needed);
                        s.as_mut_vec().set_len(needed);
                    }
                } else {
                    other_str.clone_into(s);
                }
            }
            Cow::Borrowed(_) => {
                self.value = Cow::Owned(other_str.to_owned());
            }
        }
        self.subtype = other.subtype();
        Ok(())
    }
}

impl<T: AnyBlob> Extendable<T> for ValueBlob {
    #[inline(always)]
    fn do_extend(&mut self, other: &T) -> Result<()> {
        let other_slice = other.as_slice();
        let needed = other_slice.len();
        if self.capacity() >= needed {
            // SAFETY: capacity >= needed
            turso_debug_assert!(
                self.as_ptr().wrapping_add(self.len()) <= other_slice.as_ptr()
                    || other_slice.as_ptr().wrapping_add(other_slice.len()) <= self.as_ptr(),
                "source and destination ranges must not overlap"
            );
            unsafe {
                copy_nonoverlapping_inline(other_slice.as_ptr(), self.as_mut_ptr(), needed);
                self.set_len(needed);
            }
        } else {
            // Reserve before mutation so an allocation failure leaves the old value intact.
            self.try_reserve(needed - self.len())?;
            self.clear();
            self.extend_from_slice(other_slice);
        }
        Ok(())
    }
}

pub trait AnyText: AsRef<str> {
    fn subtype(&self) -> TextSubtype;
}

impl AnyText for Text {
    fn subtype(&self) -> TextSubtype {
        self.subtype
    }
}

impl AnyText for &str {
    fn subtype(&self) -> TextSubtype {
        TextSubtype::Text
    }
}

pub trait AnyBlob {
    fn as_slice(&self) -> &[u8];
}

impl AnyBlob for ValueBlob {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(nightly)]
impl AnyBlob for std::vec::Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AnyBlob for &[u8] {
    fn as_slice(&self) -> &[u8] {
        self
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Text {
            value: value.to_owned().into(),
            subtype: TextSubtype::Text,
        }
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Text {
            value: Cow::from(value),
            subtype: TextSubtype::Text,
        }
    }
}

impl From<Text> for String {
    fn from(value: Text) -> Self {
        value.value.into_owned()
    }
}

// Note: Struct and union values are serialized directly in VDBE instructions
// (MakeArray for structs, op_union_pack for unions) using the SQLite record format for structs
// and [tag_name_len: 1 byte][tag_name: N bytes][record] for unions.
// No intermediate StructValue/UnionValue types are needed — blobs are
// constructed from registers and extracted directly into registers.

/// Owned bytes stored by [`Value::Blob`].
///
/// Stable builds use `std::vec::Vec`; allocator-enabled nightly builds retain
/// [`TursoAllocator`] in the vector type.
pub type ValueBlob = crate::alloc::Vec<u8>;

#[inline]
pub(crate) fn value_blob_from_slice(
    bytes: &[u8],
) -> std::result::Result<ValueBlob, crate::alloc::TryReserveError> {
    bytes.try_to_vec()
}

#[cfg(feature = "serde")]
mod value_blob_serde {
    use super::ValueBlob;
    use crate::alloc::{TursoAllocExt, TursoTryWithCapacityExt, TursoVecExt};
    use serde::de::{Error as _, SeqAccess, Visitor};
    use serde::{Deserializer, Serialize as _, Serializer};
    use std::fmt;

    pub fn serialize<S>(value: &ValueBlob, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_slice().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ValueBlob, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueBlobVisitor;

        impl<'de> Visitor<'de> for ValueBlobVisitor {
            type Value = ValueBlob;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a sequence of bytes")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut value = match sequence.size_hint() {
                    Some(capacity) => {
                        <ValueBlob as TursoTryWithCapacityExt>::try_with_capacity_ext(capacity)
                            .map_err(A::Error::custom)?
                    }
                    None => <ValueBlob as TursoAllocExt>::new(),
                };
                while let Some(byte) = sequence.next_element()? {
                    value.try_push(byte).map_err(A::Error::custom)?;
                }
                Ok(value)
            }
        }

        deserializer.deserialize_seq(ValueBlobVisitor)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    Null,
    Numeric(Numeric),
    Text(Text),
    Blob(#[cfg_attr(feature = "serde", serde(with = "value_blob_serde"))] ValueBlob),
}

impl TryClone for Value {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        match self {
            Self::Null => Ok(Self::Null),
            Self::Numeric(numeric) => Ok(Self::Numeric(*numeric)),
            Self::Text(text) => {
                let mut value = String::new();
                value.try_reserve(text.as_str().len())?;
                value.push_str(text.as_str());
                Ok(Self::Text(Text {
                    value: Cow::Owned(value),
                    subtype: text.subtype,
                }))
            }
            Self::Blob(blob) => Self::from_slice(blob),
        }
    }

    /// Fallibly copies `source` into `self`, reusing the existing Text/Blob
    /// allocation when the variants match, so hot per-row copies are
    /// allocation-free once buffers have grown to the row size. On allocation
    /// failure `self` is left valid but unspecified (an empty Text/Blob).
    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::CloneFrom)]
    fn try_clone_from(&mut self, source: &Self) -> Result<(), Self::Error> {
        match (self, source) {
            (Self::Text(dst), Self::Text(src)) => {
                let src_str = src.as_str();
                match &mut dst.value {
                    Cow::Owned(s) => {
                        s.clear();
                        s.try_reserve(src_str.len())?;
                        s.push_str(src_str);
                    }
                    borrowed => {
                        let mut s = String::new();
                        s.try_reserve(src_str.len())?;
                        s.push_str(src_str);
                        *borrowed = Cow::Owned(s);
                    }
                }
                dst.subtype = src.subtype;
            }
            (Self::Blob(dst), Self::Blob(src)) => {
                dst.clear();
                dst.try_extend(src.iter().copied())?;
            }
            (dst, Self::Null) => *dst = Self::Null,
            (dst, Self::Numeric(n)) => *dst = Self::Numeric(*n),
            (dst, src) => {
                *dst = src.try_clone()?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Numeric(Numeric),
    Text(TextRef<'a>),
    Blob(&'a [u8]),
}

impl Debug for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueRef::Null => write!(f, "Null"),
            ValueRef::Numeric(Numeric::Integer(i)) => f.debug_tuple("Integer").field(i).finish(),
            ValueRef::Numeric(Numeric::Float(float)) => {
                let fval: f64 = (*float).into();
                f.debug_tuple("Float").field(&fval).finish()
            }
            ValueRef::Text(text_ref) => {
                // truncate string to at most 256 chars
                let text = text_ref.as_str();
                let max_len = text.len().min(256);
                f.debug_struct("Text")
                    .field("data", &&text[0..max_len])
                    // Indicates to the developer debugging that the data is truncated for printing
                    .field("truncated", &(text.len() > max_len))
                    .finish()
            }
            ValueRef::Blob(blob) => {
                // truncate blob_slice to at most 32 bytes
                let max_len = blob.len().min(32);
                f.debug_struct("Blob")
                    .field("data", &&blob[0..max_len])
                    // Indicates to the developer debugging that the data is truncated for printing
                    .field("truncated", &(blob.len() > max_len))
                    .finish()
            }
        }
    }
}

pub trait AsValueRef {
    fn as_value_ref(&'_ self) -> ValueRef<'_>;
}

impl<'b> AsValueRef for ValueRef<'b> {
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        *self
    }
}

impl AsValueRef for Value {
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        self.as_ref()
    }
}

impl AsValueRef for &mut Value {
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        self.as_ref()
    }
}

impl<V1, V2> AsValueRef for Either<V1, V2>
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    #[inline]
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        match self {
            Either::Left(left) => left.as_value_ref(),
            Either::Right(right) => right.as_value_ref(),
        }
    }
}

impl<V: AsValueRef> AsValueRef for &V {
    fn as_value_ref(&'_ self) -> ValueRef<'_> {
        (*self).as_value_ref()
    }
}

impl Value {
    pub const fn from_f64(f: f64) -> Self {
        match NonNan::new(f) {
            Some(nn) => Self::Numeric(Numeric::Float(nn)),
            None => Self::Null,
        }
    }

    pub const fn from_i64(i: i64) -> Self {
        Self::Numeric(Numeric::Integer(i))
    }

    pub fn as_ref(&'_ self) -> ValueRef<'_> {
        match self {
            Value::Null => ValueRef::Null,
            Value::Numeric(n) => ValueRef::Numeric(*n),
            Value::Text(v) => ValueRef::Text(TextRef {
                value: &v.value,
                subtype: v.subtype,
            }),
            Value::Blob(v) => ValueRef::Blob(v.as_slice()),
        }
    }

    // A helper function that makes building a text Value easier.
    pub fn build_text(text: impl Into<Cow<'static, str>>) -> Self {
        Self::Text(Text::new(text))
    }

    pub fn to_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(blob) => Some(blob),
            _ => None,
        }
    }

    pub const fn from_blob(data: ValueBlob) -> Self {
        Value::Blob(data)
    }

    #[inline]
    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::FromSlice)]
    pub fn from_slice(data: &[u8]) -> std::result::Result<Self, TryReserveError> {
        Ok(Value::Blob(value_blob_from_slice(data)?))
    }

    pub fn to_text(&self) -> Option<&str> {
        match self {
            Value::Text(t) => Some(t.as_str()),
            _ => None,
        }
    }

    pub const fn as_blob(&self) -> &ValueBlob {
        match self {
            Value::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }

    pub const fn as_blob_mut(&mut self) -> &mut ValueBlob {
        match self {
            Value::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }
    pub fn as_float(&self) -> f64 {
        match self {
            Value::Numeric(Numeric::Float(f)) => f64::from(*f),
            Value::Numeric(Numeric::Integer(i)) => *i as f64,
            _ => panic!("as_float must be called only for Value::Numeric"),
        }
    }

    pub fn to_float_or_zero(&self) -> f64 {
        match self {
            Value::Numeric(Numeric::Float(f)) => f64::from(*f),
            Value::Numeric(Numeric::Integer(i)) => *i as f64,
            _ => 0.0,
        }
    }

    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Value::Numeric(Numeric::Integer(i)) => Some(*i),
            _ => None,
        }
    }

    pub const fn as_uint(&self) -> u64 {
        match self {
            Value::Numeric(Numeric::Integer(i)) => (*i).cast_unsigned(),
            _ => 0,
        }
    }

    pub fn from_text(text: impl Into<Cow<'static, str>>) -> Self {
        Value::Text(Text::new(text))
    }

    pub const fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Numeric(Numeric::Integer(_)) => ValueType::Integer,
            Value::Numeric(Numeric::Float(_)) => ValueType::Float,
            Value::Text(_) => ValueType::Text,
            Value::Blob(_) => ValueType::Blob,
        }
    }
    pub fn serialize_serial(&self, out: &mut std::vec::Vec<u8>) {
        match self {
            Value::Null => {}
            Value::Numeric(Numeric::Integer(i)) => {
                let serial_type = SerialType::from(self);
                match serial_type.kind() {
                    SerialTypeKind::I8 => out.extend_from_slice(&(*i as i8).to_be_bytes()),
                    SerialTypeKind::I16 => out.extend_from_slice(&(*i as i16).to_be_bytes()),
                    SerialTypeKind::I24 => out.extend_from_slice(&(*i as i32).to_be_bytes()[1..]), // remove most significant byte
                    SerialTypeKind::I32 => out.extend_from_slice(&(*i as i32).to_be_bytes()),
                    SerialTypeKind::I48 => out.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                    SerialTypeKind::I64 => out.extend_from_slice(&i.to_be_bytes()),
                    _ => unreachable!(),
                }
            }
            Value::Numeric(Numeric::Float(f)) => {
                let fval: f64 = (*f).into();
                out.extend_from_slice(&fval.to_be_bytes());
            }
            Value::Text(t) => out.extend_from_slice(t.value.as_bytes()),
            Value::Blob(b) => out.extend_from_slice(b),
        };
    }

    /// Cast Value to String, if Value is NULL returns None
    pub fn cast_text(&self) -> Option<String> {
        Some(match self {
            Value::Null => return None,
            v => v.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalAggState {
    pub context: usize,
    pub state: *mut AggCtx,
    pub argc: usize,
    pub step_fn: StepFunction,
    pub finalize_fn: FinalizeFunction,
    pub aggregate_destructor: Option<ContextDestructor>,
    pub value_destructor: Option<ValueDestructor>,
}

/// Please use Display trait for all limbo output so we have single origin of truth
/// When you need value as string:
/// ---GOOD---
/// format!("{}", value);
/// ---BAD---
/// match value {
///   Value::Numeric(Numeric::Integer(i)) => i.to_string(),
///   Value::Numeric(Numeric::Float(f)) => f64::from(*f).to_string(),
///   ....
/// }
impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, ""),
            Self::Numeric(Numeric::Integer(i)) => write!(f, "{i}"),
            Self::Numeric(Numeric::Float(fl)) => f.write_str(&format_float(f64::from(*fl))),
            Self::Text(s) => write!(f, "{}", s.as_str()),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

impl Value {
    pub fn to_ffi(&self) -> ExtValue {
        match self {
            Self::Null => ExtValue::null(),
            Self::Numeric(Numeric::Integer(i)) => ExtValue::from_integer(*i),
            Self::Numeric(Numeric::Float(fl)) => ExtValue::from_float(f64::from(*fl)),
            Self::Text(text) => ExtValue::from_text(text.as_str().to_string()),
            Self::Blob(blob) => ExtValue::from_blob(blob.to_vec()),
        }
    }

    pub(crate) fn from_ffi_ref(v: &ExtValue) -> Result<Self> {
        match v.value_type() {
            ExtValueType::Null => Ok(Value::Null),
            ExtValueType::Integer => {
                let Some(int) = v.to_integer() else {
                    return Ok(Value::Null);
                };
                Ok(Value::from_i64(int))
            }
            ExtValueType::Float => {
                let Some(float) = v.to_float() else {
                    return Ok(Value::Null);
                };
                Ok(Value::from_f64(float))
            }
            ExtValueType::Text => {
                let Some(text) = v.to_text() else {
                    return Ok(Value::Null);
                };
                #[cfg(feature = "json")]
                if v.is_json() {
                    return Ok(Value::Text(Text::json(text.to_string())));
                }
                Ok(Value::build_text(text.to_string()))
            }
            ExtValueType::Blob => {
                let Some(blob) = v.to_blob() else {
                    return Ok(Value::Null);
                };
                Ok(Value::from_slice(&blob)?)
            }
            ExtValueType::Error => {
                let Some(err) = v.to_error_details() else {
                    return Ok(Value::Null);
                };
                match err {
                    (_, Some(msg)) => Err(LimboError::ExtensionError(msg)),
                    (code, None) => Err(LimboError::ExtensionError(code.to_string())),
                }
            }
        }
    }

    pub fn from_ffi(v: ExtValue) -> Result<Self> {
        let res = Self::from_ffi_ref(&v);
        unsafe { v.__free_internal_type() };
        res
    }
}

/// Convert a `Value` into the implementors type.
pub trait FromValue: Sealed {
    fn from_sql(val: Value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for Value {
    fn from_sql(val: Value) -> Result<Self> {
        Ok(val)
    }
}
impl Sealed for crate::Value {}

macro_rules! impl_int_from_value {
    ($ty:ty) => {
        impl FromValue for $ty {
            fn from_sql(val: Value) -> Result<Self> {
                match val {
                    Value::Null => Err(LimboError::NullValue),
                    Value::Numeric(Numeric::Integer(i)) => {
                        <$ty>::try_from(i).map_err(|_| LimboError::IntegerOverflow)
                    }
                    _ => Err(LimboError::InvalidColumnType),
                }
            }
        }

        impl Sealed for $ty {}
    };
}

impl_int_from_value!(i32);
impl_int_from_value!(u32);
impl_int_from_value!(i64);
impl_int_from_value!(u64);

impl FromValue for f64 {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Numeric(Numeric::Float(f)) => Ok(f64::from(f)),
            Value::Numeric(Numeric::Integer(i)) => Ok(i as f64),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl Sealed for f64 {}

impl FromValue for ValueBlob {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Blob(blob) => Ok(blob),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl Sealed for ValueBlob {}

impl<const N: usize> FromValue for [u8; N] {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Blob(blob) => blob
                .as_slice()
                .try_into()
                .map_err(|_| LimboError::InvalidBlobSize(N)),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl<const N: usize> Sealed for [u8; N] {}

impl FromValue for String {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Text(s) => Ok(s.to_string()),
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl Sealed for String {}

impl FromValue for bool {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(LimboError::NullValue),
            Value::Numeric(Numeric::Integer(i)) => match i {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(LimboError::InvalidColumnType),
            },
            _ => Err(LimboError::InvalidColumnType),
        }
    }
}
impl Sealed for bool {}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Ok(None),
            _ => T::from_sql(val).map(Some),
        }
    }
}
impl<T> Sealed for Option<T> {}

mod sealed {
    pub trait Sealed {}
}
#[allow(unused_imports)] //used in doc comments
use crate::vdbe::insn::Insn;
use sealed::Sealed;

#[derive(Debug, Clone, PartialEq)]
pub struct SumAggState {
    pub r_err: f64,   // Error term for Kahan-Babushka-Neumaier summation
    pub approx: bool, // True if any non-integer value was input to the sum
    pub ovrfl: bool,  // Integer overflow seen
}
impl Default for SumAggState {
    fn default() -> Self {
        Self {
            r_err: 0.0,
            approx: false,
            ovrfl: false,
        }
    }
}

/// Aggregate context for accumulating values during GROUP BY.
/// Built-in aggregates use a flat payload representation for efficiency and
/// to share code between register-based and hash-based aggregation (future enhancement).
#[derive(Debug, Clone, PartialEq)]
pub enum AggContext {
    /// Built-in aggregates store state as a flat Vec<Value> payload.
    /// The layout depends on the aggregate function (see init_agg_payload).
    Builtin(Vec<Value>),
    /// External (extension) aggregates need FFI state that can't be serialized.
    External(ExternalAggState),
}

impl TryClone for AggContext {
    type Error = TryReserveError;

    /// Fallible clone: the builtin payload's Vec and each contained Text/Blob
    /// go through fallible reservation. External state holds only FFI
    /// pointers and copies without allocating.
    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::CloneFrom)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        match self {
            Self::Builtin(payload) => {
                let mut values = Vec::try_with_capacity_ext(payload.len())?;
                for value in payload {
                    let mut copy = Value::Null;
                    copy.try_clone_from(value)?;
                    values.push(copy);
                }
                Ok(Self::Builtin(values))
            }
            Self::External(_) => Ok(self.clone()),
        }
    }
}

impl AggContext {
    pub fn compute_external(&self) -> Result<Value> {
        if let Self::External(ext_state) = self {
            let mut final_value =
                unsafe { (ext_state.finalize_fn)(ext_state.context, ext_state.state) };
            let value = Value::from_ffi_ref(&final_value);
            if let Some(value_destructor) = ext_state.value_destructor {
                unsafe { value_destructor(&mut final_value) };
            } else {
                unsafe { final_value.__free_internal_type() };
            }
            if let Some(aggregate_destructor) = ext_state.aggregate_destructor {
                unsafe { aggregate_destructor(ext_state.state as usize) };
            }
            value
        } else {
            panic!("AggContext::compute_external() expected External, found {self:?}");
        }
    }

    /// Get a mutable reference to the builtin payload as a slice
    pub fn payload_mut(&mut self) -> &mut [Value] {
        match self {
            Self::Builtin(payload) => payload,
            Self::External(_) => panic!("payload_mut() called on External aggregate"),
        }
    }

    /// Get a mutable reference to the builtin payload Vec (for aggregates that
    /// grow the payload, e.g. array_agg).
    pub fn payload_vec_mut(&mut self) -> &mut Vec<Value> {
        match self {
            Self::Builtin(payload) => payload,
            Self::External(_) => panic!("payload_vec_mut() called on External aggregate"),
        }
    }

    /// Get an immutable reference to the builtin payload
    pub fn payload(&self) -> &[Value] {
        match self {
            Self::Builtin(payload) => payload,
            Self::External(_) => panic!("payload() called on External aggregate"),
        }
    }
}

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        let (left, right) = (self.as_value_ref(), other.as_value_ref());
        left.eq(&right)
    }
}

impl PartialOrd<Value> for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd<AggContext> for AggContext {
    fn partial_cmp(&self, other: &AggContext) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Builtin(a), Self::Builtin(b)) => {
                // Compare by first element (the accumulator) if present
                match (a.first(), b.first()) {
                    (Some(a), Some(b)) => a.partial_cmp(b),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let (left, right) = (self.as_value_ref(), other.as_value_ref());
        left.cmp(&right)
    }
}

impl std::ops::Add<Value> for Value {
    type Output = Value;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::ops::Add<f64> for Value {
    type Output = Value;

    fn add(mut self, rhs: f64) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::ops::Add<i64> for Value {
    type Output = Value;

    fn add(mut self, rhs: i64) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::ops::AddAssign for Value {
    fn add_assign(mut self: &mut Self, rhs: Self) {
        match (&mut self, &rhs) {
            (Self::Numeric(_), Self::Numeric(_)) => {
                let sum = (|| {
                    let lhs_num = Numeric::from_value(&self)?;
                    let rhs_num = Numeric::from_value(&rhs)?;
                    lhs_num.checked_add(rhs_num)
                })();
                *self = sum.into();
            }
            (Self::Text(string_left), Self::Text(string_right)) => {
                string_left.value.to_mut().push_str(&string_right.value);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Text(string_left), Self::Numeric(Numeric::Integer(int_right))) => {
                let string_right = int_right.to_string();
                string_left.value.to_mut().push_str(&string_right);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Numeric(Numeric::Integer(int_left)), Self::Text(string_right)) => {
                let string_left = int_left.to_string();
                *self = Self::build_text(string_left + string_right.as_str());
            }
            (Self::Text(string_left), Self::Numeric(Numeric::Float(_))) => {
                let string_right = rhs.to_string();
                string_left.value.to_mut().push_str(&string_right);
                string_left.subtype = TextSubtype::Text;
            }
            (Self::Numeric(Numeric::Float(_)), Self::Text(string_right)) => {
                let string_left = self.to_string();
                *self = Self::build_text(string_left + string_right.as_str());
            }
            (_, Self::Null) => {}
            (Self::Null, _) => *self = rhs,
            _ => *self = Self::from_f64(0.0),
        }
    }
}

impl std::ops::AddAssign<i64> for Value {
    fn add_assign(&mut self, rhs: i64) {
        let sum = (|| {
            let lhs_num = Numeric::from_value(&self)?;
            let rhs_num = Numeric::Integer(rhs);
            lhs_num.checked_add(rhs_num)
        })();
        *self = sum.into();
    }
}

impl std::ops::AddAssign<f64> for Value {
    fn add_assign(&mut self, rhs: f64) {
        let sum = (|| {
            let lhs_num = Numeric::from_value(&self)?;
            let rhs_num = NonNan::new(rhs).map(Numeric::Float)?;
            lhs_num.checked_add(rhs_num)
        })();

        *self = sum.into();
    }
}

impl std::ops::Div<Value> for Value {
    type Output = Value;

    fn div(self, rhs: Value) -> Self::Output {
        let div = (|| {
            let lhs_num = Numeric::from_value(self)?;
            let rhs_num = Numeric::from_value(rhs)?;
            lhs_num.checked_div(rhs_num)
        })();
        div.into()
    }
}

impl std::ops::DivAssign<Value> for Value {
    fn div_assign(&mut self, rhs: Value) {
        *self = self.clone() / rhs;
    }
}

impl TryFrom<ValueRef<'_>> for Value {
    type Error = TryReserveError;

    fn try_from(value: ValueRef<'_>) -> std::result::Result<Self, Self::Error> {
        value.to_owned()
    }
}

impl TryFrom<ValueRef<'_>> for i64 {
    type Error = LimboError;

    fn try_from(value: ValueRef<'_>) -> Result<Self, Self::Error> {
        match value {
            ValueRef::Numeric(Numeric::Integer(i)) => Ok(i),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl TryFrom<ValueRef<'_>> for String {
    type Error = LimboError;

    #[inline]
    fn try_from(value: ValueRef<'_>) -> Result<Self, Self::Error> {
        Ok(<&str>::try_from(value)?.to_string())
    }
}

impl<'a> TryFrom<ValueRef<'a>> for &'a str {
    type Error = LimboError;

    #[inline]
    fn try_from(value: ValueRef<'a>) -> Result<Self, Self::Error> {
        match value {
            ValueRef::Text(s) => Ok(s.as_str()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

mod immutable_record {
    use super::*;

    /// This struct serves the purpose of not allocating multiple vectors of bytes if not needed.
    /// A value in a record that has already been serialized can stay serialized and what this struct offsers
    /// is easy acces to each value which point to the payload.
    /// The name might be contradictory as it is immutable in the sense that you cannot modify the values without modifying the payload.
    pub struct ImmutableRecord {
        // We have to be super careful with this buffer since we make values point to the payload we need to take care reallocations
        // happen in a controlled manner. If we realocate with values that should be correct, they will now point to undefined data.
        // We don't use pin here because it would make it imposible to reuse the buffer if we need to push a new record in the same struct.
        //
        // payload is the std::vec::Vec<u8> but in order to use Register which holds ImmutableRecord as a Value - we store std::vec::Vec<u8> as Value::Blob
        payload: Value,
    }

    // SAFETY: all ImmutableRecord instances are intended to be used in a single thread
    // by a single connection.
    unsafe impl Send for ImmutableRecord {}
    unsafe impl Sync for ImmutableRecord {}

    impl Clone for ImmutableRecord {
        fn clone(&self) -> Self {
            Self {
                payload: self.payload.clone(),
            }
        }
    }

    impl PartialEq for ImmutableRecord {
        fn eq(&self, other: &Self) -> bool {
            self.payload == other.payload // Only compare payload, ignore cursor state
        }
    }

    impl Eq for ImmutableRecord {}

    impl PartialOrd for ImmutableRecord {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for ImmutableRecord {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.payload.cmp(&other.payload) // Only compare payload, ignore cursor state
        }
    }

    impl std::fmt::Debug for ImmutableRecord {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match &self.payload {
                Value::Blob(bytes) => {
                    let preview = if bytes.len() > 20 {
                        format!("{:?} ... ({} bytes total)", &bytes[..20], bytes.len())
                    } else {
                        format!("{bytes:?}")
                    };
                    write!(f, "ImmutableRecord {{ payload: {preview} }}")
                }
                Value::Text(s) => {
                    let string = s.as_str();
                    let preview = if string.len() > 20 {
                        format!("{:?} ... ({} chars total)", &string[..20], string.len())
                    } else {
                        format!("{string:?}")
                    };
                    write!(f, "ImmutableRecord {{ payload: {preview} }}")
                }
                other => write!(f, "ImmutableRecord {{ payload: {other:?} }}"),
            }
        }
    }

    #[derive(Clone)]
    pub struct ImmutableRecordRef<'a> {
        payload: ImmutableRecordRefPayload<'a>,
    }

    #[derive(Clone)]
    enum ImmutableRecordRefPayload<'a> {
        Borrowed(&'a [u8]),
        Owned(ValueBlob),
        Shared(crate::alloc::ArcSlice<u8>),
    }

    impl std::fmt::Debug for ImmutableRecordRef<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let bytes = self.get_payload();
            let preview = if bytes.len() > 20 {
                format!("{:?} ... ({} bytes total)", &bytes[..20], bytes.len())
            } else {
                format!("{bytes:?}")
            };
            write!(f, "ImmutableRecordRef {{ payload: {preview} }}")
        }
    }

    impl PartialEq for ImmutableRecordRef<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.get_payload() == other.get_payload()
        }
    }

    impl Eq for ImmutableRecordRef<'_> {}

    impl AsRef<[u8]> for ImmutableRecordRef<'_> {
        fn as_ref(&self) -> &[u8] {
            self.get_payload()
        }
    }

    impl AsRef<[u8]> for ImmutableRecord {
        fn as_ref(&self) -> &[u8] {
            self.get_payload()
        }
    }

    /// [`write_varint`] with the one-byte case inline.
    #[inline(always)]
    fn write_short_varint(out: &mut [u8], value: u64) -> usize {
        if value <= 0x7f {
            out[0] = value as u8;
            return 1;
        }
        write_varint(out, value)
    }

    /// Writes the bytes of `value` for `serial_type` at the start of `out`
    /// and returns how many it wrote.
    #[inline(always)]
    fn write_value(out: &mut [u8], value: ValueRef<'_>, serial_type: SerialType) -> usize {
        // Numbers have a size known per serial type, so each arm stores its
        // bytes with a copy of constant length; a copy of variable length is
        // a memcpy call for every number of every built record.
        let bytes: &[u8] = match value {
            ValueRef::Null => return 0,
            ValueRef::Numeric(Numeric::Integer(i)) => {
                return match serial_type.kind() {
                    SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 => 0,
                    SerialTypeKind::I8 => {
                        out[0] = i as u8;
                        1
                    }
                    SerialTypeKind::I16 => {
                        out[..2].copy_from_slice(&(i as i16).to_be_bytes());
                        2
                    }
                    // Without the most significant byte.
                    SerialTypeKind::I24 => {
                        out[..3].copy_from_slice(&(i as i32).to_be_bytes()[1..]);
                        3
                    }
                    SerialTypeKind::I32 => {
                        out[..4].copy_from_slice(&(i as i32).to_be_bytes());
                        4
                    }
                    // Without the two most significant bytes.
                    SerialTypeKind::I48 => {
                        out[..6].copy_from_slice(&i.to_be_bytes()[2..]);
                        6
                    }
                    SerialTypeKind::I64 => {
                        out[..8].copy_from_slice(&i.to_be_bytes());
                        8
                    }
                    other => panic!("Serial type is not an integer: {other:?}"),
                };
            }
            ValueRef::Numeric(Numeric::Float(f)) => {
                let fval: f64 = f.into();
                out[..8].copy_from_slice(&fval.to_be_bytes());
                return 8;
            }
            ValueRef::Text(t) => t.value.as_bytes(),
            ValueRef::Blob(b) => b,
        };
        out[..bytes.len()].copy_from_slice(bytes);
        bytes.len()
    }

    #[inline(always)]
    fn iter(payload: &[u8]) -> Result<ValueIterator<'_>, LimboError> {
        ValueIterator::new(payload)
    }

    fn values(payload: &[u8]) -> Result<Vec<ValueRef<'_>>> {
        let iter = iter(payload)?;
        let values = iter.try_collect::<Result<_>>()??;
        Ok(values)
    }

    fn values_range(payload: &[u8], range: std::ops::Range<usize>) -> Result<Vec<ValueRef<'_>>> {
        let mut iter = iter(payload)?;
        let mut values = Vec::try_with_capacity_ext(range.end - range.start)?;
        if let Some(value) = iter.nth(range.start) {
            values.push(value?);
        } else {
            return Ok(values);
        }
        for _ in range.start + 1..range.end {
            if let Some(value) = iter.next() {
                values.push(value?);
            } else {
                break;
            }
        }
        Ok(values)
    }

    fn two_values(
        payload: &[u8],
        idx1: usize,
        idx2: usize,
    ) -> Result<(ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = iter(payload)?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1);
        match (val1, val2) {
            (Some(v1), Some(v2)) => Ok((v1?, v2?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    fn three_values(
        payload: &[u8],
        idx1: usize,
        idx2: usize,
        idx3: usize,
    ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = iter(payload)?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1);
        let val3 = iter.nth(idx3 - idx2 - 1);
        match (val1, val2, val3) {
            (Some(v1), Some(v2), Some(v3)) => Ok((v1?, v2?, v3?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    fn four_values(
        payload: &[u8],
        idx1: usize,
        idx2: usize,
        idx3: usize,
        idx4: usize,
    ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = iter(payload)?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1);
        let val3 = iter.nth(idx3 - idx2 - 1);
        let val4 = iter.nth(idx4 - idx3 - 1);
        match (val1, val2, val3, val4) {
            (Some(v1), Some(v2), Some(v3), Some(v4)) => Ok((v1?, v2?, v3?, v4?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    fn values_owned(payload: &[u8]) -> Result<Vec<Value>> {
        let iter = iter(payload).expect("Failed to create payload iterator");
        let values = iter
            .map(|v| Ok::<_, LimboError>(v?.to_owned()?))
            .try_collect::<Result<_>>()??;
        Ok(values)
    }

    fn values_owned_range(payload: &[u8], range: std::ops::Range<usize>) -> Result<Vec<Value>> {
        let mut iter = iter(payload).expect("Failed to create payload iterator");
        let mut values = Vec::try_with_capacity_ext(range.end - range.start)?;
        if let Some(value) = iter.nth(range.start) {
            values.push(value?.to_owned()?);
        } else {
            return Ok(values);
        }
        for _ in range.start + 1..range.end {
            if let Some(value) = iter.next() {
                values.push(value?.to_owned()?);
            } else {
                break;
            }
        }
        Ok(values)
    }

    fn contains_null(payload: &[u8]) -> Result<bool> {
        let (header_size, header_varint_len) = read_varint(payload)?;
        let header_size = header_size as usize;

        if header_size > payload.len() || header_varint_len > payload.len() {
            return Err(LimboError::Corrupt(
                "Payload too small for indicated header size".into(),
            ));
        }

        let mut header = &payload[header_varint_len..header_size];

        while !header.is_empty() {
            let (serial_type, bytes_read) = read_varint(header)?;
            if serial_type == 0 {
                return Ok(true);
            }
            header = &header[bytes_read..];
        }

        Ok(false)
    }

    fn last_value(payload: &[u8]) -> Option<Result<ValueRef<'_>>> {
        if unlikely(payload.is_empty()) {
            return Some(Err(LimboError::InternalError(
                "Record is invalidated".into(),
            )));
        }
        let iter = match iter(payload) {
            Ok(it) => it,
            Err(e) => return Some(Err(e)),
        };
        iter.last()
    }

    fn first_value(payload: &[u8]) -> Result<ValueRef<'_>> {
        if unlikely(payload.is_empty()) {
            return Err(LimboError::InternalError("Record is invalidated".into()));
        }
        match iter(payload)?.next() {
            Some(v) => v,
            None => Err(LimboError::InternalError("Record has no columns".into())),
        }
    }

    fn value(payload: &[u8], idx: usize) -> Result<ValueRef<'_>> {
        if unlikely(payload.is_empty()) {
            return Err(LimboError::InternalError("Record is invalidated".into()));
        }
        let mut iter = iter(payload)?;
        iter.nth(idx)
            .transpose()?
            .ok_or_else(|| LimboError::InternalError("Index out of bounds".into()))
    }

    fn value_opt(payload: &[u8], idx: usize) -> Option<ValueRef<'_>> {
        let mut iter = match iter(payload) {
            Ok(it) => it,
            Err(_) => {
                mark_unlikely();
                return None;
            }
        };
        match iter.nth(idx) {
            Some(Ok(v)) => Some(v),
            _ => {
                mark_unlikely();
                None
            }
        }
    }

    fn column_count(payload: &[u8]) -> usize {
        iter(payload).map(|it| it.count()).unwrap_or_default()
    }

    /// A spent record buffer, obtained by retiring a record ([ImmutableRecord::retire]).
    /// Hot-path record construction requires one, so per-row callers either recycle a
    /// previous record's allocation or go through the single explicit entry point,
    /// [RecordBuf::alloc]. The wrapped buffer is always empty; only its capacity carries
    /// over.
    #[must_use = "dropping a RecordBuf discards a reusable allocation"]
    pub struct RecordBuf(ValueBlob);

    impl RecordBuf {
        pub fn alloc() -> Self {
            Self(crate::alloc::vec![])
        }
    }

    impl ImmutableRecord {
        // Don't use this in performance critical paths, prefer using `iter()` instead
        pub fn get_values(&self) -> Result<Vec<ValueRef<'_>>> {
            values(self.get_payload())
        }

        // Don't use this in performance critical paths, prefer using `iter()` instead
        pub fn get_values_range(&self, range: std::ops::Range<usize>) -> Result<Vec<ValueRef<'_>>> {
            values_range(self.get_payload(), range)
        }

        // Idx values must be sorted ascending
        pub fn get_two_values(
            &self,
            idx1: usize,
            idx2: usize,
        ) -> Result<(ValueRef<'_>, ValueRef<'_>)> {
            two_values(self.get_payload(), idx1, idx2)
        }

        // Idx values must be sorted ascending
        pub fn get_three_values(
            &self,
            idx1: usize,
            idx2: usize,
            idx3: usize,
        ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
            three_values(self.get_payload(), idx1, idx2, idx3)
        }

        // Idx values must be sorted ascending
        pub fn get_four_values(
            &self,
            idx1: usize,
            idx2: usize,
            idx3: usize,
            idx4: usize,
        ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
            four_values(self.get_payload(), idx1, idx2, idx3, idx4)
        }

        // Don't use this in performance critical paths, prefer using `iter()` instead
        pub fn get_values_owned(&self) -> Result<Vec<Value>> {
            values_owned(self.get_payload())
        }

        // Don't use this in performance critical paths, prefer using `iter()` instead
        pub fn get_values_owned_range(&self, range: std::ops::Range<usize>) -> Result<Vec<Value>> {
            values_owned_range(self.get_payload(), range)
        }

        #[inline(always)]
        pub fn iter(&self) -> Result<ValueIterator<'_>, LimboError> {
            iter(self.get_payload())
        }

        #[inline]
        /// Returns true if the record contains any NULL values.
        /// This is an optimization that only examines the header (serial types)
        /// without deserializing the data section.
        pub fn contains_null(&self) -> Result<bool> {
            contains_null(self.get_payload())
        }

        #[inline]
        pub fn last_value(&self) -> Option<Result<ValueRef<'_>>> {
            last_value(self.get_payload())
        }

        #[inline]
        pub fn first_value(&self) -> Result<ValueRef<'_>> {
            first_value(self.get_payload())
        }

        #[inline]
        pub fn get_value(&self, idx: usize) -> Result<ValueRef<'_>> {
            value(self.get_payload(), idx)
        }

        #[inline]
        pub fn get_value_opt(&self, idx: usize) -> Option<ValueRef<'_>> {
            value_opt(self.get_payload(), idx)
        }

        pub fn column_count(&self) -> usize {
            column_count(self.get_payload())
        }
    }

    impl<'a> ImmutableRecordRef<'a> {
        #[inline(always)]
        pub fn iter(&self) -> Result<ValueIterator<'_>, LimboError> {
            iter(self.get_payload())
        }

        pub fn get_values(&self) -> Result<Vec<ValueRef<'_>>> {
            values(self.get_payload())
        }

        pub fn get_two_values(
            &self,
            idx1: usize,
            idx2: usize,
        ) -> Result<(ValueRef<'_>, ValueRef<'_>)> {
            two_values(self.get_payload(), idx1, idx2)
        }

        pub fn get_three_values(
            &self,
            idx1: usize,
            idx2: usize,
            idx3: usize,
        ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
            three_values(self.get_payload(), idx1, idx2, idx3)
        }

        pub fn get_values_owned(&self) -> Result<Vec<Value>> {
            values_owned(self.get_payload())
        }

        #[inline]
        pub fn contains_null(&self) -> Result<bool> {
            contains_null(self.get_payload())
        }

        #[inline]
        pub fn last_value(&self) -> Option<Result<ValueRef<'_>>> {
            last_value(self.get_payload())
        }

        #[inline]
        pub fn get_value_opt(&self, idx: usize) -> Option<ValueRef<'_>> {
            value_opt(self.get_payload(), idx)
        }

        pub fn column_count(&self) -> usize {
            column_count(self.get_payload())
        }
    }

    impl ImmutableRecord {
        pub fn new(payload_capacity: usize) -> Result<Self> {
            let mut payload = crate::alloc::vec![];
            payload.try_reserve_exact(payload_capacity)?;
            Ok(Self {
                payload: Value::Blob(payload),
            })
        }

        /// Consumes the record, keeping its allocation for reuse.
        pub fn retire(self) -> RecordBuf {
            let mut buf = self.into_payload();
            buf.clear();
            RecordBuf(buf)
        }

        /// An invalidated record backed by `buf`'s allocation.
        pub fn from_buf(buf: RecordBuf) -> Self {
            Self {
                payload: Value::Blob(buf.0),
            }
        }

        /// A record holding a copy of `payload`, serialized into `buf`.
        #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::RecordCopy)]
        pub fn copy_payload(payload: &[u8], buf: RecordBuf) -> Result<Self, TryReserveError> {
            let RecordBuf(mut buf) = buf;
            buf.try_extend(payload.iter().copied())?;
            Ok(Self {
                payload: Value::Blob(buf),
            })
        }

        pub const fn from_bin_record(payload: ValueBlob) -> Self {
            Self {
                payload: Value::Blob(payload),
            }
        }

        pub fn as_record_ref(&self) -> ImmutableRecordRef<'_> {
            ImmutableRecordRef::from_bin_record(self.get_payload())
        }

        pub fn from_registers<'a, I: Iterator<Item = &'a Register> + Clone>(
            // we need to accept both &[Register] and &[&Register] values - that's why non-trivial signature
            //
            // std::slice::Iter under the hood just stores pointer and length of slice and also implements a Clone which just copy those meta-values
            // (without copying the data itself)
            registers: impl IntoIterator<Item = &'a Register, IntoIter = I>,
            len: usize,
        ) -> Result<Self> {
            Self::from_values(registers.into_iter().map(|x| x.get_value()), len)
        }

        /// Like [Self::from_registers], but serializes into `buf`; see [Self::build].
        #[inline]
        pub fn build_from_registers<'a, I: Iterator<Item = &'a Register> + Clone>(
            registers: impl IntoIterator<Item = &'a Register, IntoIter = I>,
            buf: RecordBuf,
        ) -> Result<Self> {
            Self::build(registers.into_iter().map(|x| x.get_value()), buf)
        }

        pub fn from_values<'a>(
            values: impl IntoIterator<Item = impl AsValueRef + 'a> + Clone,
            _len: usize,
        ) -> Result<Self> {
            Self::build(values, RecordBuf::alloc())
        }

        /// Serializes `values` into `buf`, allocating only when the buffer's
        /// capacity is insufficient. Per-row callers recycle the destination
        /// register's previous record buffer, making steady-state record
        /// construction allocation-free.
        #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::RecordBuild)]
        #[inline]
        pub fn build<'a>(
            values: impl IntoIterator<Item = impl AsValueRef + 'a> + Clone,
            buf: RecordBuf,
        ) -> Result<Self> {
            let RecordBuf(mut buf) = buf;
            let mut size_header = 0;
            let mut size_values = 0;

            // Sizing pass: the cloneable iterator is walked again below to
            // write the serial types and the values.
            for value in values.clone() {
                let serial_type = SerialType::from(value.as_value_ref());
                size_header += varint_len(serial_type.into());
                size_values += serial_type.size();
            }

            let header_size = Record::calc_header_size(size_header);
            let total_size = header_size + size_values;
            buf.try_reserve_exact(total_size)?;
            buf.resize(total_size, 0);

            // Writing pass: each serial type goes into the header and each
            // value after it, the varints straight into their place.
            let mut header_pos = write_short_varint(&mut buf[..header_size], header_size as u64);
            let mut value_pos = header_size;
            for value in values {
                let value = value.as_value_ref();
                let serial_type = SerialType::from(value);
                header_pos +=
                    write_short_varint(&mut buf[header_pos..header_size], serial_type.into());
                value_pos += write_value(&mut buf[value_pos..], value, serial_type);
            }
            crate::turso_assert!(
                header_pos == header_size && value_pos == total_size,
                "record sizing pass and writing pass disagree"
            );
            Ok(Self {
                payload: Value::Blob(buf),
            })
        }

        #[inline]
        pub fn into_payload(self) -> ValueBlob {
            match self.payload {
                Value::Blob(b) => b,
                _ => panic!("payload must be a blob"),
            }
        }

        #[inline]
        pub const fn as_blob(&self) -> &ValueBlob {
            match &self.payload {
                Value::Blob(b) => b,
                _ => panic!("payload must be a blob"),
            }
        }

        #[inline]
        pub const fn as_blob_mut(&mut self) -> &mut ValueBlob {
            match &mut self.payload {
                Value::Blob(b) => b,
                _ => panic!("payload must be a blob"),
            }
        }

        #[inline]
        pub const fn as_blob_value(&self) -> &Value {
            &self.payload
        }

        #[inline]
        #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::RecordCopy)]
        pub fn start_serialization(&mut self, payload: &[u8]) -> Result<()> {
            let blob = self.as_blob_mut();
            blob.try_reserve(payload.len())?;
            blob.extend_from_slice(payload);
            Ok(())
        }

        #[inline]
        pub fn invalidate(&mut self) {
            self.as_blob_mut().clear();
        }

        #[inline]
        pub const fn is_invalidated(&self) -> bool {
            self.as_blob().is_empty()
        }

        #[inline]
        pub fn get_payload(&self) -> &[u8] {
            self.as_blob()
        }
    }

    impl<'a> ImmutableRecordRef<'a> {
        pub const fn from_bin_record(payload: &'a [u8]) -> Self {
            Self {
                payload: ImmutableRecordRefPayload::Borrowed(payload),
            }
        }

        pub(crate) fn from_owned_record(record: ImmutableRecord) -> ImmutableRecordRef<'static> {
            ImmutableRecordRef {
                payload: ImmutableRecordRefPayload::Owned(record.into_payload()),
            }
        }

        pub(crate) fn from_shared_record(
            payload: crate::alloc::ArcSlice<u8>,
        ) -> ImmutableRecordRef<'static> {
            ImmutableRecordRef {
                payload: ImmutableRecordRefPayload::Shared(payload),
            }
        }

        #[inline]
        pub fn get_payload(&self) -> &[u8] {
            match &self.payload {
                ImmutableRecordRefPayload::Borrowed(payload) => payload,
                ImmutableRecordRefPayload::Owned(payload) => payload,
                ImmutableRecordRefPayload::Shared(payload) => payload,
            }
        }

        #[inline]
        pub fn is_invalidated(&self) -> bool {
            self.get_payload().is_empty()
        }

        pub fn reborrow(&self) -> ImmutableRecordRef<'_> {
            ImmutableRecordRef {
                payload: ImmutableRecordRefPayload::Borrowed(self.get_payload()),
            }
        }
    }
}

pub use immutable_record::{ImmutableRecord, ImmutableRecordRef, RecordBuf};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Record {
    values: Vec<Value>,
}

impl Record {
    // pub fn get<'a, T: FromValue<'a> + 'a>(&'a self, idx: usize) -> Result<T> {
    //     let value = &self.values[idx];
    //     T::from_value(value)
    // }

    pub fn count(&self) -> usize {
        self.values.len()
    }

    pub fn last_value(&self) -> Option<&Value> {
        self.values.last()
    }

    pub fn get_values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn get_value(&self, idx: usize) -> &Value {
        &self.values[idx]
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// A zero-allocation iterator over SQLite record payload data.
///
/// This iterator provides efficient, lazy parsing of SQLite records without
/// any heap allocation. It processes record data on-the-fly, returning `ValueRef`
/// instances that borrow directly from the underlying payload.
///
/// # Memory Layout
///
/// SQLite records follow this binary format:
/// ```text
/// [header_size: varint][serial_type1: varint][serial_type2: varint]...
/// [data1][data2][data3]...
/// ```
///
/// - **header_size**: Total bytes in the header section (including this varint)
/// - **serial_typeN**: Encodes the type and size of column N's data
/// - **dataN**: The actual data for column N (length determined by serial_typeN)
pub struct ValueIterator<'a> {
    /// Reference to header section up to data offset
    header_section: Cell<&'a [u8]>,
    /// Reference to data section only
    data_section: Cell<&'a [u8]>,
}

impl<'a> ValueIterator<'a> {
    /// Creates a new payload iterator from a raw payload slice.
    ///
    /// # Arguments
    ///
    /// * `payload` - The serialized SQLite record payload
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` if the header can be parsed, or an error if the
    /// payload is malformed.
    #[inline(always)]
    pub fn new(payload: &'a [u8]) -> Result<Self> {
        let (header_size, header_varint_len) = read_varint(payload)?;
        let header_size = header_size as usize;

        if header_size > payload.len()
            || header_varint_len > payload.len()
            || header_varint_len > header_size
        {
            return Err(LimboError::Corrupt(
                "Payload too small for indicated header size".into(),
            ));
        }

        Ok(Self {
            header_section: Cell::new(&payload[header_varint_len..header_size]),
            data_section: Cell::new(&payload[header_size..]),
        })
    }

    /// Returns `true` if the payload is empty or the record has no columns.
    pub const fn is_empty(&self) -> bool {
        self.header_section.get().is_empty()
    }

    /// Returns a reference to the current header section.
    #[inline(always)]
    pub const fn header_section_ref(&self) -> &'a [u8] {
        self.header_section.get()
    }

    /// Returns a reference to the current data section.
    #[inline(always)]
    pub const fn data_section_ref(&self) -> &'a [u8] {
        self.data_section.get()
    }

    /// Sets the header section to a new slice.
    #[inline(always)]
    pub fn set_header_section(&self, header: &'a [u8]) {
        self.header_section.set(header);
    }

    /// Sets the data section to a new slice.
    #[inline(always)]
    pub fn set_data_section(&self, data: &'a [u8]) {
        self.data_section.set(data);
    }
}

impl<'a> Iterator for ValueIterator<'a> {
    type Item = Result<ValueRef<'a>, LimboError>;

    #[inline(always)]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        let mut count = 0;
        let mut header = self.header_section.get();
        while !header.is_empty() {
            match read_varint(header) {
                Ok((_, bytes_read)) => {
                    count += 1;
                    header = &header[bytes_read..];
                }
                Err(_) => break,
            }
        }
        count
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut count = 0;
        let mut header = self.header_section.get();
        while !header.is_empty() {
            match read_varint(header) {
                Ok((_, bytes_read)) => {
                    count += 1;
                    header = &header[bytes_read..];
                }
                Err(_) => break,
            }
        }
        (count, Some(count))
    }

    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        for item in self {
            acc = f(acc, item);
        }
        acc
    }

    /// Returns the nth element of the iterator.
    #[inline(always)]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut header = self.header_section.get();
        let mut data = self.data_section.get();

        let mut data_sum = 0;
        for _ in 0..n {
            if unlikely(header.is_empty()) {
                return None;
            }

            let (serial_type, bytes_read) = match read_varint(header) {
                Ok(v) => v,
                Err(e) => {
                    mark_unlikely();
                    return Some(Err(e));
                }
            };
            header = &header[bytes_read..];

            data_sum += match get_serial_type_size(serial_type) {
                Ok(size) => size,
                Err(e) => {
                    mark_unlikely();
                    return Some(Err(e));
                }
            };
        }

        if unlikely(data_sum > data.len()) {
            return Some(Err(LimboError::Corrupt(
                "Data section too small for indicated serial type size".into(),
            )));
        }
        data = &data[data_sum..];

        // Update iterator state
        self.header_section.set(header);
        self.data_section.set(data);

        // Return the nth value
        self.next()
    }

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let header = self.header_section.get();
        if unlikely(header.is_empty()) {
            return None;
        }

        // Read next serial type
        let (serial_type, bytes_read) = match read_varint(header) {
            Ok(v) => v,
            Err(e) => {
                mark_unlikely();
                return Some(Err(e));
            }
        };

        // Update header section to remove the consumed serial type
        self.header_section.set(&header[bytes_read..]);

        let data_section = self.data_section.get();

        match crate::storage::sqlite3_ondisk::read_value_serial_type(data_section, serial_type) {
            Ok((value, n)) => {
                self.data_section.set(&data_section[n..]);
                Some(Ok(value))
            }
            Err(e) => {
                mark_unlikely();
                Some(Err(e))
            }
        }
    }
}

// Optimization: indicate that once the iterator is exhausted, it will always return None.
impl<'a> FusedIterator for ValueIterator<'a> {}

impl<'a> Clone for ValueIterator<'a> {
    fn clone(&self) -> Self {
        Self {
            header_section: Cell::new(self.header_section.get()),
            data_section: Cell::new(self.data_section.get()),
        }
    }
}

impl<'a> ValueRef<'a> {
    pub fn from_f64(f: f64) -> Self {
        match NonNan::new(f) {
            Some(nn) => Self::Numeric(Numeric::Float(nn)),
            None => Self::Null,
        }
    }

    pub fn from_i64(i: i64) -> Self {
        Self::Numeric(Numeric::Integer(i))
    }

    pub fn to_ffi(&self) -> ExtValue {
        match self {
            Self::Null => ExtValue::null(),
            Self::Numeric(Numeric::Integer(i)) => ExtValue::from_integer(*i),
            Self::Numeric(Numeric::Float(fl)) => ExtValue::from_float(f64::from(*fl)),
            Self::Text(text) => ExtValue::from_text(text.as_str().to_string()),
            Self::Blob(blob) => ExtValue::from_blob(blob.to_vec()),
        }
    }

    pub fn to_blob(&self) -> Option<&'a [u8]> {
        match self {
            Self::Blob(blob) => Some(*blob),
            _ => None,
        }
    }

    pub fn to_text(&self) -> Option<&'a str> {
        match self {
            Self::Text(t) => Some(t.as_str()),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> &'a [u8] {
        match self {
            Self::Blob(b) => b,
            _ => panic!("as_blob must be called only for Value::Blob"),
        }
    }

    pub fn as_float(&self) -> f64 {
        match self {
            Self::Numeric(Numeric::Float(f)) => f64::from(*f),
            Self::Numeric(Numeric::Integer(i)) => *i as f64,
            _ => panic!("as_float must be called only for ValueRef::Numeric"),
        }
    }

    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Self::Numeric(Numeric::Integer(i)) => Some(*i),
            _ => None,
        }
    }

    pub const fn as_uint(&self) -> u64 {
        match self {
            Self::Numeric(Numeric::Integer(i)) => (*i).cast_unsigned(),
            _ => 0,
        }
    }

    #[inline]
    pub fn to_owned(&self) -> std::result::Result<Value, TryReserveError> {
        Ok(match self {
            ValueRef::Null => Value::Null,
            ValueRef::Numeric(n) => Value::from(*n),
            ValueRef::Text(text) => Value::Text(Text {
                value: text.value.to_string().into(),
                subtype: text.subtype,
            }),
            ValueRef::Blob(b) => return Value::from_slice(b),
        })
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Numeric(Numeric::Integer(_)) => ValueType::Integer,
            Self::Numeric(Numeric::Float(_)) => ValueType::Float,
            Self::Text(_) => ValueType::Text,
            Self::Blob(_) => ValueType::Blob,
        }
    }
}

impl Display for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Numeric(Numeric::Integer(i)) => write!(f, "{i}"),
            Self::Numeric(Numeric::Float(fl)) => {
                let fval: f64 = (*fl).into();
                write!(f, "{fval:?}")
            }
            Self::Text(s) => write!(f, "{}", s.as_str()),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        }
    }
}

impl<'a> PartialEq<ValueRef<'a>> for ValueRef<'a> {
    fn eq(&self, other: &ValueRef<'a>) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Numeric(a), Self::Numeric(b)) => a == b,
            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.as_bytes() == text_right.value.as_bytes()
            }
            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.eq(blob_right),
            _ => false,
        }
    }
}

impl<'a> PartialEq<Value> for ValueRef<'a> {
    fn eq(&self, other: &Value) -> bool {
        let other = other.as_value_ref();
        self.eq(&other)
    }
}

impl<'a> Eq for ValueRef<'a> {}

impl<'a> PartialOrd<ValueRef<'a>> for ValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ValueRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => std::cmp::Ordering::Equal,
            (Self::Null, _) => std::cmp::Ordering::Less,
            (_, Self::Null) => std::cmp::Ordering::Greater,

            (Self::Numeric(a), Self::Numeric(b)) => a.cmp(b),

            // Numeric < Text < Blob
            (Self::Numeric(_), _) => std::cmp::Ordering::Less,
            (_, Self::Numeric(_)) => std::cmp::Ordering::Greater,

            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.as_bytes().cmp(text_right.value.as_bytes())
            }
            (Self::Text(_), Self::Blob(_)) => std::cmp::Ordering::Less,
            (Self::Blob(_), Self::Text(_)) => std::cmp::Ordering::Greater,

            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.cmp(blob_right),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyInfo {
    pub sort_order: SortOrder,
    pub collation: CollationSeq,
    pub nulls_order: Option<turso_parser::ast::NullsOrder>,
}

#[cfg(not(nightly))]
pub type IndexKeyInfo = Vec<KeyInfo>;
#[cfg(nightly)]
pub type IndexKeyInfo = Vec<KeyInfo, DynAllocator>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Metadata about an index, used for handling and comparing index keys.
///
/// This struct provides information about the sorting order of columns,
/// whether the index includes a row ID, and the total number of columns
/// in the index.
pub struct IndexInfo {
    /// Specifies the sorting order (ascending or descending) for each column in the index.
    pub key_info: IndexKeyInfo,
    /// Indicates whether the index includes a row ID column.
    pub has_rowid: bool,
    /// The total number of columns in the index, including the row ID column if present.
    pub num_cols: usize,
    /// Indicates whether index rows should be unique.
    pub is_unique: bool,
}

impl Default for IndexInfo {
    fn default() -> Self {
        Self {
            key_info: Self::key_info_in(TursoAllocator),
            has_rowid: true,
            num_cols: 1,
            is_unique: false,
        }
    }
}

impl IndexInfo {
    pub fn key_info_in<A: ConcurrentAllocator>(alloc: A) -> IndexKeyInfo {
        <IndexKeyInfo as TursoVecInExt<KeyInfo, DynAllocator>>::new_in(DynAllocator::new(alloc))
    }

    #[cfg(not(nightly))]
    pub fn key_info_from_iter_in<A, I>(
        key_info: I,
        _alloc: A,
    ) -> Result<IndexKeyInfo, TryReserveError>
    where
        A: ConcurrentAllocator,
        I: IntoIterator<Item = KeyInfo>,
    {
        key_info.into_iter().try_collect()
    }

    #[cfg(nightly)]
    pub fn key_info_from_iter_in<A, I>(
        key_info: I,
        alloc: A,
    ) -> Result<IndexKeyInfo, TryReserveError>
    where
        A: ConcurrentAllocator,
        I: IntoIterator<Item = KeyInfo>,
    {
        key_info
            .into_iter()
            .try_collect_in(DynAllocator::new(alloc))
    }

    pub fn new<I>(
        key_info: I,
        has_rowid: bool,
        num_cols: usize,
        is_unique: bool,
    ) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = KeyInfo>,
    {
        Self::new_in(key_info, has_rowid, num_cols, is_unique, TursoAllocator)
    }

    pub fn new_in<A, I>(
        key_info: I,
        has_rowid: bool,
        num_cols: usize,
        is_unique: bool,
        alloc: A,
    ) -> Result<Self, TryReserveError>
    where
        A: ConcurrentAllocator,
        I: IntoIterator<Item = KeyInfo>,
    {
        Ok(Self {
            key_info: Self::key_info_from_iter_in(key_info, alloc)?,
            has_rowid,
            num_cols,
            is_unique,
        })
    }

    pub fn new_from_index(index: &Index) -> Result<Self, TryReserveError> {
        Self::new_from_index_in(index, TursoAllocator)
    }

    pub fn new_from_index_in<A: ConcurrentAllocator>(
        index: &Index,
        alloc: A,
    ) -> Result<Self, TryReserveError> {
        // A backing_btree stores the index method's complete opaque key. Unlike
        // an ordinary secondary index, it must not append the base-table rowid.
        // MVCC's commit-time conflict validation handles these
        // `has_rowid == false` records by treating the whole key as the
        // uniqueness prefix (see `check_index_for_conflicts`).
        let has_rowid = index.has_rowid && !index.is_backing_btree_index();
        let key_info = index
            .columns
            .iter()
            .map(|c| KeyInfo {
                sort_order: c.order,
                collation: c.collation.unwrap_or_default(),
                nulls_order: c.nulls_order,
            })
            .chain(has_rowid.then_some(KeyInfo {
                sort_order: SortOrder::Asc,
                collation: CollationSeq::Binary,
                nulls_order: None,
            }));
        Self::new_in(
            key_info,
            has_rowid,
            index.columns.len() + (has_rowid as usize),
            index.unique,
            alloc,
        )
    }
}

pub fn compare_immutable<V1, V2, E1, E2, I1, I2>(
    l: I1,
    r: I2,
    column_info: &[KeyInfo],
) -> std::cmp::Ordering
where
    V1: AsValueRef,
    V2: AsValueRef,
    E1: ExactSizeIterator<Item = V1>,
    E2: ExactSizeIterator<Item = V2>,
    I1: IntoIterator<IntoIter = E1, Item = E1::Item>,
    I2: IntoIterator<IntoIter = E2, Item = E2::Item>,
{
    let (l, r): (E1, E2) = (l.into_iter(), r.into_iter());
    assert!(
        l.len() >= column_info.len(),
        "{} < {}",
        l.len(),
        column_info.len()
    );
    assert!(
        r.len() >= column_info.len(),
        "{} < {}",
        r.len(),
        column_info.len()
    );
    let (l, r) = (l.take(column_info.len()), r.take(column_info.len()));
    for (i, (l, r)) in l.zip(r).enumerate() {
        let cmp = cmp_in_column(&l.as_value_ref(), &r.as_value_ref(), &column_info[i]);
        if !cmp.is_eq() {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
}

pub fn compare_immutable_iter<V, E1, E2>(
    mut l: E1,
    mut r: E2,
    column_info: &[KeyInfo],
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    E1: Iterator<Item = Result<V>>,
    E2: Iterator<Item = Result<V>>,
{
    for col_info in column_info.iter() {
        let l = match l.next() {
            Some(v) => v,
            None => break,
        };
        let r = match r.next() {
            Some(v) => v,
            None => break,
        };
        let cmp = cmp_in_column(&l?.as_value_ref(), &r?.as_value_ref(), col_info);
        if !cmp.is_eq() {
            return Ok(cmp);
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

/// Treats `NULL` as equal to itself and smaller than other values.
pub fn compare_immutable_single<V1, V2>(l: V1, r: V2, collation: CollationSeq) -> std::cmp::Ordering
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    let l = l.as_value_ref();
    let r = r.as_value_ref();
    match (l, r) {
        (ValueRef::Text(left), ValueRef::Text(right)) => collation.compare_strings(&left, &right),
        _ => l.cmp(&r),
    }
}

pub fn cmp_in_column(a: &ValueRef, b: &ValueRef, key: &KeyInfo) -> Ordering {
    cmp_with_sort(compare_immutable_single(a, b, key.collation), a, b, key)
}

/// Outputs a modified [Ordering] that takes into account the sort order and the NULLS order.
#[must_use]
pub fn cmp_with_sort(cmp: Ordering, a: &ValueRef, b: &ValueRef, key: &KeyInfo) -> Ordering {
    if cmp != Ordering::Equal {
        let involves_null = matches!(a, ValueRef::Null) || matches!(b, ValueRef::Null);
        if involves_null {
            if let Some(nulls_order) = key.nulls_order {
                // ValueRef ordering: NULL < non-NULL.
                // NULLS FIRST: keep that natural order regardless of ASC/DESC.
                // NULLS LAST: reverse it regardless of ASC/DESC.
                return match nulls_order {
                    turso_parser::ast::NullsOrder::First => cmp,
                    turso_parser::ast::NullsOrder::Last => cmp.reverse(),
                };
            }
        }
        return match key.sort_order {
            SortOrder::Asc => cmp,
            SortOrder::Desc => cmp.reverse(),
        };
    }
    Ordering::Equal
}

#[derive(Debug, Clone, Copy)]
pub enum RecordCompare {
    Int,
    String,
    Generic,
}

impl RecordCompare {
    pub fn compare<V, E, I>(
        &self,
        serialized: &ImmutableRecord,
        unpacked: I,
        index_info: &IndexInfo,
        skip: usize,
        tie_breaker: std::cmp::Ordering,
    ) -> Result<std::cmp::Ordering>
    where
        V: AsValueRef,
        E: ExactSizeIterator<Item = V>,
        I: IntoIterator<IntoIter = E, Item = E::Item>,
    {
        let unpacked = unpacked.into_iter();
        match self {
            RecordCompare::Int => {
                compare_records_int(serialized, unpacked, index_info, tie_breaker)
            }
            RecordCompare::String => {
                compare_records_string(serialized, unpacked, index_info, tie_breaker)
            }
            RecordCompare::Generic => {
                compare_records_generic(serialized, unpacked, index_info, skip, tie_breaker)
            }
        }
    }
}

pub fn find_compare<I, E, V>(unpacked: I, index_info: &IndexInfo) -> RecordCompare
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = Peekable<E>, Item = V>,
{
    let mut unpacked = unpacked.into_iter();
    if unpacked.len() != 0 && index_info.num_cols <= 13 {
        let val = unpacked.peek().unwrap();
        match val.as_value_ref() {
            ValueRef::Numeric(Numeric::Integer(_)) => RecordCompare::Int,
            ValueRef::Text(_) if index_info.key_info[0].collation == CollationSeq::Binary => {
                RecordCompare::String
            }
            _ => RecordCompare::Generic,
        }
    } else {
        RecordCompare::Generic
    }
}

pub fn get_tie_breaker_from_seek_op(seek_op: SeekOp) -> std::cmp::Ordering {
    match seek_op {
        // exact‐match “key == X” opcodes
        SeekOp::GE { eq_only: true } | SeekOp::LE { eq_only: true } => std::cmp::Ordering::Equal,

        // forward search – want the *first* ≥ / > key
        SeekOp::GE { eq_only: false } => std::cmp::Ordering::Greater,
        SeekOp::GT => std::cmp::Ordering::Less,

        // backward search – want the *last* ≤ / < key
        SeekOp::LE { eq_only: false } => std::cmp::Ordering::Less,
        SeekOp::LT => std::cmp::Ordering::Greater,
    }
}

/// Optimized integer-first record comparison function.
///
/// This function is an optimized version of `compare_records_generic()` for the
/// common case where:
/// - (a) The first field of the unpacked record is an integer
/// - (b) The serialized record's first field is also an integer
/// - (c) The header size varint fits in a single byte and is ≤ 63 bytes
///
/// The 63-byte header limit prevents buffer overreads and ensures safe direct
/// memory access patterns. This optimization avoids generic parsing overhead
/// by directly extracting and comparing integer values using known layouts.
///
/// # Fast Path Conditions
///
/// The function uses the optimized path when ALL of these conditions are met:
/// - Payload is at least 2 bytes (header size + first serial type)
/// - First serial type indicates integer (`1-6`, `8`, or `9`)
/// - First unpacked field is a `ValueRef::Numeric(Numeric::Integer)`
///
/// If any condition fails, it falls back to `compare_records_generic()`.
///
/// # Arguments
///
/// * `serialized` - The left-hand side record in serialized format
/// * `unpacked` - The right-hand side record as an array of parsed values
/// * `index_info` - Contains sort order information for each field
/// * `collations` - Array of collation sequences (unused for integers)
/// * `tie_breaker` - Result to return when all compared fields are equal
///
/// /// # Comparison Logic
///
/// The function follows optimized integer comparison semantics:
///
/// 1. **Type validation**: Ensures both sides are integers, otherwise falls back
/// 2. **Direct extraction**: Reads integer value using specialized decoder
/// 3. **Native comparison**: Uses Rust's built-in `i64::cmp()` for speed
/// 4. **Sort order**: Applies ascending/descending order to comparison result
/// 5. **Remaining fields**: If first field is equal and more fields exist,
///    delegates to `compare_records_generic()` with `skip=1`
fn compare_records_int<V, I>(
    serialized: &ImmutableRecord,
    unpacked: I,
    index_info: &IndexInfo,
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    I: ExactSizeIterator<Item = V>,
{
    let payload = serialized.get_payload();
    if payload.len() < 2 {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let (header_size, offset_1st_serialtype) = read_varint(payload)?;
    let header_size = header_size as usize;

    if payload.len() < header_size {
        return Err(LimboError::Corrupt(format!(
            "Record payload too short: claimed header size {} but payload only {} bytes",
            header_size,
            payload.len()
        )));
    }

    let (first_serial_type, _) = read_varint(&payload[offset_1st_serialtype..])?;

    let serialtype_is_integer = matches!(first_serial_type, 1..=6 | 8 | 9);
    if !serialtype_is_integer {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let data_start = header_size;

    let lhs_int = read_integer(&payload[data_start..], first_serial_type as u8)?;
    let mut unpacked = unpacked.peekable();
    // Do not consume iterator here
    let ValueRef::Numeric(Numeric::Integer(rhs_int)) = unpacked.peek().unwrap().as_value_ref()
    else {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    };
    let comparison = match index_info.key_info[0].sort_order {
        SortOrder::Asc => lhs_int.cmp(&rhs_int),
        SortOrder::Desc => lhs_int.cmp(&rhs_int).reverse(),
    };
    match comparison {
        std::cmp::Ordering::Equal => {
            // First fields equal, compare remaining fields if any
            if unpacked.len() > 1 {
                return compare_records_generic(serialized, unpacked, index_info, 1, tie_breaker);
            }
            Ok(tie_breaker)
        }
        other => Ok(other),
    }
}

/// This function is an optimized version of `compare_records_generic()` for the
/// common case where:
/// - (a) The first field of the unpacked record is a string
/// - (b) The serialized record's first field is also a string
/// - (c) The header size varint fits in a single byte (most records)
///
/// This optimization avoids the overhead of generic field parsing by directly
/// accessing the first string field using known offsets, then falling back to
/// the generic comparison for remaining fields if needed.
///
/// # Fast Path Conditions
///
/// The function uses the optimized path when ALL of these conditions are met:
/// - Payload is at least 2 bytes (header size + first serial type)
/// - Header size fits in single byte (`payload[0] < 0x80`)
/// - First serial type indicates string (`>= 13` and odd number)
/// - First unpacked field is a `RefValue::Text`
///
/// If any condition fails, it falls back to `compare_records_generic()`.
///
/// # Arguments
///
/// * `serialized` - The left-hand side record in serialized format
/// * `unpacked` - The right-hand side record as an array of parsed values
/// * `index_info` - Contains sort order information for each field
/// * `collations` - Array of collation sequences for string comparisons
/// * `tie_breaker` - Result to return when all compared fields are equal
///
/// # Comparison Logic
///
/// The function follows SQLite's string comparison semantics:
///
/// 1. **Type checking**: Ensures both sides are strings, otherwise falls back
/// 2. **String comparison**: Uses collation if provided, binary otherwise
/// 3. **Sort order**: Applies ascending/descending order to comparison result
/// 4. **Length comparison**: If strings are equal, compares lengths
/// 5. **Remaining fields**: If first field is equal and more fields exist,
///    delegates to `compare_records_generic()` with `skip=1`
fn compare_records_string<V, I>(
    serialized: &ImmutableRecord,
    unpacked: I,
    index_info: &IndexInfo,
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    I: ExactSizeIterator<Item = V>,
{
    let payload = serialized.get_payload();
    if payload.len() < 2 {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let (header_size, offset_1st_serialtype) = read_varint(payload)?;
    let header_size = header_size as usize;

    if payload.len() < header_size {
        return Err(LimboError::Corrupt(format!(
            "Record payload too short: claimed header size {} but payload only {} bytes",
            header_size,
            payload.len()
        )));
    }

    let (first_serial_type, _) = read_varint(&payload[offset_1st_serialtype..])?;

    let serialtype_is_string = first_serial_type >= 13 && (first_serial_type & 1) == 1;
    if !serialtype_is_string {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    }

    let mut unpacked = unpacked.peekable();

    let ValueRef::Text(rhs_text) = unpacked.peek().unwrap().as_value_ref() else {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    };

    let string_len = (first_serial_type as usize - 13) / 2;
    let data_start = header_size;

    turso_debug_assert!(data_start + string_len <= payload.len());

    let serial_type = SerialType::try_from(first_serial_type)?;
    let (lhs_value, _) = read_value(&payload[data_start..], serial_type)?;

    let ValueRef::Text(lhs_text) = lhs_value else {
        return compare_records_generic(serialized, unpacked, index_info, 0, tie_breaker);
    };

    let collation = index_info.key_info[0].collation;
    let comparison = collation.compare_strings(&lhs_text, &rhs_text);

    let final_comparison = match index_info.key_info[0].sort_order {
        SortOrder::Asc => comparison,
        SortOrder::Desc => comparison.reverse(),
    };

    match final_comparison {
        std::cmp::Ordering::Equal => {
            let len_cmp = lhs_text.len().cmp(&rhs_text.len());
            if len_cmp != std::cmp::Ordering::Equal {
                let adjusted = match index_info.key_info[0].sort_order {
                    SortOrder::Asc => len_cmp,
                    SortOrder::Desc => len_cmp.reverse(),
                };
                return Ok(adjusted);
            }

            if unpacked.len() > 1 {
                return compare_records_generic(serialized, unpacked, index_info, 1, tie_breaker);
            }
            Ok(tie_breaker)
        }
        other => Ok(other),
    }
}

/// Compare two table rows or index records.
///
/// This function compares a serialized record (`serialized`) with an unpacked
/// record (`unpacked`) and returns a comparison result. It returns `Less`, `Equal`,
/// or `Greater` if the serialized record is less than, equal to, or greater than
/// the unpacked record.
///
/// The `serialized` record must be a blob created by the record serialization
/// process (equivalent to SQLite's OP_MakeRecord opcode). The `unpacked` record
/// must be a parsed key array of `RefValue` objects.
///
/// # Arguments
///
/// * `serialized` - The left-hand side record in serialized format
/// * `unpacked` - The right-hand side record as an array of parsed values
/// * `index_info` - Contains sort order information for each field
/// * `skip` - Number of initial fields to skip (assumes caller verified equality)
/// * `tie_breaker` - Result to return when all compared fields are equal
///
/// # Skipping Fields
///
/// If `skip` is non-zero, it is assumed that the caller has already determined
/// that the first `skip` fields of the records are equal. This function will
/// begin comparing at field index `skip`, skipping over the header and data
/// portions of the already-verified fields.
///
/// # Field Count Differences
///
/// The serialized and unpacked records do not have to contain the same number
/// of fields. If all fields that appear in both records are equal, then
/// `tie_breaker` is returned.
pub fn compare_records_generic<V, I>(
    serialized: &ImmutableRecord,
    unpacked: I,
    index_info: &IndexInfo,
    skip: usize,
    tie_breaker: std::cmp::Ordering,
) -> Result<std::cmp::Ordering>
where
    V: AsValueRef,
    I: ExactSizeIterator<Item = V>,
{
    let payload = serialized.get_payload();
    if payload.is_empty() {
        return Ok(std::cmp::Ordering::Less);
    }

    let (header_size, mut header_pos) = read_varint(payload)?;
    let header_end = header_size as usize;
    turso_debug_assert!(header_end <= payload.len());

    let mut data_pos = header_size as usize;

    // Skip over `skip` number of fields
    for _ in 0..skip {
        if header_pos >= header_end {
            break;
        }

        let (serial_type_raw, bytes_read) = read_varint(&payload[header_pos..])?;
        header_pos += bytes_read;

        let serial_type = SerialType::try_from(serial_type_raw)?;
        if !matches!(
            serial_type.kind(),
            SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 | SerialTypeKind::Null
        ) {
            data_pos += serial_type.size();
        }
    }

    let mut field_idx = skip;
    let field_limit = unpacked.len().min(index_info.key_info.len());

    // assumes that that the `unpacked' iterator was not skipped outside this function call`
    for rhs_value in unpacked.skip(skip) {
        let rhs_value = &rhs_value.as_value_ref();
        if field_idx >= field_limit || header_pos >= header_end {
            break;
        }
        let (serial_type_raw, bytes_read) = read_varint(&payload[header_pos..])?;
        header_pos += bytes_read;

        let serial_type = SerialType::try_from(serial_type_raw)?;

        let lhs_value = match serial_type.kind() {
            SerialTypeKind::ConstInt0 => ValueRef::Numeric(Numeric::Integer(0)),
            SerialTypeKind::ConstInt1 => ValueRef::Numeric(Numeric::Integer(1)),
            SerialTypeKind::Null => ValueRef::Null,
            _ => {
                let (value, field_size) = read_value(&payload[data_pos..], serial_type)?;
                data_pos += field_size;
                value
            }
        };

        let key_info = &index_info.key_info[field_idx];
        let comparison = match (&lhs_value, rhs_value) {
            (ValueRef::Text(lhs_text), ValueRef::Text(rhs_text)) => {
                key_info.collation.compare_strings(lhs_text, rhs_text)
            }
            _ => lhs_value.cmp(rhs_value),
        };

        let final_comparison = cmp_with_sort(comparison, &lhs_value, rhs_value, key_info);

        if final_comparison != std::cmp::Ordering::Equal {
            return Ok(final_comparison);
        }

        field_idx += 1;
    }

    Ok(tie_breaker)
}

const I8_LOW: i64 = -128;
const I8_HIGH: i64 = 127;
const I16_LOW: i64 = -32768;
const I16_HIGH: i64 = 32767;
const I24_LOW: i64 = -8388608;
const I24_HIGH: i64 = 8388607;
const I32_LOW: i64 = -2147483648;
const I32_HIGH: i64 = 2147483647;
const I48_LOW: i64 = -140737488355328;
const I48_HIGH: i64 = 140737488355327;

/// Sqlite Serial Types
/// https://www.sqlite.org/fileformat.html#record_format
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SerialType(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerialTypeKind {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    ConstInt0,
    ConstInt1,
    Text,
    Blob,
}

impl SerialType {
    #[inline(always)]
    pub fn u64_is_valid_serial_type(n: u64) -> bool {
        n != 10 && n != 11
    }

    const NULL: Self = Self(0);
    const I8: Self = Self(1);
    const I16: Self = Self(2);
    const I24: Self = Self(3);
    const I32: Self = Self(4);
    const I48: Self = Self(5);
    const I64: Self = Self(6);
    const F64: Self = Self(7);
    const CONST_INT0: Self = Self(8);
    const CONST_INT1: Self = Self(9);

    pub const fn null() -> Self {
        Self::NULL
    }

    pub const fn i8() -> Self {
        Self::I8
    }

    pub const fn i16() -> Self {
        Self::I16
    }

    pub const fn i24() -> Self {
        Self::I24
    }

    pub const fn i32() -> Self {
        Self::I32
    }

    pub const fn i48() -> Self {
        Self::I48
    }

    pub const fn i64() -> Self {
        Self::I64
    }

    pub const fn f64() -> Self {
        Self::F64
    }

    pub const fn const_int0() -> Self {
        Self::CONST_INT0
    }

    pub const fn const_int1() -> Self {
        Self::CONST_INT1
    }

    pub const fn blob(size: u64) -> Self {
        Self(12 + size * 2)
    }

    pub const fn text(size: u64) -> Self {
        Self(13 + size * 2)
    }

    #[inline(always)]
    pub const fn kind(&self) -> SerialTypeKind {
        match self.0 {
            0 => SerialTypeKind::Null,
            1 => SerialTypeKind::I8,
            2 => SerialTypeKind::I16,
            3 => SerialTypeKind::I24,
            4 => SerialTypeKind::I32,
            5 => SerialTypeKind::I48,
            6 => SerialTypeKind::I64,
            7 => SerialTypeKind::F64,
            8 => SerialTypeKind::ConstInt0,
            9 => SerialTypeKind::ConstInt1,
            n if n >= 12 => match n % 2 {
                0 => SerialTypeKind::Blob,
                1 => SerialTypeKind::Text,
                _ => {
                    mark_unlikely();
                    unreachable!();
                }
            },
            _ => {
                mark_unlikely();
                unreachable!();
            }
        }
    }

    pub const fn size(&self) -> usize {
        match self.kind() {
            SerialTypeKind::Null => 0,
            SerialTypeKind::I8 => 1,
            SerialTypeKind::I16 => 2,
            SerialTypeKind::I24 => 3,
            SerialTypeKind::I32 => 4,
            SerialTypeKind::I48 => 6,
            SerialTypeKind::I64 => 8,
            SerialTypeKind::F64 => 8,
            SerialTypeKind::ConstInt0 => 0,
            SerialTypeKind::ConstInt1 => 0,
            SerialTypeKind::Text => (self.0 as usize - 13) / 2,
            SerialTypeKind::Blob => (self.0 as usize - 12) / 2,
        }
    }
}

#[inline(always)]
pub fn get_serial_type_size(serial: u64) -> Result<usize> {
    match serial {
        0 | 8 | 9 => Ok(0),
        1 => Ok(1),
        2 => Ok(2),
        3 => Ok(3),
        4 => Ok(4),
        5 => Ok(6),
        6 | 7 => Ok(8),
        n if n >= 12 => match n % 2 {
            0 => Ok(((n - 12) / 2) as usize), // Blob
            1 => Ok(((n - 13) / 2) as usize), // Text
            _ => {
                mark_unlikely();
                unreachable!();
            }
        },
        _ => {
            mark_unlikely();
            Err(LimboError::Corrupt(format!(
                "Invalid serial type: {serial}"
            )))
        }
    }
}

impl<T: AsValueRef> From<T> for SerialType {
    fn from(value: T) -> Self {
        let value = value.as_value_ref();
        match value {
            ValueRef::Null => SerialType::null(),
            ValueRef::Numeric(Numeric::Integer(i)) => match i {
                0 => SerialType::const_int0(),
                1 => SerialType::const_int1(),
                i if (I8_LOW..=I8_HIGH).contains(&i) => SerialType::i8(),
                i if (I16_LOW..=I16_HIGH).contains(&i) => SerialType::i16(),
                i if (I24_LOW..=I24_HIGH).contains(&i) => SerialType::i24(),
                i if (I32_LOW..=I32_HIGH).contains(&i) => SerialType::i32(),
                i if (I48_LOW..=I48_HIGH).contains(&i) => SerialType::i48(),
                _ => SerialType::i64(),
            },
            ValueRef::Numeric(Numeric::Float(_)) => SerialType::f64(),
            ValueRef::Text(t) => SerialType::text(t.value.len() as u64),
            ValueRef::Blob(b) => SerialType::blob(b.len() as u64),
        }
    }
}

impl From<SerialType> for u64 {
    fn from(serial_type: SerialType) -> Self {
        serial_type.0
    }
}

impl TryFrom<u64> for SerialType {
    type Error = LimboError;

    #[inline(always)]
    fn try_from(uint: u64) -> Result<Self> {
        if unlikely(uint == 10 || uint == 11) {
            return Err(LimboError::Corrupt(format!("Invalid serial type: {uint}")));
        }
        Ok(SerialType(uint))
    }
}

impl Record {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Calculates the total size needed for a SQLite record header.
    ///
    /// The record header consists of:
    /// 1. A varint encoding the total header size (self-referentially, e.g. a 100 byte header literally has the number '100' in the header suffix)
    /// 2. A sequence of varints encoding the serial types
    ///
    /// For small headers (<=126 bytes), we only need 1 byte to encode the header size, because 127 fits in 7 bits (varint uses 7 bits for the value and 1 continuation bit)
    /// For larger headers, we need to account for the variable length of the header size varint.
    pub fn calc_header_size(sizeof_serial_types: usize) -> usize {
        if sizeof_serial_types < i8::MAX as usize {
            return sizeof_serial_types + 1;
        }

        let mut header_size = sizeof_serial_types;
        // For larger headers, calculate how many bytes we need for the header size varint
        let mut temp_buf = [0u8; 9];
        let mut prev_header_size;

        loop {
            prev_header_size = header_size;
            let varint_len = write_varint(&mut temp_buf, header_size as u64);
            header_size = sizeof_serial_types + varint_len;

            if header_size == prev_header_size {
                break;
            }
        }

        header_size
    }

    pub fn serialize(&self, buf: &mut std::vec::Vec<u8>) {
        let initial_i = buf.len();

        // write serial types
        for value in &self.values {
            let serial_type = SerialType::from(value);
            buf.resize(buf.len() + 9, 0); // Ensure space for varint (1-9 bytes in length)
            let len = buf.len();
            let n = write_varint(&mut buf[len - 9..], serial_type.into());
            buf.truncate(buf.len() - 9 + n); // Remove unused bytes
        }

        let mut header_size = buf.len() - initial_i;
        // write content
        for value in &self.values {
            match value {
                Value::Null => {}
                Value::Numeric(Numeric::Integer(i)) => {
                    let serial_type = SerialType::from(value);
                    match serial_type.kind() {
                        SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 => {}
                        SerialTypeKind::I8 => buf.extend_from_slice(&(*i as i8).to_be_bytes()),
                        SerialTypeKind::I16 => buf.extend_from_slice(&(*i as i16).to_be_bytes()),
                        SerialTypeKind::I24 => {
                            buf.extend_from_slice(&(*i as i32).to_be_bytes()[1..])
                        } // remove most significant byte
                        SerialTypeKind::I32 => buf.extend_from_slice(&(*i as i32).to_be_bytes()),
                        SerialTypeKind::I48 => buf.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialTypeKind::I64 => buf.extend_from_slice(&i.to_be_bytes()),
                        _ => {
                            mark_unlikely();
                            unreachable!();
                        }
                    }
                }
                Value::Numeric(Numeric::Float(f)) => {
                    buf.extend_from_slice(&f64::from(*f).to_be_bytes())
                }
                Value::Text(t) => buf.extend_from_slice(t.value.as_bytes()),
                Value::Blob(b) => buf.extend_from_slice(b),
            };
        }

        let mut header_bytes_buf = std::vec::Vec::new();
        header_size = Record::calc_header_size(header_size);
        header_bytes_buf.extend(std::iter::repeat_n(0, 9));
        let n = write_varint(header_bytes_buf.as_mut_slice(), header_size as u64);
        header_bytes_buf.truncate(n);
        buf.splice(initial_i..initial_i, header_bytes_buf.iter().cloned());
    }
}

pub enum Cursor {
    BTree(Box<dyn CursorTrait>),
    IndexMethod(Box<dyn IndexMethodCursor>),
    Pseudo(Box<PseudoCursor>),
    Sorter(Box<Sorter>),
    Virtual(VirtualTableCursor),
    MaterializedView(Box<crate::incremental::cursor::MaterializedViewCursor>),
    /// Permanently-null placeholder installed by `NullRow` on a
    /// never-opened cursor slot; all reads yield NULL.
    NullRow,
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BTree(..) => f.debug_tuple("BTree").finish(),
            Self::IndexMethod(..) => f.debug_tuple("IndexMethod").finish(),
            Self::Pseudo(..) => f.debug_tuple("Pseudo").finish(),
            Self::Sorter(..) => f.debug_tuple("Sorter").finish(),
            Self::Virtual(..) => f.debug_tuple("Virtual").finish(),
            Self::MaterializedView(..) => f.debug_tuple("MaterializedView").finish(),
            Self::NullRow => f.debug_tuple("NullRow").finish(),
        }
    }
}

impl Cursor {
    pub fn new_btree(cursor: Box<dyn CursorTrait>) -> Self {
        // Matches sqlite3BtreeCursor adding to BtShared.pCursor (btree.c:4699).
        cursor.register_with_pager();
        Self::BTree(cursor)
    }

    pub fn new_pseudo(cursor: PseudoCursor) -> Self {
        Self::Pseudo(Box::new(cursor))
    }

    pub fn new_sorter(cursor: Sorter) -> Self {
        Self::Sorter(Box::new(cursor))
    }

    pub fn new_materialized_view(
        cursor: crate::incremental::cursor::MaterializedViewCursor,
    ) -> Self {
        Self::MaterializedView(Box::new(cursor))
    }

    pub fn as_btree_mut(&mut self) -> &mut dyn CursorTrait {
        match self {
            Self::BTree(cursor) => cursor.as_mut(),
            _ => {
                mark_unlikely();
                panic!("Cursor is not a btree cursor");
            }
        }
    }

    pub fn as_pseudo_mut(&mut self) -> &mut PseudoCursor {
        match self {
            Self::Pseudo(cursor) => cursor,
            _ => {
                mark_unlikely();
                panic!("Cursor is not a pseudo cursor");
            }
        }
    }

    pub fn as_sorter_mut(&mut self) -> &mut Sorter {
        match self {
            Self::Sorter(cursor) => cursor,
            _ => {
                mark_unlikely();
                panic!("Cursor is not a sorter cursor")
            }
        }
    }

    pub fn as_virtual_mut(&mut self) -> &mut VirtualTableCursor {
        match self {
            Self::Virtual(cursor) => cursor,
            _ => {
                mark_unlikely();
                panic!("Cursor is not a virtual cursor")
            }
        }
    }

    pub fn as_materialized_view_mut(
        &mut self,
    ) -> &mut crate::incremental::cursor::MaterializedViewCursor {
        match self {
            Self::MaterializedView(cursor) => cursor,
            _ => {
                mark_unlikely();
                panic!("Cursor is not a materialized view cursor");
            }
        }
    }

    pub fn as_index_method_mut(&mut self) -> &mut dyn IndexMethodCursor {
        match self {
            Self::IndexMethod(cursor) => cursor.as_mut(),
            _ => {
                mark_unlikely();
                panic!("Cursor is not an IndexMethod cursor");
            }
        }
    }

    /// Move the cursor to a synthetic null row. See [Insn::NullRow]
    pub fn set_null_flag(&mut self, flag: bool) {
        match self {
            Self::BTree(cursor) => cursor.set_null_flag(flag),
            Self::Virtual(cursor) => cursor.set_null_flag(flag),
            // A pseudo cursor always decodes columns from its content
            // register. SQLite's OP_NullRow likewise leaves pseudo-cursor
            // column reads untouched: nullRow is the steady state for pseudo
            // cursors there, and OP_Column keeps routing to the register.
            Self::Pseudo(_) => {}
            // Permanently null; the flag is a no-op.
            Self::NullRow => {}
            // The FTS side of an outer join: columns are decoded from the
            // base-table cursor (which receives its own NullRow), never from
            // the index-method cursor, so there is no column state to null
            // out here.
            Self::IndexMethod(_) => {}
            _ => {
                mark_unlikely();
                panic!("set_null_flag on unexpected cursor type");
            }
        }
    }
}

#[derive(Debug)]
#[must_use]
pub struct IOCompletions(pub Completion);

pub struct IOCompletionAsync<'a, I: ?Sized + IO> {
    io: &'a I,
    completion: Completion,
}

impl<'a, I: ?Sized + IO> Future for IOCompletionAsync<'a, I> {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let completion = std::pin::pin!(&mut self.as_mut().completion);
        match completion.poll(cx) {
            Poll::Pending => {
                self.io.step()?;
                Poll::Pending
            }
            res => res,
        }
    }
}

impl IOCompletions {
    /// Wais for the Completions to complete
    pub fn wait<I: ?Sized + IO>(self, io: &I) -> Result<()> {
        io.wait_for_completion(self.0)
    }

    /// Waits for Completion to complete and `steps` IO. Ideally the user should do the stepping,
    /// but we do not have yet a good api for this
    pub async fn wait_async<I: ?Sized + IO>(self, io: &I) -> Result<()> {
        IOCompletionAsync {
            io,
            completion: self.0,
        }
        .await
    }

    pub fn finished(&self) -> bool {
        self.0.finished()
    }

    /// Returns true if this is an explicit yield — a signal to return control
    /// to the cooperative scheduler so other fibers can make progress.
    pub fn is_explicit_yield(&self) -> bool {
        self.0.is_explicit_yield()
    }

    /// Send abort signal to completions
    pub fn abort(&self) {
        self.0.abort()
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        self.0.get_error()
    }

    pub fn set_waker(&self, waker: Option<&Waker>) {
        if let Some(waker) = waker {
            self.0.set_waker(waker)
        }
    }
}

/// Return type for storage-layer functions that may suspend for I/O.
/// The boxed error keeps the whole value register-sized for small `T`
/// (a bare `LimboError` is 40 bytes and forces a memory return on every
/// call in cursor and pager hot paths); `?` boxes automatically.
pub type IOResultOr<T> = std::result::Result<IOResult<T>, Box<crate::LimboError>>;

#[derive(Debug)]
#[must_use]
pub enum IOResult<T> {
    Done(T),
    IO(IOCompletions),
}

impl<T> IOResult<T> {
    #[inline]
    pub fn is_io(&self) -> bool {
        matches!(self, IOResult::IO(..))
    }

    #[inline]
    pub fn io(self) -> Option<IOCompletions> {
        match self {
            IOResult::Done(_) => None,
            IOResult::IO(io) => Some(io),
        }
    }

    #[inline]
    pub fn map<U>(self, func: impl FnOnce(T) -> U) -> IOResult<U> {
        match self {
            IOResult::Done(t) => IOResult::Done(func(t)),
            IOResult::IO(io) => IOResult::IO(io),
        }
    }
}

/// Evaluate a IOResultOr<T>, if IO return IO.
#[macro_export]
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
            Err(err) => {
                branches::mark_unlikely();
                // Polymorphic over boxed and unboxed LimboError contexts.
                return Err(err.into());
            }
        }
    };
}

#[macro_export]
macro_rules! return_and_restore_if_io {
    ($field:expr, $saved_state:expr, $e:expr) => {
        match $e {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => {
                let _ = std::mem::replace($field, $saved_state);
                return Ok(IOResult::IO(io));
            }
            Err(e) => {
                let _ = std::mem::replace($field, $saved_state);
                return Err(e);
            }
        }
    };
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SeekResult {
    /// Record matching the [SeekOp] found in the B-tree and cursor was positioned to point onto that record
    Found,
    /// Record matching the [SeekOp] doesn't exists in the B-tree
    NotFound,
    /// This result can happen only if eq_only for [SeekOp] is false
    /// In this case Seek can position cursor to the leaf page boundaries (before the start, after the end)
    /// (e.g. if leaf page holds rows with keys from range [1..10], key 10 is absent and [SeekOp] is >= 10)
    ///
    /// turso-db has this extra [SeekResult] in order to make [BTreeCursor::seek] method to position cursor at
    /// the leaf of potential insertion, but also communicate to caller the fact that current cursor position
    /// doesn't hold a matching entry
    /// (necessary for Seek{XX} VM op-codes, so these op-codes will try to advance cursor in order to move it to matching entry)
    TryAdvance,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// The match condition of a table/index seek.
pub enum SeekOp {
    /// If eq_only is true, this means in practice:
    /// We are iterating forwards, but we are really looking for an exact match on the seek key.
    GE {
        eq_only: bool,
    },
    GT,
    /// If eq_only is true, this means in practice:
    /// We are iterating backwards, but we are really looking for an exact match on the seek key.
    LE {
        eq_only: bool,
    },
    LT,
}

impl SeekOp {
    /// A given seek op implies an iteration direction.
    ///
    /// For example, a seek with SeekOp::GT implies:
    /// Find the first table/index key that compares greater than the seek key
    /// -> used in forwards iteration.
    ///
    /// A seek with SeekOp::LE implies:
    /// Find the last table/index key that compares less than or equal to the seek key
    /// -> used in backwards iteration.
    #[inline(always)]
    pub fn iteration_direction(&self) -> IterationDirection {
        match self {
            SeekOp::GE { .. } | SeekOp::GT => IterationDirection::Forwards,
            SeekOp::LE { .. } | SeekOp::LT => IterationDirection::Backwards,
        }
    }

    pub fn eq_only(&self) -> bool {
        match self {
            SeekOp::GE { eq_only } | SeekOp::LE { eq_only } => *eq_only,
            _ => false,
        }
    }

    pub fn reverse(&self) -> Self {
        match self {
            SeekOp::GE { eq_only } => SeekOp::LE { eq_only: *eq_only },
            SeekOp::GT => SeekOp::LT,
            SeekOp::LE { eq_only } => SeekOp::GE { eq_only: *eq_only },
            SeekOp::LT => SeekOp::GT,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SeekKey<'a> {
    TableRowId(i64),
    IndexKey(ImmutableRecordRef<'a>),
}

impl<'a> SeekKey<'a> {
    pub fn index_record(&self) -> Option<&ImmutableRecordRef<'a>> {
        match self {
            Self::TableRowId(_) => None,
            Self::IndexKey(record) => Some(record),
        }
    }
}

#[derive(Debug)]
pub enum DatabaseChangeType {
    Delete,
    Update { bin_record: std::vec::Vec<u8> },
    Insert { bin_record: std::vec::Vec<u8> },
}

#[derive(Debug)]
pub struct DatabaseChange {
    pub change_id: i64,
    pub change_time: u64,
    pub change: DatabaseChangeType,
    pub table_name: String,
    pub id: i64,
}

#[derive(Debug)]
pub struct WalFrameInfo {
    pub page_no: u32,
    pub db_size: u32,
}

#[derive(Debug, PartialEq)]
pub struct WalState {
    pub checkpoint_seq_no: u32,
    pub max_frame: u64,
}

impl WalFrameInfo {
    pub fn is_commit_frame(&self) -> bool {
        self.db_size > 0
    }
    pub fn from_frame_header(frame: &[u8]) -> Self {
        let page_no = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let db_size = u32::from_be_bytes(frame[4..8].try_into().unwrap());
        Self { page_no, db_size }
    }
    pub fn put_to_frame_header(&self, frame: &mut [u8]) {
        frame[0..4].copy_from_slice(&self.page_no.to_be_bytes());
        frame[4..8].copy_from_slice(&self.db_size.to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::vec;
    use crate::translate::collate::CollationSeq;

    #[test]
    fn is_ascii_checks_every_byte_of_every_length() {
        for len in 0..40 {
            let mut ascii: Vec<u8> = vec![];
            ascii.extend((0..len).map(|i| b'a' + (i % 26) as u8));
            assert!(is_ascii(&ascii), "length {len}");
            assert_eq!(validate_utf8(&ascii), std::str::from_utf8(&ascii).ok());
            for position in 0..len {
                let mut bytes = ascii.clone();
                bytes[position] = 0xc3;
                assert!(!is_ascii(&bytes), "length {len}, byte {position}");
                assert_eq!(validate_utf8(&bytes), std::str::from_utf8(&bytes).ok());
            }
        }
        let text = "héllo wörld, ünïcödé";
        assert!(!is_ascii(text.as_bytes()));
        assert_eq!(validate_utf8(text.as_bytes()), Some(text));
    }

    fn assert_integer_conversions<T>(in_range: &[(i64, T)], out_of_range: &[i64])
    where
        T: Copy + std::fmt::Debug + PartialEq + FromValue,
    {
        for &(input, expected) in in_range {
            assert_eq!(T::from_sql(Value::from_i64(input)).unwrap(), expected);
        }
        for &input in out_of_range {
            assert!(
                matches!(
                    T::from_sql(Value::from_i64(input)),
                    Err(LimboError::IntegerOverflow)
                ),
                "{input} should overflow {}",
                std::any::type_name::<T>()
            );
        }
    }

    #[test]
    fn from_value_checks_integer_ranges() {
        assert_integer_conversions::<i32>(
            &[
                (i32::MIN as i64, i32::MIN),
                (-1, -1),
                (0, 0),
                (1, 1),
                (i32::MAX as i64, i32::MAX),
            ],
            &[i32::MIN as i64 - 1, i32::MAX as i64 + 1],
        );
        assert_integer_conversions::<u32>(
            &[(0, 0), (1, 1), (u32::MAX as i64, u32::MAX)],
            &[-2, -1, u32::MAX as i64 + 1],
        );
        assert_integer_conversions::<i64>(
            &[
                (i64::MIN, i64::MIN),
                (-1, -1),
                (0, 0),
                (1, 1),
                (i64::MAX, i64::MAX),
            ],
            &[],
        );
        assert_integer_conversions::<u64>(
            &[(0, 0), (1, 1), (i64::MAX, i64::MAX as u64)],
            &[-2, -1],
        );
    }

    #[test]
    fn from_value_converts_integers_to_f64() {
        for input in [i64::MIN, -1, 0, 1, (1_i64 << 53) + 1, i64::MAX] {
            assert_eq!(f64::from_sql(Value::from_i64(input)).unwrap(), input as f64);
        }
        assert_eq!(f64::from_sql(Value::from_f64(1.5)).unwrap(), 1.5);
    }

    #[cfg(nightly)]
    #[test]
    fn moving_blobs_through_value_preserves_allocation() {
        fn assert_move_preserves_allocation(blob: ValueBlob) {
            let pointer = blob.as_ptr();
            let capacity = blob.capacity();
            let len = blob.len();

            let value = Value::from_blob(blob);
            let Value::Blob(blob) = value else {
                unreachable!();
            };

            assert_eq!(blob.as_ptr(), pointer);
            assert_eq!(blob.capacity(), capacity);
            assert_eq!(blob.len(), len);
        }

        assert_move_preserves_allocation(vec![]);
        assert_move_preserves_allocation(vec![1, 2, 3, 4]);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn value_blob_serde_preserves_sequence_format() {
        let value = Value::from_slice(&[1, 2, 3, 4]).expect(crate::alloc::ALLOC_ERR_MSG);
        let encoded = serde_json::to_string(&value).unwrap();
        assert_eq!(encoded, r#"{"Blob":[1,2,3,4]}"#);

        let decoded: Value = serde_json::from_str(&encoded).unwrap();
        let Value::Blob(blob) = decoded else {
            panic!("expected blob value");
        };
        assert_eq!(blob.as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_value_iterator_simple() {
        let mut buf = std::vec::Vec::new();
        let record = Record::new(vec![Value::from_i64(42), Value::Text(Text::new("hello"))]);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();
        assert!(!iter.is_empty());
        assert_eq!(iter.clone().count(), 2);

        let mut iter = ValueIterator::new(&buf).unwrap();

        let val = iter.next().unwrap().unwrap();
        assert_eq!(val, ValueRef::from_i64(42));

        let val = iter.next().unwrap().unwrap();
        assert_eq!(
            val,
            ValueRef::Text(TextRef::new("hello", TextSubtype::Text))
        );

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_value_iterator_nulls() {
        let mut buf = std::vec::Vec::new();
        let record = Record::new(vec![Value::Null, Value::Null, Value::Null]);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();

        for val in iter {
            assert_eq!(val.unwrap(), ValueRef::Null);
        }
    }

    #[test]
    fn test_value_iterator_mixed_types() {
        let mut buf = std::vec::Vec::new();
        let record = Record::new(vec![
            Value::Null,
            Value::from_i64(100),
            Value::from_f64(std::f64::consts::PI),
            Value::Text(Text::new("test")),
            Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG),
            Value::from_i64(0),
            Value::from_i64(1),
        ]);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();
        let values: Vec<_> = iter.try_collect::<Result<Vec<_>>>().unwrap().unwrap();

        assert_eq!(values[0], ValueRef::Null);
        assert_eq!(values[1], ValueRef::from_i64(100));
        assert_eq!(values[2], ValueRef::from_f64(std::f64::consts::PI));
        assert_eq!(
            values[3],
            ValueRef::Text(TextRef::new("test", TextSubtype::Text))
        );
        assert_eq!(values[4], ValueRef::Blob(&[1, 2, 3]));
        assert_eq!(values[5], ValueRef::from_i64(0));
        assert_eq!(values[6], ValueRef::from_i64(1));
    }

    #[test]
    fn test_value_iterator_large_record() {
        let mut buf = std::vec::Vec::new();
        let values: Vec<Value> = (0..20)
            .map(|i| Value::from_i64(i as i64))
            .try_collect()
            .unwrap();
        let record = Record::new(values);
        record.serialize(&mut buf);

        let iter = ValueIterator::new(&buf).unwrap();
        assert_eq!(iter.count(), 20);

        let iter = ValueIterator::new(&buf).unwrap();
        for (i, val) in iter.enumerate() {
            assert_eq!(val.unwrap(), ValueRef::from_i64(i as i64));
        }
    }

    #[test]
    fn test_value_iterator_zero_allocation() {
        let mut buf = std::vec::Vec::new();
        let values: Vec<Value> = (0..5)
            .map(|i| Value::from_i64(i as i64))
            .try_collect()
            .unwrap();
        let record = Record::new(values);
        record.serialize(&mut buf);

        let mut iter = ValueIterator::new(&buf).unwrap();
        let _ = iter.next();
        let _ = iter.next();
    }

    pub fn compare_immutable_for_testing(
        l: &[ValueRef],
        r: &[ValueRef],
        index_key_info: &[KeyInfo],
        tie_breaker: std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        let min_len = l.len().min(r.len());

        for i in 0..min_len {
            let column_order = index_key_info[i].sort_order;
            let collation = index_key_info[i].collation;

            let cmp = match (&l[i], &r[i]) {
                (ValueRef::Text(left), ValueRef::Text(right)) => {
                    collation.compare_strings(left, right)
                }
                _ => l[i].partial_cmp(&r[i]).unwrap_or(std::cmp::Ordering::Equal),
            };

            if cmp != std::cmp::Ordering::Equal {
                return match column_order {
                    SortOrder::Asc => cmp,
                    SortOrder::Desc => cmp.reverse(),
                };
            }
        }

        tie_breaker
    }

    fn create_record(values: Vec<Value>) -> ImmutableRecord {
        let registers: Vec<Register> = values
            .into_iter()
            .map(Register::Value)
            .try_collect()
            .unwrap();
        ImmutableRecord::from_registers(&registers, registers.len()).unwrap()
    }

    #[test]
    fn immutable_record_ref_borrows_bin_record_payload() {
        let expected_values = vec![Value::from_i64(42), Value::build_text("borrowed")];
        let record = create_record(expected_values.clone());
        let payload = record.get_payload();

        let borrowed = ImmutableRecordRef::from_bin_record(payload);

        assert_eq!(borrowed.get_payload().as_ptr(), payload.as_ptr());
        assert_eq!(borrowed.column_count(), 2);
        assert_eq!(borrowed.get_values_owned().unwrap(), expected_values);
    }

    fn create_index_info(
        num_cols: usize,
        sort_orders: Vec<SortOrder>,
        collations: Vec<CollationSeq>,
    ) -> IndexInfo {
        IndexInfo::new(
            sort_orders
                .into_iter()
                .zip(collations)
                .map(|(sort_order, collation)| KeyInfo {
                    sort_order,
                    collation,
                    nulls_order: None,
                }),
            false,
            num_cols,
            false,
        )
        .unwrap()
    }

    fn assert_compare_matches_full_comparison(
        serialized_values: Vec<Value>,
        unpacked_values: Vec<ValueRef>,
        index_info: &IndexInfo,
        test_name: &str,
    ) {
        let serialized = create_record(serialized_values.clone());

        let serialized_ref_values: Vec<ValueRef> = serialized_values
            .iter()
            .map(Value::as_ref)
            .try_collect()
            .unwrap();

        let tie_breaker = std::cmp::Ordering::Equal;

        let gold_result = compare_immutable_for_testing(
            &serialized_ref_values,
            &unpacked_values,
            &index_info.key_info,
            tie_breaker,
        );

        let comparer = find_compare(unpacked_values.iter().peekable(), index_info);
        let optimized_result = comparer
            .compare(&serialized, &unpacked_values, index_info, 0, tie_breaker)
            .unwrap();

        assert_eq!(
            gold_result, optimized_result,
            "Test '{test_name}' failed: Full Comparison: {gold_result:?}, Optimized: {optimized_result:?}, Strategy: {comparer:?}"
        );

        let generic_result = compare_records_generic(
            &serialized,
            unpacked_values.iter(),
            index_info,
            0,
            tie_breaker,
        )
        .unwrap();
        assert_eq!(
            gold_result, generic_result,
            "Test '{test_name}' failed with generic: Full Comparison: {gold_result:?}, Generic: {generic_result:?}\n LHS: {serialized_values:?}\n RHS: {unpacked_values:?}"
        );
    }

    #[test]
    fn test_calc_header_size() {
        // Test 1-byte header size (serial type sizes 0 to 126)
        const MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER: usize = 0;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1
        );
        const BITS_7_MAX: usize = (1 << 7) - 1; // varints use 7 bits for the value and 1 continuation bit
        const MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER: usize = BITS_7_MAX - 1;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1
        );

        // Test 2-byte header size (serial type sizes 127 to 16381)
        const MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER: usize =
            MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 2
        );
        const BITS_14_MAX: usize = (1 << 14) - 1;
        const MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER: usize = BITS_14_MAX - 2;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 2
        );

        // Test 3-byte header size (serial type sizes 16382 to 2097148)
        const MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER: usize =
            MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 1;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 3
        );
        const BITS_21_MAX: usize = (1 << 21) - 1;
        const MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER: usize = BITS_21_MAX - 3;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 3
        );

        // Test 4-byte header size (serial type sizes 2097149 to 268435451)
        const MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER: usize =
            MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 1;
        assert_eq!(
            Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER),
            MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER + 4
        );
        const BITS_28_MAX: usize = (1 << 28) - 1;
        const MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER: usize = BITS_28_MAX - 4;
        assert_eq!(
            Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER),
            MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER + 4
        );
    }

    #[test]
    fn test_integer_fast_path() {
        let index_info = create_index_info(
            2,
            vec![SortOrder::Asc, SortOrder::Asc],
            vec![CollationSeq::Binary; 2],
        );

        let test_cases = vec![
            (
                vec![Value::from_i64(42)],
                vec![ValueRef::from_i64(42)],
                "equal_integers",
            ),
            (
                vec![Value::from_i64(10)],
                vec![ValueRef::from_i64(20)],
                "less_than_integers",
            ),
            (
                vec![Value::from_i64(30)],
                vec![ValueRef::from_i64(20)],
                "greater_than_integers",
            ),
            (
                vec![Value::from_i64(0)],
                vec![ValueRef::from_i64(0)],
                "zero_integers",
            ),
            (
                vec![Value::from_i64(-5)],
                vec![ValueRef::from_i64(-5)],
                "negative_integers",
            ),
            (
                vec![Value::from_i64(i64::MAX)],
                vec![ValueRef::from_i64(i64::MAX)],
                "max_integers",
            ),
            (
                vec![Value::from_i64(i64::MIN)],
                vec![ValueRef::from_i64(i64::MIN)],
                "min_integers",
            ),
            (
                vec![Value::from_i64(42), Value::Text(Text::new("hello"))],
                vec![
                    ValueRef::from_i64(42),
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                ],
                "integer_text_equal",
            ),
            (
                vec![Value::from_i64(42), Value::Text(Text::new("hello"))],
                vec![
                    ValueRef::from_i64(42),
                    ValueRef::Text(TextRef::new("world", TextSubtype::Text)),
                ],
                "integer_equal_text_different",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            println!(
                "Testing integer fast path `{test_name}`\nLHS: {serialized_values:?}\nRHS: {unpacked_values:?}"
            );
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_string_fast_path() {
        let index_info = create_index_info(
            2,
            vec![SortOrder::Asc, SortOrder::Asc],
            vec![CollationSeq::Binary; 2],
        );

        let test_cases = vec![
            (
                vec![Value::Text(Text::new("hello"))],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "equal_strings",
            ),
            (
                vec![Value::Text(Text::new("abc"))],
                vec![ValueRef::Text(TextRef::new("def", TextSubtype::Text))],
                "less_than_strings",
            ),
            (
                vec![Value::Text(Text::new("xyz"))],
                vec![ValueRef::Text(TextRef::new("abc", TextSubtype::Text))],
                "greater_than_strings",
            ),
            (
                vec![Value::Text(Text::new(""))],
                vec![ValueRef::Text(TextRef::new("", TextSubtype::Text))],
                "empty_strings",
            ),
            (
                vec![Value::Text(Text::new("a"))],
                vec![ValueRef::Text(TextRef::new("aa", TextSubtype::Text))],
                "prefix_strings",
            ),
            // Multi-field with string first
            (
                vec![Value::Text(Text::new("hello")), Value::from_i64(42)],
                vec![
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                    ValueRef::from_i64(42),
                ],
                "string_integer_equal",
            ),
            (
                vec![Value::Text(Text::new("hello")), Value::from_i64(42)],
                vec![
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                    ValueRef::from_i64(99),
                ],
                "string_equal_integer_different",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_type_precedence() {
        let index_info = create_index_info(1, vec![SortOrder::Asc], vec![CollationSeq::Binary]);

        // Test SQLite type precedence: NULL < Numbers < Text < Blob
        let test_cases = vec![
            // NULL vs others
            (
                vec![Value::Null],
                vec![ValueRef::from_i64(42)],
                "null_vs_integer",
            ),
            (
                vec![Value::Null],
                vec![ValueRef::from_f64(64.4)],
                "null_vs_float",
            ),
            (
                vec![Value::Null],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "null_vs_text",
            ),
            (
                vec![Value::Null],
                vec![ValueRef::Blob(b"blob")],
                "null_vs_blob",
            ),
            // Numbers vs Text/Blob
            (
                vec![Value::from_i64(42)],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "integer_vs_text",
            ),
            (
                vec![Value::from_f64(64.4)],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "float_vs_text",
            ),
            (
                vec![Value::from_i64(42)],
                vec![ValueRef::Blob(b"blob")],
                "integer_vs_blob",
            ),
            (
                vec![Value::from_f64(64.4)],
                vec![ValueRef::Blob(b"blob")],
                "float_vs_blob",
            ),
            // Text vs Blob
            (
                vec![Value::Text(Text::new("hello"))],
                vec![ValueRef::Blob(b"blob")],
                "text_vs_blob",
            ),
            // Integer vs Float (affinity conversion)
            (
                vec![Value::from_i64(42)],
                vec![ValueRef::from_f64(42.0)],
                "integer_vs_equal_float",
            ),
            (
                vec![Value::from_i64(42)],
                vec![ValueRef::from_f64(42.5)],
                "integer_vs_different_float",
            ),
            (
                vec![Value::from_f64(42.5)],
                vec![ValueRef::from_i64(42)],
                "float_vs_integer",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_sort_order_desc() {
        let index_info = create_index_info(
            2,
            vec![SortOrder::Desc, SortOrder::Asc],
            vec![CollationSeq::Binary; 2],
        );

        let test_cases = vec![
            // DESC order should reverse first field comparison
            (
                vec![Value::from_i64(10)],
                vec![ValueRef::from_i64(20)],
                "desc_integer_reversed",
            ),
            (
                vec![Value::Text(Text::new("abc"))],
                vec![ValueRef::Text(TextRef::new("def", TextSubtype::Text))],
                "desc_string_reversed",
            ),
            // Mixed sort orders
            (
                vec![Value::from_i64(10), Value::Text(Text::new("hello"))],
                vec![
                    ValueRef::from_i64(20),
                    ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                ],
                "desc_first_asc_second",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_edge_cases() {
        let index_info =
            create_index_info(15, vec![SortOrder::Asc; 15], vec![CollationSeq::Binary; 15]);

        let test_cases = vec![
            (
                vec![Value::from_i64(42)],
                vec![
                    ValueRef::from_i64(42),
                    ValueRef::Text(TextRef::new("extra", TextSubtype::Text)),
                ],
                "fewer_serialized_fields",
            ),
            (
                vec![Value::from_i64(42), Value::Text(Text::new("extra"))],
                vec![ValueRef::from_i64(42)],
                "fewer_unpacked_fields",
            ),
            (vec![], vec![], "both_empty"),
            (vec![], vec![ValueRef::from_i64(42)], "empty_serialized"),
            (
                (0..15).map(Value::from_i64).try_collect().unwrap(),
                (0..15).map(ValueRef::from_i64).try_collect().unwrap(),
                "large_field_count",
            ),
            (
                vec![Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG)],
                vec![ValueRef::Blob(&[1, 2, 3])],
                "blob_first_field",
            ),
            (
                vec![Value::Text(Text::new("hello")), Value::from_i64(5)],
                vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
                "equal_text_prefix_but_more_serialized_fields",
            ),
            (
                vec![Value::Text(Text::new("same")), Value::from_i64(5)],
                vec![
                    ValueRef::Text(TextRef::new("same", TextSubtype::Text)),
                    ValueRef::from_i64(5),
                ],
                "equal_text_then_equal_int",
            ),
        ];

        for (serialized_values, unpacked_values, test_name) in test_cases {
            assert_compare_matches_full_comparison(
                serialized_values,
                unpacked_values,
                &index_info,
                test_name,
            );
        }
    }

    #[test]
    fn test_skip_parameter() {
        let index_info = create_index_info(
            3,
            vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc],
            vec![CollationSeq::Binary; 3],
        );

        let serialized = create_record(vec![
            Value::from_i64(1),
            Value::from_i64(2),
            Value::from_i64(3),
        ]);
        let unpacked = [
            ValueRef::from_i64(1),
            ValueRef::from_i64(99),
            ValueRef::from_i64(3),
        ];

        let tie_breaker = std::cmp::Ordering::Equal;
        let result_skip_0 =
            compare_records_generic(&serialized, unpacked.iter(), &index_info, 0, tie_breaker)
                .unwrap();
        let result_skip_1 =
            compare_records_generic(&serialized, unpacked.iter(), &index_info, 1, tie_breaker)
                .unwrap();

        assert_eq!(result_skip_0, std::cmp::Ordering::Less);

        assert_eq!(result_skip_1, std::cmp::Ordering::Less);
    }

    #[test]
    fn test_strategy_selection() {
        let collations_small = vec![CollationSeq::Binary; 3];
        let collations_large = vec![CollationSeq::Binary; 15];
        let index_info_small = create_index_info(
            3,
            vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc],
            collations_small,
        );
        let index_info_large = create_index_info(15, vec![SortOrder::Asc; 15], collations_large);

        let int_values = [
            ValueRef::from_i64(42),
            ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
        ];
        assert!(matches!(
            find_compare(int_values.iter().peekable(), &index_info_small),
            RecordCompare::Int
        ));

        let string_values = [
            ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
            ValueRef::from_i64(42),
        ];
        assert!(matches!(
            find_compare(string_values.iter().peekable(), &index_info_small),
            RecordCompare::String
        ));

        let large_values: Vec<ValueRef> = (0..15).map(ValueRef::from_i64).try_collect().unwrap();
        assert!(matches!(
            find_compare(large_values.iter().peekable(), &index_info_large),
            RecordCompare::Generic
        ));

        let blob_values = [ValueRef::Blob(&[1, 2, 3])];
        assert!(matches!(
            find_compare(blob_values.iter().peekable(), &index_info_small),
            RecordCompare::Generic
        ));
    }

    #[test]
    fn test_serialize_null() {
        let record = Record::new(vec![Value::Null]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::null()));
        // Check that the buffer is empty after the header
        assert_eq!(buf.len(), header_length);
    }

    #[test]
    fn test_serialize_integers() {
        let record = Record::new(vec![
            Value::from_i64(0),                 // Should use ConstInt0
            Value::from_i64(1),                 // Should use ConstInt1
            Value::from_i64(42),                // Should use SERIAL_TYPE_I8
            Value::from_i64(1000),              // Should use SERIAL_TYPE_I16
            Value::from_i64(1_000_000),         // Should use SERIAL_TYPE_I24
            Value::from_i64(1_000_000_000),     // Should use SERIAL_TYPE_I32
            Value::from_i64(1_000_000_000_000), // Should use SERIAL_TYPE_I48
            Value::from_i64(i64::MAX),          // Should use SERIAL_TYPE_I64
        ]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8); // Header should be larger than number of values

        // Check that correct serial types were chosen
        assert_eq!(header[1] as u64, u64::from(SerialType::const_int0())); // 8
        assert_eq!(header[2] as u64, u64::from(SerialType::const_int1())); // 9
        assert_eq!(header[3] as u64, u64::from(SerialType::i8())); // 1
        assert_eq!(header[4] as u64, u64::from(SerialType::i16())); // 2
        assert_eq!(header[5] as u64, u64::from(SerialType::i24())); // 3
        assert_eq!(header[6] as u64, u64::from(SerialType::i32())); // 4
        assert_eq!(header[7] as u64, u64::from(SerialType::i48())); // 5
        assert_eq!(header[8] as u64, u64::from(SerialType::i64())); // 6

        // test that the bytes after the header can be interpreted as the correct values
        let mut cur_offset = header_length;

        // Value::from_i64(0) - ConstInt0: NO PAYLOAD BYTES
        // Value::from_i64(1) - ConstInt1: NO PAYLOAD BYTES

        // Value::from_i64(42) - I8: 1 byte
        let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
        cur_offset += size_of::<i8>();

        // Value::from_i64(1000) - I16: 2 bytes
        let i16_bytes = &buf[cur_offset..cur_offset + size_of::<i16>()];
        cur_offset += size_of::<i16>();

        // Value::from_i64(1_000_000) - I24: 3 bytes
        let i24_bytes = &buf[cur_offset..cur_offset + 3];
        cur_offset += 3;

        // Value::from_i64(1_000_000_000) - I32: 4 bytes
        let i32_bytes = &buf[cur_offset..cur_offset + size_of::<i32>()];
        cur_offset += size_of::<i32>();

        // Value::from_i64(1_000_000_000_000) - I48: 6 bytes
        let i48_bytes = &buf[cur_offset..cur_offset + 6];
        cur_offset += 6;

        // Value::from_i64(i64::MAX) - I64: 8 bytes
        let i64_bytes = &buf[cur_offset..cur_offset + size_of::<i64>()];

        // Verify the payload values
        let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
        let val_int16 = i16::from_be_bytes(i16_bytes.try_into().unwrap());

        let mut i24_with_padding = vec![0];
        i24_with_padding.extend(i24_bytes);
        let val_int24 = i32::from_be_bytes(i24_with_padding.try_into().unwrap());

        let val_int32 = i32::from_be_bytes(i32_bytes.try_into().unwrap());

        let mut i48_with_padding = vec![0, 0];
        i48_with_padding.extend(i48_bytes);
        let val_int48 = i64::from_be_bytes(i48_with_padding.try_into().unwrap());

        let val_int64 = i64::from_be_bytes(i64_bytes.try_into().unwrap());

        assert_eq!(val_int8, 42);
        assert_eq!(val_int16, 1000);
        assert_eq!(val_int24, 1_000_000);
        assert_eq!(val_int32, 1_000_000_000);
        assert_eq!(val_int48, 1_000_000_000_000);
        assert_eq!(val_int64, i64::MAX);

        //Size of buffer = header + payload bytes
        // ConstInt0 and ConstInt1 contribute 0 bytes to payload
        assert_eq!(
            buf.len(),
            header_length  // 9 bytes (header size + 8 serial types)
                + size_of::<i8>()        // I8: 1 byte
                + size_of::<i16>()        // I16: 2 bytes
                + (size_of::<i32>() - 1)        // I24: 3 bytes
                + size_of::<i32>()        // I32: 4 bytes
                + (size_of::<i64>() - 2)        // I48: 6 bytes
                + size_of::<i64>() // I64: 8 bytes
        );
    }

    #[test]
    fn test_serialize_const_integers() {
        let record = Record::new(vec![Value::from_i64(0), Value::from_i64(1)]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        // [header_size, serial_type_0, serial_type_1] + no payload bytes
        let expected_header_size = 3; // 1 byte for header size + 2 bytes for serial types

        assert_eq!(buf.len(), expected_header_size);

        // Check header size
        assert_eq!(buf[0], expected_header_size as u8);

        assert_eq!(buf[1] as u64, u64::from(SerialType::const_int0())); // Should be 8
        assert_eq!(buf[2] as u64, u64::from(SerialType::const_int1())); // Should be 9

        assert_eq!(buf[1], 8); // ConstInt0 serial type
        assert_eq!(buf[2], 9); // ConstInt1 serial type
    }

    #[test]
    fn test_serialize_single_const_int0() {
        let record = Record::new(vec![Value::from_i64(0)]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        // Expected: [header_size=2, serial_type=8]
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 2); // Header size
        assert_eq!(buf[1], 8); // ConstInt0 serial type
    }

    #[test]
    fn test_serialize_float() {
        #[warn(clippy::approx_constant)]
        let record = Record::new(vec![Value::from_f64(3.15555)]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for FLOAT
        assert_eq!(header[1] as u64, u64::from(SerialType::f64()));
        // Check that the bytes after the header can be interpreted as the float
        let float_bytes = &buf[header_length..header_length + size_of::<f64>()];
        let float = f64::from_be_bytes(float_bytes.try_into().unwrap());
        assert_eq!(float, 3.15555);
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + size_of::<f64>());
    }

    #[test]
    fn test_serialize_text() {
        let text = "hello";
        let record = Record::new(vec![Value::Text(Text::new(text))]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for TEXT, which is (len * 2 + 13)
        assert_eq!(header[1], (5 * 2 + 13) as u8);
        // Check the actual text bytes
        assert_eq!(&buf[2..7], b"hello");
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + text.len());
    }

    #[test]
    fn test_serialize_blob() {
        let blob = std::vec![1, 2, 3, 4, 5];
        let record = Record::new(vec![
            Value::from_slice(&blob).expect(crate::alloc::ALLOC_ERR_MSG)
        ]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for BLOB, which is (len * 2 + 12)
        assert_eq!(header[1], (5 * 2 + 12) as u8);
        // Check the actual blob bytes
        assert_eq!(&buf[2..7], &[1, 2, 3, 4, 5]);
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + blob.len());
    }

    #[test]
    fn test_serialize_mixed_types() {
        let text = "test";
        let record = Record::new(vec![
            Value::Null,
            Value::from_i64(42),
            Value::from_f64(3.15),
            Value::Text(Text::new(text)),
        ]);
        let mut buf = std::vec::Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::null()));
        // Third byte should be serial type for I8
        assert_eq!(header[2] as u64, u64::from(SerialType::i8()));
        // Fourth byte should be serial type for F64
        assert_eq!(header[3] as u64, u64::from(SerialType::f64()));
        // Fifth byte should be serial type for TEXT, which is (len * 2 + 13)
        assert_eq!(header[4] as u64, (4 * 2 + 13) as u64);

        // Check that the bytes after the header can be interpreted as the correct values
        let mut cur_offset = header_length;
        let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
        cur_offset += size_of::<i8>();
        let f64_bytes = &buf[cur_offset..cur_offset + size_of::<f64>()];
        cur_offset += size_of::<f64>();
        let text_bytes = &buf[cur_offset..cur_offset + text.len()];

        let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
        let val_float = f64::from_be_bytes(f64_bytes.try_into().unwrap());
        let val_text = String::from_utf8(text_bytes.to_vec()).unwrap();

        assert_eq!(val_int8, 42);
        assert_eq!(val_float, 3.15);
        assert_eq!(val_text, "test");

        // Check that buffer length is correct
        assert_eq!(
            buf.len(),
            header_length + size_of::<i8>() + size_of::<f64>() + text.len()
        );
    }

    /// Before the Numeric refactor, ValueRef had separate Float(f64) and Integer(i64)
    /// variants. A raw f64::NAN could be stored in Float, and comparing two NaN floats
    /// via partial_cmp returned None. The .unwrap() in Ord::cmp and
    /// compare_immutable_single would then panic.
    ///
    /// Now Numeric::Float wraps NonNan, which rejects NaN at construction time.
    /// This makes it impossible to represent NaN in a ValueRef, so partial_cmp
    /// is total and can never return None for any representable value.
    #[test]
    fn test_valueref_partial_cmp_no_panic_on_nan() {
        use crate::numeric::nonnan::NonNan;

        // NonNan::new rejects NaN — this is the type-level guarantee that
        // prevents the old panic. No ValueRef::Float(NAN) can be constructed.
        assert!(NonNan::new(f64::NAN).is_none());

        // from_f64(NAN) falls back to Null instead of storing a NaN float.
        assert_eq!(ValueRef::from_f64(f64::NAN), ValueRef::Null);

        // Exercise every representable float edge case through partial_cmp,
        // Ord::cmp, and compare_immutable_single — none of these can panic now.
        let values: Vec<ValueRef> = vec![
            ValueRef::Null,
            ValueRef::from_i64(0),
            ValueRef::from_i64(-1),
            ValueRef::from_i64(i64::MAX),
            ValueRef::from_i64(i64::MIN),
            ValueRef::from_f64(0.0),
            ValueRef::from_f64(-0.0),
            ValueRef::from_f64(1.5),
            ValueRef::from_f64(-1.5),
            ValueRef::from_f64(f64::MAX),
            ValueRef::from_f64(f64::MIN),
            ValueRef::from_f64(f64::MIN_POSITIVE),
            ValueRef::from_f64(f64::INFINITY),
            ValueRef::from_f64(f64::NEG_INFINITY),
            ValueRef::from_f64(f64::NAN), // becomes Null
            ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
            ValueRef::Text(TextRef::new("", TextSubtype::Text)),
            ValueRef::Blob(&[1, 2, 3]),
            ValueRef::Blob(&[]),
        ];

        // partial_cmp must return Some for every pair — the old code panicked
        // here when either side was Float(NAN).
        for (i, a) in values.iter().enumerate() {
            for (j, b) in values.iter().enumerate() {
                let result = a.partial_cmp(b);
                assert!(
                    result.is_some(),
                    "partial_cmp returned None for values[{i}]={a:?} vs values[{j}]={b:?}"
                );
                // Ord::cmp (which previously called partial_cmp().unwrap()) must agree.
                assert_eq!(result.unwrap(), a.cmp(b));
            }
        }

        // compare_immutable_single is where the unwrap panic originally surfaced.
        for a in &values {
            for b in &values {
                let _ = compare_immutable_single(*a, *b, CollationSeq::Binary);
            }
        }

        // Antisymmetry holds for all pairs.
        for a in &values {
            for b in &values {
                let ab = a.cmp(b);
                let ba = b.cmp(a);
                assert_eq!(ab, ba.reverse(), "antisymmetry failed for {a:?} vs {b:?}");
            }
        }
    }

    #[test]
    fn test_column_count_matches_values_written() {
        // Test with different numbers of values
        for num_values in 1..=10 {
            let values: Vec<Value> = (0..num_values)
                .map(|i| Value::from_i64(i as i64))
                .try_collect()
                .unwrap();

            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
            let cnt = record.column_count();
            assert_eq!(
                cnt, num_values,
                "column_count should be {num_values}, not {cnt}"
            );
        }
    }

    #[test]
    fn test_value_try_clone_from_reuses_allocations() {
        let src = Value::build_text(String::from("short"));
        let mut dst =
            Value::build_text(String::from("a destination string with plenty of capacity"));
        let ptr = match &dst {
            Value::Text(t) => t.as_str().as_ptr(),
            _ => unreachable!(),
        };
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);
        match &dst {
            Value::Text(t) => assert_eq!(t.as_str().as_ptr(), ptr),
            _ => unreachable!(),
        }

        let mut dst = Value::build_text("static text");
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);

        let src = Value::Blob(vec![1, 2, 3]);
        let mut big_blob: ValueBlob = vec![];
        big_blob.extend(0u8..32);
        let mut dst = Value::Blob(big_blob);
        let ptr = match &dst {
            Value::Blob(b) => b.as_ptr(),
            _ => unreachable!(),
        };
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);
        match &dst {
            Value::Blob(b) => assert_eq!(b.as_ptr(), ptr),
            _ => unreachable!(),
        }

        let src = Value::from_i64(9);
        let mut dst = Value::build_text("text");
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);
        let src = Value::build_text("into an integer slot");
        let mut dst = Value::from_i64(3);
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);
    }

    #[test]
    fn test_build_reuses_retired_buffer_and_matches_from_values() {
        let mut blob: ValueBlob = vec![];
        blob.extend(0u8..64);
        let big = vec![
            Value::build_text("a longer text value that forces a real allocation"),
            Value::Blob(blob),
            Value::from_i64(42),
        ];
        let small = vec![Value::from_i64(1), Value::Null];

        let expected_big = ImmutableRecord::from_values(&big, big.len()).unwrap();
        let expected_small = ImmutableRecord::from_values(&small, small.len()).unwrap();

        let record = ImmutableRecord::build(&big, RecordBuf::alloc()).unwrap();
        assert_eq!(record.get_payload(), expected_big.get_payload());

        let capacity = record.as_blob().capacity();
        let ptr = record.get_payload().as_ptr();
        let record = ImmutableRecord::build(&small, record.retire()).unwrap();
        assert_eq!(record.get_payload(), expected_small.get_payload());
        assert_eq!(record.as_blob().capacity(), capacity);
        assert_eq!(record.get_payload().as_ptr(), ptr);

        let record = ImmutableRecord::build(&big, expected_small.retire()).unwrap();
        assert_eq!(record.get_payload(), expected_big.get_payload());
    }

    #[test]
    fn test_copy_payload_reuses_buffer() {
        let values = vec![Value::build_text("payload to copy"), Value::from_i64(7)];
        let source = ImmutableRecord::from_values(&values, values.len()).unwrap();
        let spare = ImmutableRecord::from_values(&values, values.len()).unwrap();

        let ptr = spare.get_payload().as_ptr();
        let copy = ImmutableRecord::copy_payload(source.get_payload(), spare.retire()).unwrap();
        assert_eq!(copy.get_payload(), source.get_payload());
        assert_eq!(copy.get_payload().as_ptr(), ptr);
    }

    #[test]
    fn test_value_try_clone() {
        let values = [
            Value::Null,
            Value::from_i64(7),
            Value::build_text("text"),
            Value::Blob(vec![1, 2, 3]),
        ];

        for value in values {
            assert_eq!(value.try_clone().unwrap(), value);
        }
    }
}
