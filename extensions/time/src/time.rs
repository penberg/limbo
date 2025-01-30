use std::str::FromStr;

use chrono::prelude::*;
use chrono::{self, DateTime, NaiveDate, Timelike, Utc};

use limbo_ext::Value;

use crate::{Result, TimeError};

const DAYS_BEFORE_EPOCH: i64 = 719162;
const TIME_BLOB_SIZE: usize = 13;
const VERSION: u8 = 1;

#[derive(Debug)]
enum TimePrecision {
    Seconds,
    Millis,
    Micro,
    Nano,
}

#[derive(Debug)]
pub struct Time {
    // seconds: i64,
    // nanoseconds: u32,
    inner: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Duration {
    inner: i64,
}

#[derive(strum_macros::Display, strum_macros::EnumString)]
pub enum TimeField {
    #[strum(to_string = "millennium")]
    Millennium,
    #[strum(to_string = "century")]
    Century,
    #[strum(to_string = "decade")]
    Decade,
    #[strum(to_string = "year")]
    Year,
    #[strum(to_string = "quarter")]
    Quarter,
    #[strum(to_string = "month")]
    Month,
    #[strum(to_string = "day")]
    Day,
    #[strum(to_string = "hour")]
    Hour,
    #[strum(to_string = "minute")]
    Minute,
    #[strum(to_string = "second")]
    Second,
    #[strum(to_string = "millisecond")]
    MilliSecond,
    #[strum(to_string = "milli")]
    Milli,
    #[strum(to_string = "microsecond")]
    MicroSecond,
    #[strum(to_string = "micro")]
    Micro,
    #[strum(to_string = "nanosecond")]
    NanoSecond,
    #[strum(to_string = "nano")]
    Nano,
    #[strum(to_string = "isoyear")]
    IsoYear,
    #[strum(to_string = "isoweek")]
    IsoWeek,
    #[strum(to_string = "isodow")]
    IsoDow,
    #[strum(to_string = "yearday")]
    YearDay,
    #[strum(to_string = "weekday")]
    WeekDay,
    #[strum(to_string = "epoch")]
    Epoch,
}

impl Time {
    pub fn new() -> Self {
        Self { inner: Utc::now() }
    }

    pub fn into_blob(self) -> Value {
        let blob: [u8; 13] = self.into();
        Value::from_blob(blob.to_vec())
    }

    pub fn fmt_iso(&self, offset_sec: i32) -> Result<String> {
        if offset_sec == 0 {
            if self.inner.nanosecond() == 0 {
                return Ok(self.inner.format("%FT%TZ").to_string());
            } else {
                return Ok(self.inner.format("%FT%T%.9fZ").to_string());
            }
        }
        // I do not see how this can error
        let offset = &FixedOffset::east_opt(offset_sec).ok_or(TimeError::InvalidFormat)?;

        let timezone_date = self.inner.with_timezone(offset);

        if timezone_date.nanosecond() == 0 {
            return Ok(timezone_date.format("%FT%T%:z").to_string());
        } else {
            return Ok(timezone_date.format("%FT%T%.9f%:z").to_string());
        }
    }

    /// Adjust the datetime to the offset
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self { inner: dt }
    }

    pub fn time_get(&self, field: &str) -> Result<Value> {
        use TimeField::*;

        chrono::format::strftime
        self.inner.format_with_items(items)

        let val = match TimeField::from_str(field)? {
            Millennium => Value::from_integer((self.inner.year() / 1000) as i64),
            Century => Value::from_integer((self.inner.year() / 100) as i64),
            Decade => Value::from_integer((self.inner.year() / 10) as i64),
            Year => Value::from_integer(self.inner.year() as i64),
            Quarter => Value::from_integer((self.inner.month().div_ceil(4) + 1) as i64),
            Month => Value::from_integer(self.inner.month() as i64),
            Day => Value::from_integer(self.inner.day() as i64),
            Hour => Value::from_integer(self.inner.hour() as i64),
            Minute => Value::from_integer(self.inner.minute() as i64),
            Second => Value::from_float(f64::from_str(s))
            MilliSecond | Milli => Value:: from_integer(self.inner.)

        };

        Ok(val)
    }
}

impl From<Time> for [u8; TIME_BLOB_SIZE] {
    fn from(value: Time) -> Self {
        let mut blob = [0u8; 13];

        let seconds = value.inner.timestamp() + (3600 * 24 * DAYS_BEFORE_EPOCH);
        let nanoseconds = value.inner.timestamp_subsec_nanos();

        blob[0] = VERSION;
        blob[1] = (seconds >> 56) as u8; // bytes 1-8: seconds
        blob[2] = (seconds >> 48) as u8;
        blob[3] = (seconds >> 40) as u8;
        blob[4] = (seconds >> 32) as u8;
        blob[5] = (seconds >> 24) as u8;
        blob[6] = (seconds >> 16) as u8;
        blob[7] = (seconds >> 8) as u8;
        blob[8] = seconds as u8;
        blob[9] = (nanoseconds >> 24) as u8; // bytes 9-12: nanoseconds
        blob[10] = (nanoseconds >> 16) as u8;
        blob[11] = (nanoseconds >> 8) as u8;
        blob[12] = (nanoseconds) as u8;

        blob
    }
}

impl TryFrom<Vec<u8>> for Time {
    type Error = TimeError;

    fn try_from(value: Vec<u8>) -> Result<Time> {
        if value.len() != TIME_BLOB_SIZE {
            return Err(TimeError::InvalidSize);
        }
        if value[0] != VERSION {
            return Err(TimeError::MismatchVersion);
        }

        let seconds = value[8] as i64
            | (value[7] as i64) << 8
            | (value[6] as i64) << 16
            | (value[5] as i64) << 24
            | (value[4] as i64) << 32
            | (value[3] as i64) << 40
            | (value[2] as i64) << 48
            | (value[1] as i64) << 56;

        let nanoseconds = value[12] as u32
            | (value[11] as u32) << 8
            | (value[10] as u32) << 16
            | (value[9] as u32) << 24;

        Ok(Self {
            inner: DateTime::from_timestamp(seconds - (3600 * 24 * DAYS_BEFORE_EPOCH), nanoseconds)
                .ok_or_else(|| TimeError::InvalidFormat)?
                .to_utc(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let t = Time::new();
    }
}
