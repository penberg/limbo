use std::ops::{Deref, Sub};

use chrono::{self, DateTime, Timelike, Utc};
use chrono::{prelude::*, DurationRound};

use limbo_ext::Value;

use crate::{Result, TimeError};

const DAYS_BEFORE_EPOCH: i64 = 719162;
const TIME_BLOB_SIZE: usize = 13;
const VERSION: u8 = 1;

#[derive(Debug, PartialEq, PartialOrd, Eq)]
pub struct Time {
    inner: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub struct Duration {
    inner: chrono::Duration,
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

#[derive(strum_macros::Display, strum_macros::EnumString)]
pub enum TimeRoundField {
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
    #[strum(to_string = "week")]
    Week,
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
}

impl Time {
    /// Returns a new instance of Time with tracking UTC::now
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
            Ok(timezone_date.format("%FT%T%:z").to_string())
        } else {
            Ok(timezone_date.format("%FT%T%.9f%:z").to_string())
        }
    }

    pub fn fmt_datetime(&self, offset_sec: i32) -> Result<String> {
        let fmt = "%F %T";

        if offset_sec == 0 {
            return Ok(self.inner.format(fmt).to_string());
        }
        // I do not see how this can error
        let offset = &FixedOffset::east_opt(offset_sec).ok_or(TimeError::InvalidFormat)?;

        let timezone_date = self.inner.with_timezone(offset);

        Ok(timezone_date.format(fmt).to_string())
    }

    pub fn fmt_date(&self, offset_sec: i32) -> Result<String> {
        let fmt = "%F";

        if offset_sec == 0 {
            return Ok(self.inner.format(fmt).to_string());
        }
        // I do not see how this can error
        let offset = &FixedOffset::east_opt(offset_sec).ok_or(TimeError::InvalidFormat)?;

        let timezone_date = self.inner.with_timezone(offset);

        Ok(timezone_date.format(fmt).to_string())
    }

    pub fn fmt_time(&self, offset_sec: i32) -> Result<String> {
        let fmt = "%T";

        if offset_sec == 0 {
            return Ok(self.inner.format(fmt).to_string());
        }
        // I do not see how this can error
        let offset = &FixedOffset::east_opt(offset_sec).ok_or(TimeError::InvalidFormat)?;

        let timezone_date = self.inner.with_timezone(offset);

        Ok(timezone_date.format(fmt).to_string())
    }

    /// Adjust the datetime to the offset
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self { inner: dt }
    }

    //
    #[allow(clippy::too_many_arguments)]
    pub fn time_date(
        year: i32,
        month: i32,
        day: i64,
        hour: i64,
        minutes: i64,
        seconds: i64,
        nano_secs: i64,
        offset: FixedOffset,
    ) -> Result<Self> {
        let mut dt: NaiveDateTime = NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        match year.cmp(&0) {
            std::cmp::Ordering::Greater => {
                dt = dt
                    .checked_add_months(chrono::Months::new((year - 1).unsigned_abs() * 12))
                    .ok_or(TimeError::CreationError)?
            }
            std::cmp::Ordering::Less => {
                dt = dt
                    .checked_sub_months(chrono::Months::new((year - 1).unsigned_abs() * 12))
                    .ok_or(TimeError::CreationError)?
            }
            std::cmp::Ordering::Equal => (),
        };

        match month.cmp(&0) {
            std::cmp::Ordering::Greater => {
                dt = dt
                    .checked_add_months(chrono::Months::new((month - 1).unsigned_abs()))
                    .ok_or(TimeError::CreationError)?
            }
            std::cmp::Ordering::Less => {
                dt = dt
                    .checked_sub_months(chrono::Months::new((month - 1).unsigned_abs()))
                    .ok_or(TimeError::CreationError)?
            }
            std::cmp::Ordering::Equal => (),
        };

        dt += chrono::Duration::try_days(day - 1).ok_or(TimeError::CreationError)?;

        dt += chrono::Duration::try_hours(hour).ok_or(TimeError::CreationError)?;
        dt += chrono::Duration::try_minutes(minutes).ok_or(TimeError::CreationError)?;
        dt += chrono::Duration::try_seconds(seconds).ok_or(TimeError::CreationError)?;

        dt += chrono::Duration::nanoseconds(nano_secs);

        dt = dt
            .and_local_timezone(offset)
            .single()
            .ok_or(TimeError::CreationError)?
            .naive_utc();

        Ok(dt.into())
    }

    pub fn time_add_date(self, years: i32, months: i32, days: i64) -> Result<Self> {
        let mut dt: NaiveDateTime = self.into();

        match years.cmp(&0) {
            std::cmp::Ordering::Greater => {
                dt = dt
                    .checked_add_months(chrono::Months::new(years.unsigned_abs() * 12))
                    .ok_or(TimeError::CreationError)?;
            }
            std::cmp::Ordering::Less => {
                dt = dt
                    .checked_sub_months(chrono::Months::new(years.unsigned_abs() * 12))
                    .ok_or(TimeError::CreationError)?;
            }
            std::cmp::Ordering::Equal => (),
        };

        match months.cmp(&0) {
            std::cmp::Ordering::Greater => {
                dt = dt
                    .checked_add_months(chrono::Months::new(months.unsigned_abs()))
                    .ok_or(TimeError::CreationError)?
            }
            std::cmp::Ordering::Less => {
                dt = dt
                    .checked_sub_months(chrono::Months::new(months.unsigned_abs()))
                    .ok_or(TimeError::CreationError)?
            }
            std::cmp::Ordering::Equal => (),
        };

        dt += chrono::Duration::try_days(days).ok_or(TimeError::CreationError)?;

        Ok(dt.into())
    }

    pub fn get_second(&self) -> i64 {
        self.inner.second() as i64
    }

    pub fn get_nanosecond(&self) -> i64 {
        self.inner.timestamp_subsec_nanos() as i64
    }

    pub fn to_unix(&self) -> i64 {
        self.inner.timestamp()
    }

    pub fn to_unix_milli(&self) -> i64 {
        self.inner.timestamp_millis()
    }

    pub fn to_unix_micro(&self) -> i64 {
        self.inner.timestamp_micros()
    }

    pub fn to_unix_nano(&self) -> Option<i64> {
        self.inner.timestamp_nanos_opt()
    }

    pub fn add_duration(&self, d: Duration) -> Self {
        Self {
            inner: self.inner + d.inner,
        }
    }

    pub fn sub_duration(&self, d: Duration) -> Self {
        Self {
            inner: self.inner - d.inner,
        }
    }

    pub fn trunc_duration(&self, d: Duration) -> Result<Self> {
        Ok(Self {
            inner: self.inner.duration_trunc(d.inner)?,
        })
    }

    pub fn trunc_field(&self, field: TimeRoundField) -> Result<Self> {
        use TimeRoundField::*;

        let year: i32;
        let mut month: i32 = 1;
        let mut week: i32 = 0;
        let mut day: i64 = 1;
        let mut hour: i64 = 0;
        let mut minutes: i64 = 0;
        let mut seconds: i64 = 0;
        let mut nano_secs: i64 = 0;
        let offset = FixedOffset::east_opt(0).unwrap(); // UTC

        match field {
            Millennium => {
                let millennium = (self.inner.year() / 1000) * 1000;
                year = millennium;
            }
            Century => {
                let century = (self.inner.year() / 100) * 100;
                year = century;
            }
            Decade => {
                let decade = (self.inner.year() / 10) * 10;
                year = decade;
            }
            Year => {
                year = self.inner.year();
            }
            Quarter => {
                let quarter = ((self.inner.month() - 1) / 3) as i32;
                year = self.inner.year();
                month = (quarter * 3) + 1;
            }
            Month => {
                year = self.inner.year();
                month = self.inner.month() as i32;
            }
            Week => {
                let isoweek = self.inner.iso_week();
                year = isoweek.year();
                week = isoweek.week() as i32;
            }
            Day => {
                year = self.inner.year();
                month = self.inner.month() as i32;
                day = self.inner.day() as i64;
            }
            Hour => {
                year = self.inner.year();
                month = self.inner.month() as i32;
                day = self.inner.day() as i64;
                hour = self.inner.hour() as i64;
            }
            Minute => {
                year = self.inner.year();
                month = self.inner.month() as i32;
                day = self.inner.day() as i64;
                hour = self.inner.hour() as i64;
                minutes = self.inner.minute() as i64;
            }
            Second => {
                year = self.inner.year();
                month = self.inner.month() as i32;
                day = self.inner.day() as i64;
                hour = self.inner.hour() as i64;
                minutes = self.inner.minute() as i64;
                seconds = self.inner.second() as i64;
            }
            MilliSecond | Milli => {
                year = self.inner.year();
                month = self.inner.month() as i32;
                day = self.inner.day() as i64;
                hour = self.inner.hour() as i64;
                minutes = self.inner.minute() as i64;
                seconds = self.inner.second() as i64;
                nano_secs = (self.inner.nanosecond() / 1_000_000 * 1_000_000) as i64;
            }
            MicroSecond | Micro => {
                year = self.inner.year();
                month = self.inner.month() as i32;
                day = self.inner.day() as i64;
                hour = self.inner.hour() as i64;
                minutes = self.inner.minute() as i64;
                seconds = self.inner.second() as i64;
                nano_secs = (self.inner.nanosecond() / 1_000 * 1_000) as i64;
            }
        };

        let mut ret = Self::time_date(year, month, day, hour, minutes, seconds, nano_secs, offset)?;

        // Means we have to adjust for the week
        if week != 0 {
            ret = ret.time_add_date(0, 0, ((week - 1) * 7) as i64)?;
        }

        Ok(ret)
    }

    pub fn round_duration(&self, d: Duration) -> Result<Self> {
        Ok(Self {
            inner: self.inner.duration_round(d.inner)?,
        })
    }

    pub fn time_get(&self, field: TimeField) -> Value {
        use TimeField::*;

        match field {
            Millennium => Value::from_integer((self.inner.year() / 1000) as i64),
            Century => Value::from_integer((self.inner.year() / 100) as i64),
            Decade => Value::from_integer((self.inner.year() / 10) as i64),
            Year => Value::from_integer(self.inner.year() as i64),
            Quarter => Value::from_integer(self.inner.month().div_ceil(3) as i64),
            Month => Value::from_integer(self.inner.month() as i64),
            Day => Value::from_integer(self.inner.day() as i64),
            Hour => Value::from_integer(self.inner.hour() as i64),
            Minute => Value::from_integer(self.inner.minute() as i64),
            Second => Value::from_float(
                self.inner.second() as f64 + (self.inner.nanosecond() as f64) / (1_000_000_000_f64),
            ),
            MilliSecond | Milli => {
                Value::from_integer((self.inner.nanosecond() / 1_000_000 % 1_000) as i64)
            }
            MicroSecond | Micro => {
                Value::from_integer((self.inner.nanosecond() / 1_000 % 1_000_000) as i64)
            }
            NanoSecond | Nano => {
                Value::from_integer((self.inner.nanosecond() % 1_000_000_000) as i64)
            }
            IsoYear => Value::from_integer(self.inner.iso_week().year() as i64),
            IsoWeek => Value::from_integer(self.inner.iso_week().week() as i64),
            IsoDow => Value::from_integer(self.inner.weekday().days_since(Weekday::Sun) as i64),
            YearDay => Value::from_integer(self.inner.ordinal() as i64),
            WeekDay => Value::from_integer(self.inner.weekday().num_days_from_sunday() as i64),
            Epoch => Value::from_float(
                self.inner.timestamp() as f64 + self.inner.nanosecond() as f64 / 1_000_000_000_f64,
            ),
        }
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
                .ok_or(TimeError::InvalidFormat)?
                .to_utc(),
        })
    }
}

impl From<NaiveDateTime> for Time {
    fn from(value: NaiveDateTime) -> Self {
        Self {
            inner: value.and_utc(),
        }
    }
}

impl From<Time> for NaiveDateTime {
    fn from(value: Time) -> Self {
        value.inner.naive_utc()
    }
}

impl Sub for Time {
    type Output = Duration;
    fn sub(self, rhs: Self) -> Self::Output {
        Duration {
            inner: self.inner - rhs.inner,
        }
    }
}

impl From<i64> for Duration {
    fn from(value: i64) -> Self {
        Self {
            inner: chrono::Duration::nanoseconds(value),
        }
    }
}

impl Deref for Duration {
    type Target = chrono::Duration;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
