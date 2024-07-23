use crate::types::OwnedValue;
use anyhow;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use log::trace;
use std::{error::Error, fmt::Display};

#[derive(Debug)]
enum DateTimeError {
    InvalidArgument(String),
    Other(String),
}

impl Display for DateTimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DateTimeError::InvalidArgument(s) => write!(f, "Invalid argument: {}", s),
            DateTimeError::Other(s) => write!(f, "Other error: {}", s),
        }
    }
}

impl Error for DateTimeError {}

#[derive(Debug, Clone, Copy)]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

/*
** The following table defines various date transformations of the form
**
**            'NNN days'
**
** Where NNN is an arbitrary floating-point number and "days" can be one
** of several units of time.
*/
impl TimeUnit {
    pub fn name(&self) -> &'static str {
        match self {
            TimeUnit::Second => "second",
            TimeUnit::Minute => "minute",
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Month => "month",
            TimeUnit::Year => "year",
        }
    }

    // Maximum value for each unit in Julian calendar
    // Each corresponds to ~14713 years in the Julian calendar, which equals
    // 10000 years in the Gregorian calendar
    pub fn max_value_julian(&self) -> f64 {
        match self {
            TimeUnit::Second => 4.6427e14,
            TimeUnit::Minute => 7.7379e12,
            TimeUnit::Hour => 1.2897e11,
            TimeUnit::Day => 5373485.0,
            TimeUnit::Month => 176546.0,
            TimeUnit::Year => 14713.0,
        }
    }

    // Conversion factor from the unit to seconds
    pub fn seconds_conversion(&self) -> f64 {
        match self {
            TimeUnit::Second => 1.0,
            TimeUnit::Minute => 60.0,
            TimeUnit::Hour => 3600.0,
            TimeUnit::Day => 86400.0,
            TimeUnit::Month => 2592000.0,
            TimeUnit::Year => 31536000.0,
        }
    }
}

fn get_max_datetime_exclusive() -> NaiveDateTime {
    // The maximum date in SQLite is 9999-12-31
    NaiveDateTime::new(
        NaiveDate::from_ymd_opt(10000, 01, 01).unwrap(),
        NaiveTime::from_hms_milli_opt(00, 00, 00, 000).unwrap(),
    )
}

pub fn get_date_from_time_value(time_value: &OwnedValue) -> anyhow::Result<String> {
    let dt = match time_value {
        OwnedValue::Text(s) => get_date_time_from_time_value_string(s),
        OwnedValue::Integer(i) => get_date_time_from_time_value_integer(*i),
        OwnedValue::Float(f) => get_date_time_from_time_value_float(*f),
        _ => Err(DateTimeError::InvalidArgument(format!(
            "Invalid time value: {}",
            time_value
        ))),
    };
    if dt.is_ok() {
        return Ok(get_date_from_naive_datetime(dt.unwrap()));
    } else {
        match dt.unwrap_err() {
            DateTimeError::InvalidArgument(_) => {
                trace!("Invalid time value: {}", time_value);
                Ok(String::new())
            }
            DateTimeError::Other(s) => {
                trace!("Other date time error: {}", s);
                anyhow::bail!(s)
            }
        }
    }
}

fn get_date_time_from_time_value_string(value: &str) -> Result<NaiveDateTime, DateTimeError> {
    // Time-value formats:
    // 1-7. YYYY-MM-DD[THH:MM[:SS[.SSS]]]
    // 8-10. HH:MM[:SS[.SSS]]
    // 11. 'now'
    // 12. DDDDDDDDDD (Julian day number as integer or float)
    //
    // Ref: https://sqlite.org/lang_datefunc.html#tmval

    // Check for 'now'
    if value.trim().eq_ignore_ascii_case("now") {
        return Ok(chrono::Local::now().to_utc().naive_utc());
    }

    // Check for Julian day number (integer or float)
    if let Ok(julian_day) = value.parse::<f64>() {
        return get_date_time_from_time_value_float(julian_day);
    }

    // Attempt to parse with various formats
    let date_only_format = "%Y-%m-%d";
    let datetime_formats: [&str; 9] = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%H:%M",
        "%H:%M:%S",
        "%H:%M:%S%.f",
    ];

    // First, try to parse as date-only format
    if let Ok(date) = NaiveDate::parse_from_str(value, date_only_format) {
        return Ok(date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()));
    }

    for format in &datetime_formats {
        if let Ok(dt) = if format.starts_with("%H") {
            // For time-only formats, assume date 2000-01-01
            // Ref: https://sqlite.org/lang_datefunc.html#tmval
            parse_datetime_with_optional_tz(
                &format!("2000-01-01 {}", value),
                &format!("%Y-%m-%d {}", format),
            )
        } else {
            parse_datetime_with_optional_tz(value, format)
        } {
            return Ok(dt);
        }
    }

    return Err(DateTimeError::InvalidArgument(format!(
        "Invalid time value: {}",
        value
    )));
}

fn parse_datetime_with_optional_tz(
    value: &str,
    format: &str,
) -> Result<NaiveDateTime, DateTimeError> {
    // Try parsing with timezone
    let with_tz_format = format.to_owned() + "%:z";
    if let Ok(dt) = DateTime::parse_from_str(value, &with_tz_format) {
        return Ok(dt.with_timezone(&Utc).naive_utc());
    }

    let mut value_without_tz = value;
    if value.ends_with("Z") {
        value_without_tz = &value[0..value.len() - 1];
    }

    // Parse without timezone
    NaiveDateTime::parse_from_str(value_without_tz, format)
        .map_err(|_| DateTimeError::InvalidArgument(format!("Invalid time value: {}", value)))
}

fn get_date_time_from_time_value_integer(value: i64) -> Result<NaiveDateTime, DateTimeError> {
    i32::try_from(value).map_or_else(
        |_| {
            Err(DateTimeError::InvalidArgument(format!(
                "Invalid julian day: {}",
                value
            )))
        },
        |value| get_date_time_from_time_value_float(value as f64),
    )
}

fn get_date_time_from_time_value_float(value: f64) -> Result<NaiveDateTime, DateTimeError> {
    if value.is_infinite()
        || value.is_nan()
        || value < 0.0
        || value >= TimeUnit::Day.max_value_julian()
    {
        return Err(DateTimeError::InvalidArgument(format!(
            "Invalid julian day: {}",
            value
        )));
    }
    let dt = julian_day_converter::julian_day_to_datetime(value)
        .map_err(|_| DateTimeError::Other("Failed parsing the julian date".to_string()))?;
    Ok(dt)
}

fn is_leap_second(dt: &NaiveDateTime) -> bool {
    // The range from 1,000,000,000 to 1,999,999,999 represents the leap second.
    dt.nanosecond() >= 1_000_000_000 && dt.nanosecond() <= 1_999_999_999
}

fn get_date_from_naive_datetime(value: NaiveDateTime) -> String {
    // NaiveDateTime supports leap seconds, but SQLite does not.
    // So we ignore them.
    if is_leap_second(&value) || value > get_max_datetime_exclusive() {
        return String::new();
    }
    value.format("%Y-%m-%d").to_string()
}

mod tests {
    use super::*;
    use std::rc::Rc;

    #[test]
    fn test_valid_get_date_from_time_value() {
        let now = chrono::Local::now().to_utc().format("%Y-%m-%d").to_string();

        let prev_date_str = "2024-07-20";
        let test_date_str = "2024-07-21";
        let next_date_str = "2024-07-22";

        let test_cases = [
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (OwnedValue::Text(Rc::new("2024-07-21".to_string())), test_date_str),

            // Format 2: YYYY-MM-DD HH:MM
            (OwnedValue::Text(Rc::new("2024-07-21 22:30".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30+02:00".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30-05:00".to_string())), next_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 01:30+05:00".to_string())), prev_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30Z".to_string())), test_date_str),

            // Format 3: YYYY-MM-DD HH:MM:SS
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45+02:00".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45-05:00".to_string())), next_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 01:30:45+05:00".to_string())), prev_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45Z".to_string())), test_date_str),

            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123+02:00".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123-05:00".to_string())), next_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 01:30:45.123+05:00".to_string())), prev_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123Z".to_string())), test_date_str),

            // Format 5: YYYY-MM-DDTHH:MM
            (OwnedValue::Text(Rc::new("2024-07-21T22:30".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30+02:00".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30-05:00".to_string())), next_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T01:30+05:00".to_string())), prev_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30Z".to_string())), test_date_str),

            // Format 6: YYYY-MM-DDTHH:MM:SS
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45+02:00".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45-05:00".to_string())), next_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T01:30:45+05:00".to_string())), prev_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45Z".to_string())), test_date_str),

            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123+02:00".to_string())), test_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123-05:00".to_string())), next_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T01:30:45.123+05:00".to_string())), prev_date_str),
            (OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123Z".to_string())), test_date_str),

            // Format 8: HH:MM
            (OwnedValue::Text(Rc::new("22:30".to_string())), "2000-01-01"),
            (OwnedValue::Text(Rc::new("22:30+02:00".to_string())), "2000-01-01"),
            (OwnedValue::Text(Rc::new("22:30-05:00".to_string())), "2000-01-02"),
            (OwnedValue::Text(Rc::new("01:30+05:00".to_string())), "1999-12-31"),
            (OwnedValue::Text(Rc::new("22:30Z".to_string())), "2000-01-01"),

            // Format 9: HH:MM:SS
            (OwnedValue::Text(Rc::new("22:30:45".to_string())), "2000-01-01"),
            (OwnedValue::Text(Rc::new("22:30:45+02:00".to_string())), "2000-01-01"),
            (OwnedValue::Text(Rc::new("22:30:45-05:00".to_string())), "2000-01-02"),
            (OwnedValue::Text(Rc::new("01:30:45+05:00".to_string())), "1999-12-31"),
            (OwnedValue::Text(Rc::new("22:30:45Z".to_string())), "2000-01-01"),

            // Format 10: HH:MM:SS.SSS
            (OwnedValue::Text(Rc::new("22:30:45.123".to_string())), "2000-01-01"),
            (OwnedValue::Text(Rc::new("22:30:45.123+02:00".to_string())), "2000-01-01"),
            (OwnedValue::Text(Rc::new("22:30:45.123-05:00".to_string())), "2000-01-02"),
            (OwnedValue::Text(Rc::new("01:30:45.123+05:00".to_string())), "1999-12-31"),
            (OwnedValue::Text(Rc::new("22:30:45.123Z".to_string())), "2000-01-01"),

            // Test Format 11: 'now'
            (OwnedValue::Text(Rc::new("now".to_string())), &now),
        
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (OwnedValue::Float(2460512.5), test_date_str),
            (OwnedValue::Integer(2460513), test_date_str),
        ];

        for (input, expected) in test_cases {
            assert_eq!(
                get_date_from_time_value(&input).unwrap(),
                expected,
                "Failed for input: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_invalid_get_date_from_time_value() {
        let invalid_cases = [
            OwnedValue::Text(Rc::new("2024-07-21 25:00".to_string())),       // Invalid hour
            OwnedValue::Text(Rc::new("2024-07-21 24:00:00".to_string())),    // Invalid hour
            OwnedValue::Text(Rc::new("2024-07-21 23:60:00".to_string())),    // Invalid minute
            OwnedValue::Text(Rc::new("2024-07-21 22:58:60".to_string())),    // Invalid second
            OwnedValue::Text(Rc::new("2024-07-32".to_string())),             // Invalid day
            OwnedValue::Text(Rc::new("2024-13-01".to_string())),             // Invalid month
            OwnedValue::Text(Rc::new("invalid_date".to_string())),           // Completely invalid string
            OwnedValue::Text(Rc::new("".to_string())),                 // Empty string
            OwnedValue::Integer(i64::MAX),                             // Large Julian day
            OwnedValue::Integer(-1),                                   // Negative Julian day
            OwnedValue::Float(f64::MAX),                               // Large float
            OwnedValue::Float(-1.0),          // Negative Julian day as float
            OwnedValue::Float(f64::NAN),      // NaN
            OwnedValue::Float(f64::INFINITY), // Infinity
            OwnedValue::Null,                 // Null value
            OwnedValue::Blob(vec![1, 2, 3].into()), // Blob (unsupported type)

            // Invalid timezone tests
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+24:00".to_string())),  // Invalid timezone offset (too large)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00-24:00".to_string())),  // Invalid timezone offset (too small)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:60".to_string())),  // Invalid timezone minutes
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:00:00".to_string())), // Invalid timezone format (extra seconds)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+".to_string())),       // Incomplete timezone
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+Z".to_string())),      // Invalid timezone format
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:00Z".to_string())), // Mixing offset and Z
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00UTC".to_string())),     // Named timezone (not supported)

        ];

        for case in invalid_cases.iter() {
            let result = get_date_from_time_value(case);
            assert!(
                result.is_ok(),
                "Error encountered while parsing time value {}: {}",
                case,
                result.unwrap_err()
            );
            let result_str = result.unwrap();
            assert!(
                result_str.is_empty(),
                "Expected empty string for input: {:?}, but got: {:?}",
                case,
                result_str
            );
        }
    }
}
