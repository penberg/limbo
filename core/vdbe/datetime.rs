use chrono::{
    DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Timelike, Utc,
};
use std::rc::Rc;

use crate::types::OwnedValue;
use crate::LimboError::InvalidModifier;
use crate::Result;

/// Implementation of the date() SQL function.
pub fn exec_date(values: &[OwnedValue]) -> OwnedValue {
    let maybe_dt = match values.first() {
        None => parse_naive_date_time(&OwnedValue::Text(Rc::new("now".to_string()))),
        Some(value) => parse_naive_date_time(value),
    };
    // early return, no need to look at modifiers if result invalid
    if maybe_dt.is_none() {
        return OwnedValue::Text(Rc::new(String::new()));
    }

    // apply modifiers if result is valid
    let mut dt = maybe_dt.unwrap();
    for modifier in values.iter().skip(1) {
        if let OwnedValue::Text(modifier_str) = modifier {
            if apply_modifier(&mut dt, modifier_str).is_err() {
                return OwnedValue::Text(Rc::new(String::new()));
            }
        } else {
            return OwnedValue::Text(Rc::new(String::new()));
        }
    }

    OwnedValue::Text(Rc::new(get_date_from_naive_datetime(dt)))
}

/// Implementation of the time() SQL function.
pub fn exec_time(time_value: &[OwnedValue]) -> OwnedValue {
    let maybe_dt = match time_value.first() {
        None => parse_naive_date_time(&OwnedValue::Text(Rc::new("now".to_string()))),
        Some(value) => parse_naive_date_time(value),
    };
    // early return, no need to look at modifiers if result invalid
    if maybe_dt.is_none() {
        return OwnedValue::Text(Rc::new(String::new()));
    }

    // apply modifiers if result is valid
    let mut dt = maybe_dt.unwrap();
    for modifier in time_value.iter().skip(1) {
        if let OwnedValue::Text(modifier_str) = modifier {
            if apply_modifier(&mut dt, modifier_str).is_err() {
                return OwnedValue::Text(Rc::new(String::new()));
            }
        } else {
            return OwnedValue::Text(Rc::new(String::new()));
        }
    }

    OwnedValue::Text(Rc::new(get_time_from_naive_datetime(dt)))
}

fn apply_modifier(dt: &mut NaiveDateTime, modifier: &str) -> Result<()> {
    let parsed_modifier = parse_modifier(modifier)?;

    match parsed_modifier {
        Modifier::Days(days) => *dt += TimeDelta::days(days),
        Modifier::Hours(hours) => *dt += TimeDelta::hours(hours),
        Modifier::Minutes(minutes) => *dt += TimeDelta::minutes(minutes),
        Modifier::Seconds(seconds) => *dt += TimeDelta::seconds(seconds),
        Modifier::Months(_months) => todo!(),
        Modifier::Years(_years) => todo!(),
        Modifier::TimeOffset(offset) => *dt += offset,
        Modifier::DateOffset {
            years,
            months,
            days,
        } => {
            *dt = dt
                .checked_add_months(chrono::Months::new((years * 12 + months) as u32))
                .ok_or_else(|| InvalidModifier("Invalid date offset".to_string()))?;
            *dt += TimeDelta::days(days as i64);
        }
        Modifier::DateTimeOffset { date: _, time: _ } => todo!(),
        Modifier::Ceiling => todo!(),
        Modifier::Floor => todo!(),
        Modifier::StartOfMonth => todo!(),
        Modifier::StartOfYear => {
            *dt = NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
        }
        Modifier::StartOfDay => {
            *dt = dt.date().and_hms_opt(0, 0, 0).unwrap();
        }
        Modifier::Weekday(_day) => todo!(),
        Modifier::UnixEpoch => todo!(),
        Modifier::JulianDay => todo!(),
        Modifier::Auto => todo!(),
        Modifier::Localtime => {
            let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(*dt, Utc);
            *dt = utc_dt.with_timezone(&chrono::Local).naive_local();
        }
        Modifier::Utc => {
            let local_dt = chrono::Local.from_local_datetime(dt).unwrap();
            *dt = local_dt.with_timezone(&Utc).naive_utc();
        }
        Modifier::Subsec => todo!(),
    }

    Ok(())
}

pub fn exec_unixepoch(time_value: &OwnedValue) -> Result<String> {
    let dt = parse_naive_date_time(time_value);
    match dt {
        Some(dt) => Ok(get_unixepoch_from_naive_datetime(dt)),
        None => Ok(String::new()),
    }
}

fn get_unixepoch_from_naive_datetime(value: NaiveDateTime) -> String {
    value.and_utc().timestamp().to_string()
}

fn parse_naive_date_time(time_value: &OwnedValue) -> Option<NaiveDateTime> {
    match time_value {
        OwnedValue::Text(s) => get_date_time_from_time_value_string(s),
        OwnedValue::Integer(i) => get_date_time_from_time_value_integer(*i),
        OwnedValue::Float(f) => get_date_time_from_time_value_float(*f),
        _ => None,
    }
}

fn get_date_time_from_time_value_string(value: &str) -> Option<NaiveDateTime> {
    // Time-value formats:
    // 1-7. YYYY-MM-DD[THH:MM[:SS[.SSS]]]
    // 8-10. HH:MM[:SS[.SSS]]
    // 11. 'now'
    // 12. DDDDDDDDDD (Julian day number as integer or float)
    //
    // Ref: https://sqlite.org/lang_datefunc.html#tmval

    // Check for 'now'
    if value.trim().eq_ignore_ascii_case("now") {
        return Some(chrono::Local::now().to_utc().naive_utc());
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
        return Some(date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()));
    }

    for format in &datetime_formats {
        if let Some(dt) = if format.starts_with("%H") {
            // For time-only formats, assume date 2000-01-01
            // Ref: https://sqlite.org/lang_datefunc.html#tmval
            parse_datetime_with_optional_tz(
                &format!("2000-01-01 {}", value),
                &format!("%Y-%m-%d {}", format),
            )
        } else {
            parse_datetime_with_optional_tz(value, format)
        } {
            return Some(dt);
        }
    }
    None
}

fn parse_datetime_with_optional_tz(value: &str, format: &str) -> Option<NaiveDateTime> {
    // Try parsing with timezone
    let with_tz_format = format.to_owned() + "%:z";
    if let Ok(dt) = DateTime::parse_from_str(value, &with_tz_format) {
        return Some(dt.with_timezone(&Utc).naive_utc());
    }

    let mut value_without_tz = value;
    if value.ends_with('Z') {
        value_without_tz = &value[0..value.len() - 1];
    }

    // Parse without timezone
    if let Ok(dt) = NaiveDateTime::parse_from_str(value_without_tz, format) {
        return Some(dt);
    }
    None
}

fn get_date_time_from_time_value_integer(value: i64) -> Option<NaiveDateTime> {
    i32::try_from(value).map_or_else(
        |_| None,
        |value| get_date_time_from_time_value_float(value as f64),
    )
}

fn get_date_time_from_time_value_float(value: f64) -> Option<NaiveDateTime> {
    if value.is_infinite() || value.is_nan() || !(0.0..5373484.5).contains(&value) {
        return None;
    }
    match julian_day_converter::julian_day_to_datetime(value) {
        Ok(dt) => Some(dt),
        Err(_) => None,
    }
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

fn get_time_from_naive_datetime(value: NaiveDateTime) -> String {
    // NaiveDateTime supports leap seconds, but SQLite does not.
    // So we ignore them.
    if is_leap_second(&value) || value > get_max_datetime_exclusive() {
        return String::new();
    }
    value.format("%H:%M:%S").to_string()
}

fn get_max_datetime_exclusive() -> NaiveDateTime {
    // The maximum date in SQLite is 9999-12-31
    NaiveDateTime::new(
        NaiveDate::from_ymd_opt(10000, 1, 1).unwrap(),
        NaiveTime::from_hms_milli_opt(00, 00, 00, 000).unwrap(),
    )
}

/// Modifier doc https://www.sqlite.org/lang_datefunc.html#modifiers
#[allow(dead_code)]
#[derive(Debug, PartialEq)]
enum Modifier {
    Days(i64),
    Hours(i64),
    Minutes(i64),
    Seconds(i64),
    Months(i32),
    Years(i32),
    TimeOffset(TimeDelta),
    DateOffset {
        years: i32,
        months: i32,
        days: i32,
    },
    DateTimeOffset {
        date: NaiveDate,
        time: Option<NaiveTime>,
    },
    Ceiling,
    Floor,
    StartOfMonth,
    StartOfYear,
    StartOfDay,
    Weekday(u32),
    UnixEpoch,
    JulianDay,
    Auto,
    Localtime,
    Utc,
    Subsec,
}

fn parse_modifier_number(s: &str) -> Result<i64> {
    s.trim()
        .parse::<i64>()
        .map_err(|_| InvalidModifier(format!("Invalid number: {}", s)))
}

/// supports YYYY-MM-DD format for time shift modifiers
fn parse_modifier_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|_| InvalidModifier("Invalid date format".to_string()))
}

/// supports following formats for time shift modifiers
/// - HH:MM
/// - HH:MM:SS
/// - HH:MM:SS.SSS
fn parse_modifier_time(s: &str) -> Result<NaiveTime> {
    match s.len() {
        5 => NaiveTime::parse_from_str(s, "%H:%M"),
        8 => NaiveTime::parse_from_str(s, "%H:%M:%S"),
        12 => NaiveTime::parse_from_str(s, "%H:%M:%S.%3f"),
        _ => return Err(InvalidModifier(format!("Invalid time format: {}", s))),
    }
    .map_err(|_| InvalidModifier(format!("Invalid time format: {}", s)))
}

fn parse_modifier(modifier: &str) -> Result<Modifier> {
    let modifier = modifier.trim().to_lowercase();

    match modifier.as_str() {
        s if s.ends_with(" days") => Ok(Modifier::Days(parse_modifier_number(&s[..s.len() - 5])?)),
        s if s.ends_with(" hours") => {
            Ok(Modifier::Hours(parse_modifier_number(&s[..s.len() - 6])?))
        }
        s if s.ends_with(" minutes") => {
            Ok(Modifier::Minutes(parse_modifier_number(&s[..s.len() - 8])?))
        }
        s if s.ends_with(" seconds") => {
            Ok(Modifier::Seconds(parse_modifier_number(&s[..s.len() - 8])?))
        }
        s if s.ends_with(" months") => Ok(Modifier::Months(
            parse_modifier_number(&s[..s.len() - 7])? as i32,
        )),
        s if s.ends_with(" years") => Ok(Modifier::Years(
            parse_modifier_number(&s[..s.len() - 6])? as i32,
        )),
        s if s.starts_with('+') || s.starts_with('-') => {
            // Parse as DateOffset or DateTimeOffset
            let parts: Vec<&str> = s[1..].split(' ').collect();
            match parts.len() {
                1 => {
                    // first part can be either date ±YYYY-MM-DD or 3 types of time modifiers
                    let date = parse_modifier_date(parts[0]);
                    if let Ok(date) = date {
                        Ok(Modifier::DateTimeOffset { date, time: None })
                    } else {
                        // try to parse time if error parsing date
                        let time = parse_modifier_time(parts[0])?;
                        // TODO handle nanoseconds
                        let time_delta = if s.starts_with('-') {
                            TimeDelta::seconds(-(time.num_seconds_from_midnight() as i64))
                        } else {
                            TimeDelta::seconds(time.num_seconds_from_midnight() as i64)
                        };
                        Ok(Modifier::TimeOffset(time_delta))
                    }
                }
                2 => {
                    let date = parse_modifier_date(parts[0])?;
                    let time = parse_modifier_time(parts[1])?;
                    Ok(Modifier::DateTimeOffset {
                        date,
                        time: Some(time),
                    })
                }
                _ => Err(InvalidModifier(
                    "Invalid date/time offset format".to_string(),
                )),
            }
        }
        "ceiling" => Ok(Modifier::Ceiling),
        "floor" => Ok(Modifier::Floor),
        "start of month" => Ok(Modifier::StartOfMonth),
        "start of year" => Ok(Modifier::StartOfYear),
        "start of day" => Ok(Modifier::StartOfDay),
        s if s.starts_with("weekday ") => {
            let day = parse_modifier_number(&s[8..])?;
            if !(0..=6).contains(&day) {
                Err(InvalidModifier(
                    "Weekday must be between 0 and 6".to_string(),
                ))
            } else {
                Ok(Modifier::Weekday(day as u32))
            }
        }
        "unixepoch" => Ok(Modifier::UnixEpoch),
        "julianday" => Ok(Modifier::JulianDay),
        "auto" => Ok(Modifier::Auto),
        "localtime" => Ok(Modifier::Localtime),
        "utc" => Ok(Modifier::Utc),
        "subsec" | "subsecond" => Ok(Modifier::Subsec),
        _ => Err(InvalidModifier(format!("Unknown modifier: {}", modifier))),
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;

    #[test]
    fn test_valid_get_date_from_time_value() {
        let now = chrono::Local::now().to_utc().format("%Y-%m-%d").to_string();

        let prev_date_str = "2024-07-20";
        let test_date_str = "2024-07-21";
        let next_date_str = "2024-07-22";

        let test_cases = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (
                OwnedValue::Text(Rc::new("2024-07-21".to_string())),
                test_date_str,
            ),
            // Format 2: YYYY-MM-DD HH:MM
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 01:30+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30Z".to_string())),
                test_date_str,
            ),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 01:30:45+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45Z".to_string())),
                test_date_str,
            ),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 01:30:45.123+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123Z".to_string())),
                test_date_str,
            ),
            // Format 5: YYYY-MM-DDTHH:MM
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T01:30+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30Z".to_string())),
                test_date_str,
            ),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T01:30:45+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45Z".to_string())),
                test_date_str,
            ),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T01:30:45.123+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123Z".to_string())),
                test_date_str,
            ),
            // Format 8: HH:MM
            (OwnedValue::Text(Rc::new("22:30".to_string())), "2000-01-01"),
            (
                OwnedValue::Text(Rc::new("22:30+02:00".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::Text(Rc::new("22:30-05:00".to_string())),
                "2000-01-02",
            ),
            (
                OwnedValue::Text(Rc::new("01:30+05:00".to_string())),
                "1999-12-31",
            ),
            (
                OwnedValue::Text(Rc::new("22:30Z".to_string())),
                "2000-01-01",
            ),
            // Format 9: HH:MM:SS
            (
                OwnedValue::Text(Rc::new("22:30:45".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45+02:00".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45-05:00".to_string())),
                "2000-01-02",
            ),
            (
                OwnedValue::Text(Rc::new("01:30:45+05:00".to_string())),
                "1999-12-31",
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45Z".to_string())),
                "2000-01-01",
            ),
            // Format 10: HH:MM:SS.SSS
            (
                OwnedValue::Text(Rc::new("22:30:45.123".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45.123+02:00".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45.123-05:00".to_string())),
                "2000-01-02",
            ),
            (
                OwnedValue::Text(Rc::new("01:30:45.123+05:00".to_string())),
                "1999-12-31",
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45.123Z".to_string())),
                "2000-01-01",
            ),
            // Test Format 11: 'now'
            (OwnedValue::Text(Rc::new("now".to_string())), &now),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (OwnedValue::Float(2460512.5), test_date_str),
            (OwnedValue::Integer(2460513), test_date_str),
        ];

        for (input, expected) in test_cases {
            let result = exec_date(&[input.clone()]);
            assert_eq!(
                result,
                OwnedValue::Text(Rc::new(expected.to_string())),
                "Failed for input: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_invalid_get_date_from_time_value() {
        let invalid_cases = vec![
            OwnedValue::Text(Rc::new("2024-07-21 25:00".to_string())), // Invalid hour
            OwnedValue::Text(Rc::new("2024-07-21 24:00:00".to_string())), // Invalid hour
            OwnedValue::Text(Rc::new("2024-07-21 23:60:00".to_string())), // Invalid minute
            OwnedValue::Text(Rc::new("2024-07-21 22:58:60".to_string())), // Invalid second
            OwnedValue::Text(Rc::new("2024-07-32".to_string())),       // Invalid day
            OwnedValue::Text(Rc::new("2024-13-01".to_string())),       // Invalid month
            OwnedValue::Text(Rc::new("invalid_date".to_string())),     // Completely invalid string
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
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+24:00".to_string())), // Invalid timezone offset (too large)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00-24:00".to_string())), // Invalid timezone offset (too small)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:60".to_string())), // Invalid timezone minutes
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:00:00".to_string())), // Invalid timezone format (extra seconds)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+".to_string())), // Incomplete timezone
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+Z".to_string())), // Invalid timezone format
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:00Z".to_string())), // Mixing offset and Z
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00UTC".to_string())), // Named timezone (not supported)
        ];

        for case in invalid_cases.iter() {
            let result = exec_date(&[case.clone()]);
            match result {
                OwnedValue::Text(ref result_str) if result_str.is_empty() => (),
                _ => panic!(
                    "Expected empty string for input: {:?}, but got: {:?}",
                    case, result
                ),
            }
        }
    }

    #[test]
    fn test_valid_get_time_from_datetime_value() {
        let now = chrono::Local::now().to_utc().format("%H:%M:%S").to_string();

        let test_time_str = "22:30:45";
        let prev_time_str = "20:30:45";
        let next_time_str = "03:30:45";

        let test_cases = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (
                OwnedValue::Text(Rc::new("2024-07-21".to_string())),
                "00:00:00",
            ),
            // Format 2: YYYY-MM-DD HH:MM
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30".to_string())),
                "22:30:00",
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30+02:00".to_string())),
                "20:30:00",
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30-05:00".to_string())),
                "03:30:00",
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30Z".to_string())),
                "22:30:00",
            ),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45Z".to_string())),
                test_time_str,
            ),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21 22:30:45.123Z".to_string())),
                test_time_str,
            ),
            // Format 5: YYYY-MM-DDTHH:MM
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30".to_string())),
                "22:30:00",
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30+02:00".to_string())),
                "20:30:00",
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30-05:00".to_string())),
                "03:30:00",
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30Z".to_string())),
                "22:30:00",
            ),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45Z".to_string())),
                test_time_str,
            ),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("2024-07-21T22:30:45.123Z".to_string())),
                test_time_str,
            ),
            // Format 8: HH:MM
            (OwnedValue::Text(Rc::new("22:30".to_string())), "22:30:00"),
            (
                OwnedValue::Text(Rc::new("22:30+02:00".to_string())),
                "20:30:00",
            ),
            (
                OwnedValue::Text(Rc::new("22:30-05:00".to_string())),
                "03:30:00",
            ),
            (OwnedValue::Text(Rc::new("22:30Z".to_string())), "22:30:00"),
            // Format 9: HH:MM:SS
            (
                OwnedValue::Text(Rc::new("22:30:45".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45Z".to_string())),
                test_time_str,
            ),
            // Format 10: HH:MM:SS.SSS
            (
                OwnedValue::Text(Rc::new("22:30:45.123".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45.123+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45.123-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::Text(Rc::new("22:30:45.123Z".to_string())),
                test_time_str,
            ),
            // Test Format 11: 'now'
            (OwnedValue::Text(Rc::new("now".to_string())), &now),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (OwnedValue::Float(2460082.1), "14:24:00"),
            (OwnedValue::Integer(2460082), "12:00:00"),
        ];

        for (input, expected) in test_cases {
            let result = exec_time(&[input]);
            if let OwnedValue::Text(result_str) = result {
                assert_eq!(result_str.as_str(), expected);
            } else {
                panic!("Expected OwnedValue::Text, but got: {:?}", result);
            }
        }
    }

    #[test]
    fn test_invalid_get_time_from_datetime_value() {
        let invalid_cases = vec![
            OwnedValue::Text(Rc::new("2024-07-21 25:00".to_string())), // Invalid hour
            OwnedValue::Text(Rc::new("2024-07-21 24:00:00".to_string())), // Invalid hour
            OwnedValue::Text(Rc::new("2024-07-21 23:60:00".to_string())), // Invalid minute
            OwnedValue::Text(Rc::new("2024-07-21 22:58:60".to_string())), // Invalid second
            OwnedValue::Text(Rc::new("2024-07-32".to_string())),       // Invalid day
            OwnedValue::Text(Rc::new("2024-13-01".to_string())),       // Invalid month
            OwnedValue::Text(Rc::new("invalid_date".to_string())),     // Completely invalid string
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
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+24:00".to_string())), // Invalid timezone offset (too large)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00-24:00".to_string())), // Invalid timezone offset (too small)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:60".to_string())), // Invalid timezone minutes
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:00:00".to_string())), // Invalid timezone format (extra seconds)
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+".to_string())), // Incomplete timezone
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+Z".to_string())), // Invalid timezone format
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00+00:00Z".to_string())), // Mixing offset and Z
            OwnedValue::Text(Rc::new("2024-07-21T12:00:00UTC".to_string())), // Named timezone (not supported)
        ];

        for case in invalid_cases {
            let result = exec_time(&[case.clone()]);
            match result {
                OwnedValue::Text(ref result_str) if result_str.is_empty() => (),
                _ => panic!(
                    "Expected empty string for input: {:?}, but got: {:?}",
                    case, result
                ),
            }
        }
    }

    #[test]
    fn test_parse_days() {
        assert_eq!(parse_modifier("5 days").unwrap(), Modifier::Days(5));
        assert_eq!(parse_modifier("-3 days").unwrap(), Modifier::Days(-3));
        assert_eq!(parse_modifier("+2 days").unwrap(), Modifier::Days(2));
        assert_eq!(parse_modifier("4  days").unwrap(), Modifier::Days(4));
        assert_eq!(parse_modifier("6   DAYS").unwrap(), Modifier::Days(6));
        assert_eq!(parse_modifier("+5  DAYS").unwrap(), Modifier::Days(5));
    }

    #[test]
    fn test_parse_hours() {
        assert_eq!(parse_modifier("12 hours").unwrap(), Modifier::Hours(12));
        assert_eq!(parse_modifier("-2 hours").unwrap(), Modifier::Hours(-2));
        assert_eq!(parse_modifier("+3  HOURS").unwrap(), Modifier::Hours(3));
    }

    #[test]
    fn test_parse_minutes() {
        assert_eq!(parse_modifier("30 minutes").unwrap(), Modifier::Minutes(30));
        assert_eq!(
            parse_modifier("-15 minutes").unwrap(),
            Modifier::Minutes(-15)
        );
        assert_eq!(
            parse_modifier("+45  MINUTES").unwrap(),
            Modifier::Minutes(45)
        );
    }

    #[test]
    fn test_parse_seconds() {
        assert_eq!(parse_modifier("45 seconds").unwrap(), Modifier::Seconds(45));
        assert_eq!(
            parse_modifier("-10 seconds").unwrap(),
            Modifier::Seconds(-10)
        );
        assert_eq!(
            parse_modifier("+20  SECONDS").unwrap(),
            Modifier::Seconds(20)
        );
    }

    #[test]
    fn test_parse_months() {
        assert_eq!(parse_modifier("3 months").unwrap(), Modifier::Months(3));
        assert_eq!(parse_modifier("-1 months").unwrap(), Modifier::Months(-1));
        assert_eq!(parse_modifier("+6  MONTHS").unwrap(), Modifier::Months(6));
    }

    #[test]
    fn test_parse_years() {
        assert_eq!(parse_modifier("2 years").unwrap(), Modifier::Years(2));
        assert_eq!(parse_modifier("-1 years").unwrap(), Modifier::Years(-1));
        assert_eq!(parse_modifier("+10  YEARS").unwrap(), Modifier::Years(10));
    }

    #[test]
    fn test_parse_time_offset() {
        assert_eq!(
            parse_modifier("+01:30").unwrap(),
            Modifier::TimeOffset(TimeDelta::hours(1) + TimeDelta::minutes(30))
        );
        assert_eq!(
            parse_modifier("-00:45").unwrap(),
            Modifier::TimeOffset(TimeDelta::minutes(-45))
        );
        assert_eq!(
            parse_modifier("+02:15:30").unwrap(),
            Modifier::TimeOffset(
                TimeDelta::hours(2) + TimeDelta::minutes(15) + TimeDelta::seconds(30)
            )
        );
        assert_eq!(
            parse_modifier("+02:15:30.250").unwrap(),
            Modifier::TimeOffset(
                TimeDelta::hours(2) + TimeDelta::minutes(15) + TimeDelta::seconds(30)
            )
        );
    }

    #[test]
    fn test_parse_date_offset() {
        let expected_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        assert_eq!(
            parse_modifier("+2023-05-15").unwrap(),
            Modifier::DateTimeOffset {
                date: expected_date,
                time: None,
            }
        );
        assert_eq!(
            parse_modifier("-2023-05-15").unwrap(),
            Modifier::DateTimeOffset {
                date: expected_date,
                time: None,
            }
        );
    }

    #[test]
    fn test_parse_date_time_offset() {
        let expected_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
        let expected_time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        assert_eq!(
            parse_modifier("+2023-05-15 14:30").unwrap(),
            Modifier::DateTimeOffset {
                date: expected_date,
                time: Some(expected_time),
            }
        );
        assert_eq!(
            parse_modifier("-2023-05-15 14:30").unwrap(),
            Modifier::DateTimeOffset {
                date: expected_date,
                time: Some(expected_time),
            }
        );
    }

    #[test]
    fn test_parse_start_of() {
        assert_eq!(
            parse_modifier("start of month").unwrap(),
            Modifier::StartOfMonth
        );
        assert_eq!(
            parse_modifier("START OF MONTH").unwrap(),
            Modifier::StartOfMonth
        );
        assert_eq!(
            parse_modifier("start of year").unwrap(),
            Modifier::StartOfYear
        );
        assert_eq!(
            parse_modifier("START OF YEAR").unwrap(),
            Modifier::StartOfYear
        );
        assert_eq!(
            parse_modifier("start of day").unwrap(),
            Modifier::StartOfDay
        );
        assert_eq!(
            parse_modifier("START OF DAY").unwrap(),
            Modifier::StartOfDay
        );
    }

    #[test]
    fn test_parse_weekday() {
        assert_eq!(parse_modifier("weekday 0").unwrap(), Modifier::Weekday(0));
        assert_eq!(parse_modifier("WEEKDAY 6").unwrap(), Modifier::Weekday(6));
    }

    #[test]
    fn test_parse_other_modifiers() {
        assert_eq!(parse_modifier("unixepoch").unwrap(), Modifier::UnixEpoch);
        assert_eq!(parse_modifier("UNIXEPOCH").unwrap(), Modifier::UnixEpoch);
        assert_eq!(parse_modifier("julianday").unwrap(), Modifier::JulianDay);
        assert_eq!(parse_modifier("JULIANDAY").unwrap(), Modifier::JulianDay);
        assert_eq!(parse_modifier("auto").unwrap(), Modifier::Auto);
        assert_eq!(parse_modifier("AUTO").unwrap(), Modifier::Auto);
        assert_eq!(parse_modifier("localtime").unwrap(), Modifier::Localtime);
        assert_eq!(parse_modifier("LOCALTIME").unwrap(), Modifier::Localtime);
        assert_eq!(parse_modifier("utc").unwrap(), Modifier::Utc);
        assert_eq!(parse_modifier("UTC").unwrap(), Modifier::Utc);
        assert_eq!(parse_modifier("subsec").unwrap(), Modifier::Subsec);
        assert_eq!(parse_modifier("SUBSEC").unwrap(), Modifier::Subsec);
        assert_eq!(parse_modifier("subsecond").unwrap(), Modifier::Subsec);
        assert_eq!(parse_modifier("SUBSECOND").unwrap(), Modifier::Subsec);
    }

    #[test]
    fn test_parse_invalid_modifier() {
        assert!(parse_modifier("invalid modifier").is_err());
        assert!(parse_modifier("5").is_err());
        assert!(parse_modifier("days").is_err());
        assert!(parse_modifier("++5 days").is_err());
        assert!(parse_modifier("weekday 7").is_err());
    }

    fn create_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
    ) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, min, sec)
            .unwrap()
    }

    fn setup_datetime() -> NaiveDateTime {
        create_datetime(2023, 6, 15, 12, 30, 45)
    }

    #[test]
    fn test_apply_modifier_days() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "5 days").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 20, 12, 30, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-3 days").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 12, 12, 30, 45));
    }

    #[test]
    fn test_apply_modifier_hours() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "6 hours").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 18, 30, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-2 hours").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 10, 30, 45));
    }

    #[test]
    fn test_apply_modifier_minutes() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "45 minutes").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 13, 15, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-15 minutes").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 12, 15, 45));
    }

    #[test]
    fn test_apply_modifier_seconds() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "30 seconds").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 12, 31, 15));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-20 seconds").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 12, 30, 25));
    }

    #[test]
    fn test_apply_modifier_time_offset() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "+01:30").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 14, 0, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-00:45").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 11, 45, 45));
    }

    #[test]
    #[ignore] // enable when implemented this modifier
    fn test_apply_modifier_date_time_offset() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "+01-01-01 01:01").unwrap();
        assert_eq!(dt, create_datetime(2024, 7, 16, 13, 31, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-01-01-01 01:01").unwrap();
        assert_eq!(dt, create_datetime(2022, 5, 14, 11, 29, 45));

        // Test with larger offsets
        dt = setup_datetime();
        apply_modifier(&mut dt, "+02-03-04 05:06").unwrap();
        assert_eq!(dt, create_datetime(2025, 9, 19, 17, 36, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-02-03-04 05:06").unwrap();
        assert_eq!(dt, create_datetime(2021, 3, 11, 7, 24, 45));
    }

    #[test]
    fn test_apply_modifier_start_of_year() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "start of year").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_start_of_day() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "start of day").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 0, 0, 0));
    }
}
