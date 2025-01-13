use crate::types::OwnedValue;
use crate::LimboError::InvalidModifier;
use crate::Result;
use chrono::{
    DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Timelike, Utc,
};
use std::rc::Rc;

/// Execution of date/time/datetime functions
#[inline(always)]
pub fn exec_date(values: &[OwnedValue]) -> OwnedValue {
    exec_datetime(values, DateTimeOutput::Date)
}

#[inline(always)]
pub fn exec_time(values: &[OwnedValue]) -> OwnedValue {
    exec_datetime(values, DateTimeOutput::Time)
}

#[inline(always)]
pub fn exec_datetime_full(values: &[OwnedValue]) -> OwnedValue {
    exec_datetime(values, DateTimeOutput::DateTime)
}

enum DateTimeOutput {
    Date,
    Time,
    DateTime,
}

fn exec_datetime(values: &[OwnedValue], output_type: DateTimeOutput) -> OwnedValue {
    if values.is_empty() {
        return OwnedValue::build_text(Rc::new(
            parse_naive_date_time(&OwnedValue::build_text(Rc::new("now".to_string())))
                .unwrap()
                .format(match output_type {
                    DateTimeOutput::DateTime => "%Y-%m-%d %H:%M:%S",
                    DateTimeOutput::Time => "%H:%M:%S",
                    DateTimeOutput::Date => "%Y-%m-%d",
                })
                .to_string(),
        ));
    }
    if let Some(mut dt) = parse_naive_date_time(&values[0]) {
        // if successful, treat subsequent entries as modifiers
        modify_dt(&mut dt, &values[1..], output_type)
    } else {
        // if the first argument is NOT a valid date/time, treat the entire set of values as modifiers.
        let mut dt = chrono::Local::now().to_utc().naive_utc();
        modify_dt(&mut dt, values, output_type)
    }
}

fn modify_dt(
    dt: &mut NaiveDateTime,
    mods: &[OwnedValue],
    output_type: DateTimeOutput,
) -> OwnedValue {
    let mut subsec_requested = false;

    for modifier in mods {
        if let OwnedValue::Text(ref text_rc) = modifier {
            // TODO: to prevent double conversion and properly support 'utc'/'localtime', we also
            // need to keep track of the current timezone and apply it to the modifier.
            match apply_modifier(dt, &text_rc.value) {
                Ok(true) => subsec_requested = true,
                Ok(false) => {}
                Err(_) => return OwnedValue::build_text(Rc::new(String::new())),
            }
        } else {
            return OwnedValue::build_text(Rc::new(String::new()));
        }
    }
    if is_leap_second(dt) || *dt > get_max_datetime_exclusive() {
        return OwnedValue::build_text(Rc::new(String::new()));
    }
    let formatted = format_dt(*dt, output_type, subsec_requested);
    OwnedValue::build_text(Rc::new(formatted))
}

fn format_dt(dt: NaiveDateTime, output_type: DateTimeOutput, subsec: bool) -> String {
    match output_type {
        DateTimeOutput::Date => dt.format("%Y-%m-%d").to_string(),
        DateTimeOutput::Time => {
            if subsec {
                dt.format("%H:%M:%S%.3f").to_string()
            } else {
                dt.format("%H:%M:%S").to_string()
            }
        }
        DateTimeOutput::DateTime => {
            if subsec {
                dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            } else {
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            }
        }
    }
}

// to prevent stripping the modifier string and comparing multiple times, this returns
// whether the modifier was a subsec modifier because it impacts the format string
fn apply_modifier(dt: &mut NaiveDateTime, modifier: &str) -> Result<bool> {
    let parsed_modifier = parse_modifier(modifier)?;

    match parsed_modifier {
        Modifier::Days(days) => *dt += TimeDelta::days(days),
        Modifier::Hours(hours) => *dt += TimeDelta::hours(hours),
        Modifier::Minutes(minutes) => *dt += TimeDelta::minutes(minutes),
        Modifier::Seconds(seconds) => *dt += TimeDelta::seconds(seconds),
        Modifier::Months(m) => {
            // Convert months to years + leftover months
            let years = m / 12;
            let leftover = m % 12;
            add_years_and_months(dt, years, leftover)?;
        }
        Modifier::Years(y) => {
            add_years_and_months(dt, y, 0)?;
        }
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
        Modifier::DateTimeOffset {
            years,
            months,
            days,
            seconds,
        } => {
            add_years_and_months(dt, years, months)?;
            *dt += chrono::Duration::days(days as i64);
            *dt += chrono::Duration::seconds(seconds.into());
        }
        Modifier::Ceiling => todo!(),
        Modifier::Floor => todo!(),
        Modifier::StartOfMonth => {
            *dt = NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
        }
        Modifier::StartOfYear => {
            *dt = NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
        }
        Modifier::StartOfDay => {
            *dt = dt.date().and_hms_opt(0, 0, 0).unwrap();
        }
        Modifier::Weekday(day) => {
            let current_day = dt.weekday().num_days_from_sunday();
            let target_day = day;
            let days_to_add = (target_day + 7 - current_day) % 7;
            *dt += TimeDelta::days(days_to_add as i64);
        }
        Modifier::Auto => todo!(), // Will require storing info about the original arg passed when
        Modifier::UnixEpoch => todo!(), // applying modifiers. All numbers passed to date/time/dt are
        Modifier::JulianDay => todo!(), // assumed to be julianday, so adding these now is redundant
        Modifier::Localtime => {
            let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(*dt, Utc);
            *dt = utc_dt.with_timezone(&chrono::Local).naive_local();
        }
        Modifier::Utc => {
            // TODO: handle datetime('now', 'utc') no-op
            let local_dt = chrono::Local.from_local_datetime(dt).unwrap();
            *dt = local_dt.with_timezone(&Utc).naive_utc();
        }
        Modifier::Subsec => {
            *dt = dt.with_nanosecond(dt.nanosecond()).unwrap();
            return Ok(true);
        }
    }

    Ok(false)
}

fn is_julian_day_value(value: f64) -> bool {
    (0.0..5373484.5).contains(&value)
}

fn add_years_and_months(dt: &mut NaiveDateTime, years: i32, months: i32) -> Result<()> {
    add_whole_years(dt, years)?;
    add_months_in_increments(dt, months)?;
    Ok(())
}

fn add_whole_years(dt: &mut NaiveDateTime, years: i32) -> Result<()> {
    if years == 0 {
        return Ok(());
    }
    let target_year = dt.year() + years;
    let (m, d, hh, mm, ss) = (dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second());

    // attempt same (month, day) in new year
    if let Some(date) = NaiveDate::from_ymd_opt(target_year, m, d) {
        *dt = date
            .and_hms_opt(hh, mm, ss)
            .ok_or_else(|| InvalidModifier("Invalid datetime format".to_string()))?;
        return Ok(());
    }

    // if invalid: compute overflow days
    let last_day_in_feb = last_day_in_month(target_year, m);
    if d > last_day_in_feb {
        // leftover = d - last_day_in_feb
        let leftover = d - last_day_in_feb;
        // base date is last_day_in_feb
        let base_date = NaiveDate::from_ymd_opt(target_year, m, last_day_in_feb)
            .ok_or_else(|| InvalidModifier("Invalid datetime format".to_string()))?
            .and_hms_opt(hh, mm, ss)
            .ok_or_else(|| InvalidModifier("Invalid time format".to_string()))?;

        *dt = base_date + chrono::Duration::days(leftover as i64);
    } else {
        // do we fall back here?
    }
    Ok(())
}

fn add_months_in_increments(dt: &mut NaiveDateTime, months: i32) -> Result<()> {
    let step = if months >= 0 { 1 } else { -1 };
    for _ in 0..months.abs() {
        add_one_month(dt, step)?;
    }
    Ok(())
}

// sqlite resolves any ambiguity between advancing months by using the 'ceiling'
// value, computing overflow days and advancing to the next valid date
// e.g. 2024-01-31 + 1 month = 2024-03-02
//
// the modifiers 'ceiling' and 'floor' will determine behavior, so we'll need to eagerly
// evaluate modifiers in the future to support those, and 'julianday'/'unixepoch'
fn add_one_month(dt: &mut NaiveDateTime, step: i32) -> Result<()> {
    let (y0, m0, d0) = (dt.year(), dt.month(), dt.day());
    let (hh, mm, ss) = (dt.hour(), dt.minute(), dt.second());

    let mut new_year = y0;
    let mut new_month = m0 as i32 + step;
    if new_month > 12 {
        new_month -= 12;
        new_year += 1;
    } else if new_month < 1 {
        new_month += 12;
        new_year -= 1;
    }

    let last_day = last_day_in_month(new_year, new_month as u32);
    if d0 <= last_day {
        // valid date
        *dt = NaiveDate::from_ymd_opt(new_year, new_month as u32, d0)
            .ok_or_else(|| InvalidModifier("Invalid Auto format".to_string()))?
            .and_hms_opt(hh, mm, ss)
            .ok_or_else(|| InvalidModifier("Invalid Auto format".to_string()))?;
    } else {
        let leftover = d0 - last_day;
        let base_date = NaiveDate::from_ymd_opt(new_year, new_month as u32, last_day)
            .ok_or_else(|| InvalidModifier("Invalid Auto format".to_string()))?
            .and_hms_opt(hh, mm, ss)
            .ok_or_else(|| InvalidModifier("Invalid Auto format".to_string()))?;

        *dt = base_date + chrono::Duration::days(leftover as i64);
    }
    Ok(())
}

#[inline(always)]
fn last_day_in_month(year: i32, month: u32) -> u32 {
    for day in (28..=31).rev() {
        if NaiveDate::from_ymd_opt(year, month, day).is_some() {
            return day;
        }
    }
    28
}

pub fn exec_julianday(time_value: &OwnedValue) -> Result<String> {
    let dt = parse_naive_date_time(time_value);
    match dt {
        // if we did something heinous like: parse::<f64>().unwrap().to_string()
        // that would solve the precision issue, but dear lord...
        Some(dt) => Ok(format!("{:.1$}", to_julian_day_exact(&dt), 8)),
        None => Ok(String::new()),
    }
}

fn to_julian_day_exact(dt: &NaiveDateTime) -> f64 {
    let year = dt.year();
    let month = dt.month() as i32;
    let day = dt.day() as i32;
    let (adjusted_year, adjusted_month) = if month <= 2 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };

    let a = adjusted_year / 100;
    let b = 2 - a + a / 4;
    let jd_days = (365.25 * ((adjusted_year + 4716) as f64)).floor()
        + (30.6001 * ((adjusted_month + 1) as f64)).floor()
        + (day as f64)
        + (b as f64)
        - 1524.5;

    let seconds = dt.hour() as f64 * 3600.0
        + dt.minute() as f64 * 60.0
        + dt.second() as f64
        + (dt.nanosecond() as f64) / 1_000_000_000.0;

    let jd_fraction = seconds / 86400.0;
    jd_days + jd_fraction
}

pub fn exec_unixepoch(time_value: &OwnedValue) -> Result<String> {
    let dt = parse_naive_date_time(time_value);
    match dt {
        Some(dt) => Ok(get_unixepoch_from_naive_datetime(dt)),
        None => Ok(String::new()),
    }
}

fn get_unixepoch_from_naive_datetime(value: NaiveDateTime) -> String {
    if is_leap_second(&value) {
        return String::new();
    }
    value.and_utc().timestamp().to_string()
}

fn parse_naive_date_time(time_value: &OwnedValue) -> Option<NaiveDateTime> {
    match time_value {
        OwnedValue::Text(s) => get_date_time_from_time_value_string(&s.value),
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
        |value| {
            if value.is_negative() || !is_julian_day_value(value as f64) {
                return None;
            }
            get_date_time_from_time_value_float(value as f64)
        },
    )
}

fn get_date_time_from_time_value_float(value: f64) -> Option<NaiveDateTime> {
    if value.is_infinite() || value.is_nan() || !is_julian_day_value(value) {
        return None;
    }
    match julian_day_converter::julian_day_to_datetime(value) {
        Ok(dt) => Some(dt),
        Err(_) => None,
    }
}

fn is_leap_second(dt: &NaiveDateTime) -> bool {
    // The range from 1,000,000,000 to 1,999,999,999 represents the leap second.
    dt.second() == 59 && dt.nanosecond() > 999_999_999
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
        years: i32,
        months: i32,
        days: i32,
        seconds: i32,
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
        // exact matches first
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
        s if s.ends_with(" day") => Ok(Modifier::Days(parse_modifier_number(&s[..s.len() - 4])?)),
        s if s.ends_with(" days") => Ok(Modifier::Days(parse_modifier_number(&s[..s.len() - 5])?)),
        s if s.ends_with(" hour") => Ok(Modifier::Hours(parse_modifier_number(&s[..s.len() - 5])?)),
        s if s.ends_with(" hours") => {
            Ok(Modifier::Hours(parse_modifier_number(&s[..s.len() - 6])?))
        }
        s if s.ends_with(" minute") => {
            Ok(Modifier::Minutes(parse_modifier_number(&s[..s.len() - 7])?))
        }
        s if s.ends_with(" minutes") => {
            Ok(Modifier::Minutes(parse_modifier_number(&s[..s.len() - 8])?))
        }
        s if s.ends_with(" second") => {
            Ok(Modifier::Seconds(parse_modifier_number(&s[..s.len() - 7])?))
        }
        s if s.ends_with(" seconds") => {
            Ok(Modifier::Seconds(parse_modifier_number(&s[..s.len() - 8])?))
        }
        s if s.ends_with(" month") => Ok(Modifier::Months(
            parse_modifier_number(&s[..s.len() - 6])? as i32,
        )),
        s if s.ends_with(" months") => Ok(Modifier::Months(
            parse_modifier_number(&s[..s.len() - 7])? as i32,
        )),
        s if s.ends_with(" year") => Ok(Modifier::Years(
            parse_modifier_number(&s[..s.len() - 5])? as i32
        )),
        s if s.ends_with(" years") => Ok(Modifier::Years(
            parse_modifier_number(&s[..s.len() - 6])? as i32,
        )),
        s if s.starts_with('+') || s.starts_with('-') => {
            let sign = if s.starts_with('-') { -1 } else { 1 };
            let parts: Vec<&str> = s[1..].split(' ').collect();
            let digits_in_date = 10;
            match parts.len() {
                1 => {
                    if parts[0].len() == digits_in_date {
                        let date = parse_modifier_date(parts[0])?;
                        Ok(Modifier::DateOffset {
                            years: sign * date.year() as i32,
                            months: sign * date.month() as i32,
                            days: sign * date.day() as i32,
                        })
                    } else {
                        // time values are either 12, 8 or 5 digits
                        let time = parse_modifier_time(parts[0])?;
                        let time_delta = (sign * (time.num_seconds_from_midnight() as i32)) as i32;
                        Ok(Modifier::TimeOffset(TimeDelta::seconds(time_delta.into())))
                    }
                }
                2 => {
                    let date = parse_modifier_date(parts[0])?;
                    let time = parse_modifier_time(parts[1])?;
                    // Convert time to total seconds (with sign)
                    let time_delta = sign * (time.num_seconds_from_midnight() as i32);
                    Ok(Modifier::DateTimeOffset {
                        years: sign * (date.year() as i32),
                        months: sign * (date.month() as i32),
                        days: sign * date.day() as i32,
                        seconds: time_delta,
                    })
                }
                _ => Err(InvalidModifier(
                    "Invalid date/time offset format".to_string(),
                )),
            }
        }
        _ => Err(InvalidModifier(
            "Invalid date/time offset format".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;

    #[test]
    fn test_valid_get_date_from_time_value() {
        let now = chrono::Local::now().to_utc().format("%Y-%m-%d").to_string();

        let prev_date_str = "2024-07-20";
        let test_date_str = "2024-07-21";
        let next_date_str = "2024-07-22";

        let test_cases = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (
                OwnedValue::build_text(Rc::new("2024-07-21".to_string())),
                test_date_str,
            ),
            // Format 2: YYYY-MM-DD HH:MM
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 01:30+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30Z".to_string())),
                test_date_str,
            ),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 01:30:45+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45Z".to_string())),
                test_date_str,
            ),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 01:30:45.123+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123Z".to_string())),
                test_date_str,
            ),
            // Format 5: YYYY-MM-DDTHH:MM
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T01:30+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30Z".to_string())),
                test_date_str,
            ),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T01:30:45+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45Z".to_string())),
                test_date_str,
            ),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123+02:00".to_string())),
                test_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123-05:00".to_string())),
                next_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T01:30:45.123+05:00".to_string())),
                prev_date_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123Z".to_string())),
                test_date_str,
            ),
            // Format 8: HH:MM
            (
                OwnedValue::build_text(Rc::new("22:30".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30+02:00".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30-05:00".to_string())),
                "2000-01-02",
            ),
            (
                OwnedValue::build_text(Rc::new("01:30+05:00".to_string())),
                "1999-12-31",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30Z".to_string())),
                "2000-01-01",
            ),
            // Format 9: HH:MM:SS
            (
                OwnedValue::build_text(Rc::new("22:30:45".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45+02:00".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45-05:00".to_string())),
                "2000-01-02",
            ),
            (
                OwnedValue::build_text(Rc::new("01:30:45+05:00".to_string())),
                "1999-12-31",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45Z".to_string())),
                "2000-01-01",
            ),
            // Format 10: HH:MM:SS.SSS
            (
                OwnedValue::build_text(Rc::new("22:30:45.123".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45.123+02:00".to_string())),
                "2000-01-01",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45.123-05:00".to_string())),
                "2000-01-02",
            ),
            (
                OwnedValue::build_text(Rc::new("01:30:45.123+05:00".to_string())),
                "1999-12-31",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45.123Z".to_string())),
                "2000-01-01",
            ),
            // Test Format 11: 'now'
            (OwnedValue::build_text(Rc::new("now".to_string())), &now),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (OwnedValue::Float(2460512.5), test_date_str),
            (OwnedValue::Integer(2460513), test_date_str),
        ];

        for (input, expected) in test_cases {
            let result = exec_date(&[input.clone()]);
            assert_eq!(
                result,
                OwnedValue::build_text(Rc::new(expected.to_string())),
                "Failed for input: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_invalid_get_date_from_time_value() {
        let invalid_cases = vec![
            OwnedValue::build_text(Rc::new("2024-07-21 25:00".to_string())), // Invalid hour
            OwnedValue::build_text(Rc::new("2024-07-21 24:00:00".to_string())), // Invalid hour
            OwnedValue::build_text(Rc::new("2024-07-21 23:60:00".to_string())), // Invalid minute
            OwnedValue::build_text(Rc::new("2024-07-21 22:58:60".to_string())), // Invalid second
            OwnedValue::build_text(Rc::new("2024-07-32".to_string())),       // Invalid day
            OwnedValue::build_text(Rc::new("2024-13-01".to_string())),       // Invalid month
            OwnedValue::build_text(Rc::new("invalid_date".to_string())), // Completely invalid string
            OwnedValue::build_text(Rc::new("".to_string())),             // Empty string
            OwnedValue::Integer(i64::MAX),                               // Large Julian day
            OwnedValue::Integer(-1),                                     // Negative Julian day
            OwnedValue::Float(f64::MAX),                                 // Large float
            OwnedValue::Float(-1.0),          // Negative Julian day as float
            OwnedValue::Float(f64::NAN),      // NaN
            OwnedValue::Float(f64::INFINITY), // Infinity
            OwnedValue::Null,                 // Null value
            OwnedValue::Blob(vec![1, 2, 3].into()), // Blob (unsupported type)
            // Invalid timezone tests
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+24:00".to_string())), // Invalid timezone offset (too large)
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00-24:00".to_string())), // Invalid timezone offset (too small)
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+00:60".to_string())), // Invalid timezone minutes
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+00:00:00".to_string())), // Invalid timezone format (extra seconds)
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+".to_string())), // Incomplete timezone
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+Z".to_string())), // Invalid timezone format
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+00:00Z".to_string())), // Mixing offset and Z
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00UTC".to_string())), // Named timezone (not supported)
        ];

        for case in invalid_cases.iter() {
            let result = exec_date(&[case.clone()]);
            match result {
                OwnedValue::Text(ref result_str) if result_str.value.is_empty() => (),
                _ => panic!(
                    "Expected empty string for input: {:?}, but got: {:?}",
                    case, result
                ),
            }
        }
    }

    #[test]
    fn test_valid_get_time_from_datetime_value() {
        let test_time_str = "22:30:45";
        let prev_time_str = "20:30:45";
        let next_time_str = "03:30:45";

        let test_cases = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (
                OwnedValue::build_text(Rc::new("2024-07-21".to_string())),
                "00:00:00",
            ),
            // Format 2: YYYY-MM-DD HH:MM
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30".to_string())),
                "22:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30+02:00".to_string())),
                "20:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30-05:00".to_string())),
                "03:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30Z".to_string())),
                "22:30:00",
            ),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45Z".to_string())),
                test_time_str,
            ),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21 22:30:45.123Z".to_string())),
                test_time_str,
            ),
            // Format 5: YYYY-MM-DDTHH:MM
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30".to_string())),
                "22:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30+02:00".to_string())),
                "20:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30-05:00".to_string())),
                "03:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30Z".to_string())),
                "22:30:00",
            ),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45Z".to_string())),
                test_time_str,
            ),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("2024-07-21T22:30:45.123Z".to_string())),
                test_time_str,
            ),
            // Format 8: HH:MM
            (
                OwnedValue::build_text(Rc::new("22:30".to_string())),
                "22:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30+02:00".to_string())),
                "20:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30-05:00".to_string())),
                "03:30:00",
            ),
            (
                OwnedValue::build_text(Rc::new("22:30Z".to_string())),
                "22:30:00",
            ),
            // Format 9: HH:MM:SS
            (
                OwnedValue::build_text(Rc::new("22:30:45".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45Z".to_string())),
                test_time_str,
            ),
            // Format 10: HH:MM:SS.SSS
            (
                OwnedValue::build_text(Rc::new("22:30:45.123".to_string())),
                test_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45.123+02:00".to_string())),
                prev_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45.123-05:00".to_string())),
                next_time_str,
            ),
            (
                OwnedValue::build_text(Rc::new("22:30:45.123Z".to_string())),
                test_time_str,
            ),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (OwnedValue::Float(2460082.1), "14:24:00"),
            (OwnedValue::Integer(2460082), "12:00:00"),
        ];

        for (input, expected) in test_cases {
            let result = exec_time(&[input]);
            if let OwnedValue::Text(result_str) = result {
                assert_eq!(result_str.value.as_str(), expected);
            } else {
                panic!("Expected OwnedValue::Text, but got: {:?}", result);
            }
        }
    }

    #[test]
    fn test_invalid_get_time_from_datetime_value() {
        let invalid_cases = vec![
            OwnedValue::build_text(Rc::new("2024-07-21 25:00".to_string())), // Invalid hour
            OwnedValue::build_text(Rc::new("2024-07-21 24:00:00".to_string())), // Invalid hour
            OwnedValue::build_text(Rc::new("2024-07-21 23:60:00".to_string())), // Invalid minute
            OwnedValue::build_text(Rc::new("2024-07-21 22:58:60".to_string())), // Invalid second
            OwnedValue::build_text(Rc::new("2024-07-32".to_string())),       // Invalid day
            OwnedValue::build_text(Rc::new("2024-13-01".to_string())),       // Invalid month
            OwnedValue::build_text(Rc::new("invalid_date".to_string())), // Completely invalid string
            OwnedValue::build_text(Rc::new("".to_string())),             // Empty string
            OwnedValue::Integer(i64::MAX),                               // Large Julian day
            OwnedValue::Integer(-1),                                     // Negative Julian day
            OwnedValue::Float(f64::MAX),                                 // Large float
            OwnedValue::Float(-1.0),          // Negative Julian day as float
            OwnedValue::Float(f64::NAN),      // NaN
            OwnedValue::Float(f64::INFINITY), // Infinity
            OwnedValue::Null,                 // Null value
            OwnedValue::Blob(vec![1, 2, 3].into()), // Blob (unsupported type)
            // Invalid timezone tests
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+24:00".to_string())), // Invalid timezone offset (too large)
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00-24:00".to_string())), // Invalid timezone offset (too small)
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+00:60".to_string())), // Invalid timezone minutes
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+00:00:00".to_string())), // Invalid timezone format (extra seconds)
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+".to_string())), // Incomplete timezone
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+Z".to_string())), // Invalid timezone format
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00+00:00Z".to_string())), // Mixing offset and Z
            OwnedValue::build_text(Rc::new("2024-07-21T12:00:00UTC".to_string())), // Named timezone (not supported)
        ];

        for case in invalid_cases {
            let result = exec_time(&[case.clone()]);
            match result {
                OwnedValue::Text(ref result_str) if result_str.value.is_empty() => (),
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
        assert_eq!(
            parse_modifier("+2023-05-15").unwrap(),
            Modifier::DateOffset {
                years: 2023,
                months: 5,
                days: 15,
            }
        );
        assert_eq!(
            parse_modifier("-2023-05-15").unwrap(),
            Modifier::DateOffset {
                years: -2023,
                months: -5,
                days: -15,
            }
        );
    }

    #[test]
    fn test_parse_date_time_offset() {
        assert_eq!(
            parse_modifier("+2023-05-15 14:30").unwrap(),
            Modifier::DateTimeOffset {
                years: 2023,
                months: 5,
                days: 15,
                seconds: (14 * 60 + 30) * 60,
            }
        );
        assert_eq!(
            parse_modifier("-0001-05-15 14:30").unwrap(),
            Modifier::DateTimeOffset {
                years: -1,
                months: -5,
                days: -15,
                seconds: -((14 * 60 + 30) * 60),
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
    fn test_apply_modifier_date_time_offset() {
        let mut dt = setup_datetime();
        apply_modifier(&mut dt, "+0001-01-01 01:01").unwrap();
        assert_eq!(dt, create_datetime(2024, 7, 16, 13, 31, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-0001-01-01 01:01").unwrap();
        assert_eq!(dt, create_datetime(2022, 5, 14, 11, 29, 45));

        // Test with larger offsets
        dt = setup_datetime();
        apply_modifier(&mut dt, "+0002-03-04 05:06").unwrap();
        assert_eq!(dt, create_datetime(2025, 9, 19, 17, 36, 45));

        dt = setup_datetime();
        apply_modifier(&mut dt, "-0002-03-04 05:06").unwrap();
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

    fn text(value: &str) -> OwnedValue {
        OwnedValue::build_text(Rc::new(value.to_string()))
    }

    fn format(dt: NaiveDateTime) -> String {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }
    fn weekday_sunday_based(dt: &chrono::NaiveDateTime) -> u32 {
        dt.weekday().num_days_from_sunday()
    }

    #[test]
    fn test_single_modifier() {
        let time = setup_datetime();
        let expected = format(time - TimeDelta::days(1));
        let result = exec_datetime(
            &[text("2023-06-15 12:30:45"), text("-1 day")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_multiple_modifiers() {
        let time = setup_datetime();
        let expected = format(time - TimeDelta::days(1) + TimeDelta::hours(3));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("-1 day"),
                text("+3 hours"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_subsec_modifier() {
        let time = setup_datetime();
        let result = exec_datetime(
            &[text("2023-06-15 12:30:45"), text("subsec")],
            DateTimeOutput::Time,
        );
        let result =
            chrono::NaiveTime::parse_from_str(&result.to_string(), "%H:%M:%S%.3f").unwrap();
        assert_eq!(time.time(), result);
    }

    #[test]
    fn test_start_of_day_modifier() {
        let time = setup_datetime();
        let start_of_day = time.date().and_hms_opt(0, 0, 0).unwrap();
        let expected = format(start_of_day - TimeDelta::days(1));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("start of day"),
                text("-1 day"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_start_of_month_modifier() {
        let time = setup_datetime();
        let start_of_month = NaiveDate::from_ymd_opt(time.year(), time.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let expected = format(start_of_month + TimeDelta::days(1));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("start of month"),
                text("+1 day"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_start_of_year_modifier() {
        let time = setup_datetime();
        let start_of_year = NaiveDate::from_ymd_opt(time.year(), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let expected = format(start_of_year + TimeDelta::days(30) + TimeDelta::hours(5));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("start of year"),
                text("+30 days"),
                text("+5 hours"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_timezone_modifiers() {
        let dt = setup_datetime();
        let result_local = exec_datetime(
            &[text("2023-06-15 12:30:45"), text("localtime")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result_local,
            text(
                &dt.and_utc()
                    .with_timezone(&chrono::Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            )
        );
        // TODO: utc modifier assumes time given is not already utc
        // add test when fixed in the future
    }

    #[test]
    fn test_combined_modifiers() {
        let time = create_datetime(2000, 1, 1, 0, 0, 0);
        let expected = time - TimeDelta::days(1)
            + TimeDelta::hours(5)
            + TimeDelta::minutes(30)
            + TimeDelta::seconds(15);
        let result = exec_datetime(
            &[
                text("2000-01-01 00:00:00"),
                text("-1 day"),
                text("+5 hours"),
                text("+30 minutes"),
                text("+15 seconds"),
                text("subsec"),
            ],
            DateTimeOutput::DateTime,
        );
        let result =
            chrono::NaiveDateTime::parse_from_str(&result.to_string(), "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_max_datetime_limit() {
        // max datetime limit
        let max = NaiveDate::from_ymd_opt(9999, 12, 31)
            .unwrap()
            .and_hms_opt(23, 59, 59)
            .unwrap();
        let expected = format(max);
        let result = exec_datetime(&[text("9999-12-31 23:59:59")], DateTimeOutput::DateTime);
        assert_eq!(result, text(&expected));
    }

    // leap second
    #[test]
    fn test_leap_second_ignored() {
        let leap_second = NaiveDate::from_ymd_opt(2024, 6, 30)
            .unwrap()
            .and_hms_nano_opt(23, 59, 59, 1_500_000_000)
            .unwrap();
        let expected = String::new(); // SQLite ignores leap seconds
        let result = exec_datetime(&[text(&leap_second.to_string())], DateTimeOutput::DateTime);
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_already_on_weekday_no_change() {
        // 2023-01-01 is a Sunday => weekday 0
        let mut dt = create_datetime(2023, 1, 1, 12, 0, 0);
        apply_modifier(&mut dt, "weekday 0").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 1, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 0);
    }

    #[test]
    fn test_move_forward_if_different() {
        // 2023-01-01 is a Sunday => weekday 0
        // "weekday 1" => next Monday => 2023-01-02
        let mut dt = create_datetime(2023, 1, 1, 12, 0, 0);
        apply_modifier(&mut dt, "weekday 1").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 2, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 1);

        // 2023-01-03 is a Tuesday => weekday 2
        // "weekday 5" => next Friday => 2023-01-06
        let mut dt = create_datetime(2023, 1, 3, 12, 0, 0);
        apply_modifier(&mut dt, "weekday 5").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 6, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 5);
    }

    #[test]
    fn test_wrap_around_weekend() {
        // 2023-01-06 is a Friday => weekday 5
        // "weekday 0" => next Sunday => 2023-01-08
        let mut dt = create_datetime(2023, 1, 6, 12, 0, 0);
        apply_modifier(&mut dt, "weekday 0").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 8, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 0);

        // Now confirm that being on Sunday (weekday 0) and asking for "weekday 0" stays put
        apply_modifier(&mut dt, "weekday 0").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 8, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 0);
    }

    #[test]
    fn test_same_day_stays_put() {
        // 2023-01-05 is a Thursday => weekday 4
        // Asking for weekday 4 => no change
        let mut dt = create_datetime(2023, 1, 5, 12, 0, 0);
        apply_modifier(&mut dt, "weekday 4").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 5, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 4);
    }

    #[test]
    fn test_already_on_friday_no_change() {
        // 2023-01-06 is a Friday => weekday 5
        // Asking for weekday 5 => no change if already on Friday
        let mut dt = create_datetime(2023, 1, 6, 12, 0, 0);
        apply_modifier(&mut dt, "weekday 5").unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 6, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 5);
    }

    #[test]
    fn test_apply_modifier_julianday() {
        let dt = create_datetime(2000, 1, 1, 12, 0, 0);
        let julian_day = julian_day_converter::datetime_to_julian_day(&dt.to_string()).unwrap();
        let mut dt_result = NaiveDateTime::default();
        if let Ok(result) = julian_day_converter::julian_day_to_datetime(julian_day) {
            dt_result = result;
        }
        assert_eq!(dt_result, dt);
    }

    #[test]
    fn test_apply_modifier_start_of_month() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        apply_modifier(&mut dt, "start of month").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_subsec() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let dt_with_nanos = dt.with_nanosecond(123_456_789).unwrap();
        dt = dt_with_nanos;
        apply_modifier(&mut dt, "subsec").unwrap();
        assert_eq!(dt, dt_with_nanos);
    }

    #[test]
    fn test_apply_modifier_start_of_month_basic() {
        // Basic check: from mid-month to the 1st at 00:00:00.
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        apply_modifier(&mut dt, "start of month").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_start_of_month_already_at_first() {
        // If we're already at the start of the month, no change.
        let mut dt = create_datetime(2023, 6, 1, 0, 0, 0);
        apply_modifier(&mut dt, "start of month").unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_start_of_month_edge_case() {
        // edge case: month boundary. 2023-07-31 -> start of July.
        let mut dt = create_datetime(2023, 7, 31, 23, 59, 59);
        apply_modifier(&mut dt, "start of month").unwrap();
        assert_eq!(dt, create_datetime(2023, 7, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_subsec_no_change() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let dt_with_nanos = dt.with_nanosecond(123_456_789).unwrap();
        dt = dt_with_nanos;
        apply_modifier(&mut dt, "subsec").unwrap();
        assert_eq!(dt, dt_with_nanos);
    }

    #[test]
    fn test_apply_modifier_subsec_preserves_fractional_seconds() {
        let mut dt = create_datetime(2025, 1, 2, 4, 12, 21)
            .with_nanosecond(891_000_000) // 891 milliseconds
            .unwrap();
        apply_modifier(&mut dt, "subsec").unwrap();

        let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        assert_eq!(formatted, "2025-01-02 04:12:21.891");
    }

    #[test]
    fn test_apply_modifier_subsec_no_fractional_seconds() {
        let mut dt = create_datetime(2025, 1, 2, 4, 12, 21);
        apply_modifier(&mut dt, "subsec").unwrap();

        let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        assert_eq!(formatted, "2025-01-02 04:12:21.000");
    }

    #[test]
    fn test_apply_modifier_subsec_truncate_to_milliseconds() {
        let mut dt = create_datetime(2025, 1, 2, 4, 12, 21)
            .with_nanosecond(891_123_456)
            .unwrap();
        apply_modifier(&mut dt, "subsec").unwrap();

        let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        assert_eq!(formatted, "2025-01-02 04:12:21.891");
    }

    #[test]
    fn test_is_leap_second() {
        let dt = DateTime::from_timestamp(1483228799, 999_999_999)
            .unwrap()
            .naive_utc();
        assert!(!is_leap_second(&dt));

        let dt = DateTime::from_timestamp(1483228799, 1_500_000_000)
            .unwrap()
            .naive_utc();
        assert!(is_leap_second(&dt));
    }
}
