use std::str::FromStr as _;

use chrono::prelude::*;
use core::cmp::Ordering;
use limbo_ext::ValueType;
use thiserror::Error;

use limbo_ext::{register_extension, scalar, ResultCode, Value};

mod time;

use time::*;

register_extension! {
    scalars: {
        time_now,
        time_date,
        make_date,
        make_timestamp,
        time_get,
        time_get_year,
        time_get_month,
        time_get_day,
        time_get_hour,
        time_get_minute,
        time_get_second,
        time_get_nano,
        time_get_weekday,
        time_get_yearday,
        time_get_isoyear,
        time_get_isoweek,
        time_unix,
        to_timestamp,
        time_milli,
        time_micro,
        time_nano,
        time_to_unix,
        time_to_milli,
        time_to_micro,
        time_to_nano,
        time_after,
        time_before,
        time_compare,
        time_equal,
        dur_ns,
        dur_us,
        dur_ms,
        dur_s,
        dur_m,
        dur_h,
        time_add,
        time_add_date,
        time_sub,
        time_since,
        time_until,
        time_trunc,
        time_round,
        time_fmt_iso,
        time_fmt_datetime,
        time_fmt_date,
        time_fmt_time,
        time_parse,
    },
}

macro_rules! ok_tri {
    ($e:expr) => {
        match $e {
            Some(val) => val,
            None => return Value::error(ResultCode::Error),
        }
    };
    ($e:expr, $msg:expr) => {
        match $e {
            Some(val) => val,
            None => return Value::error_with_message($msg.to_string()),
        }
    };
}

macro_rules! tri {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => return Value::error_with_message(err.to_string()),
        }
    };
    ($e:expr, $msg:expr) => {
        match $e {
            Ok(val) => val,
            Err(_) => return Value::error_with_message($msg.to_string()),
        }
    };
}

/// Checks to see if e's enum is of type val
macro_rules! value_tri {
    ($e:expr, $val:pat) => {
        match $e {
            $val => (),
            _ => return Value::error(ResultCode::InvalidArgs),
        }
    };
    ($e:expr, $val:pat, $msg:expr) => {
        match $e {
            $val => (),
            _ => return Value::error_with_message($msg.to_string()),
        }
    };
}

#[derive(Error, Debug)]
pub enum TimeError {
    /// Timezone offset is invalid
    #[error("invalid timezone offset")]
    InvalidOffset,
    #[error("invalid datetime format")]
    InvalidFormat,
    /// Blob is not size of `TIME_BLOB_SIZE`
    #[error("invalid time blob size")]
    InvalidSize,
    /// Blob time version not matching
    #[error("mismatch time blob version")]
    MismatchVersion,
    #[error("unknown field")]
    UnknownField(#[from] <TimeField as ::core::str::FromStr>::Err),
    #[error("rounding error")]
    RoundingError(#[from] chrono::RoundingError),
    #[error("time creation error")]
    CreationError,
}

type Result<T> = core::result::Result<T, TimeError>;

#[scalar(name = "time_now", alias = "now")]
fn time_now(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::error(ResultCode::InvalidArgs);
    }
    let t = Time::new();

    t.into_blob()
}

/// ```text
/// time_date(year, month, day[, hour, min, sec[, nsec[, offset_sec]]])
/// ```
///
/// Returns the Time corresponding to a given date/time. The time part (hour+minute+second), the nanosecond part, and the timezone offset part are all optional.
///
/// The `month`, `day`, `hour`, `min`, `sec`, and `nsec` values may be outside their usual ranges and will be normalized during the conversion. For example, October 32 converts to November 1.
///
/// If `offset_sec` is not 0, the source time is treated as being in a given timezone (with an offset in seconds east of UTC) and converted back to UTC.
fn time_date_internal(args: &[Value]) -> Value {
    if args.len() != 3 && args.len() != 6 && args.len() != 7 && args.len() != 8 {
        return Value::error(ResultCode::InvalidArgs);
    }

    for arg in args {
        value_tri!(
            arg.value_type(),
            ValueType::Integer,
            "all parameters should be integers"
        );
    }

    let year = ok_tri!(args[0].to_integer());
    let month = ok_tri!(args[1].to_integer());
    let day = ok_tri!(args[2].to_integer());
    let mut hour = 0;
    let mut minutes = 0;
    let mut seconds = 0;
    let mut nano_secs = 0;
    let mut offset = FixedOffset::east_opt(0).unwrap();

    if args.len() >= 6 {
        hour = ok_tri!(args[3].to_integer());
        minutes = ok_tri!(args[4].to_integer());
        seconds = ok_tri!(args[5].to_integer());
    }

    if args.len() >= 7 {
        nano_secs = ok_tri!(args[6].to_integer());
    }

    if args.len() == 8 {
        let offset_sec = ok_tri!(args[7].to_integer()) as i32;
        // TODO offset is not normalized. Maybe could just increase/decrease the number of seconds
        // instead of relying in this offset
        offset = ok_tri!(FixedOffset::east_opt(offset_sec));
    }

    let t = Time::time_date(
        year as i32,
        month as i32,
        day,
        hour,
        minutes,
        seconds,
        nano_secs,
        offset,
    );

    let t = tri!(t);

    t.into_blob()
}

#[scalar(name = "time_date")]
fn time_date(args: &[Value]) {
    time_date_internal(args)
}

#[scalar(name = "make_date")]
fn make_date(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::error(ResultCode::InvalidArgs);
    }

    time_date_internal(args)
}

#[scalar(name = "make_timestamp")]
fn make_timestamp(args: &[Value]) -> Value {
    if args.len() != 6 {
        return Value::error(ResultCode::InvalidArgs);
    }

    time_date_internal(args)
}

#[scalar(name = "time_get", alias = "date_part")]
fn time_get(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let field = ok_tri!(args[1].to_text(), "2nd parameter: should be a field name");

    let field = tri!(TimeField::from_str(field));

    t.time_get(field)
}

#[scalar(name = "time_get_year")]
fn time_get_year(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::Year)
}

#[scalar(name = "time_get_month")]
fn time_get_month(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::Month)
}

#[scalar(name = "time_get_day")]
fn time_get_day(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::Day)
}

#[scalar(name = "time_get_hour")]
fn time_get_hour(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::Hour)
}

#[scalar(name = "time_get_minute")]
fn time_get_minute(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::Minute)
}

#[scalar(name = "time_get_second")]
fn time_get_second(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    Value::from_integer(t.get_second())
}

#[scalar(name = "time_get_nano")]
fn time_get_nano(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    Value::from_integer(t.get_nanosecond())
}

#[scalar(name = "time_get_weekday")]
fn time_get_weekday(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::WeekDay)
}

#[scalar(name = "time_get_yearday")]
fn time_get_yearday(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::YearDay)
}

#[scalar(name = "time_get_isoyear")]
fn time_get_isoyear(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::IsoYear)
}

#[scalar(name = "time_get_isoweek")]
fn time_get_isoweek(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    t.time_get(TimeField::IsoWeek)
}

fn time_unix_internal(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    for arg in args {
        value_tri!(
            arg.value_type(),
            ValueType::Integer,
            "all parameters should be integers"
        );
    }

    let seconds = ok_tri!(args[0].to_integer());

    let mut nano_sec = 0;

    if args.len() == 2 {
        nano_sec = ok_tri!(args[1].to_integer());
    }

    let dt = ok_tri!(DateTime::from_timestamp(seconds, nano_sec as u32));

    let t = Time::from_datetime(dt);

    t.into_blob()
}

#[scalar(name = "time_unix")]
fn time_unix(args: &[Value]) -> Value {
    time_unix_internal(args)
}

#[scalar(name = "to_timestamp")]
fn to_timestamp(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    time_unix_internal(args)
}

#[scalar(name = "time_milli")]
fn time_milli(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    value_tri!(
        &args[0].value_type(),
        ValueType::Integer,
        "parameter should be an integer"
    );

    let millis = ok_tri!(args[0].to_integer());

    let dt = ok_tri!(DateTime::from_timestamp_millis(millis));

    let t = Time::from_datetime(dt);

    t.into_blob()
}

#[scalar(name = "time_micro")]
fn time_micro(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    value_tri!(
        &args[0].value_type(),
        ValueType::Integer,
        "parameter should be an integer"
    );

    let micros = ok_tri!(args[0].to_integer());

    let dt = ok_tri!(DateTime::from_timestamp_micros(micros));

    let t = Time::from_datetime(dt);

    t.into_blob()
}

#[scalar(name = "time_nano")]
fn time_nano(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    value_tri!(
        &args[0].value_type(),
        ValueType::Integer,
        "parameter should be an integer"
    );

    let nanos = ok_tri!(args[0].to_integer());

    let dt = DateTime::from_timestamp_nanos(nanos);

    let t = Time::from_datetime(dt);

    t.into_blob()
}

#[scalar(name = "time_to_unix")]
fn time_to_unix(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    Value::from_integer(t.to_unix())
}

#[scalar(name = "time_to_milli")]
fn time_to_milli(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    Value::from_integer(t.to_unix_milli())
}

#[scalar(name = "time_to_micro")]
fn time_to_micro(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    Value::from_integer(t.to_unix_micro())
}

#[scalar(name = "time_to_nano")]
fn time_to_nano(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");

    let t = tri!(Time::try_from(blob));

    Value::from_integer(ok_tri!(t.to_unix_nano()))
}

// Comparisons

#[scalar(name = "time_after")]
fn time_after(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let blob = ok_tri!(args[1].to_blob(), "2nd parameter: should be a time blob");

    let u = tri!(Time::try_from(blob));

    Value::from_integer((t > u).into())
}

#[scalar(name = "time_before")]
fn time_before(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let blob = ok_tri!(args[1].to_blob(), "2nd parameter: should be a time blob");

    let u = tri!(Time::try_from(blob));

    Value::from_integer((t < u).into())
}

#[scalar(name = "time_compare")]
fn time_compare(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let blob = ok_tri!(args[1].to_blob(), "2nd parameter: should be a time blob");

    let u = tri!(Time::try_from(blob));

    let cmp = match ok_tri!(t.partial_cmp(&u)) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    };

    Value::from_integer(cmp)
}

#[scalar(name = "time_equal")]
fn time_equal(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let blob = ok_tri!(args[1].to_blob(), "2nd parameter: should be a time blob");

    let u = tri!(Time::try_from(blob));

    Value::from_integer(t.eq(&u).into())
}

// Duration Constants

/// 1 nanosecond
#[scalar(name = "dur_ns")]
fn dur_ns(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::error(ResultCode::InvalidArgs);
    }

    Value::from_integer(chrono::Duration::nanoseconds(1).num_nanoseconds().unwrap())
}

/// 1 microsecond
#[scalar(name = "dur_us")]
fn dur_us(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::error(ResultCode::InvalidArgs);
    }

    Value::from_integer(chrono::Duration::microseconds(1).num_nanoseconds().unwrap())
}

/// 1 millisecond
#[scalar(name = "dur_ms")]
fn dur_ms(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::error(ResultCode::InvalidArgs);
    }

    Value::from_integer(chrono::Duration::milliseconds(1).num_nanoseconds().unwrap())
}

/// 1 second
#[scalar(name = "dur_s")]
fn dur_s(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::error(ResultCode::InvalidArgs);
    }

    Value::from_integer(chrono::Duration::seconds(1).num_nanoseconds().unwrap())
}

/// 1 minute
#[scalar(name = "dur_m")]
fn dur_m(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::error(ResultCode::InvalidArgs);
    }

    Value::from_integer(chrono::Duration::minutes(1).num_nanoseconds().unwrap())
}

/// 1 hour
#[scalar(name = "dur_h")]
fn dur_h(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::error(ResultCode::InvalidArgs);
    }

    Value::from_integer(chrono::Duration::hours(1).num_nanoseconds().unwrap())
}

// Time Arithmetic

/// Do not use `time_add` to add days, months or years. Use `time_add_date` instead.
#[scalar(name = "time_add", alias = "date_add")]
fn time_add(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");
    let t = tri!(Time::try_from(blob));

    value_tri!(
        args[1].value_type(),
        ValueType::Integer,
        "2nd parameter: should be an integer"
    );

    let d = ok_tri!(args[1].to_integer());

    let d = Duration::from(d);

    t.add_duration(d).into_blob()
}

#[scalar(name = "time_add_date")]
fn time_add_date(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 && args.len() != 4 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");
    let t = tri!(Time::try_from(blob));

    value_tri!(
        args[1].value_type(),
        ValueType::Integer,
        "2nd parameter: should be an integer"
    );

    let years = ok_tri!(args[1].to_integer());
    let mut months = 0;
    let mut days = 0;

    if args.len() >= 3 {
        value_tri!(
            args[2].value_type(),
            ValueType::Integer,
            "3rd parameter: should be an integer"
        );

        months = ok_tri!(args[2].to_integer());
    }

    if args.len() == 4 {
        value_tri!(
            args[3].value_type(),
            ValueType::Integer,
            "4th parameter: should be an integer"
        );

        days = ok_tri!(args[3].to_integer());
    }

    let t: Time = tri!(t.time_add_date(years as i32, months as i32, days));

    t.into_blob()
}

/// Returns the duration between two time values t and u (in nanoseconds).
/// If the result exceeds the maximum (or minimum) value that can be stored in a Duration,
/// the maximum (or minimum) duration will be returned.
fn time_sub_internal(t: Time, u: Time) -> Value {
    let cmp = ok_tri!(t.partial_cmp(&u));

    let diff = t - u;

    let nano_secs = match diff.num_nanoseconds() {
        Some(nano) => nano,
        None => match cmp {
            Ordering::Equal => ok_tri!(diff.num_nanoseconds()),
            Ordering::Less => i64::MIN,
            Ordering::Greater => i64::MAX,
        },
    };

    Value::from_integer(nano_secs)
}

#[scalar(name = "time_sub", alias = "age")]
fn time_sub(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");
    let t = tri!(Time::try_from(blob));

    let blob = ok_tri!(args[1].to_blob(), "2nd parameter: should be a time blob");
    let u = tri!(Time::try_from(blob));

    time_sub_internal(t, u)
}

#[scalar(name = "time_since")]
fn time_since(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let now = Time::new();

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");
    let t = tri!(Time::try_from(blob));

    time_sub_internal(now, t)
}

#[scalar(name = "time_until")]
fn time_until(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let now = Time::new();

    let blob = ok_tri!(args[0].to_blob(), "parameter should be a time blob");
    let t = tri!(Time::try_from(blob));

    time_sub_internal(t, now)
}

// Rouding

#[scalar(name = "time_trunc", alias = "date_trunc")]
fn time_trunc(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    match args[1].value_type() {
        ValueType::Text => {
            let field = ok_tri!(args[1].to_text());

            let field = tri!(TimeRoundField::from_str(field));

            tri!(t.trunc_field(field)).into_blob()
        }
        ValueType::Integer => {
            let duration = ok_tri!(args[1].to_integer());
            let duration = Duration::from(duration);

            tri!(t.trunc_duration(duration)).into_blob()
        }
        _ => Value::error_with_message("2nd parameter: should be a field name".to_string()),
    }
}

#[scalar(name = "time_round")]
fn time_round(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    value_tri!(
        args[1].value_type(),
        ValueType::Integer,
        "2nd parameter: should be an integer"
    );

    let duration = ok_tri!(args[1].to_integer());
    let duration = Duration::from(duration);

    tri!(t.round_duration(duration)).into_blob()
}

// Formatting

#[scalar(name = "time_fmt_iso")]
fn time_fmt_iso(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }
    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let offset_sec = {
        if args.len() == 2 {
            value_tri!(
                args[1].value_type(),
                ValueType::Integer,
                "2nd parameter: should be an integer"
            );
            ok_tri!(args[1].to_integer()) as i32
        } else {
            0
        }
    };

    let fmt_str = tri!(t.fmt_iso(offset_sec));

    Value::from_text(fmt_str)
}

#[scalar(name = "time_fmt_datetime")]
fn time_fmt_datetime(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }
    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let offset_sec = {
        if args.len() == 2 {
            value_tri!(
                args[1].value_type(),
                ValueType::Integer,
                "2nd parameter: should be an integer"
            );
            ok_tri!(args[1].to_integer()) as i32
        } else {
            0
        }
    };

    let fmt_str = tri!(t.fmt_datetime(offset_sec));

    Value::from_text(fmt_str)
}

#[scalar(name = "time_fmt_date")]
fn time_fmt_date(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }
    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let offset_sec = {
        if args.len() == 2 {
            value_tri!(
                args[1].value_type(),
                ValueType::Integer,
                "2nd parameter: should be an integer"
            );
            ok_tri!(args[1].to_integer()) as i32
        } else {
            0
        }
    };

    let fmt_str = tri!(t.fmt_date(offset_sec));

    Value::from_text(fmt_str)
}

#[scalar(name = "time_fmt_time")]
fn time_fmt_time(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::error(ResultCode::InvalidArgs);
    }
    let blob = ok_tri!(args[0].to_blob(), "1st parameter: should be a time blob");

    let t = tri!(Time::try_from(blob));

    let offset_sec = {
        if args.len() == 2 {
            value_tri!(
                args[1].value_type(),
                ValueType::Integer,
                "2nd parameter: should be an integer"
            );
            ok_tri!(args[1].to_integer()) as i32
        } else {
            0
        }
    };

    let fmt_str = tri!(t.fmt_time(offset_sec));

    Value::from_text(fmt_str)
}

#[scalar(name = "time_parse")]
fn time_parse(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::InvalidArgs);
    }

    let dt_str = ok_tri!(args[0].to_text());

    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(dt_str) {
        return Time::from_datetime(dt.to_utc()).into_blob();
    }

    if let Ok(mut dt) = chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S") {
        // Unwrap is safe here
        dt = dt.with_nanosecond(0).unwrap();
        return Time::from_datetime(dt.and_utc()).into_blob();
    }

    if let Ok(date) = chrono::NaiveDate::parse_from_str(dt_str, "%Y-%m-%d") {
        // Unwrap is safe here

        let dt = date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        return Time::from_datetime(dt.and_utc()).into_blob();
    }

    let time = tri!(
        chrono::NaiveTime::parse_from_str(dt_str, "%H:%M:%S"),
        "error parsing datetime string"
    );
    let dt = NaiveDateTime::new(NaiveDate::from_ymd_opt(1, 1, 1).unwrap(), time)
        .with_nanosecond(0)
        .unwrap();

    Time::from_datetime(dt.and_utc()).into_blob()
}
