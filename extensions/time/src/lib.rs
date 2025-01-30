use chrono::prelude::*;
use chrono::NaiveDateTime;
use thiserror::Error;

use limbo_ext::{register_extension, scalar, ResultCode, Value};

mod time;

use time::*;

register_extension! {
    scalars: {time_now, time_fmt_iso, time_date},
}

macro_rules! ok_tri {
    ($e:expr $(,)?) => {
        match $e {
            Some(val) => val,
            None => return Value::error(ResultCode::Error),
        }
    };
}

macro_rules! tri {
    ($e:expr $(,)?) => {
        match $e {
            Ok(val) => val,
            Err(_) => return Value::error(ResultCode::Error),
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
    #[error("blob is not correct size")]
    InvalidSize,
    /// Blob time version not matching
    #[error("time blob version mismatch")]
    MismatchVersion,
    #[error("time blob version mismatch")]
    UnknownField(#[from] <TimeField as ::core::str::FromStr>::Err),
}

type Result<T> = core::result::Result<T, TimeError>;

#[scalar(name = "time_now", alias = "now")]
fn time_now(args: &[Value]) -> Value {
    if args.len() != 0 {
        return Value::error(ResultCode::Error);
    }
    let t = Time::new();

    t.into_blob()
}

#[scalar(name = "time_fmt_iso")]
fn time_fmt_iso(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::error(ResultCode::Error);
    }
    let blob = ok_tri!(args[0].to_blob());

    let t = tri!(Time::try_from(blob));

    let offset_sec = {
        if args.len() == 2 {
            ok_tri!(args[1].to_integer()) as i32
        } else {
            0
        }
    };

    let fmt_str = tri!(t.fmt_iso(offset_sec));

    Value::from_text(fmt_str)
}

/// \
/// Caveat: this function differs from sqlean's as it does not support normalizing the inputs
/// For example, October 32 will error. It will not normalize to November 1.\
/// Also due to a current limitation in the extension system, the function can only have one alias
/// and the alias cannot have different number of arguments. So no aliasing for now for this one.
#[scalar(name = "time_date")]
fn time_date(args: &[Value]) -> Value {
    if args.len() != 3 && args.len() != 6 && args.len() != 7 && args.len() != 8 {
        return Value::error(ResultCode::Error);
    }

    let year = ok_tri!(&args[0].to_integer()).to_owned() as i32;
    let month = ok_tri!(&args[1].to_integer()).to_owned() as u32;
    let day = ok_tri!(&args[2].to_integer()).to_owned() as u32;

    let mut datetime: NaiveDateTime = ok_tri!(NaiveDate::from_ymd_opt(year, month, day))
        .and_hms_opt(0, 0, 0)
        .unwrap();

    if args.len() >= 6 {
        let hour = ok_tri!(&args[3].to_integer()).to_owned() as u32;
        let minute = ok_tri!(&args[4].to_integer()).to_owned() as u32;
        let seconds = ok_tri!(&args[5].to_integer()).to_owned() as u32;
        datetime = ok_tri!(datetime.with_hour(hour));
        datetime = ok_tri!(datetime.with_minute(minute));
        datetime = ok_tri!(datetime.with_second(seconds));
    }

    if args.len() >= 7 {
        let nano_sec = ok_tri!(&args[6].to_integer()).to_owned() as u32;
        datetime = ok_tri!(datetime.with_nanosecond(nano_sec));
    }

    if args.len() == 8 {
        let offset_sec = ok_tri!(&args[7].to_integer()).to_owned() as i32;
        let offset = ok_tri!(FixedOffset::east_opt(offset_sec));
        // I believe this is not a double conversion here
        datetime = ok_tri!(datetime.and_local_timezone(offset).single()).naive_utc();
    }

    let t = Time::from_datetime(datetime.and_utc());

    t.into_blob()
}
