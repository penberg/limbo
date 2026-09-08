use crate::numeric::Numeric;
use crate::types::AsValueRef;
use crate::types::{TextRef, TextSubtype, Value};
use crate::LimboError::InvalidModifier;
use crate::{Result, ValueRef};
// chrono isn't used more due to incompatibility with sqlite
use chrono::{Local, Offset, TimeZone};
use std::borrow::Cow;
use std::fmt::Write;

const JD_TO_MS: i64 = 86_400_000;
const MAX_JD: i64 = 464269060799999; // 9999-12-31 23:59:59.999

#[derive(Debug, Clone, Copy)]
struct DateTime {
    i_jd: i64, // The julian day number times 86400000
    y: i32,
    m: i32,
    d: i32,
    h: i32,
    min: i32,
    s: f64,
    tz: i32, // Timezone offset in minutes
    n_floor: i32,
    valid_jd: bool,
    valid_ymd: bool,
    valid_hms: bool,
    raw_s: bool, // Raw numeric value stored in s
    is_error: bool,
    use_subsec: bool,
    is_utc: bool,
    is_local: bool,
}

impl Default for DateTime {
    fn default() -> Self {
        DateTime {
            i_jd: 0,
            y: 2000,
            m: 1,
            d: 1,
            h: 0,
            min: 0,
            s: 0.0,
            tz: 0,
            n_floor: 0,
            valid_jd: false,
            valid_ymd: false,
            valid_hms: false,
            raw_s: false,
            is_error: false,
            use_subsec: false,
            is_utc: false,
            is_local: false,
        }
    }
}

impl DateTime {
    fn set_error(&mut self) {
        *self = DateTime::default();
        self.is_error = true;
    }

    fn compute_jd(&mut self) {
        if self.valid_jd {
            return;
        }
        let mut y: i32;
        let mut m: i32;
        let d: i32;
        if self.valid_ymd {
            y = self.y;
            m = self.m;
            d = self.d;
        } else {
            y = 2000;
            m = 1;
            d = 1;
        }
        if !(-4713..=9999).contains(&y) || self.raw_s {
            self.set_error();
            return;
        }
        if m <= 2 {
            y -= 1;
            m += 12;
        }
        let a = (y + 4800) / 100;
        let b = 38 - a + (a / 4);
        let x1 = 36525 * (y + 4716) / 100;
        let x2 = 306001 * (m + 1) / 10000;
        self.i_jd = (x1 as i64 + x2 as i64 + d as i64 + b as i64) * 86400000 - 131716800000;
        self.valid_jd = true;
        if self.valid_hms {
            self.i_jd += self.h as i64 * 3_600_000
                + self.min as i64 * 60_000
                + (self.s * 1000.0 + 0.5) as i64;
            if self.tz != 0 {
                self.i_jd -= self.tz as i64 * 60_000;
                self.valid_ymd = false;
                self.valid_hms = false;
                self.tz = 0;
                self.is_utc = true;
                self.is_local = false;
            }
        }
    }

    fn compute_ymd(&mut self) {
        if self.valid_ymd {
            return;
        }
        if !self.valid_jd {
            self.y = 2000;
            self.m = 1;
            self.d = 1;
        } else if self.i_jd < 0 || self.i_jd > MAX_JD {
            self.set_error();
            return;
        } else {
            let z = ((self.i_jd + 43200000) / JD_TO_MS) as i32;
            let alpha = ((z as f64 + 32044.75) / 36524.25) as i32 - 52;
            let a = z + 1 + alpha - ((alpha + 100) / 4) + 25;
            let b = a + 1524;
            let c = ((b as f64 - 122.1) / 365.25) as i32;
            let d_calc = (36525 * (c & 32767)) / 100;
            let e = ((b - d_calc) as f64 / 30.6001) as i32;
            let x1 = (30.6001 * e as f64) as i32;

            self.d = b - d_calc - x1;
            self.m = if e < 14 { e - 1 } else { e - 13 };
            self.y = if self.m > 2 { c - 4716 } else { c - 4715 };
        }
        self.valid_ymd = true;
    }

    fn compute_hms(&mut self) {
        if self.valid_hms {
            return;
        }
        self.compute_jd();
        let day_ms = ((self.i_jd + 43200000) % 86400000) as i32;
        self.s = (day_ms % 60000) as f64 / 1000.0;
        let day_min = day_ms / 60000;
        self.min = day_min % 60;
        self.h = day_min / 60;
        self.raw_s = false;
        self.valid_hms = true;
    }

    fn compute_ymd_hms(&mut self) {
        self.compute_ymd();
        self.compute_hms();
    }

    fn clear_ymd_hms_tz(&mut self) {
        self.valid_ymd = false;
        self.valid_hms = false;
        self.tz = 0;
    }

    fn compute_floor(&mut self) {
        assert!(self.valid_ymd || self.is_error);
        assert!(self.d >= 0 && self.d <= 31);
        assert!(self.m >= 0 && self.m <= 12);
        if self.d <= 28 || ((1 << self.m) & 0x15aa) != 0 {
            self.n_floor = 0;
        } else if self.m != 2 {
            self.n_floor = if self.d == 31 { 1 } else { 0 };
        } else if self.y % 4 != 0 || (self.y % 100 == 0 && self.y % 400 != 0) {
            self.n_floor = self.d - 28;
        } else {
            self.n_floor = self.d - 29;
        }
    }
}

fn get_digits(z: &str, digits: usize, min_val: i32, max_val: i32) -> Option<(i32, &str)> {
    if z.len() < digits {
        return None;
    }
    if !z.is_char_boundary(digits) {
        return None;
    }
    let bytes = z.as_bytes();
    if !bytes.iter().take(digits).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let slice = &z[..digits];
    let val = slice.parse::<i32>().ok()?;
    if val < min_val || val > max_val {
        return None;
    }
    Some((val, &z[digits..]))
}

fn set_to_current(p: &mut DateTime) {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    const UNIX_EPOCH_IJD: i64 = 210866760000000;
    p.i_jd = UNIX_EPOCH_IJD + duration.as_millis() as i64;
    p.valid_jd = true;
    p.is_utc = true;
    p.is_local = false;
    p.clear_ymd_hms_tz();
}

fn parse_date_or_time(value: &str, p: &mut DateTime) -> Result<()> {
    if parse_yyyy_mm_dd(value, p) {
        return Ok(());
    }
    if parse_hh_mm_ss(value, p) {
        return Ok(());
    }
    if value.eq_ignore_ascii_case("now") {
        set_to_current(p);
        return Ok(());
    }
    let numeric_value = value.trim_matches(|c: char| c.is_ascii_whitespace());
    if let Ok(val) = numeric_value.parse::<f64>() {
        p.s = val;
        p.raw_s = true;
        if (0.0..5373484.5).contains(&val) {
            p.i_jd = (val * JD_TO_MS as f64 + 0.5) as i64;
            p.valid_jd = true;
        }
        return Ok(());
    }
    if value.eq_ignore_ascii_case("subsec") || value.eq_ignore_ascii_case("subsecond") {
        p.use_subsec = true;
        set_to_current(p);
        return Ok(());
    }
    Err(crate::LimboError::InvalidModifier("Parse Failed".into()))
}

fn parse_yyyy_mm_dd(mut z: &str, p: &mut DateTime) -> bool {
    let y: i32;
    let m: i32;
    let d: i32;
    let neg: bool;

    if z.starts_with('-') {
        z = &z[1..];
        neg = true;
    } else {
        neg = false;
    }

    if let Some((val, rem)) = get_digits(z, 4, 0, 9999) {
        y = val;
        z = rem;
    } else {
        return false;
    }

    if !z.starts_with('-') {
        return false;
    }
    z = &z[1..];

    if let Some((val, rem)) = get_digits(z, 2, 1, 12) {
        m = val;
        z = rem;
    } else {
        return false;
    }

    if !z.starts_with('-') {
        return false;
    }
    z = &z[1..];

    if let Some((val, rem)) = get_digits(z, 2, 1, 31) {
        d = val;
        z = rem;
    } else {
        return false;
    }

    while !z.is_empty() {
        let c = z.as_bytes()[0] as char;
        if c.is_ascii_whitespace() || c == 'T' {
            z = &z[1..];
        } else {
            break;
        }
    }

    if parse_hh_mm_ss(z, p) {
    } else if z.is_empty() {
        p.valid_hms = false;
    } else {
        return false;
    }

    p.valid_jd = false;
    p.valid_ymd = true;
    p.y = if neg { -y } else { y };
    p.m = m;
    p.d = d;

    p.compute_floor();

    if p.tz != 0 {
        p.compute_jd();
    }
    true
}

fn parse_hh_mm_ss(mut z: &str, p: &mut DateTime) -> bool {
    let h: i32;
    let m: i32;
    let s: i32;
    let mut ms: f64 = 0.0;

    if let Some((val, rem)) = get_digits(z, 2, 0, 24) {
        h = val;
        z = rem;
    } else {
        return false;
    }

    if !z.starts_with(':') {
        return false;
    }
    z = &z[1..];

    if let Some((val, rem)) = get_digits(z, 2, 0, 59) {
        m = val;
        z = rem;
    } else {
        return false;
    }

    if z.starts_with(':') {
        z = &z[1..];

        if let Some((val, rem)) = get_digits(z, 2, 0, 59) {
            s = val;
            z = rem;
        } else {
            return false;
        }

        if z.starts_with('.') && z.len() > 1 && z.as_bytes()[1].is_ascii_digit() {
            let mut r_scale = 1.0;
            z = &z[1..]; // Skip '.'

            while !z.is_empty() && z.as_bytes()[0].is_ascii_digit() {
                let digit = (z.as_bytes()[0] - b'0') as f64;
                ms = ms * 10.0 + digit;
                r_scale *= 10.0;
                z = &z[1..];
            }
            ms /= r_scale;

            if ms > 0.999 {
                ms = 0.999;
            }
        }
    } else {
        s = 0;
    }

    p.valid_jd = false;
    p.raw_s = false;
    p.valid_hms = true;
    p.h = h;
    p.min = m;
    p.s = s as f64 + ms;

    if parse_timezone(z, p) {
        return false;
    }
    true
}

fn parse_timezone(mut z: &str, p: &mut DateTime) -> bool {
    while !z.is_empty() {
        let c = z.as_bytes()[0] as char;
        if c.is_ascii_whitespace() {
            z = &z[1..];
        } else {
            break;
        }
    }

    p.tz = 0;

    if z.is_empty() {
        return false;
    }

    let c = z.as_bytes()[0] as char;
    let sgn: i32;

    if c == '-' {
        sgn = -1;
    } else if c == '+' {
        sgn = 1;
    } else if c == 'Z' || c == 'z' {
        z = &z[1..];
        p.is_local = false;
        p.is_utc = true;
        return check_trailing_garbage(z);
    } else {
        return true;
    }

    z = &z[1..];

    let n_hr: i32;
    if let Some((val, rem)) = get_digits(z, 2, 0, 14) {
        n_hr = val;
        z = rem;
    } else {
        return true;
    }

    if !z.starts_with(':') {
        return true;
    }
    z = &z[1..];

    let n_mn: i32;
    if let Some((val, rem)) = get_digits(z, 2, 0, 59) {
        n_mn = val;
        z = rem;
    } else {
        return true;
    }

    p.tz = sgn * (n_mn + n_hr * 60);

    if p.tz == 0 {
        p.is_local = false;
        p.is_utc = true;
    }

    check_trailing_garbage(z)
}

// Helper to mimic the "zulu_time" label logic:
// while( sqlite3Isspace(*zDate) ){ zDate++; }
// return *zDate!=0;
fn check_trailing_garbage(mut z: &str) -> bool {
    while !z.is_empty() {
        let c = z.as_bytes()[0] as char;
        if c.is_ascii_whitespace() {
            z = &z[1..];
        } else {
            break;
        }
    }
    // Return true if garbage remains (Error), false if empty (Success)
    !z.is_empty()
}

fn auto_adjust_date(p: &mut DateTime) {
    if !p.raw_s || p.valid_jd {
        p.raw_s = false;
    } else if p.s >= -210866760000.0 && p.s <= 253402300799.0 {
        let r = p.s * 1000.0 + 210866760000000.0;
        p.i_jd = (r + 0.5) as i64;
        p.valid_jd = true;
        p.raw_s = false;
        p.clear_ymd_hms_tz();
    }
}

fn parse_modifier(p: &mut DateTime, z: &str, idx: usize) -> Result<()> {
    let mut chars = z.chars();
    let first_char = match chars.next() {
        Some(c) => c.to_ascii_lowercase(),
        None => return Err(InvalidModifier(format!("Unknown modifier: {z}"))),
    };

    match first_char {
        'a' if z.eq_ignore_ascii_case("auto") => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'auto' must be first: {z}"
                )));
            }
            auto_adjust_date(p);
            Ok(())
        }
        'c' if z.eq_ignore_ascii_case("ceiling") => {
            p.compute_jd();
            p.clear_ymd_hms_tz();
            p.n_floor = 0;
            Ok(())
        }
        'f' if z.eq_ignore_ascii_case("floor") => {
            p.compute_jd();
            if p.n_floor != 0 {
                p.i_jd -= p.n_floor as i64 * JD_TO_MS;
                p.n_floor = 0;
            }
            p.clear_ymd_hms_tz();
            Ok(())
        }
        'j' if z.eq_ignore_ascii_case("julianday") => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'julianday' must be first: {z}"
                )));
            }
            if p.valid_jd && p.raw_s {
                p.raw_s = false;
                Ok(())
            } else {
                Err(InvalidModifier(format!(
                    "Invalid use of julianday modifier: {z}"
                )))
            }
        }
        'l' if z.eq_ignore_ascii_case("localtime") => {
            if !p.is_local {
                p.compute_jd();
                let timestamp = (p.i_jd - 210866760000000) / 1000;
                let offset_sec = match Local.timestamp_opt(timestamp, 0) {
                    chrono::LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
                    _ => 0,
                };
                p.i_jd += (offset_sec as i64) * 1000;
                p.clear_ymd_hms_tz();
                p.is_local = true;
                p.is_utc = false;
            }
            Ok(())
        }
        'u' if z.eq_ignore_ascii_case("unixepoch") => {
            if idx > 0 {
                return Err(InvalidModifier(format!(
                    "Modifier 'unixepoch' must be first: {z}"
                )));
            }
            if p.raw_s {
                let r = p.s * 1000.0 + 210866760000000.0;
                // Range check before the cast, as SQLite's date.c does. `as i64`
                // saturates, so an out-of-range value would park i64::MIN in
                // `i_jd`, which 'localtime'/'utc' then subtract the epoch from.
                // The check in exec_datetime_general runs too late. Rejects NaN.
                if !(r >= 0.0 && r < (MAX_JD + 1) as f64) {
                    return Err(InvalidModifier(format!(
                        "Unixepoch value out of range: {z}"
                    )));
                }
                p.i_jd = (r + 0.5) as i64;
                p.valid_jd = true;
                p.raw_s = false;
                p.clear_ymd_hms_tz();
                Ok(())
            } else {
                Err(InvalidModifier(format!(
                    "Invalid use of unixepoch modifier: {z}"
                )))
            }
        }
        'u' if z.eq_ignore_ascii_case("utc") => {
            if !p.is_utc {
                p.compute_jd();
                let timestamp = (p.i_jd - 210866760000000) / 1000;
                let offset_sec = match Local.timestamp_opt(timestamp, 0) {
                    chrono::LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
                    _ => 0,
                };
                p.i_jd -= (offset_sec as i64) * 1000;
                p.clear_ymd_hms_tz();
                p.is_utc = true;
                p.is_local = false;
            }
            Ok(())
        }
        'w' if z
            .get(..8)
            .is_some_and(|s| s.eq_ignore_ascii_case("weekday ")) =>
        {
            if let Ok(val) = z[8..].trim().parse::<f64>() {
                if (0.0..7.0).contains(&val) && (val as i64 as f64) == val {
                    let n = val as i64;
                    p.compute_ymd_hms();
                    p.valid_jd = false;
                    p.compute_jd();
                    let mut z = ((p.i_jd + 129600000) / 86400000) % 7;
                    if z > n {
                        z -= 7;
                    }
                    p.i_jd += (n - z) * 86400000;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                }
            }
            Err(InvalidModifier(format!("Invalid weekday: {z}")))
        }
        's' => {
            if z.eq_ignore_ascii_case("subsec") || z.eq_ignore_ascii_case("subsecond") {
                p.use_subsec = true;
                Ok(())
            } else if z
                .get(..9)
                .is_some_and(|s| s.eq_ignore_ascii_case("start of "))
            {
                if !p.valid_jd && !p.valid_ymd && !p.valid_hms {
                    return Err(InvalidModifier(format!("Invalid start of: {z}")));
                }
                p.compute_ymd();
                p.valid_hms = true;
                p.h = 0;
                p.min = 0;
                p.s = 0.0;
                p.raw_s = false;
                p.valid_jd = false;
                p.tz = 0;
                p.n_floor = 0;

                let suffix = &z[9..];
                if suffix.eq_ignore_ascii_case("month") {
                    p.d = 1;
                    Ok(())
                } else if suffix.eq_ignore_ascii_case("year") {
                    p.m = 1;
                    p.d = 1;
                    Ok(())
                } else if suffix.eq_ignore_ascii_case("day") {
                    Ok(())
                } else {
                    Err(InvalidModifier(format!("Invalid start of: {z}")))
                }
            } else {
                Err(InvalidModifier(format!("Unknown modifier: {z}")))
            }
        }
        '+' | '-' | '0'..='9' => parse_arithmetic_modifier(p, z),
        _ => Err(InvalidModifier(format!("Unknown modifier: {z}"))),
    }
}

fn parse_arithmetic_modifier(p: &mut DateTime, z: &str) -> Result<()> {
    let z = z.trim();
    let is_neg = z.starts_with('-');
    let sign = if is_neg { -1 } else { 1 };

    let clean_z = if z.starts_with('+') || z.starts_with('-') {
        &z[1..]
    } else {
        z
    };

    // Case 1: YYYY-MM-DD Arithmetic
    if clean_z.len() >= 10
        && clean_z.as_bytes().get(4) == Some(&b'-')
        && clean_z.as_bytes().get(7) == Some(&b'-')
        && clean_z.is_char_boundary(4)
        && clean_z.is_char_boundary(5)
        && clean_z.is_char_boundary(7)
        && clean_z.is_char_boundary(8)
        && clean_z.is_char_boundary(10)
    {
        let y_res = get_digits(&clean_z[0..4], 4, 0, 9999);
        let m_res = get_digits(&clean_z[5..7], 2, 0, 11);
        let d_res = get_digits(&clean_z[8..10], 2, 0, 30);

        if let (Some((y, _)), Some((m, _)), Some((d, _))) = (y_res, m_res, d_res) {
            let rem = &clean_z[10..];
            let mut valid_format = true;
            let mut time_str = None;

            if !rem.is_empty() {
                if rem.starts_with(' ') {
                    time_str = Some(rem.trim_start());
                } else {
                    valid_format = false;
                }
            }

            if valid_format {
                p.compute_ymd_hms();
                p.valid_jd = false;

                let y_adj = y as i64;
                let m_adj = m as i64;
                let d_adj = d as i64;

                if is_neg {
                    p.y = p.y.wrapping_sub(y_adj as i32);
                    p.m = p.m.wrapping_sub(m_adj as i32);
                } else {
                    p.y = p.y.wrapping_add(y_adj as i32);
                    p.m = p.m.wrapping_add(m_adj as i32);
                }

                // Normalize months
                let m_current = p.m as i64;
                let x = if m_current > 0 {
                    (m_current - 1) / 12
                } else {
                    (m_current - 12) / 12
                };
                p.y = p.y.wrapping_add(x as i32);
                p.m = (m_current - x * 12) as i32;

                p.compute_floor();
                p.compute_jd();

                // Apply day offset
                let day_diff = if is_neg { -d_adj } else { d_adj };
                p.i_jd = p.i_jd.wrapping_add(day_diff.wrapping_mul(JD_TO_MS));

                // Apply time offset if present
                if let Some(t_val) = time_str {
                    let mut tx = DateTime::default();
                    if parse_hh_mm_ss(t_val, &mut tx) {
                        tx.compute_jd();
                        let ms = (tx.h as i64 * 3600000)
                            + (tx.min as i64 * 60000)
                            + (tx.s * 1000.0) as i64;
                        p.i_jd = p.i_jd.wrapping_add((sign as i64).wrapping_mul(ms));
                    } else {
                        // If time parsing failed, the whole modifier is invalid
                        return Err(InvalidModifier(format!(
                            "Invalid time in arithmetic modifier: {z}"
                        )));
                    }
                }

                p.clear_ymd_hms_tz();
                return Ok(());
            }
        }
    }

    // Case 2: HH:MM:SS Arithmetic
    if z.contains(':') {
        let mut tx = DateTime::default();
        let time_str = if z.starts_with('+') || z.starts_with('-') {
            &z[1..]
        } else {
            z
        };
        if parse_hh_mm_ss(time_str, &mut tx) {
            tx.compute_jd();
            let ms = (tx.h as i64 * 3600000) + (tx.min as i64 * 60000) + (tx.s * 1000.0) as i64;
            p.compute_jd();
            p.i_jd = p.i_jd.wrapping_add((sign as i64).wrapping_mul(ms));
            p.clear_ymd_hms_tz();
            return Ok(());
        }
    }

    // Case 3: NNN Units
    let mut parts = z.split_whitespace();
    if let Some(val_str) = parts.next() {
        if let Ok(val) = val_str.parse::<f64>() {
            if let Some(unit) = parts.next() {
                let limit_check = |v: f64, limit: f64| v.abs() < limit;
                if unit.eq_ignore_ascii_case("day") || unit.eq_ignore_ascii_case("days") {
                    if !limit_check(val, 5373485.0) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 86400000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("hour") || unit.eq_ignore_ascii_case("hours") {
                    if !limit_check(val, 1.2897e+11) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 3600000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("minute")
                    || unit.eq_ignore_ascii_case("minutes")
                {
                    if !limit_check(val, 7.7379e+12) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 60000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("second")
                    || unit.eq_ignore_ascii_case("seconds")
                {
                    if !limit_check(val, 4.6427e+14) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_jd();
                    let ms = val * 1000.0;
                    let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                    p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    p.n_floor = 0;
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("month") || unit.eq_ignore_ascii_case("months")
                {
                    if !limit_check(val, 176546.0) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_ymd_hms();
                    let int_months = val as i64;
                    let frac_months = val - int_months as f64;

                    let total_months = (p.m as i64) + int_months;
                    let x = if total_months > 0 {
                        (total_months - 1) / 12
                    } else {
                        (total_months - 12) / 12
                    };
                    p.y = p.y.wrapping_add(x as i32);
                    p.m = (total_months - x * 12) as i32;

                    p.compute_floor();
                    p.valid_jd = false;
                    p.compute_jd();

                    if frac_months.abs() > f64::EPSILON {
                        let ms = frac_months * 30.0 * JD_TO_MS as f64;
                        let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                        p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    }
                    p.clear_ymd_hms_tz();
                    return Ok(());
                } else if unit.eq_ignore_ascii_case("year") || unit.eq_ignore_ascii_case("years") {
                    if !limit_check(val, 14713.0) {
                        return Err(InvalidModifier(format!("Modifier out of range: {z}")));
                    }
                    p.compute_ymd_hms();
                    let int_years = val as i64;
                    let frac_years = val - int_years as f64;

                    p.y = p.y.wrapping_add(int_years as i32);

                    p.compute_floor();
                    p.valid_jd = false;
                    p.compute_jd();

                    if frac_years.abs() > f64::EPSILON {
                        let ms = frac_years * 365.0 * JD_TO_MS as f64;
                        let rounder = if ms < 0.0 { -0.5 } else { 0.5 };
                        p.i_jd = p.i_jd.wrapping_add((ms + rounder) as i64);
                    }
                    p.clear_ymd_hms_tz();
                    return Ok(());
                }
            }
        }
    }

    Err(InvalidModifier(format!("Invalid arithmetic modifier: {z}")))
}

pub fn exec_datetime_general<I, E, V>(values: I, func_type: &str) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    let mut p = DateTime::default();
    let mut has_modifier = false;

    if values.len() == 0 {
        set_to_current(&mut p);
    } else {
        let first = values.next().unwrap();
        match blob_as_text(first.as_value_ref()) {
            ValueRef::Text(s) => {
                if parse_date_or_time(s.as_str(), &mut p).is_err() {
                    return Value::Null;
                }
            }
            ValueRef::Numeric(Numeric::Integer(i)) => {
                p.s = i as f64;
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            ValueRef::Numeric(Numeric::Float(f)) => {
                p.s = f64::from(f);
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            _ => return Value::Null,
        }
    }

    for (i, val) in values.enumerate() {
        has_modifier = true;
        if let ValueRef::Text(s) = blob_as_text(val.as_value_ref()) {
            if parse_modifier(&mut p, s.as_str(), i).is_err() {
                return Value::Null;
            }
        } else {
            return Value::Null;
        }
    }

    p.compute_jd();
    if p.is_error || p.i_jd < 0 || p.i_jd > MAX_JD {
        return Value::Null;
    }

    if !has_modifier && p.valid_ymd && p.d > 28 {
        p.valid_ymd = false;
    }

    match func_type {
        "julianday" => Value::from_f64(p.i_jd as f64 / 86400000.0),
        "unixepoch" => {
            let unix = (p.i_jd - 210866760000000) as f64 / 1000.0;
            if p.use_subsec {
                Value::from_f64(unix)
            } else {
                Value::from_i64(unix.floor() as i64)
            }
        }
        _ => {
            p.compute_ymd_hms();
            if p.is_error {
                return Value::Null;
            }

            let mut res = String::new();
            if func_type == "date" {
                if p.y < 0 {
                    write!(res, "-{:04}-{:02}-{:02}", p.y.abs(), p.m, p.d).unwrap();
                } else {
                    write!(res, "{:04}-{:02}-{:02}", p.y, p.m, p.d).unwrap();
                }
            } else if func_type == "time" {
                write!(res, "{:02}:{:02}", p.h, p.min).unwrap();
                if p.use_subsec {
                    write!(res, ":{:06.3}", p.s).unwrap();
                } else {
                    write!(res, ":{:02}", p.s as i32).unwrap();
                }
            } else {
                if p.y < 0 {
                    write!(
                        res,
                        "-{:04}-{:02}-{:02} {:02}:{:02}",
                        p.y.abs(),
                        p.m,
                        p.d,
                        p.h,
                        p.min
                    )
                    .unwrap();
                } else {
                    write!(
                        res,
                        "{:04}-{:02}-{:02} {:02}:{:02}",
                        p.y, p.m, p.d, p.h, p.min
                    )
                    .unwrap();
                }

                if p.use_subsec {
                    write!(res, ":{:06.3}", p.s).unwrap();
                } else {
                    write!(res, ":{:02}", p.s as i32).unwrap();
                }
            }
            Value::from_text(res)
        }
    }
}

pub fn exec_date<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "date")
}

pub fn exec_time<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "time")
}

pub fn exec_datetime_full<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "datetime")
}

pub fn exec_julianday<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "julianday")
}

pub fn exec_unixepoch<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime_general(values, "unixepoch")
}

pub fn exec_timediff<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() < 2 {
        return Value::Null;
    }

    let mut d1 = DateTime::default();
    let mut d2 = DateTime::default();

    // Parse first argument (d1)
    let val1 = values.next().unwrap();
    match blob_as_text(val1.as_value_ref()) {
        ValueRef::Text(s) => {
            if parse_date_or_time(s.as_str(), &mut d1).is_err() {
                return Value::Null;
            }
        }
        ValueRef::Numeric(Numeric::Integer(i)) => {
            d1.s = i as f64;
            d1.raw_s = true;
            if d1.s >= 0.0 && d1.s < 5373484.5 {
                d1.i_jd = (d1.s * JD_TO_MS as f64 + 0.5) as i64;
                d1.valid_jd = true;
            }
        }
        ValueRef::Numeric(Numeric::Float(f)) => {
            d1.s = f64::from(f);
            d1.raw_s = true;
            if d1.s >= 0.0 && d1.s < 5373484.5 {
                d1.i_jd = (d1.s * JD_TO_MS as f64 + 0.5) as i64;
                d1.valid_jd = true;
            }
        }
        _ => return Value::Null,
    }

    // Parse second argument (d2)
    let val2 = values.next().unwrap();
    match blob_as_text(val2.as_value_ref()) {
        ValueRef::Text(s) => {
            if parse_date_or_time(s.as_str(), &mut d2).is_err() {
                return Value::Null;
            }
        }
        ValueRef::Numeric(Numeric::Integer(i)) => {
            d2.s = i as f64;
            d2.raw_s = true;
            if d2.s >= 0.0 && d2.s < 5373484.5 {
                d2.i_jd = (d2.s * JD_TO_MS as f64 + 0.5) as i64;
                d2.valid_jd = true;
            }
        }
        ValueRef::Numeric(Numeric::Float(f)) => {
            d2.s = f64::from(f);
            d2.raw_s = true;
            if d2.s >= 0.0 && d2.s < 5373484.5 {
                d2.i_jd = (d2.s * JD_TO_MS as f64 + 0.5) as i64;
                d2.valid_jd = true;
            }
        }
        _ => return Value::Null,
    }

    d1.compute_jd();
    d2.compute_jd();

    // Validate inputs after computation
    if d1.is_error || d2.is_error {
        return Value::Null;
    }

    d1.compute_ymd_hms();
    d2.compute_ymd_hms();

    // Month arithmetic is not symmetric: adding a month clamps a day-of-month
    // overflow forward, so subtracting a month is not the inverse of adding
    // one. To keep datetime(B, timediff(A, B)) == datetime(A), the Y/M shift
    // must be applied to the second argument (d2) in the same direction that
    // the resulting modifier will be applied, exactly as SQLite does.
    let sign: char;
    let mut y: i32;
    let mut m: i32;
    let diff_ms: i64;
    if d1.i_jd >= d2.i_jd {
        sign = '+';
        y = d1.y - d2.y;
        if y != 0 {
            d2.y = d1.y;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        m = d1.m - d2.m;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        if m != 0 {
            d2.m = d1.m;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        // If shifting d2 forward by Y years and M months overshot d1, back
        // off one month at a time.
        while d1.i_jd < d2.i_jd {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            d2.m -= 1;
            if d2.m < 1 {
                d2.m = 12;
                d2.y -= 1;
            }
            d2.valid_jd = false;
            d2.compute_jd();
        }
        diff_ms = d1.i_jd - d2.i_jd;
    } else {
        sign = '-';
        y = d2.y - d1.y;
        if y != 0 {
            d2.y = d1.y;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        m = d2.m - d1.m;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        if m != 0 {
            d2.m = d1.m;
            d2.valid_jd = false;
            d2.compute_jd();
        }
        // If shifting d2 backward by Y years and M months overshot d1, move
        // forward one month at a time.
        while d1.i_jd > d2.i_jd {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            d2.m += 1;
            if d2.m > 12 {
                d2.m = 1;
                d2.y += 1;
            }
            d2.valid_jd = false;
            d2.compute_jd();
        }
        diff_ms = d2.i_jd - d1.i_jd;
    }
    let days = diff_ms / 86400000;
    let rem_ms = diff_ms % 86400000;
    let hours = rem_ms / 3600000;
    let rem_ms = rem_ms % 3600000;
    let mins = rem_ms / 60000;
    let rem_ms = rem_ms % 60000;
    let secs = rem_ms as f64 / 1000.0;

    let mut res = String::new();
    write!(
        res,
        "{sign}{y:04}-{m:02}-{days:02} {hours:02}:{mins:02}:{secs:06.3}"
    )
    .unwrap();

    Value::from_text(res)
}

pub fn exec_strftime<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() < 1 {
        return Value::Null;
    }

    let fmt_val = values.next().unwrap();
    let fmt_str = match fmt_val.as_value_ref() {
        ValueRef::Text(s) => Cow::Borrowed(s.as_str()),
        ValueRef::Null => return Value::Null,
        val => Cow::Owned(val.to_string()),
    };

    let mut p = DateTime::default();
    if values.len() == 0 {
        set_to_current(&mut p);
    } else {
        let init_val = values.next().unwrap();
        match blob_as_text(init_val.as_value_ref()) {
            ValueRef::Text(s) => {
                let s_str = s.as_str();
                if s_str.eq_ignore_ascii_case("now") {
                    set_to_current(&mut p);
                } else if let Ok(val) = s_str.parse::<f64>() {
                    p.s = val;
                    p.raw_s = true;
                    if p.s >= 0.0 && p.s < 5373484.5 {
                        p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                        p.valid_jd = true;
                    }
                } else {
                    let mut temp_p = DateTime::default();
                    if parse_date_or_time(s_str, &mut temp_p).is_ok() {
                        p = temp_p;
                    } else {
                        return Value::Null;
                    }
                }
            }
            ValueRef::Numeric(Numeric::Integer(i)) => {
                p.s = i as f64;
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            ValueRef::Numeric(Numeric::Float(f)) => {
                p.s = f64::from(f);
                p.raw_s = true;
                if p.s >= 0.0 && p.s < 5373484.5 {
                    p.i_jd = (p.s * JD_TO_MS as f64 + 0.5) as i64;
                    p.valid_jd = true;
                }
            }
            _ => return Value::Null,
        }

        for (i, val) in values.enumerate() {
            if let ValueRef::Text(s) = blob_as_text(val.as_value_ref()) {
                if parse_modifier(&mut p, s.as_str(), i).is_err() {
                    return Value::Null;
                }
            } else {
                return Value::Null;
            }
        }
    }

    p.compute_jd();
    if p.is_error {
        return Value::Null;
    }

    p.compute_ymd_hms();

    let mut res = String::new();
    let mut chars = fmt_str.chars().peekable();

    let days_after_jan1 = |curr: &DateTime| -> i64 {
        let jan1 = DateTime {
            y: curr.y,
            m: 1,
            d: 1,
            valid_ymd: true,
            ..Default::default()
        };
        let mut j1 = jan1;
        j1.compute_jd();
        let curr_norm = DateTime {
            y: curr.y,
            m: curr.m,
            d: curr.d,
            valid_ymd: true,
            ..Default::default()
        };
        let mut c1 = curr_norm;
        c1.compute_jd();
        (c1.i_jd - j1.i_jd) / JD_TO_MS
    };

    let days_after_mon = |curr: &DateTime| -> i64 { ((curr.i_jd + 43200000) / JD_TO_MS) % 7 };
    let days_after_sun = |curr: &DateTime| -> i64 { ((curr.i_jd + 129600000) / JD_TO_MS) % 7 };

    while let Some(c) = chars.next() {
        if c != '%' {
            res.push(c);
            continue;
        }

        match chars.next() {
            Some('d') => write!(res, "{:02}", p.d).unwrap(),
            Some('e') => write!(res, "{:2}", p.d).unwrap(),
            Some('F') => write!(res, "{:04}-{:02}-{:02}", p.y, p.m, p.d).unwrap(),
            Some('f') => {
                let mut s = p.s;
                if s > 59.999 {
                    s = 59.999;
                }
                write!(res, "{s:06.3}").unwrap()
            }
            Some('g') => {
                let mut y_iso = p;
                y_iso.i_jd += (3 - days_after_mon(&p)) * 86400000;
                y_iso.valid_ymd = false;
                y_iso.compute_ymd();
                write!(res, "{:02}", y_iso.y % 100).unwrap();
            }
            Some('G') => {
                let mut y_iso = p;
                y_iso.i_jd += (3 - days_after_mon(&p)) * 86400000;
                y_iso.valid_ymd = false;
                y_iso.compute_ymd();
                write!(res, "{:04}", y_iso.y).unwrap();
            }
            Some('H') => write!(res, "{:02}", p.h).unwrap(),
            Some('I') => {
                let h = if p.h % 12 == 0 { 12 } else { p.h % 12 };
                write!(res, "{h:02}").unwrap();
            }
            Some('j') => {
                write!(res, "{:03}", days_after_jan1(&p) + 1).unwrap();
            }
            Some('J') => {
                let val = p.i_jd as f64 / 86400000.0;
                if val.abs() >= 1_000_000.0 && val.abs() < 10_000_000.0 {
                    let s = format!("{val:.9}");
                    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
                    write!(res, "{trimmed}").unwrap();
                } else {
                    write!(res, "{val}").unwrap();
                }
            }
            Some('k') => write!(res, "{:2}", p.h).unwrap(),
            Some('l') => {
                let h = if p.h % 12 == 0 { 12 } else { p.h % 12 };
                write!(res, "{h:2}").unwrap();
            }
            Some('m') => write!(res, "{:02}", p.m).unwrap(),
            Some('M') => write!(res, "{:02}", p.min).unwrap(),
            Some('p') => write!(res, "{}", if p.h >= 12 { "PM" } else { "AM" }).unwrap(),
            Some('P') => write!(res, "{}", if p.h >= 12 { "pm" } else { "am" }).unwrap(),
            Some('R') => write!(res, "{:02}:{:02}", p.h, p.min).unwrap(),
            Some('s') => {
                let s = (p.i_jd - 210866760000000) as f64 / 1000.0;
                if p.use_subsec {
                    write!(res, "{s:.3}").unwrap();
                } else {
                    write!(res, "{}", s.floor()).unwrap();
                }
            }
            Some('S') => write!(res, "{:02}", p.s as i32).unwrap(),
            Some('T') => write!(res, "{:02}:{:02}:{:02}", p.h, p.min, p.s as i32).unwrap(),
            Some('u') => {
                let mut w = days_after_sun(&p);
                if w == 0 {
                    w = 7;
                }
                write!(res, "{w}").unwrap();
            }
            Some('U') => {
                let w = (days_after_jan1(&p) - days_after_sun(&p) + 7) / 7;
                write!(res, "{w:02}").unwrap();
            }
            Some('V') => {
                let mut temp = p;
                temp.i_jd += (3 - days_after_mon(&p)) * 86400000;
                temp.valid_ymd = false;
                temp.compute_ymd();
                let w = days_after_jan1(&temp) / 7 + 1;
                write!(res, "{w:02}").unwrap();
            }
            Some('w') => {
                write!(res, "{}", days_after_sun(&p)).unwrap();
            }
            Some('W') => {
                let w = (days_after_jan1(&p) - days_after_mon(&p) + 7) / 7;
                write!(res, "{w:02}").unwrap();
            }
            Some('Y') => write!(res, "{:04}", p.y).unwrap(),
            Some('%') => res.push('%'),
            _ => return Value::Null,
        }
    }

    Value::from_text(res)
}

/// SQLite reads date/time arguments with sqlite3_value_text(), which hands back a
/// BLOB's bytes unchanged, so a blob holding '2024-01-01' parses like that text.
/// Bytes that are not UTF-8 stay a blob and the caller rejects them.
fn blob_as_text(value: ValueRef<'_>) -> ValueRef<'_> {
    let ValueRef::Blob(bytes) = value else {
        return value;
    };
    match std::str::from_utf8(bytes) {
        Ok(text) => ValueRef::Text(TextRef::new(text, TextSubtype::Text)),
        Err(_) => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_get_date_from_time_value() {
        let now = chrono::Local::now().to_utc().format("%Y-%m-%d").to_string();

        let prev_date_str = "2024-07-20";
        let test_date_str = "2024-07-21";
        let next_date_str = "2024-07-22";

        let test_cases: Vec<(Value, &str)> = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (Value::build_text("2024-07-21"), test_date_str),
            // Format 2: YYYY-MM-DD HH:MM
            (Value::build_text("2024-07-21 22:30"), test_date_str),
            (Value::build_text("2024-07-21 22:30+02:00"), test_date_str),
            (Value::build_text("2024-07-21 22:30-05:00"), next_date_str),
            (Value::build_text("2024-07-21 01:30+05:00"), prev_date_str),
            (Value::build_text("2024-07-21 22:30Z"), test_date_str),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (Value::build_text("2024-07-21 22:30:45"), test_date_str),
            (
                Value::build_text("2024-07-21 22:30:45+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21 01:30:45+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21 22:30:45Z"), test_date_str),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (Value::build_text("2024-07-21 22:30:45.123"), test_date_str),
            (
                Value::build_text("2024-07-21 22:30:45.123+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45.123-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21 01:30:45.123+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21 22:30:45.123Z"), test_date_str),
            // Format 5: YYYY-MM-DDTHH:MM
            (Value::build_text("2024-07-21T22:30"), test_date_str),
            (Value::build_text("2024-07-21T22:30+02:00"), test_date_str),
            (Value::build_text("2024-07-21T22:30-05:00"), next_date_str),
            (Value::build_text("2024-07-21T01:30+05:00"), prev_date_str),
            (Value::build_text("2024-07-21T22:30Z"), test_date_str),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (Value::build_text("2024-07-21T22:30:45"), test_date_str),
            (
                Value::build_text("2024-07-21T22:30:45+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21T01:30:45+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21T22:30:45Z"), test_date_str),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (Value::build_text("2024-07-21T22:30:45.123"), test_date_str),
            (
                Value::build_text("2024-07-21T22:30:45.123+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45.123-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21T01:30:45.123+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21T22:30:45.123Z"), test_date_str),
            // Format 8: HH:MM
            (Value::build_text("22:30"), "2000-01-01"),
            (Value::build_text("22:30+02:00"), "2000-01-01"),
            (Value::build_text("22:30-05:00"), "2000-01-02"),
            (Value::build_text("01:30+05:00"), "1999-12-31"),
            (Value::build_text("22:30Z"), "2000-01-01"),
            // Format 9: HH:MM:SS
            (Value::build_text("22:30:45"), "2000-01-01"),
            (Value::build_text("22:30:45+02:00"), "2000-01-01"),
            (Value::build_text("22:30:45-05:00"), "2000-01-02"),
            (Value::build_text("01:30:45+05:00"), "1999-12-31"),
            (Value::build_text("22:30:45Z"), "2000-01-01"),
            // Format 10: HH:MM:SS.SSS
            (Value::build_text("22:30:45.123"), "2000-01-01"),
            (Value::build_text("22:30:45.123+02:00"), "2000-01-01"),
            (Value::build_text("22:30:45.123-05:00"), "2000-01-02"),
            (Value::build_text("01:30:45.123+05:00"), "1999-12-31"),
            (Value::build_text("22:30:45.123Z"), "2000-01-01"),
            // Test Format 11: 'now'
            (Value::build_text("now"), &now),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (Value::from_f64(2460512.5), test_date_str),
            (Value::from_i64(2460513), test_date_str),
        ];

        for (input, expected) in test_cases {
            let result = exec_date(&[input.clone()]);
            assert_eq!(
                result,
                Value::build_text(expected.to_string()),
                "Failed for input: {input:?}"
            );
        }
    }

    #[test]
    fn test_invalid_get_date_from_time_value() {
        let invalid_cases = vec![
            Value::build_text("2024-07-21 25:00"),    // Invalid hour
            Value::build_text("2024-07-21 25:00:00"), // Invalid hour
            Value::build_text("2024-07-21 23:60:00"), // Invalid minute
            Value::build_text("2024-07-21 22:58:60"), // Invalid second
            // Note: Invalid days now overflow like SQLite (2024-07-32 -> 2024-08-01)
            Value::build_text("2024-13-01"),          // Invalid month
            Value::build_text("invalid_date"),        // Completely invalid string
            Value::build_text(""),                    // Empty string
            Value::from_i64(i64::MAX),                // Large Julian day
            Value::from_i64(-1),                      // Negative Julian day
            Value::from_f64(f64::MAX),                // Large float
            Value::from_f64(-1.0),                    // Negative Julian day as float
            Value::from_f64(f64::NAN),                // NaN
            Value::from_f64(f64::INFINITY),           // Infinity
            Value::Null,                              // Null value
            Value::Blob(crate::alloc::vec![1, 2, 3]), // Blob whose bytes are not a date
            // Invalid timezone tests
            Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
            Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
            Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
            Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
            Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
            Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
            Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
            Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
        ];

        for case in invalid_cases.iter() {
            let result = exec_date([case]);
            assert_eq!(result, Value::Null);
        }
    }

    #[test]
    fn test_valid_get_time_from_datetime_value() {
        let test_time_str = "22:30:45";
        let prev_time_str = "20:30:45";
        let next_time_str = "03:30:45";

        let test_cases = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (Value::build_text("2024-07-21"), "00:00:00"),
            // Format 2: YYYY-MM-DD HH:MM
            (Value::build_text("2024-07-21 22:30"), "22:30:00"),
            (Value::build_text("2024-07-21 22:30+02:00"), "20:30:00"),
            (Value::build_text("2024-07-21 22:30-05:00"), "03:30:00"),
            (Value::build_text("2024-07-21 22:30Z"), "22:30:00"),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (Value::build_text("2024-07-21 22:30:45"), test_time_str),
            (
                Value::build_text("2024-07-21 22:30:45+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21 22:30:45Z"), test_time_str),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (Value::build_text("2024-07-21 22:30:45.123"), test_time_str),
            (
                Value::build_text("2024-07-21 22:30:45.123+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45.123-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21 22:30:45.123Z"), test_time_str),
            // Format 5: YYYY-MM-DDTHH:MM
            (Value::build_text("2024-07-21T22:30"), "22:30:00"),
            (Value::build_text("2024-07-21T22:30+02:00"), "20:30:00"),
            (Value::build_text("2024-07-21T22:30-05:00"), "03:30:00"),
            (Value::build_text("2024-07-21T22:30Z"), "22:30:00"),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (Value::build_text("2024-07-21T22:30:45"), test_time_str),
            (
                Value::build_text("2024-07-21T22:30:45+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21T22:30:45Z"), test_time_str),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (Value::build_text("2024-07-21T22:30:45.123"), test_time_str),
            (
                Value::build_text("2024-07-21T22:30:45.123+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45.123-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21T22:30:45.123Z"), test_time_str),
            // Format 8: HH:MM
            (Value::build_text("22:30"), "22:30:00"),
            (Value::build_text("22:30+02:00"), "20:30:00"),
            (Value::build_text("22:30-05:00"), "03:30:00"),
            (Value::build_text("22:30Z"), "22:30:00"),
            // Format 9: HH:MM:SS
            (Value::build_text("22:30:45"), test_time_str),
            (Value::build_text("22:30:45+02:00"), prev_time_str),
            (Value::build_text("22:30:45-05:00"), next_time_str),
            (Value::build_text("22:30:45Z"), test_time_str),
            // Format 10: HH:MM:SS.SSS
            (Value::build_text("22:30:45.123"), test_time_str),
            (Value::build_text("22:30:45.123+02:00"), prev_time_str),
            (Value::build_text("22:30:45.123-05:00"), next_time_str),
            (Value::build_text("22:30:45.123Z"), test_time_str),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (Value::from_f64(2460082.1), "14:24:00"),
            (Value::from_i64(2460082), "12:00:00"),
        ];

        for (input, expected) in test_cases {
            let result = exec_time(&[input]);
            if let Value::Text(result_str) = result {
                assert_eq!(result_str.as_str(), expected);
            } else {
                panic!("Expected Value::Text, but got: {result:?}");
            }
        }
    }

    #[test]
    fn test_invalid_get_time_from_datetime_value() {
        let invalid_cases = vec![
            Value::build_text("2024-07-21 25:00"),    // Invalid hour
            Value::build_text("2024-07-21 25:00:00"), // Invalid hour
            Value::build_text("2024-07-21 23:60:00"), // Invalid minute
            Value::build_text("2024-07-21 22:58:60"), // Invalid second
            // Note: Invalid days now overflow like SQLite (2024-07-32 -> 2024-08-01)
            Value::build_text("2024-13-01"),          // Invalid month
            Value::build_text("invalid_date"),        // Completely invalid string
            Value::build_text(""),                    // Empty string
            Value::from_i64(i64::MAX),                // Large Julian day
            Value::from_i64(-1),                      // Negative Julian day
            Value::from_f64(f64::MAX),                // Large float
            Value::from_f64(-1.0),                    // Negative Julian day as float
            Value::from_f64(f64::NAN),                // NaN
            Value::from_f64(f64::INFINITY),           // Infinity
            Value::Null,                              // Null value
            Value::Blob(crate::alloc::vec![1, 2, 3]), // Blob whose bytes are not a date
            // Invalid timezone tests
            Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
            Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
            Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
            Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
            Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
            Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
            Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
            Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
            // Unsupported date format tests
            Value::build_text("2024/07/21"),
            Value::build_text("2024.07.21"),
            Value::build_text("07/21/2024"),
            Value::build_text("21/07/2024"),
        ];

        for case in invalid_cases {
            let result = exec_time(&[case.clone()]);
            assert_eq!(result, Value::Null);
        }
    }

    #[test]
    fn test_parse_modifier_overflow() {
        let modifiers = [
            "1e308 days",
            "1e308 hours",
            "1e308 minutes",
            "1e308 seconds",
            "1e308 months",
            "1e308 years",
            "-1e308 days",
            "-1e308 hours",
            "-1e308 minutes",
            "-1e308 seconds",
            "-1e308 months",
            "-1e308 years",
            "1e309 days",
            "1e309 hours",
            "1e309 minutes",
            "1e309 seconds",
            "1e309 months",
            "1e309 years",
        ];

        for modifier in modifiers {
            assert_eq!(
                exec_datetime_full(&[Value::build_text("now"), Value::build_text(modifier),]),
                Value::Null,
                "modifier: {modifier}"
            );
        }
    }

    #[test]
    fn test_parse_days() {
        let get_days = |s: &str| -> f64 {
            let mut p = DateTime::default();
            p.compute_jd();
            let start_jd = p.i_jd;
            parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
            (p.i_jd - start_jd) as f64 / 86_400_000.0
        };

        assert_eq!(get_days("5 days"), 5.0);
        assert_eq!(get_days("-3 days"), -3.0);
        assert_eq!(get_days("+2 days"), 2.0);
        assert_eq!(get_days("4  days"), 4.0);
        assert_eq!(get_days("6   DAYS"), 6.0);
        assert_eq!(get_days("+5  DAYS"), 5.0);
        // Fractional days
        assert_eq!(get_days("1.5 days"), 1.5);
        assert_eq!(get_days("-0.25 days"), -0.25);
    }

    #[test]
    fn test_parse_hours() {
        let get_hours = |s: &str| -> f64 {
            let mut p = DateTime::default();
            p.compute_jd();
            let start_jd = p.i_jd;
            parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
            (p.i_jd - start_jd) as f64 / 3_600_000.0
        };

        assert_eq!(get_hours("12 hours"), 12.0);
        assert_eq!(get_hours("-2 hours"), -2.0);
        assert_eq!(get_hours("+3  HOURS"), 3.0);
        // Fractional hours
        assert_eq!(get_hours("0.5 hours"), 0.5);
    }

    #[test]
    fn test_parse_minutes() {
        let get_minutes = |s: &str| -> f64 {
            let mut p = DateTime::default();
            p.compute_jd();
            let start_jd = p.i_jd;
            parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
            (p.i_jd - start_jd) as f64 / 60_000.0
        };

        assert_eq!(get_minutes("30 minutes"), 30.0);
        assert_eq!(get_minutes("-15 minutes"), -15.0);
        assert_eq!(get_minutes("+45  MINUTES"), 45.0);
    }

    #[test]
    fn test_parse_seconds() {
        let get_seconds = |s: &str| -> f64 {
            let mut p = DateTime::default();
            p.compute_jd();
            let start_jd = p.i_jd;
            parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
            (p.i_jd - start_jd) as f64 / 1000.0
        };

        assert_eq!(get_seconds("45 seconds"), 45.0);
        assert_eq!(get_seconds("-10 seconds"), -10.0);
        assert_eq!(get_seconds("+20  SECONDS"), 20.0);
    }

    #[test]
    fn test_parse_months() {
        let get_months = |s: &str| -> f64 {
            let mut p = DateTime::default();
            let start_y = p.y;
            let start_m = p.m;
            parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
            ((p.y - start_y) * 12 + (p.m - start_m)) as f64
        };

        assert_eq!(get_months("3 months"), 3.0);
        assert_eq!(get_months("-1 months"), -1.0);
        assert_eq!(get_months("+6  MONTHS"), 6.0);
    }

    #[test]
    fn test_parse_years() {
        let get_years = |s: &str| -> f64 {
            let mut p = DateTime::default();
            let start_y = p.y;
            parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
            (p.y - start_y) as f64
        };

        assert_eq!(get_years("2 years"), 2.0);
        assert_eq!(get_years("-1 years"), -1.0);
        assert_eq!(get_years("+10  YEARS"), 10.0);
    }

    #[test]
    fn test_parse_time_offset() {
        let get_ms_change = |s: &str| -> i64 {
            let mut p = DateTime::default();
            p.compute_jd();
            let start_jd = p.i_jd;
            parse_modifier(&mut p, s, 1).expect("Failed to parse modifier");
            p.i_jd - start_jd
        };

        // +01:30 = 90 mins = 5,400,000 ms
        assert_eq!(get_ms_change("+01:30"), 5_400_000);
        // -00:45 = -45 mins = -2,700,000 ms
        assert_eq!(get_ms_change("-00:45"), -2_700_000);
        // +02:15:30 = 8,130,000 ms
        assert_eq!(get_ms_change("+02:15:30"), 8_130_000);
        // +02:15:30.250 = 8,130,250 ms
        assert_eq!(get_ms_change("+02:15:30.250"), 8_130_250);
    }
    #[test]
    fn test_parse_date_offset() {
        let run = |modifier: &str| -> String {
            let args = vec![
                Value::build_text("2000-01-01 00:00:00".to_string()),
                Value::build_text(modifier.to_string()),
            ];
            let val = exec_datetime_full(args);
            val.to_text().unwrap().to_string()
        };

        assert_eq!(run("+2023-05-15"), "4023-06-16 00:00:00");
        assert_eq!(run("-2023-05-15"), "-0024-07-17 00:00:00");
    }

    #[test]
    fn test_parse_date_time_offset() {
        let run = |modifier: &str| -> String {
            let args = vec![
                Value::build_text("2000-01-01 00:00:00".to_string()),
                Value::build_text(modifier.to_string()),
            ];
            let val = exec_datetime_full(args);
            val.to_text().unwrap().to_string()
        };

        assert_eq!(run("+2023-05-15 14:30"), "4023-06-16 14:30:00");
        assert_eq!(run("-0001-05-15 14:30"), "1998-07-16 09:30:00");
    }
    #[test]
    fn test_time_offset_boundaries() {
        let valid = ["+24:59", "-24:59"];

        for modifier in valid {
            assert_ne!(
                exec_datetime_full(&[Value::build_text("now"), Value::build_text(modifier),]),
                Value::Null,
                "modifier: {modifier}"
            );
        }

        let invalid = ["+25:00", "+25:01", "-25:00", "-25:01"];

        for modifier in invalid {
            assert_eq!(
                exec_datetime_full(&[Value::build_text("now"), Value::build_text(modifier),]),
                Value::Null,
                "modifier: {modifier}"
            );
        }
    }

    #[test]
    fn test_parse_start_of() {
        let run = |start: &str, modifier: &str| -> String {
            let args = vec![
                Value::build_text(start.to_string()),
                Value::build_text(modifier.to_string()),
            ];
            let val = exec_datetime_full(args);
            val.to_text().unwrap().to_string()
        };

        let base = "2023-06-15 12:30:45";
        assert_eq!(run(base, "start of month"), "2023-06-01 00:00:00");
        assert_eq!(run(base, "START OF MONTH"), "2023-06-01 00:00:00");
        assert_eq!(run(base, "start of year"), "2023-01-01 00:00:00");
        assert_eq!(run(base, "START OF YEAR"), "2023-01-01 00:00:00");
        assert_eq!(run(base, "start of day"), "2023-06-15 00:00:00");
        assert_eq!(run(base, "START OF DAY"), "2023-06-15 00:00:00");
    }

    #[test]
    fn test_invalid_end_of_modifiers() {
        let modifiers = [
            "end of month",
            "END OF MONTH",
            "end of year",
            "END OF YEAR",
            "end of day",
            "END OF DAY",
        ];

        for modifier in modifiers {
            let result =
                exec_datetime_full(&[Value::build_text("2023-06-15"), Value::build_text(modifier)]);

            assert_eq!(result, Value::Null, "modifier: {modifier}");
        }
    }

    #[test]
    fn test_parse_weekday() {
        let run = |start: &str, modifier: &str| -> String {
            let args = vec![
                Value::build_text(start.to_string()),
                Value::build_text(modifier.to_string()),
            ];
            let val = exec_date(args);
            val.to_text().unwrap().to_string()
        };

        // 2023-01-01 was a Sunday (0)
        assert_eq!(run("2023-01-01", "weekday 0"), "2023-01-01"); // No change
        assert_eq!(run("2023-01-01", "weekday 1"), "2023-01-02"); // Next Monday
        assert_eq!(run("2023-01-01", "WEEKDAY 6"), "2023-01-07"); // Next Saturday
    }

    #[test]
    fn test_parse_ceiling_modifier() {
        let mut p = DateTime::default();
        assert!(parse_modifier(&mut p, "ceiling", 1).is_ok());
        assert!(parse_modifier(&mut p, "CEILING", 1).is_ok());
    }

    #[test]
    fn test_parse_other_modifiers() {
        // Setup state for modifiers that require specific preconditions
        let mut p = DateTime {
            valid_jd: true,
            raw_s: true,
            ..DateTime::default()
        };

        // Modifiers that should just parse OK
        assert!(parse_modifier(&mut p, "localtime", 1).is_ok());
        assert!(parse_modifier(&mut p, "LOCALTIME", 1).is_ok());
        assert!(parse_modifier(&mut p, "utc", 1).is_ok());
        assert!(parse_modifier(&mut p, "UTC", 1).is_ok());
        assert!(parse_modifier(&mut p, "subsec", 1).is_ok());
        assert!(parse_modifier(&mut p, "SUBSEC", 1).is_ok());
        assert!(parse_modifier(&mut p, "subsecond", 1).is_ok());
        assert!(parse_modifier(&mut p, "SUBSECOND", 1).is_ok());

        // These must be at index 0 to parse validly
        assert!(parse_modifier(&mut p, "unixepoch", 0).is_ok());
        p.raw_s = true;
        assert!(parse_modifier(&mut p, "UNIXEPOCH", 0).is_ok());
        p.raw_s = true;
        assert!(parse_modifier(&mut p, "julianday", 0).is_ok());
        p.raw_s = true;
        assert!(parse_modifier(&mut p, "JULIANDAY", 0).is_ok());
        p.raw_s = true;
        assert!(parse_modifier(&mut p, "auto", 0).is_ok());
        p.raw_s = true;
        assert!(parse_modifier(&mut p, "AUTO", 0).is_ok());
    }

    #[test]
    fn test_parse_invalid_modifier() {
        let mut p = DateTime::default();
        assert!(parse_modifier(&mut p, "invalid modifier", 1).is_err());
        assert!(parse_modifier(&mut p, "5", 1).is_err());
        assert!(parse_modifier(&mut p, "days", 1).is_err());
        assert!(parse_modifier(&mut p, "++5 days", 1).is_err());
        assert!(parse_modifier(&mut p, "weekday 7", 1).is_err());
    }

    #[test]
    fn test_apply_modifier_days() {
        let run = |mod_str: &str| -> String {
            let args = vec![
                Value::build_text("2023-06-15 12:30:45".to_string()),
                Value::build_text(mod_str.to_string()),
            ];
            exec_datetime_full(args).to_text().unwrap().to_string()
        };

        assert_eq!(run("5 days"), "2023-06-20 12:30:45");
        assert_eq!(run("-3 days"), "2023-06-12 12:30:45");
    }

    #[test]
    fn test_apply_modifier_hours() {
        let run = |mod_str: &str| -> String {
            let args = vec![
                Value::build_text("2023-06-15 12:30:45".to_string()),
                Value::build_text(mod_str.to_string()),
            ];
            exec_datetime_full(args).to_text().unwrap().to_string()
        };

        assert_eq!(run("6 hours"), "2023-06-15 18:30:45");
        assert_eq!(run("-2 hours"), "2023-06-15 10:30:45");
    }

    #[test]
    fn test_apply_modifier_minutes() {
        let run = |mod_str: &str| -> String {
            let args = vec![
                Value::build_text("2023-06-15 12:30:45".to_string()),
                Value::build_text(mod_str.to_string()),
            ];
            exec_datetime_full(args).to_text().unwrap().to_string()
        };

        assert_eq!(run("45 minutes"), "2023-06-15 13:15:45");
        assert_eq!(run("-15 minutes"), "2023-06-15 12:15:45");
    }

    #[test]
    fn test_apply_modifier_seconds() {
        let run = |mod_str: &str| -> String {
            let args = vec![
                Value::build_text("2023-06-15 12:30:45".to_string()),
                Value::build_text(mod_str.to_string()),
            ];
            exec_datetime_full(args).to_text().unwrap().to_string()
        };

        assert_eq!(run("30 seconds"), "2023-06-15 12:31:15");
        assert_eq!(run("-20 seconds"), "2023-06-15 12:30:25");
    }
    #[test]
    fn test_datetime_boundary_arithmetic() {
        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("9999-12-31 23:59:59"),
                Value::build_text("+1 second"),
            ]),
            Value::Null
        );

        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("9999-12-31 23:59:59"),
                Value::build_text("+1 day"),
            ]),
            Value::Null
        );

        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("9999-12-31 23:59:59"),
                Value::build_text("+1 month"),
            ]),
            Value::Null
        );

        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("9999-12-31 23:59:59"),
                Value::build_text("+1 year"),
            ]),
            Value::Null
        );

        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("0000-01-01 00:00:00"),
                Value::build_text("-1 second"),
            ]),
            Value::build_text("-0001-12-31 23:59:59")
        );

        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("0000-01-01 00:00:00"),
                Value::build_text("-1 day"),
            ]),
            Value::build_text("-0001-12-31 00:00:00")
        );

        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("0000-01-01 00:00:00"),
                Value::build_text("-1 month"),
            ]),
            Value::build_text("-0001-12-01 00:00:00")
        );

        assert_eq!(
            exec_datetime_full(&[
                Value::build_text("0000-01-01 00:00:00"),
                Value::build_text("-1 year"),
            ]),
            Value::build_text("-0001-01-01 00:00:00")
        );
    }

    #[test]
    fn test_apply_modifier_time_offset() {
        let run = |mod_str: &str| -> String {
            let args = vec![
                Value::build_text("2023-06-15 12:30:45".to_string()),
                Value::build_text(mod_str.to_string()),
            ];
            exec_datetime_full(args).to_text().unwrap().to_string()
        };

        assert_eq!(run("+01:30"), "2023-06-15 14:00:45");
        assert_eq!(run("-00:45"), "2023-06-15 11:45:45");
    }

    #[test]
    fn test_apply_modifier_date_time_offset() {
        let run = |mod_str: &str| -> String {
            let args = vec![
                Value::build_text("2023-06-15 12:30:45".to_string()),
                Value::build_text(mod_str.to_string()),
            ];
            exec_datetime_full(args).to_text().unwrap().to_string()
        };

        assert_eq!(run("+0001-01-01 01:01"), "2024-07-16 13:31:45");
        assert_eq!(run("-0001-01-01 01:01"), "2022-05-14 11:29:45");
        assert_eq!(run("+0002-03-04 05:06"), "2025-09-19 17:36:45");
        assert_eq!(run("-0002-03-04 05:06"), "2021-03-11 07:24:45");
    }

    #[test]
    fn test_apply_modifier_start_of_year() {
        let res = exec_datetime_full(&[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("start of year"),
        ]);
        assert_eq!(res.to_text().unwrap(), "2023-01-01 00:00:00");
    }

    #[test]
    fn test_apply_modifier_start_of_day() {
        let res = exec_datetime_full(&[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("start of day"),
        ]);
        assert_eq!(res.to_text().unwrap(), "2023-06-15 00:00:00");
    }

    #[test]
    fn test_single_modifier() {
        let res = exec_datetime_full(&[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("-1 day"),
        ]);
        assert_eq!(res.to_text().unwrap(), "2023-06-14 12:30:45");
    }

    #[test]
    fn test_multiple_modifiers() {
        let res = exec_datetime_full(&[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("-1 day"),
            Value::build_text("+3 hours"),
        ]);
        assert_eq!(res.to_text().unwrap(), "2023-06-14 15:30:45");
    }

    #[test]
    fn test_subsec_modifier() {
        let res = exec_datetime_general(
            &[
                Value::build_text("2023-06-15 12:30:45"),
                Value::build_text("subsec"),
            ],
            "time",
        );
        assert_eq!(res.to_text().unwrap(), "12:30:45.000");
    }

    #[test]
    fn test_start_of_day_modifier() {
        let res = exec_datetime_full(&[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("start of day"),
            Value::build_text("-1 day"),
        ]);
        assert_eq!(res.to_text().unwrap(), "2023-06-14 00:00:00");
    }

    #[test]
    fn test_start_of_month_modifier() {
        let res = exec_datetime_full(&[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("start of month"),
            Value::build_text("+1 day"),
        ]);
        assert_eq!(res.to_text().unwrap(), "2023-06-02 00:00:00");
    }

    #[test]
    fn test_start_of_year_modifier() {
        let res = exec_datetime_full(&[
            Value::build_text("2023-06-15 12:30:45"),
            Value::build_text("start of year"),
            Value::build_text("+30 days"),
            Value::build_text("+5 hours"),
        ]);
        assert_eq!(res.to_text().unwrap(), "2023-01-31 05:00:00");
    }

    #[test]
    fn test_timezone_modifiers() {
        let base_str = "2023-06-15 12:30:45";
        let naive = chrono::NaiveDate::from_ymd_opt(2023, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap();

        // 1. Test 'localtime' modifier: Input (assumed UTC) -> Output (Local)
        let args_local = vec![
            Value::build_text(base_str.to_string()),
            Value::build_text("localtime".to_string()),
        ];
        let res_local = exec_datetime_full(args_local);

        // Expected calculation: Treat naive as UTC, convert to Local
        let utc_dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc);
        let expected_local = utc_dt
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        assert_eq!(
            res_local.to_text().unwrap(),
            expected_local,
            "localtime modifier mismatch"
        );

        // 2. Test 'utc' modifier: Input (assumed Local) -> Output (UTC)
        let args_utc = vec![
            Value::build_text(base_str.to_string()),
            Value::build_text("utc".to_string()),
        ];
        let res_utc = exec_datetime_full(args_utc);

        // Expected calculation: Treat naive as Local, convert to UTC
        // We handle potential Local ambiguities (though 2023-06-15 is typically safe)
        match chrono::Local.from_local_datetime(&naive) {
            chrono::LocalResult::Single(local_input) => {
                let expected_utc = local_input
                    .with_timezone(&chrono::Utc)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();
                assert_eq!(
                    res_utc.to_text().unwrap(),
                    expected_utc,
                    "utc modifier mismatch"
                );
            }
            _ => {
                // Fallback if local time is ambiguous/invalid in test environment
                // Ensure result is at least a valid string and not Null
                assert!(res_utc.to_text().is_some());
                assert_ne!(res_utc, Value::Null);
            }
        }
    }

    #[test]
    fn test_combined_modifiers() {
        let args = vec![
            Value::build_text("2000-01-01 00:00:00".to_string()),
            Value::build_text("-1 day".to_string()),
            Value::build_text("+5 hours".to_string()),
            Value::build_text("+30 minutes".to_string()),
            Value::build_text("+15 seconds".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "1999-12-31 05:30:15.000");
    }

    #[test]
    fn test_max_datetime_limit() {
        let args = vec![Value::build_text("9999-12-31 23:59:59".to_string())];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "9999-12-31 23:59:59");
    }

    #[test]
    fn test_leap_second_ignored() {
        let args = vec![Value::build_text("2024-06-30 23:59:60".to_string())];
        let result = exec_datetime_full(args);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_already_on_weekday_no_change() {
        let args = vec![
            Value::build_text("2023-01-01 12:00:00".to_string()),
            Value::build_text("weekday 0".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2023-01-01 12:00:00");
    }

    #[test]
    fn test_move_forward_if_different() {
        let args1 = vec![
            Value::build_text("2023-01-01 12:00:00".to_string()),
            Value::build_text("weekday 1".to_string()),
        ];
        let res1 = exec_datetime_full(args1);
        assert_eq!(res1.to_text().unwrap(), "2023-01-02 12:00:00");

        let args2 = vec![
            Value::build_text("2023-01-03 12:00:00".to_string()),
            Value::build_text("weekday 5".to_string()),
        ];
        let res2 = exec_datetime_full(args2);
        assert_eq!(res2.to_text().unwrap(), "2023-01-06 12:00:00");
    }

    #[test]
    fn test_wrap_around_weekend() {
        let args1 = vec![
            Value::build_text("2023-01-06 12:00:00".to_string()),
            Value::build_text("weekday 0".to_string()),
        ];
        let res1 = exec_datetime_full(args1);
        assert_eq!(res1.to_text().unwrap(), "2023-01-08 12:00:00");

        let args2 = vec![
            Value::build_text("2023-01-08 12:00:00".to_string()),
            Value::build_text("weekday 0".to_string()),
        ];
        let res2 = exec_datetime_full(args2);
        assert_eq!(res2.to_text().unwrap(), "2023-01-08 12:00:00");
    }

    #[test]
    fn test_same_day_stays_put() {
        let args = vec![
            Value::build_text("2023-01-05 12:00:00".to_string()),
            Value::build_text("weekday 4".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-01-05 12:00:00");
    }

    #[test]
    fn test_already_on_friday_no_change() {
        let args = vec![
            Value::build_text("2023-01-06 12:00:00".to_string()),
            Value::build_text("weekday 5".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-01-06 12:00:00");
    }

    #[test]
    fn test_apply_modifier_julianday() {
        let jd_args = vec![Value::build_text("2000-01-01 12:00:00".to_string())];
        let jd_val = exec_julianday(jd_args);

        let dt_args = vec![jd_val, Value::build_text("auto".to_string())];
        let dt_res = exec_datetime_full(dt_args);
        assert_eq!(dt_res.to_text().unwrap(), "2000-01-01 12:00:00");
    }

    #[test]
    fn test_apply_modifier_start_of_month() {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text("start of month".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-06-01 00:00:00");
    }

    #[test]
    fn test_apply_modifier_subsec() {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let res = exec_datetime_general(args, "datetime");
        assert_eq!(res.to_text().unwrap(), "2023-06-15 12:30:45.000");
    }

    #[test]
    fn test_apply_modifier_floor_modifier_n_floor_gt_0() {
        let args = vec![
            Value::build_text("2023-01-31".to_string()),
            Value::build_text("+1 month".to_string()),
            Value::build_text("floor".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-02-28 00:00:00");
    }

    #[test]
    fn test_apply_modifier_floor_modifier_n_floor_le_0() {
        let args = vec![
            Value::build_text("2023-01-15".to_string()),
            Value::build_text("+1 month".to_string()),
            Value::build_text("floor".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-02-15 00:00:00");
    }

    #[test]
    fn test_apply_modifier_ceiling_modifier_sets_n_floor_to_zero() {
        let args = vec![
            Value::build_text("2023-01-31".to_string()),
            Value::build_text("ceiling".to_string()),
            Value::build_text("+1 month".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-03-03 00:00:00");
    }

    #[test]
    fn test_apply_modifier_start_of_month_basic() {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45".to_string()),
            Value::build_text("start of month".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-06-01 00:00:00");
    }

    #[test]
    fn test_apply_modifier_start_of_month_already_at_first() {
        let args = vec![
            Value::build_text("2023-06-01 00:00:00".to_string()),
            Value::build_text("start of month".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-06-01 00:00:00");
    }

    #[test]
    fn test_apply_modifier_start_of_month_edge_case() {
        let args = vec![
            Value::build_text("2023-07-31 23:59:59".to_string()),
            Value::build_text("start of month".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-07-01 00:00:00");
    }

    #[test]
    fn test_apply_modifier_subsec_no_change() {
        let args = vec![
            Value::build_text("2023-06-15 12:30:45.123".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2023-06-15 12:30:45.123");
    }

    #[test]
    fn test_apply_modifier_subsec_preserves_fractional_seconds() {
        let args = vec![
            Value::build_text("2025-01-02 04:12:21.891".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2025-01-02 04:12:21.891");
    }

    #[test]
    fn test_apply_modifier_subsec_no_fractional_seconds() {
        let args = vec![
            Value::build_text("2025-01-02 04:12:21".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2025-01-02 04:12:21.000");
    }

    #[test]
    fn test_apply_modifier_subsec_truncate_to_milliseconds() {
        let args = vec![
            Value::build_text("2025-01-02 04:12:21.891123456".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let res = exec_datetime_full(args);
        assert_eq!(res.to_text().unwrap(), "2025-01-02 04:12:21.891");
    }

    #[test]
    fn test_strftime() {
        let fmt = Value::build_text("%Y-%m-%d".to_string());
        let date = Value::build_text("2023-10-25 14:30:00".to_string());
        let expected = Value::build_text("2023-10-25".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("%H:%M:%S".to_string());
        let date = Value::build_text("2023-10-25 14:30:45".to_string());
        let expected = Value::build_text("14:30:45".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("Date: %Y-%m-%d, Time: %H:%M".to_string());
        let date = Value::build_text("2023-10-25 14:30:45".to_string());
        let expected = Value::build_text("Date: 2023-10-25, Time: 14:30".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("%Y-%m-%d".to_string());
        let date = Value::build_text("2023-10-25".to_string());
        let mod1 = Value::build_text("start of month".to_string());
        let expected = Value::build_text("2023-10-01".to_string());
        assert_eq!(exec_strftime(&[fmt, date, mod1]), expected);

        let fmt = Value::build_text("%Y-%m-%d".to_string());
        let date = Value::build_text("2023-10-25".to_string());
        let mod1 = Value::build_text("+5 days".to_string());
        let expected = Value::build_text("2023-10-30".to_string());
        assert_eq!(exec_strftime(&[fmt, date, mod1]), expected);

        let fmt = Value::build_text("%J".to_string());
        let date = Value::build_text("2023-01-01 12:00:00".to_string());
        let expected = Value::build_text("2459946".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("%s".to_string());
        let date = Value::build_text("2023-01-01 00:00:00".to_string());
        let expected = Value::build_text("1672531200".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("%S.%f".to_string());
        let date = Value::build_text("2023-01-01 12:00:05.123".to_string());
        let expected = Value::build_text("05.05.123".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("%w".to_string());
        let date = Value::build_text("2023-01-01".to_string());
        let expected = Value::build_text("0".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("%j".to_string());
        let date = Value::build_text("2023-02-01".to_string());
        let expected = Value::build_text("032".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::Null;
        let date = Value::build_text("now".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), Value::Null);

        let fmt = Value::build_text("%Y".to_string());
        let date = Value::Null;
        let expected = Value::Null;
        assert_eq!(exec_strftime(&[fmt, date]), expected);

        let fmt = Value::build_text("%Y".to_string());
        let date = Value::build_text("invalid-date".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), Value::Null);

        let fmt = Value::build_text("100%%".to_string());
        let date = Value::build_text("2023-01-01".to_string());
        let expected = Value::build_text("100%".to_string());
        assert_eq!(exec_strftime(&[fmt, date]), expected);
    }

    #[test]
    fn test_exec_timediff() {
        let start = Value::build_text("12:00:00");
        let end = Value::build_text("14:30:45");
        let expected = Value::build_text("-0000-00-00 02:30:45.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("14:30:45");
        let end = Value::build_text("12:00:00");
        let expected = Value::build_text("+0000-00-00 02:30:45.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("12:00:01.300");
        let end = Value::build_text("12:00:00.500");
        let expected = Value::build_text("+0000-00-00 00:00:00.800");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("13:30:00");
        let end = Value::build_text("16:45:30");
        let expected = Value::build_text("-0000-00-00 03:15:30.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("2023-05-10 23:30:00");
        let end = Value::build_text("2023-05-11 01:15:00");
        let expected = Value::build_text("-0000-00-00 01:45:00.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::Null;
        let end = Value::build_text("12:00:00");
        let expected = Value::Null;
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("not a time");
        let end = Value::build_text("12:00:00");
        let expected = Value::Null;
        assert_eq!(exec_timediff(&[start, end]), expected);

        // Test identical times - should return zero duration, not Null
        let start = Value::build_text("12:00:00");
        let end = Value::build_text("12:00:00");
        let expected = Value::build_text("+0000-00-00 00:00:00.000");
        assert_eq!(exec_timediff(&[start, end]), expected);
    }

    #[test]
    fn test_subsec_fixed_time_expansion() {
        let args = vec![
            Value::build_text("2024-01-01 12:00:00".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2024-01-01 12:00:00.000");
    }

    #[test]
    fn test_subsec_date_only_expansion() {
        let args = vec![
            Value::build_text("2024-01-01".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2024-01-01 00:00:00.000");
    }

    #[test]
    fn test_subsec_iso_separator() {
        let args = vec![
            Value::build_text("2024-01-01T15:30:00".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2024-01-01 15:30:00.000");
    }

    #[test]
    fn test_subsec_chaining_before_math() {
        let args = vec![
            Value::build_text("2024-01-01 12:00:00".to_string()),
            Value::build_text("subsec".to_string()),
            Value::build_text("+1 hour".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2024-01-01 13:00:00.000");
    }

    #[test]
    fn test_subsec_chaining_after_math() {
        let args = vec![
            Value::build_text("2024-01-01 12:00:00".to_string()),
            Value::build_text("+1 hour".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2024-01-01 13:00:00.000");
    }

    #[test]
    fn test_subsec_rollover_math() {
        let args = vec![
            Value::build_text("2024-01-01 12:00:00.999".to_string()),
            Value::build_text("+1 second".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2024-01-01 12:00:01.999");
    }

    #[test]
    fn test_subsec_case_insensitivity() {
        let args = vec![
            Value::build_text("2024-01-01 12:00:00".to_string()),
            Value::build_text("SuBsEc".to_string()),
        ];
        let result = exec_datetime_full(args);
        assert_eq!(result.to_text().unwrap(), "2024-01-01 12:00:00.000");
    }

    #[test]
    fn test_parse_modifier_unicode_no_panic() {
        let unicode_inputs = ["!*\u{ea37}", "\u{1F600}", "日本語", "中", "\u{0080}", ""];

        for input in unicode_inputs {
            let args = vec![
                Value::build_text("now".to_string()),
                Value::build_text(input.to_string()),
            ];
            let result = exec_datetime_full(args);
            // Expect Null for invalid modifiers, but no panic
            assert_eq!(result, Value::Null);
        }
    }

    #[test]
    fn test_unixepoch_basic_usage() {
        let result = exec_unixepoch(vec![Value::build_text("1970-01-01 00:00:00".to_string())]);
        assert_eq!(result, Value::from_i64(0));

        let result = exec_unixepoch(vec![Value::build_text("2023-01-01 00:00:00".to_string())]);
        assert_eq!(result, Value::from_i64(1672531200));

        let result = exec_unixepoch(vec![Value::build_text("1969-12-31 23:59:59".to_string())]);
        assert_eq!(result, Value::from_i64(-1));

        let result = exec_unixepoch(vec![Value::from_f64(2440587.5)]);
        assert_eq!(result, Value::from_i64(0));
    }

    #[test]
    fn test_numeric_datetime_accepts_ascii_whitespace() {
        let expected = Value::from_i64(-210866328000);
        for input in ["5 ", " 5", " 5 ", "\t5\n"] {
            let result = exec_unixepoch(vec![Value::build_text(input.to_string())]);
            assert_eq!(result, expected, "input {input:?}");
        }

        let result = exec_julianday(vec![Value::build_text("5 ".to_string())]);
        assert_eq!(result, Value::from_f64(5.0));

        let result = exec_unixepoch(vec![Value::build_text("5 trailing".to_string())]);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_unixepoch_numeric_modifiers_unixepoch() {
        let res1 = exec_unixepoch(vec![
            Value::from_i64(1672531200),
            Value::build_text("unixepoch".to_string()),
        ]);
        assert_eq!(res1, Value::from_i64(1672531200));

        let res2 = exec_unixepoch(vec![
            Value::from_i64(0),
            Value::build_text("unixepoch".to_string()),
        ]);
        assert_eq!(res2, Value::from_i64(0));

        let res3 = exec_unixepoch(vec![
            Value::from_i64(1672531200),
            Value::build_text("unixepoch".to_string()),
            Value::build_text("start of year".to_string()),
        ]);
        assert_eq!(res3, Value::from_i64(1672531200));
    }

    #[test]
    fn test_unixepoch_numeric_modifiers_julianday() {
        let res1 = exec_unixepoch(vec![
            Value::from_f64(2440587.5),
            Value::build_text("julianday".to_string()),
        ]);
        assert_eq!(res1, Value::from_i64(0));

        let res2 = exec_unixepoch(vec![
            Value::from_f64(2460311.5),
            Value::build_text("julianday".to_string()),
        ]);
        assert_eq!(res2, Value::from_i64(1704153600));

        let res3 = exec_unixepoch(vec![
            Value::from_f64(0.0),
            Value::build_text("julianday".to_string()),
        ]);
        match res3 {
            Value::Numeric(Numeric::Integer(i)) => assert_eq!(i, -210866760000),
            _ => panic!("Expected Integer result for JD 0"),
        }
    }

    #[test]
    fn test_unixepoch_numeric_modifiers_auto() {
        let res1 = exec_unixepoch(vec![
            Value::from_f64(2440587.5),
            Value::build_text("auto".to_string()),
        ]);
        assert_eq!(res1, Value::from_i64(0));

        let res2 = exec_unixepoch(vec![
            Value::from_i64(1672531200),
            Value::build_text("auto".to_string()),
        ]);
        assert_eq!(res2, Value::from_i64(1672531200));

        let res3 = exec_unixepoch(vec![
            Value::from_f64(0.0),
            Value::build_text("auto".to_string()),
        ]);
        match res3 {
            Value::Numeric(Numeric::Integer(i)) => assert!(i < 0),
            _ => panic!("Expected Integer result"),
        }
    }

    #[test]
    fn test_unixepoch_invalid_usage() {
        let res1 = exec_unixepoch(vec![
            Value::from_i64(0),
            Value::build_text("start of year".to_string()),
            Value::build_text("unixepoch".to_string()),
        ]);
        assert_eq!(res1, Value::Null);

        let res2 = exec_unixepoch(vec![
            Value::build_text("2023-01-01".to_string()),
            Value::build_text("unixepoch".to_string()),
        ]);
        assert_eq!(res2, Value::Null);

        let res3 = exec_unixepoch(vec![
            Value::from_i64(0),
            Value::build_text("unixepoch".to_string()),
            Value::build_text("julianday".to_string()),
        ]);
        assert_eq!(res3, Value::Null);
    }

    #[test]
    fn test_unixepoch_complex_calculations() {
        let res1 = exec_unixepoch(vec![
            Value::from_f64(2440587.5),
            Value::build_text("julianday".to_string()),
            Value::build_text("+1 day".to_string()),
        ]);
        assert_eq!(res1, Value::from_i64(86400));

        let res2 = exec_unixepoch(vec![
            Value::from_f64(2460311.5),
            Value::build_text("auto".to_string()),
            Value::build_text("start of month".to_string()),
            Value::build_text("+1 month".to_string()),
        ]);
        assert_eq!(res2, Value::from_i64(1706745600));
    }

    #[test]
    fn test_unixepoch_subsecond_precision() {
        let res1 = exec_unixepoch(vec![
            Value::build_text("1970-01-01 00:00:00.0006".to_string()),
            Value::build_text("subsec".to_string()),
        ]);
        match res1 {
            Value::Numeric(Numeric::Float(f)) => {
                assert!((f64::from(f) - 0.001).abs() < f64::EPSILON)
            }
            _ => panic!("Expected Float result"),
        }

        let res2 = exec_unixepoch(vec![
            Value::build_text("1970-01-01 00:00:00.9996".to_string()),
            Value::build_text("subsec".to_string()),
        ]);
        match res2 {
            Value::Numeric(Numeric::Float(f)) => {
                assert!((f64::from(f) - 0.999).abs() < f64::EPSILON)
            }
            _ => panic!("Expected Float result"),
        }
    }

    #[test]
    fn test_fast_path_date_only() {
        assert_eq!(
            exec_date(vec![Value::build_text("2024-01-01".to_string())])
                .to_text()
                .unwrap(),
            "2024-01-01"
        );
        assert_eq!(
            exec_date(vec![Value::build_text("0001-01-01".to_string())])
                .to_text()
                .unwrap(),
            "0001-01-01"
        );
        assert_eq!(
            exec_date(vec![Value::build_text("9999-12-31".to_string())])
                .to_text()
                .unwrap(),
            "9999-12-31"
        );
        assert_eq!(
            exec_date(vec![Value::build_text("2024-02-29".to_string())])
                .to_text()
                .unwrap(),
            "2024-02-29"
        );
        assert_eq!(
            exec_date(vec![Value::build_text("2023-02-29".to_string())])
                .to_text()
                .unwrap(),
            "2023-03-01"
        );

        assert_eq!(
            exec_date(vec![Value::build_text("2024-00-01".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_date(vec![Value::build_text("2024-13-01".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_date(vec![Value::build_text("2024-01-00".to_string())]),
            Value::Null
        );

        assert_eq!(
            exec_date(vec![Value::build_text("2024-01-32".to_string())]),
            Value::Null
        );

        assert_eq!(
            exec_date(vec![Value::build_text("2024/01/01".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_date(vec![Value::build_text("2024.01.01".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_date(vec![Value::build_text("202X-01-01".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_date(vec![Value::build_text("2024-0a-01".to_string())]),
            Value::Null
        );
    }

    #[test]
    fn test_fast_path_datetime_formats() {
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15 10:30".to_string())])
                .to_text()
                .unwrap(),
            "2024-01-15 10:30:00"
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15T10:30".to_string())])
                .to_text()
                .unwrap(),
            "2024-01-15 10:30:00"
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15X10:30".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15 10:30:45".to_string())])
                .to_text()
                .unwrap(),
            "2024-01-15 10:30:45"
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15T10:30:45".to_string())])
                .to_text()
                .unwrap(),
            "2024-01-15 10:30:45"
        );

        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15 25:30:45".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15 10:60:45".to_string())]),
            Value::Null
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15 10:30:60".to_string())]),
            Value::Null
        );
    }

    #[test]
    fn test_fast_path_time_only() {
        assert_eq!(
            exec_time(vec![Value::build_text("10:30".to_string())])
                .to_text()
                .unwrap(),
            "10:30:00"
        );
        assert_eq!(
            exec_time(vec![Value::build_text("00:00".to_string())])
                .to_text()
                .unwrap(),
            "00:00:00"
        );
        assert_eq!(
            exec_time(vec![Value::build_text("23:59".to_string())])
                .to_text()
                .unwrap(),
            "23:59:00"
        );
        assert_eq!(
            exec_time(vec![Value::build_text("24:00".to_string())])
                .to_text()
                .unwrap(),
            "24:00:00"
        );
        assert_eq!(
            exec_time(vec![Value::build_text("10:60".to_string())]),
            Value::Null
        );

        assert_eq!(
            exec_time(vec![Value::build_text("10:30:45".to_string())])
                .to_text()
                .unwrap(),
            "10:30:45"
        );
        assert_eq!(
            exec_time(vec![Value::build_text("00:00:00".to_string())])
                .to_text()
                .unwrap(),
            "00:00:00"
        );
        assert_eq!(
            exec_time(vec![Value::build_text("23:59:59".to_string())])
                .to_text()
                .unwrap(),
            "23:59:59"
        );

        let res1 = exec_datetime_general(
            vec![
                Value::build_text("10:30:45.123".to_string()),
                Value::build_text("subsec".to_string()),
            ],
            "time",
        );
        assert_eq!(res1.to_text().unwrap(), "10:30:45.123");

        let res2 = exec_datetime_general(
            vec![
                Value::build_text("10:30:45.1".to_string()),
                Value::build_text("subsec".to_string()),
            ],
            "time",
        );
        assert_eq!(res2.to_text().unwrap(), "10:30:45.100");
    }

    #[test]
    fn test_fast_path_skips_timezone_strings() {
        assert_eq!(
            exec_datetime_full(vec![Value::build_text("2024-01-15 10:30:45Z".to_string())])
                .to_text()
                .unwrap(),
            "2024-01-15 10:30:45"
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text(
                "2024-01-15 10:30:45+02:00".to_string()
            )])
            .to_text()
            .unwrap(),
            "2024-01-15 08:30:45"
        );
        assert_eq!(
            exec_datetime_full(vec![Value::build_text(
                "2024-01-15 10:30:45-05:00".to_string()
            )])
            .to_text()
            .unwrap(),
            "2024-01-15 15:30:45"
        );
        assert_eq!(
            exec_time(vec![Value::build_text("10:30:45+02:00".to_string())])
                .to_text()
                .unwrap(),
            "08:30:45"
        );
    }

    #[test]
    fn test_fast_path_fractional_seconds_precision() {
        let args1 = vec![
            Value::build_text("2024-01-15 10:30:45.123456789".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        assert_eq!(
            exec_datetime_full(args1).to_text().unwrap(),
            "2024-01-15 10:30:45.123"
        );

        let args2 = vec![
            Value::build_text("2024-01-15 10:30:45.1".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        assert_eq!(
            exec_datetime_full(args2).to_text().unwrap(),
            "2024-01-15 10:30:45.100"
        );

        let args3 = vec![
            Value::build_text("2024-01-15 10:30:45.100".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        assert_eq!(
            exec_datetime_full(args3).to_text().unwrap(),
            "2024-01-15 10:30:45.100"
        );

        let args4 = vec![
            Value::build_text("2024-01-15 10:30:45.000".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        assert_eq!(
            exec_datetime_full(args4).to_text().unwrap(),
            "2024-01-15 10:30:45.000"
        );
    }

    #[test]
    fn test_fast_path_month_day_boundaries() {
        let run = |s: &str| -> String {
            exec_datetime_full(&[Value::build_text(s.to_string())])
                .to_text()
                .unwrap()
                .to_string()
        };

        assert_eq!(run("2024-04-30"), "2024-04-30 00:00:00");
        assert_eq!(run("2024-04-31"), "2024-05-01 00:00:00");
        assert_eq!(run("2024-01-31"), "2024-01-31 00:00:00");
        assert_eq!(run("2024-03-31"), "2024-03-31 00:00:00");
        assert_eq!(run("2024-02-29"), "2024-02-29 00:00:00");
        assert_eq!(run("2023-02-29"), "2023-03-01 00:00:00");
        assert_eq!(run("2024-02-28"), "2024-02-28 00:00:00");
    }

    #[test]
    fn test_fast_path_edge_cases() {
        // Helper for cases expected to fail (return Null)
        let run = |s: &str| -> Value { exec_datetime_full(&[Value::build_text(s.to_string())]) };

        // Helper for cases expected to succeed (return String)
        let run_str = |s: &str| -> String { run(s).to_text().unwrap().to_string() };

        assert_eq!(run(""), Value::Null);
        assert_eq!(run("a"), Value::Null);
        assert_eq!(run("ab"), Value::Null);
        assert_eq!(run("abc"), Value::Null);
        assert_eq!(run("abcd"), Value::Null);
        assert_eq!(run_str("0000-01-01"), "0000-01-01 00:00:00");
        assert_eq!(run(" 2024-01-01"), Value::Null);
        assert_eq!(run_str("2024-01-01 "), "2024-01-01 00:00:00");
        assert_eq!(run_str("2024-01-15\t10:30:45"), "2024-01-15 10:30:45");
        assert_eq!(run("2024-1-01"), Value::Null);
        assert_eq!(run("2024-01-1"), Value::Null);
        assert_eq!(run("aaaa-bb-cc"), Value::Null);
        assert_eq!(run("2024-01-01abc"), Value::Null);
        assert_eq!(run_str("-2024-01-01"), "-2024-01-01 00:00:00");
        assert_eq!(run("10:30:45.12abc"), Value::Null);
        assert_eq!(run("2024-01-15 10:30:45.123xyz"), Value::Null);

        // Manual check for subsec since it requires 2 arguments
        let dt_args = &[
            Value::build_text("10:30:45.12".to_string()),
            Value::build_text("subsec".to_string()),
        ];
        assert_eq!(
            exec_datetime_general(dt_args, "time").to_text().unwrap(),
            "10:30:45.120"
        );
    }

    // Regression test for fuzzing crash: strftime with non-char-boundary UTF-8 modifiers
    // The modifier "swww\0\u{1}\t\0\u{fffd}\u{fffd}\u{f}W" has multi-byte chars where
    // byte index 9 is not a valid char boundary, causing panic on slice.
    #[test]
    fn test_strftime_invalid_utf8_boundary_modifier() {
        // This modifier starts with 's' so it matches the 's' => branch,
        // but byte 9 falls inside a multi-byte character
        let modifier_with_multibyte = "swww\0\u{1}\t\0\u{fffd}\u{fffd}\u{f}W";
        let args = &[
            Value::build_text("".to_string()),
            Value::from_f64(-1.8041807844761696e230),
            Value::build_text(modifier_with_multibyte.to_string()),
        ];
        // Should not panic, just return an error or null
        let _ = exec_strftime(args.iter());

        // Also test the 'w' => weekday branch with similar input
        let weekday_modifier = "weekda\u{fffd}\u{fffd}";
        let args2 = &[
            Value::build_text("".to_string()),
            Value::from_f64(0.0),
            Value::build_text(weekday_modifier.to_string()),
        ];
        let _ = exec_strftime(args2.iter());
    }
}
