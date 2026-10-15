use std::fmt;
use std::fmt::Write;
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use chrono::NaiveTime;
use futures::{pin_mut, StreamExt};
use pg_interval::Interval;
use postgres_types::{accepts, FromSql, Kind, PgLsn, ToSql, Type};
use sqllogictest::{DBOutput, DefaultColumnType};

use crate::postgres::error::PgDriverError;

use super::{Extended, Postgres, Result};

// Inspired by postgres_type::Array implementation of Display trait
fn print_array<T: std::fmt::Display>(
    arr: &postgres_array::Array<Option<T>>,
    delim: char,
    fmt: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    print_array_helper(0, arr.dimensions(), &mut arr.iter(), delim, fmt)
}

// See https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
// The array output routine will put double quotes around element value if it
// * is empty string
// * equals to NULL (case insensitive)
// Or contains
// * curly braces
// * delimiter characters(comma)
// * double quotes
// * backslashes
// * space
// It'is used (although it's simple protocol specific) to not duplicate tests for simple and
// extended protocols.
pub fn array_item_need_escape_and_quote(data: &str) -> bool {
    if data.is_empty() || data.eq_ignore_ascii_case("null") {
        return true;
    }

    data.chars()
        .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_ascii_whitespace())
}

pub fn escape_and_quote(input: &str) -> String {
    debug_assert!(array_item_need_escape_and_quote(input));
    let mut response = String::with_capacity(input.len());
    response.push('"');

    for c in input.chars() {
        if matches!(c, '"' | '\\') {
            response.push('\\');
        }
        response.push(c);
    }
    response.push('"');
    response
}

fn print_array_helper<'a, T: std::fmt::Display + 'a, I: Iterator<Item = &'a Option<T>>>(
    depth: usize,
    dims: &[postgres_array::Dimension],
    data: &mut I,
    delim: char,
    fmt: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    if dims.is_empty() {
        return write!(fmt, "{{}}");
    }

    if depth == dims.len() {
        return match data.next().unwrap() {
            Some(value) => {
                let mut item = String::new();
                write!(item, "{}", value)?;
                if array_item_need_escape_and_quote(&item) {
                    item = escape_and_quote(&item);
                }
                assert!(!item.is_empty());
                fmt.write_str(&item)
            }
            None => write!(fmt, "NULL"),
        };
    }

    write!(fmt, "{{")?;
    for i in 0..dims[depth].len {
        if i != 0 {
            fmt.write_char(delim)?;
        }
        print_array_helper(depth + 1, dims, data, delim, fmt)?;
    }
    write!(fmt, "}}")
}

struct ArrayFmt<'a, T>(&'a postgres_array::Array<Option<T>>, char);

impl<'a, T: std::fmt::Display> std::fmt::Display for ArrayFmt<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        print_array(self.0, self.1, f)
    }
}

#[derive(Debug)]
struct JsonPreservedValue {
    payload: String,
}

impl<'a> FromSql<'a> for JsonPreservedValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(JsonPreservedValue {
            payload: std::str::from_utf8(raw)?.to_string(),
        })
    }

    accepts!(JSON);
}

impl fmt::Display for JsonPreservedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.payload)
    }
}

#[derive(Debug)]
struct EnumValue(String);

impl<'a> FromSql<'a> for EnumValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // PG wire encoding for an enum value is its label as UTF-8 text in
        // both text and binary formats.
        Ok(EnumValue(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

impl fmt::Display for EnumValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// UUID. tokio-postgres has no builtin FromSql for UUID without an external
// `uuid` crate, so render the 16-byte binary directly as the canonical
// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` text form.
#[derive(Debug)]
struct UuidValue(String);

impl<'a> FromSql<'a> for UuidValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Ok(UuidValue(std::str::from_utf8(raw)?.to_string()));
        }
        use std::fmt::Write;
        let mut s = String::with_capacity(36);
        for (i, b) in raw.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                s.push('-');
            }
            let _ = write!(s, "{:02x}", b);
        }
        Ok(UuidValue(s))
    }

    accepts!(UUID);
}

impl fmt::Display for UuidValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
struct NumericValue(String);

impl<'a> FromSql<'a> for NumericValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(NumericValue(
            numeric_to_string(raw).ok_or("numeric value: malformed payload")?,
        ))
    }

    accepts!(NUMERIC);
}

impl fmt::Display for NumericValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn numeric_to_string(raw: &[u8]) -> Option<String> {
    let word = |i: usize| -> Option<u16> {
        Some(u16::from_be_bytes(
            raw.get(i * 2..i * 2 + 2)?.try_into().ok()?,
        ))
    };
    let ndigits = word(0)? as usize;
    let weight = word(1)? as i16 as i64;
    let sign = word(2)?;
    let dscale = word(3)? as usize;
    if raw.len() != (4 + ndigits) * 2 {
        return None;
    }
    let digit = |d: i64| -> u16 {
        if d < 0 || d as usize >= ndigits {
            0
        } else {
            word(4 + d as usize).unwrap_or(0)
        }
    };
    let mut s = match sign {
        0x0000 => String::new(),
        0x4000 => String::from("-"),
        0xC000 => return Some("NaN".to_string()),
        0xD000 => return Some("Infinity".to_string()),
        0xF000 => return Some("-Infinity".to_string()),
        _ => return None,
    };
    if weight < 0 {
        s.push('0');
    } else {
        let _ = write!(s, "{}", digit(0));
        for d in 1..=weight {
            let _ = write!(s, "{:04}", digit(d));
        }
    }
    if dscale > 0 {
        s.push('.');
        let start = s.len();
        let mut d = weight + 1;
        while s.len() - start < dscale {
            let _ = write!(s, "{:04}", digit(d));
            d += 1;
        }
        s.truncate(start + dscale);
    }
    Some(s)
}

// BIT / BIT VARYING. tokio-postgres has no builtin FromSql for these without
// the optional `bit-vec` feature, so decode the binary wire format directly:
// a 4-byte big-endian bit length followed by ceil(len/8) bytes, MSB-first.
// Rendered as a '0'/'1' string (e.g. BIT(3) '101' -> "101").
#[derive(Debug)]
struct BitValue(String);

impl<'a> FromSql<'a> for BitValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 4 {
            return Err("bit value: missing length header".into());
        }
        let bit_len = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
        let bytes = &raw[4..];
        let n_bytes = bit_len.div_ceil(8);
        if bytes.len() < n_bytes {
            return Err("bit value: truncated payload".into());
        }
        let mut s = String::with_capacity(bit_len);
        for i in 0..bit_len {
            let bit = (bytes[i / 8] >> (7 - (i % 8))) & 1;
            s.push(if bit == 1 { '1' } else { '0' });
        }
        Ok(BitValue(s))
    }

    accepts!(BIT, VARBIT);
}

impl fmt::Display for BitValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn read_micros(raw: &[u8]) -> std::result::Result<i64, Box<dyn std::error::Error + Sync + Send>> {
    Ok(i64::from_be_bytes(
        raw.try_into().map_err(|_| "expected an 8-byte value")?,
    ))
}

// The proleptic Gregorian (year, month, day) `days` after 2000-01-01, PG's
// epoch, for any day count the server sends: chrono stops at year 262143, and
// SereneDB's dates reach millions of years.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 10_957 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

// A date as PG prints it: years before 1 AD as "YYYY-MM-DD BC".
fn format_date_part(days: i64) -> (String, bool) {
    let (year, month, day) = civil_from_days(days);
    let bc = year <= 0;
    let year = if bc { 1 - year } else { year };
    (format!("{year:04}-{month:02}-{day:02}"), bc)
}

fn format_date(days: i64) -> String {
    match format_date_part(days) {
        (date, true) => date + " BC",
        (date, false) => date,
    }
}

// A time of day as PG prints it, with fractional seconds only when nonzero,
// trailing zeros trimmed; 24:00:00 stays 24:00:00.
fn format_time(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let base = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    match micros % 1_000_000 {
        0 => base,
        frac => format!("{base}.{}", format!("{frac:06}").trim_end_matches('0')),
    }
}

// A timestamp of `micros` after 2000-01-01 as PG prints it, followed by
// `zone` and, before 1 AD, " BC"; i64::MAX / i64::MIN are ±infinity.
fn format_timestamp(micros: i64, zone: &str) -> String {
    match micros {
        i64::MAX => "infinity".into(),
        i64::MIN => "-infinity".into(),
        _ => {
            let (date, bc) = format_date_part(micros.div_euclid(MICROS_PER_DAY));
            let time = format_time(micros.rem_euclid(MICROS_PER_DAY));
            format!("{date} {time}{zone}{}", if bc { " BC" } else { "" })
        }
    }
}

#[derive(Debug)]
struct TimestampValue(String);

impl<'a> FromSql<'a> for TimestampValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(TimestampValue(format_timestamp(read_micros(raw)?, "")))
    }

    accepts!(TIMESTAMP);
}

impl fmt::Display for TimestampValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// TIMESTAMPTZ renders in UTC with the fixed "+00" suffix.
#[derive(Debug)]
struct TimestampTzValue(String);

impl<'a> FromSql<'a> for TimestampTzValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(TimestampTzValue(format_timestamp(read_micros(raw)?, "+00")))
    }

    accepts!(TIMESTAMPTZ);
}

impl fmt::Display for TimestampTzValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
struct TimeValue(String);

impl<'a> FromSql<'a> for TimeValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(TimeValue(format_time(read_micros(raw)?)))
    }

    accepts!(TIME);
}

impl fmt::Display for TimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// DATE travels as i32 days since 2000-01-01, i32::MAX / i32::MIN being
// ±infinity.
#[derive(Debug)]
struct DateValue(String);

impl<'a> FromSql<'a> for DateValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let days = i32::from_be_bytes(raw.try_into().map_err(|_| "expected a 4-byte date")?);
        Ok(DateValue(match days {
            i32::MAX => "infinity".into(),
            i32::MIN => "-infinity".into(),
            _ => format_date(days.into()),
        }))
    }

    accepts!(DATE);
}

impl fmt::Display for DateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// TIME WITH TIME ZONE: 12-byte binary (i64 microseconds-of-day + i32 zone in
// seconds west of UTC). postgres-types has no FromSql for timetz, so decode it
// by hand and render as serened does, e.g. "11:30:00.123456+00:00".
#[derive(Debug)]
struct TimeTzValue(String);

impl<'a> FromSql<'a> for TimeTzValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 12 {
            return Err("timetz value: expected 12 bytes".into());
        }
        let micros = i64::from_be_bytes([
            raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
        ]);
        let zone = i32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]);
        let secs = (micros / 1_000_000) as u32;
        let nanos = ((micros % 1_000_000) * 1_000) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
            .ok_or("timetz value: time out of range")?;
        let off = -zone; // UTC offset (east-positive)
        let sign = if off < 0 { '-' } else { '+' };
        let off = off.abs();
        let (h, m, s) = (off / 3600, (off % 3600) / 60, off % 60);
        // Minutes/seconds only when nonzero, matching PG's EncodeTimezone.
        let suffix = if s != 0 {
            format!("{sign}{h:02}:{m:02}:{s:02}")
        } else if m != 0 {
            format!("{sign}{h:02}:{m:02}")
        } else {
            format!("{sign}{h:02}")
        };
        Ok(TimeTzValue(format!("{}{}", time, suffix)))
    }

    accepts!(TIMETZ);
}

impl fmt::Display for TimeTzValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// INET / CIDR. tokio-postgres only decodes these with the optional
// `with-ipnetwork`/`with-cidr` features, so decode PG's binary wire format
// directly. The layout (see PG `network_send`) is:
//   u8 family   (2 = IPv4, 3 = IPv6; PGSQL_AF_INET / PGSQL_AF_INET6)
//   u8 bits     (netmask length)
//   u8 is_cidr  (1 for cidr, 0 for inet)
//   u8 nb       (address byte count: 4 for IPv4, 16 for IPv6)
//   nb bytes    (address, network byte order)
// Rendered via std::net for canonical text, with the `/bits` suffix omitted
// for a full host mask on `inet` (matching PG's text output), always kept for
// `cidr`.
#[derive(Debug)]
struct InetValue(String);

impl<'a> FromSql<'a> for InetValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 4 {
            return Err("inet value: missing header".into());
        }
        let bits = raw[1];
        let is_cidr = raw[2] != 0;
        let nb = raw[3] as usize;
        let addr = &raw[4..];
        if addr.len() < nb {
            return Err("inet value: truncated address".into());
        }
        let (ip, full_bits): (std::net::IpAddr, u8) = match nb {
            4 => {
                let octets: [u8; 4] = addr[..4].try_into()?;
                (std::net::Ipv4Addr::from(octets).into(), 32)
            }
            16 => {
                let octets: [u8; 16] = addr[..16].try_into()?;
                (std::net::Ipv6Addr::from(octets).into(), 128)
            }
            other => return Err(format!("inet value: unexpected address size {other}").into()),
        };
        let rendered = if is_cidr || bits != full_bits {
            format!("{ip}/{bits}")
        } else {
            ip.to_string()
        };
        Ok(InetValue(rendered))
    }

    accepts!(INET, CIDR);
}

#[derive(Debug)]
struct GeometryValue(String);

impl<'a> FromSql<'a> for GeometryValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // Binary format carries the WKB bytes; the text format spells the same
        // bytes as upper-case hex, so render them identically.
        let mut hex = String::with_capacity(raw.len() * 2);
        for byte in raw {
            hex.push_str(&format!("{byte:02X}"));
        }
        Ok(GeometryValue(hex))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "geometry"
    }
}

impl fmt::Display for GeometryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for InetValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
struct TextValue(String);

impl<'a> FromSql<'a> for TextValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(TextValue(std::str::from_utf8(raw)?.to_string()))
    }

    accepts!(REFCURSOR, XML);
}

impl fmt::Display for TextValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
struct TidValue(u32, u16);

impl<'a> FromSql<'a> for TidValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 6 {
            return Err(format!("invalid tid length {}", raw.len()).into());
        }
        Ok(TidValue(
            u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]),
            u16::from_be_bytes([raw[4], raw[5]]),
        ))
    }

    accepts!(TID);
}

impl fmt::Display for TidValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.0, self.1)
    }
}

// int2vector and oidvector travel in the binary array format but print as
// space-separated numbers.
#[derive(Debug)]
struct VectorValue(Vec<String>);

impl<'a> FromSql<'a> for VectorValue {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let read_i32 = |at: usize| -> std::result::Result<i32, String> {
            raw.get(at..at + 4)
                .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                .ok_or_else(|| "truncated vector".to_string())
        };
        let ndim = read_i32(0)?;
        let mut pos = 12usize;
        let count = if ndim > 0 { read_i32(pos)? as usize } else { 0 };
        pos += 8 * ndim.max(0) as usize;
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            let len = read_i32(pos)? as usize;
            pos += 4;
            let data = raw.get(pos..pos + len).ok_or("truncated vector item")?;
            pos += len;
            items.push(if *ty == Type::INT2_VECTOR {
                i16::from_be_bytes([data[0], data[1]]).to_string()
            } else {
                u32::from_be_bytes([data[0], data[1], data[2], data[3]]).to_string()
            });
        }
        Ok(VectorValue(items))
    }

    accepts!(INT2_VECTOR, OID_VECTOR);
}

impl fmt::Display for VectorValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join(" "))
    }
}

#[derive(Debug)]
struct MacaddrValue(Vec<u8>);

impl<'a> FromSql<'a> for MacaddrValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(MacaddrValue(raw.to_vec()))
    }

    accepts!(MACADDR, MACADDR8);
}

impl fmt::Display for MacaddrValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let octets: Vec<String> = self.0.iter().map(|b| format!("{b:02x}")).collect();
        write!(f, "{}", octets.join(":"))
    }
}

#[derive(Debug)]
struct MoneyValue(i64);

impl<'a> FromSql<'a> for MoneyValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let cents: [u8; 8] = raw
            .try_into()
            .map_err(|_| format!("invalid money length {}", raw.len()))?;
        Ok(MoneyValue(i64::from_be_bytes(cents)))
    }

    accepts!(MONEY);
}

impl fmt::Display for MoneyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cents = self.0.unsigned_abs();
        let units = (cents / 100).to_string();
        let mut grouped = String::with_capacity(units.len() + units.len() / 3);
        for (i, digit) in units.chars().enumerate() {
            if i > 0 && (units.len() - i) % 3 == 0 {
                grouped.push(',');
            }
            grouped.push(digit);
        }
        let sign = if self.0 < 0 { "-" } else { "" };
        write!(f, "{sign}${grouped}.{:02}", cents % 100)
    }
}

// Range values (see PG `range_send`): a flags byte, then the lower and upper
// bounds that are present, each an int32 length followed by the element's
// binary value. Rendered like PG's `range_out`.
#[derive(Debug)]
struct RangeValue(String);

const RANGE_EMPTY: u8 = 0x01;
const RANGE_LB_INC: u8 = 0x02;
const RANGE_UB_INC: u8 = 0x04;
const RANGE_LB_INF: u8 = 0x08;
const RANGE_UB_INF: u8 = 0x10;

impl<'a> FromSql<'a> for RangeValue {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let Kind::Range(element) = ty.kind() else {
            return Err(format!("{} is not a range type", ty.name()).into());
        };
        let (&flags, mut rest) = raw.split_first().ok_or("range value: missing flags")?;
        if flags & RANGE_EMPTY != 0 {
            return Ok(RangeValue("empty".into()));
        }
        let mut bound = |infinite: u8| -> Option<String> {
            if flags & infinite != 0 {
                return Some(String::new());
            }
            let mut pos = 0;
            let len = usize::try_from(read_be_i32(rest, &mut pos)?).ok()?;
            let bytes = rest.get(pos..pos + len)?;
            rest = &rest[pos + len..];
            Some(escape_range_bound(&render_record_field(
                element.oid() as i32,
                bytes,
            )?))
        };
        let lower = bound(RANGE_LB_INF).ok_or("range value: bad lower bound")?;
        let upper = bound(RANGE_UB_INF).ok_or("range value: bad upper bound")?;
        let open = if flags & RANGE_LB_INC != 0 { '[' } else { '(' };
        let close = if flags & RANGE_UB_INC != 0 { ']' } else { ')' };
        Ok(RangeValue(format!("{open}{lower},{upper}{close}")))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Range(_))
    }
}

// Quote a range bound like PG's `range_bound_escape`: when empty or holding
// `"`, `\`, `(`, `)`, `[`, `]`, `,` or whitespace, with `"` and `\` doubled.
fn escape_range_bound(value: &str) -> String {
    let needs_quote = value.is_empty()
        || value.chars().any(|c| {
            matches!(c, '"' | '\\' | '(' | ')' | '[' | ']' | ',') || c.is_ascii_whitespace()
        });
    if !needs_quote {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        if c == '"' || c == '\\' {
            out.push(c);
        }
        out.push(c);
    }
    out.push('"');
    out
}

impl fmt::Display for RangeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Composite/record values. The server may send composite either as text
// (`(f1,f2,...)`) when negotiated, or as PG's binary record format when the
// client (tokio-postgres by default) requests binary. We handle both: try
// to parse as binary record first, fall back to treating the bytes as text.
#[derive(Debug)]
struct RecordValue(String);

impl<'a> FromSql<'a> for RecordValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if let Some(rendered) = parse_record_binary(raw) {
            return Ok(RecordValue(rendered));
        }
        Ok(RecordValue(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        // Both named composite types (Kind::Composite) and the anonymous
        // `record` pseudo-type (Type::RECORD) ride through here.
        matches!(ty, &Type::RECORD) || matches!(ty.kind(), Kind::Composite(_))
    }
}

// Parse PG binary record format: int32 nfields, then per field
// { int32 type_oid, int32 length (-1 = NULL), `length` bytes }.
// Returns the PG-text composite literal `(f1,f2,...)` or None if the input
// doesn't look like a well-formed binary record.
fn parse_record_binary(raw: &[u8]) -> Option<String> {
    let mut pos = 0;
    let nfields = read_be_i32(raw, &mut pos)?;
    if nfields < 0 {
        return None;
    }
    let mut out = String::from('(');
    for i in 0..nfields {
        if i > 0 {
            out.push(',');
        }
        let oid = read_be_i32(raw, &mut pos)?;
        let len = read_be_i32(raw, &mut pos)?;
        if len == -1 {
            // NULL field — empty between commas (matches PG record_out).
            continue;
        }
        if len < 0 {
            return None;
        }
        let len_usz = len as usize;
        if pos + len_usz > raw.len() {
            return None;
        }
        let field_bytes = &raw[pos..pos + len_usz];
        pos += len_usz;
        let rendered = render_record_field(oid, field_bytes)?;
        append_composite_field(&mut out, &rendered);
    }
    if pos != raw.len() {
        return None;
    }
    out.push(')');
    Some(out)
}

fn read_be_i32(buf: &[u8], pos: &mut usize) -> Option<i32> {
    if *pos + 4 > buf.len() {
        return None;
    }
    let v = i32::from_be_bytes(buf[*pos..*pos + 4].try_into().ok()?);
    *pos += 4;
    Some(v)
}

fn render_record_field(oid: i32, bytes: &[u8]) -> Option<String> {
    if oid < 0 {
        return None;
    }
    let ty = Type::from_oid(oid as u32);
    if let Some(t) = &ty {
        if *t == Type::INT2_VECTOR || *t == Type::OID_VECTOR {
            return VectorValue::from_sql(t, bytes).ok().map(|v| v.to_string());
        }
        if matches!(t.kind(), Kind::Array(_)) {
            if let Some(rendered) = parse_array_binary(bytes) {
                return Some(rendered);
            }
        }
        if matches!(t.kind(), Kind::Range(_)) {
            return RangeValue::from_sql(t, bytes).ok().map(|v| v.0);
        }
    }
    if let Some(t) = ty {
        match t {
            Type::BOOL if bytes.len() == 1 => {
                return Some(if bytes[0] != 0 {
                    "t".into()
                } else {
                    "f".into()
                });
            }
            Type::INT2 if bytes.len() == 2 => {
                return Some(i16::from_be_bytes(bytes.try_into().ok()?).to_string());
            }
            Type::INT4 if bytes.len() == 4 => {
                return Some(i32::from_be_bytes(bytes.try_into().ok()?).to_string());
            }
            Type::INT8 if bytes.len() == 8 => {
                return Some(i64::from_be_bytes(bytes.try_into().ok()?).to_string());
            }
            Type::FLOAT4 if bytes.len() == 4 => {
                return Some(float4_to_str(&f32::from_be_bytes(bytes.try_into().ok()?)));
            }
            Type::FLOAT8 if bytes.len() == 8 => {
                return Some(float8_to_str(&f64::from_be_bytes(bytes.try_into().ok()?)));
            }
            Type::TEXT
            | Type::VARCHAR
            | Type::NAME
            | Type::BPCHAR
            | Type::REFCURSOR
            | Type::XML => {
                return Some(std::str::from_utf8(bytes).ok()?.to_string());
            }
            Type::BYTEA => {
                // PG hex format: \x followed by hex digits.
                let mut s = String::with_capacity(2 + bytes.len() * 2);
                s.push_str("\\x");
                for b in bytes {
                    use std::fmt::Write;
                    let _ = write!(s, "{:02x}", b);
                }
                return Some(s);
            }
            Type::NUMERIC => {
                return numeric_to_string(bytes);
            }
            Type::TIMESTAMP => {
                return TimestampValue::from_sql(&t, bytes).ok().map(|v| v.0);
            }
            Type::TIMESTAMPTZ => {
                return TimestampTzValue::from_sql(&t, bytes).ok().map(|v| v.0);
            }
            Type::DATE => {
                return DateValue::from_sql(&t, bytes).ok().map(|v| v.0);
            }
            Type::TIME => {
                return TimeValue::from_sql(&t, bytes).ok().map(|v| v.0);
            }
            Type::INTERVAL => {
                let iv = Interval::from_sql(&Type::INTERVAL, bytes).ok()?;
                return Some(iv.to_postgres());
            }
            Type::UUID if bytes.len() == 16 => {
                let mut s = String::with_capacity(36);
                use std::fmt::Write;
                for (i, b) in bytes.iter().enumerate() {
                    if matches!(i, 4 | 6 | 8 | 10) {
                        s.push('-');
                    }
                    let _ = write!(s, "{:02x}", b);
                }
                return Some(s);
            }
            Type::INET | Type::CIDR => {
                return InetValue::from_sql(&t, bytes).ok().map(|v| v.0);
            }
            Type::TID => {
                return TidValue::from_sql(&t, bytes).ok().map(|v| v.to_string());
            }
            Type::MONEY => {
                return MoneyValue::from_sql(&t, bytes).ok().map(|v| v.to_string());
            }
            Type::MACADDR | Type::MACADDR8 => {
                return MacaddrValue::from_sql(&t, bytes)
                    .ok()
                    .map(|v| v.to_string());
            }
            Type::RECORD => {
                return parse_record_binary(bytes);
            }
            // tokio-postgres classifies RECORD_ARRAY as Kind::Pseudo (not
            // Kind::Array(Record)), so the Kind::Array branch above misses it.
            Type::RECORD_ARRAY => {
                return parse_array_binary(bytes);
            }
            _ => {}
        }
    }
    // Unknown OID -- most commonly a user-defined composite (CREATE TYPE foo
    // AS (...)) whose pg_type OID is a server-assigned value tokio-postgres
    // doesn't know about. We've already handled the primitives that share
    // ambiguous wire shapes above, so it's safe to try the record parser
    // first; if the bytes don't look like a record (e.g. enum label) the
    // parser rejects them and we fall back to UTF-8.
    if let Some(rendered) = parse_record_binary(bytes) {
        return Some(rendered);
    }
    std::str::from_utf8(bytes).ok().map(|s| s.to_string())
}

// Append a record field to `out`, applying PG's composite quoting rules
// (matches `record_out`). A field is quoted if empty, or contains `,`,
// `(`, `)`, `"`, `\`, or whitespace. Inside quotes, `"` becomes `""` and
// `\` becomes `\\` (each special char is doubled).
fn append_composite_field(out: &mut String, field: &str) {
    let needs_quote = field.is_empty()
        || field
            .chars()
            .any(|c| matches!(c, ',' | '(' | ')' | '"' | '\\') || c.is_whitespace());
    if needs_quote {
        out.push('"');
        for c in field.chars() {
            if c == '"' || c == '\\' {
                out.push(c);
            }
            out.push(c);
        }
        out.push('"');
    } else {
        out.push_str(field);
    }
}

// Append an array element to `out`, applying PG's array quoting rules
// (matches `array_out`). Quote if empty, equals "NULL" case-insensitively,
// or contains `,`, `{`, `}`, `"`, `\`, or whitespace. Inside quotes,
// `"` becomes `\"` and `\` becomes `\\`.
fn append_array_element(out: &mut String, elem: &str) {
    let needs_quote = elem.is_empty()
        || elem.eq_ignore_ascii_case("NULL")
        || elem
            .chars()
            .any(|c| matches!(c, ',' | '{' | '}' | '"' | '\\') || c.is_whitespace());
    if needs_quote {
        out.push('"');
        for c in elem.chars() {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
        out.push('"');
    } else {
        out.push_str(elem);
    }
}

impl fmt::Display for RecordValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Array of records. tokio-postgres's `Array<Option<RecordValue>>` rejects
// `RECORD_ARRAY` because RECORD is a pseudo-type — its `accepts` doesn't
// match `Kind::Array(Pseudo)`. We read the raw array bytes ourselves and
// render to PG's text array form `{"(f1,f2)","..."}`.
#[derive(Debug)]
struct RecordArrayValue(String);

impl<'a> FromSql<'a> for RecordArrayValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if let Some(rendered) = parse_array_binary(raw) {
            return Ok(RecordArrayValue(rendered));
        }
        // Fallback: treat the whole payload as text (already PG-text array).
        Ok(RecordArrayValue(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::RECORD_ARRAY)
    }
}

impl fmt::Display for RecordArrayValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// PG binary array format: int32 ndim, int32 flags, int32 elemtype,
// then per-dim {int32 size, int32 lbound}, then per-element {int32 len
// (-1=NULL), bytes}. Element bytes are rendered via render_record_field
// (which dispatches by elem_oid). Multi-dim arrays render with ndim
// levels of nesting (`{{1,2},{3,4}}`).
fn parse_array_binary(raw: &[u8]) -> Option<String> {
    let mut pos = 0;
    let ndim = read_be_i32(raw, &mut pos)?;
    let _flags = read_be_i32(raw, &mut pos)?;
    let elem_oid = read_be_i32(raw, &mut pos)?;
    if ndim < 0 {
        return None;
    }
    if ndim == 0 {
        return Some("{}".to_string());
    }
    let mut dims = Vec::with_capacity(ndim as usize);
    for _ in 0..ndim {
        let size = read_be_i32(raw, &mut pos)?;
        let _lbound = read_be_i32(raw, &mut pos)?;
        if size < 0 {
            return None;
        }
        dims.push(size as usize);
    }
    let total: usize = dims.iter().product();
    let mut elems: Vec<Option<String>> = Vec::with_capacity(total);
    for _ in 0..total {
        let len = read_be_i32(raw, &mut pos)?;
        if len == -1 {
            elems.push(None);
            continue;
        }
        if len < 0 {
            return None;
        }
        let len_usz = len as usize;
        if pos + len_usz > raw.len() {
            return None;
        }
        let elem_bytes = &raw[pos..pos + len_usz];
        pos += len_usz;
        elems.push(Some(render_record_field(elem_oid, elem_bytes)?));
    }
    if pos != raw.len() {
        return None;
    }
    let mut out = String::new();
    let mut idx = 0;
    render_array_dim(&mut out, &elems, &dims, &mut idx);
    Some(out)
}

fn render_array_dim(out: &mut String, elems: &[Option<String>], dims: &[usize], idx: &mut usize) {
    out.push('{');
    if dims.len() == 1 {
        for i in 0..dims[0] {
            if i > 0 {
                out.push(',');
            }
            match &elems[*idx] {
                Some(s) => append_array_element(out, s),
                None => out.push_str("NULL"),
            }
            *idx += 1;
        }
    } else {
        for i in 0..dims[0] {
            if i > 0 {
                out.push(',');
            }
            render_array_dim(out, elems, &dims[1..], idx);
        }
    }
    out.push('}');
}

#[derive(Debug)]
struct Void {}

impl<'a> FromSql<'a> for Void {
    fn from_sql(
        _ty: &Type,
        _raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Void {})
    }

    accepts!(VOID);
}

impl fmt::Display for Void {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

#[derive(Debug)]
struct Char(u8);

impl<'a> FromSql<'a> for Char {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Char(raw[0]))
    }

    accepts!(CHAR);
}

impl fmt::Display for Char {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            0x00 => {
                // Empty string, do nothing
                Ok(())
            }
            0x01..=0x7f => {
                write!(f, "{}", self.0 as char)
            }
            0x80..=0xff => {
                write!(f, "\\{:03o}", self.0)
            }
        }
    }
}

struct RegOid(u32);

impl<'a> FromSql<'a> for RegOid {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RegOid(<u32 as FromSql>::from_sql(ty, raw)?))
    }

    accepts!(
        REGTYPE,
        REGPROC,
        REGPROCEDURE,
        REGOPER,
        REGOPERATOR,
        REGCLASS,
        REGNAMESPACE,
        REGROLE,
        REGCONFIG,
        REGDICTIONARY,
        REGCOLLATION
    );
}

// It's required to use postgres_array::Array instead of Vec.
// See: https://github.com/rust-postgres/rust-postgres/issues/1186
macro_rules! array_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        array_process!($row, $row_vec, $idx, $t, delim = ',');
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, delim = $delim:expr) => {
        let value: Option<postgres_array::Array<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let dimensions: Vec<postgres_array::Dimension> = value.dimensions().into();
                let data: Vec<Option<String>> = value
                    .into_iter()
                    .map(|opt| opt.map(|v| format!("{}", v)))
                    .collect();
                let value = postgres_array::Array::from_parts(data, dimensions);
                let value = ArrayFmt(&value, $delim);
                let mut output = String::new();
                write!(output, "{value}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, $convert:ident) => {
        let value: Option<postgres_array::Array<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let dimensions: Vec<postgres_array::Dimension> = value.dimensions().into();
                let data: Vec<Option<String>> = value
                    .into_iter()
                    .map(|opt| opt.map(|v| $convert(&v).to_string()))
                    .collect();
                let value = postgres_array::Array::from_parts(data, dimensions);
                let value = ArrayFmt(&value, ',');
                let mut output = String::new();
                write!(output, "{value}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($self:ident, $row:ident, $row_vec:ident, $idx:ident, $t:ty, $ty_name:expr) => {
        let value: Option<postgres_array::Array<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let dimensions: Vec<postgres_array::Dimension> = value.dimensions().into();
                let mut data = Vec::<Option<String>>::new();
                for v in value.iter() {
                    match v {
                        Some(v) => {
                            let sql = format!("select ($1::{})::varchar", stringify!($ty_name));
                            let tmp_rows = $self.client()?.query(&sql, &[&v]).await.unwrap();
                            let value: &str = tmp_rows.get(0).unwrap().get(0);
                            assert!(value.len() > 0);
                            data.push(Some(value.to_string()));
                        }
                        None => {
                            data.push(Some("NULL".to_string()));
                        }
                    }
                }
                let value = postgres_array::Array::from_parts(data, dimensions);
                let value = ArrayFmt(&value, ',');
                let mut output = String::new();
                write!(output, "{value}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
}

macro_rules! vector_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<Vec<$t>> = $row.get($idx);
        match value {
            Some(value) => {
                let items: Vec<String> = value.iter().map(|v| v.to_string()).collect();
                $row_vec.push(placeholder_if_empty(items.join(" ")));
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
}

fn placeholder_if_empty(value: String) -> String {
    if value.is_empty() {
        "(empty)".into()
    } else {
        value
    }
}

macro_rules! single_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                $row_vec.push(placeholder_if_empty(value.to_string()));
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, $convert:ident) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                $row_vec.push(placeholder_if_empty($convert(&value).to_string()));
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($self:ident, $row:ident, $row_vec:ident, $idx:ident, $t:ty, $ty_name:expr) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                let sql = format!("select ($1::{})::varchar", stringify!($ty_name));
                let tmp_rows = $self.client()?.query(&sql, &[&value]).await.unwrap();
                let value: &str = tmp_rows.get(0).unwrap().get(0);
                assert!(value.len() > 0);
                $row_vec.push(placeholder_if_empty(value.to_string()));
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
}

fn bool_to_str(value: &bool) -> &'static str {
    if *value {
        "t"
    } else {
        "f"
    }
}

fn bytea_to_str(value: &[u8]) -> String {
    // It assumes that 'BYTEA_OUTPUT' variable is set to 'hex' (default value)
    let mut result = String::with_capacity("\\x".len() + 2 * value.len());
    result.push_str("\\x");
    for &b in value {
        result.push_str(&format!("{:02x}", b));
    }
    result
}

fn shortest_float_to_str(exponential: String, fixed: String, fixed_below: i32) -> String {
    let (mantissa, exponent) = exponential
        .split_once('e')
        .expect("exponential float formatting has an exponent");
    let exponent: i32 = exponent.parse().expect("float exponent is a number");
    if (-4..fixed_below).contains(&exponent) {
        fixed
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    }
}

fn float4_to_str(value: &f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if *value == f32::INFINITY {
        "Infinity".to_string()
    } else if *value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        shortest_float_to_str(format!("{value:e}"), value.to_string(), 6)
    }
}

fn float8_to_str(value: &f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if *value == f64::INFINITY {
        "Infinity".to_string()
    } else if *value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        shortest_float_to_str(format!("{value:e}"), value.to_string(), 15)
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres<Extended> {
    type Error = PgDriverError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        let mut output = vec![];

        let stmt = self.client()?.prepare(sql).await?;
        let rows = self
            .client()?
            .query_raw(&stmt, std::iter::empty::<&(dyn ToSql + Sync)>())
            .await?;

        pin_mut!(rows);

        let column_names_row: Vec<String> =
            stmt.columns().iter().map(|s| s.name().into()).collect();
        output.push(column_names_row);

        while let Some(row) = rows.next().await {
            let row = row?;
            let mut row_vec = vec![];

            for (idx, column) in row.columns().iter().enumerate() {
                match column.type_().clone() {
                    Type::CHAR => {
                        single_process!(row, row_vec, idx, Char);
                    }
                    Type::CHAR_ARRAY => {
                        array_process!(row, row_vec, idx, Char);
                    }
                    Type::INT2 => {
                        single_process!(row, row_vec, idx, i16);
                    }
                    Type::INT2_ARRAY => {
                        array_process!(row, row_vec, idx, i16);
                    }
                    Type::INT4 => {
                        single_process!(row, row_vec, idx, i32);
                    }
                    Type::INT4_ARRAY => {
                        array_process!(row, row_vec, idx, i32);
                    }
                    Type::INT8 => {
                        single_process!(row, row_vec, idx, i64);
                    }
                    Type::INT8_ARRAY => {
                        array_process!(row, row_vec, idx, i64);
                    }
                    Type::NUMERIC => {
                        single_process!(row, row_vec, idx, NumericValue);
                    }
                    Type::NUMERIC_ARRAY => {
                        array_process!(row, row_vec, idx, NumericValue);
                    }
                    Type::DATE => {
                        single_process!(row, row_vec, idx, DateValue);
                    }
                    Type::DATE_ARRAY => {
                        array_process!(row, row_vec, idx, DateValue);
                    }
                    Type::TIME => {
                        single_process!(row, row_vec, idx, TimeValue);
                    }
                    Type::TIME_ARRAY => {
                        array_process!(row, row_vec, idx, TimeValue);
                    }
                    Type::TIMETZ => {
                        single_process!(row, row_vec, idx, TimeTzValue);
                    }
                    Type::TIMETZ_ARRAY => {
                        array_process!(row, row_vec, idx, TimeTzValue);
                    }
                    Type::TIMESTAMP => {
                        single_process!(row, row_vec, idx, TimestampValue);
                    }
                    Type::TIMESTAMP_ARRAY => {
                        array_process!(row, row_vec, idx, TimestampValue);
                    }
                    Type::BOOL => {
                        single_process!(row, row_vec, idx, bool, bool_to_str);
                    }
                    Type::BOOL_ARRAY => {
                        array_process!(row, row_vec, idx, bool, bool_to_str);
                    }
                    Type::FLOAT4 => {
                        single_process!(row, row_vec, idx, f32, float4_to_str);
                    }
                    Type::FLOAT4_ARRAY => {
                        array_process!(row, row_vec, idx, f32, float4_to_str);
                    }
                    Type::FLOAT8 => {
                        single_process!(row, row_vec, idx, f64, float8_to_str);
                    }
                    Type::FLOAT8_ARRAY => {
                        array_process!(row, row_vec, idx, f64, float8_to_str);
                    }
                    Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME => {
                        single_process!(row, row_vec, idx, &str);
                    }
                    Type::REGTYPE
                    | Type::REGPROC
                    | Type::REGPROCEDURE
                    | Type::REGOPER
                    | Type::REGOPERATOR
                    | Type::REGCLASS
                    | Type::REGNAMESPACE
                    | Type::REGROLE
                    | Type::REGCONFIG
                    | Type::REGDICTIONARY
                    | Type::REGCOLLATION => {
                        let value: Option<RegOid> = row.get(idx);
                        match value {
                            Some(RegOid(oid)) => {
                                let sql =
                                    format!("select ($1::oid)::{}::varchar", column.type_().name());
                                let tmp_rows = self.client()?.query(&sql, &[&oid]).await?;
                                let text: &str = tmp_rows[0].get(0);
                                row_vec.push(placeholder_if_empty(text.to_string()));
                            }
                            None => {
                                row_vec.push("NULL".to_string());
                            }
                        }
                    }
                    Type::VARCHAR_ARRAY
                    | Type::TEXT_ARRAY
                    | Type::BPCHAR_ARRAY
                    | Type::NAME_ARRAY => {
                        array_process!(row, row_vec, idx, &str);
                    }
                    Type::INTERVAL => {
                        single_process!(self, row, row_vec, idx, Interval, INTERVAL);
                    }
                    Type::INTERVAL_ARRAY => {
                        array_process!(self, row, row_vec, idx, Interval, INTERVAL);
                    }
                    Type::TIMESTAMPTZ => {
                        single_process!(row, row_vec, idx, TimestampTzValue);
                    }
                    Type::TIMESTAMPTZ_ARRAY => {
                        array_process!(row, row_vec, idx, TimestampTzValue);
                    }
                    Type::BYTEA => {
                        single_process!(row, row_vec, idx, &[u8], bytea_to_str);
                    }
                    Type::BYTEA_ARRAY => {
                        array_process!(row, row_vec, idx, &[u8], bytea_to_str);
                    }
                    Type::JSON => {
                        single_process!(row, row_vec, idx, JsonPreservedValue);
                    }
                    Type::JSON_ARRAY => {
                        array_process!(row, row_vec, idx, JsonPreservedValue);
                    }
                    Type::JSONB => {
                        single_process!(row, row_vec, idx, serde_json::Value);
                    }
                    Type::JSONB_ARRAY => {
                        array_process!(row, row_vec, idx, serde_json::Value);
                    }
                    Type::VOID => {
                        single_process!(row, row_vec, idx, Void);
                    }
                    // SereneDB doesn't return OID type (corresponding OID value). Instead, it
                    // returns raw u64 type and there are no plans to change
                    // this behaviour. Keep this in mind in case
                    // OID type related problems.
                    Type::OID => {
                        single_process!(row, row_vec, idx, u32);
                    }
                    Type::OID_ARRAY => {
                        array_process!(row, row_vec, idx, u32);
                    }
                    Type::INT2_VECTOR => {
                        vector_process!(row, row_vec, idx, i16);
                    }
                    Type::OID_VECTOR => {
                        vector_process!(row, row_vec, idx, u32);
                    }
                    Type::UUID => {
                        single_process!(row, row_vec, idx, UuidValue);
                    }
                    Type::UUID_ARRAY => {
                        array_process!(row, row_vec, idx, UuidValue);
                    }
                    Type::PG_LSN => {
                        single_process!(row, row_vec, idx, PgLsn);
                    }
                    Type::PG_LSN_ARRAY => {
                        array_process!(row, row_vec, idx, PgLsn);
                    }
                    _ if matches!(column.type_().kind(), Kind::Enum(_)) => {
                        single_process!(row, row_vec, idx, EnumValue);
                    }
                    Type::RECORD => {
                        single_process!(row, row_vec, idx, RecordValue);
                    }
                    Type::RECORD_ARRAY => {
                        single_process!(row, row_vec, idx, RecordArrayValue);
                    }
                    _ if matches!(column.type_().kind(), Kind::Composite(_)) => {
                        single_process!(row, row_vec, idx, RecordValue);
                    }
                    _ if matches!(
                        column.type_().kind(),
                        Kind::Array(elem) if matches!(elem.kind(), Kind::Enum(_))
                    ) =>
                    {
                        array_process!(row, row_vec, idx, EnumValue);
                    }
                    _ if matches!(
                        column.type_().kind(),
                        Kind::Array(elem) if matches!(elem.kind(), Kind::Composite(_))
                    ) =>
                    {
                        array_process!(row, row_vec, idx, RecordValue);
                    }
                    Type::BIT | Type::VARBIT => {
                        single_process!(row, row_vec, idx, BitValue);
                    }
                    Type::BIT_ARRAY | Type::VARBIT_ARRAY => {
                        array_process!(row, row_vec, idx, BitValue);
                    }
                    Type::INET | Type::CIDR => {
                        single_process!(row, row_vec, idx, InetValue);
                    }
                    Type::REFCURSOR | Type::XML => {
                        single_process!(row, row_vec, idx, TextValue);
                    }
                    Type::REFCURSOR_ARRAY | Type::XML_ARRAY => {
                        array_process!(row, row_vec, idx, TextValue);
                    }
                    Type::MACADDR | Type::MACADDR8 => {
                        single_process!(row, row_vec, idx, MacaddrValue);
                    }
                    Type::TID => {
                        single_process!(row, row_vec, idx, TidValue);
                    }
                    Type::TID_ARRAY => {
                        array_process!(row, row_vec, idx, TidValue);
                    }
                    Type::INT2_VECTOR | Type::OID_VECTOR => {
                        single_process!(row, row_vec, idx, VectorValue);
                    }
                    Type::INT2_VECTOR_ARRAY | Type::OID_VECTOR_ARRAY => {
                        array_process!(row, row_vec, idx, VectorValue);
                    }
                    Type::MACADDR_ARRAY | Type::MACADDR8_ARRAY => {
                        array_process!(row, row_vec, idx, MacaddrValue);
                    }
                    Type::MONEY => {
                        single_process!(row, row_vec, idx, MoneyValue);
                    }
                    Type::MONEY_ARRAY => {
                        array_process!(row, row_vec, idx, MoneyValue);
                    }
                    Type::INET_ARRAY | Type::CIDR_ARRAY => {
                        array_process!(row, row_vec, idx, InetValue);
                    }
                    _ if matches!(column.type_().kind(), Kind::Range(_)) => {
                        single_process!(row, row_vec, idx, RangeValue);
                    }
                    _ if matches!(
                        column.type_().kind(),
                        Kind::Array(elem) if matches!(elem.kind(), Kind::Range(_))
                    ) =>
                    {
                        array_process!(row, row_vec, idx, RangeValue);
                    }
                    _ if column.type_().name() == "geometry" => {
                        single_process!(row, row_vec, idx, GeometryValue);
                    }
                    _ if column.type_().name() == "_geometry" => {
                        array_process!(row, row_vec, idx, GeometryValue, delim = ':');
                    }
                    _ => {
                        return Err(PgDriverError::Decode(format!(
                            "the extended engine can't decode type {}",
                            column.type_().name()
                        )));
                    }
                }
            }
            output.push(row_vec);
        }

        // TODO: add column names only with option and rewrite this check after that
        if output.len() == 1 && output[0].is_empty() {
            output = vec![];
        }

        if output.is_empty() {
            match rows.rows_affected() {
                Some(rows) => Ok(DBOutput::StatementComplete(rows)),
                None => Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Any; stmt.columns().len()],
                    rows: vec![],
                }),
            }
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    async fn shutdown(&mut self) {
        self.shutdown().await;
    }

    fn engine_name(&self) -> &str {
        "pg-wire-extended"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: Command) -> std::io::Result<std::process::Output> {
        tokio::process::Command::from(command).output().await
    }

    fn error_sql_state(err: &Self::Error) -> Option<String> {
        err.code().map(|s| s.code().to_owned())
    }

    fn is_connection_error(err: &Self::Error) -> bool {
        err.is_connection_error()
    }
}

#[cfg(test)]
mod inet_tests {
    use super::*;

    fn decode(raw: &[u8]) -> String {
        InetValue::from_sql(&Type::INET, raw).unwrap().0
    }

    #[test]
    fn ipv4_with_mask() {
        // family=2, bits=24, is_cidr=0, nb=4, 192.168.0.1
        assert_eq!(decode(&[2, 24, 0, 4, 192, 168, 0, 1]), "192.168.0.1/24");
    }

    #[test]
    fn ipv4_full_mask_omitted() {
        // bits=32 (host mask) on inet -> no /32 suffix
        assert_eq!(decode(&[2, 32, 0, 4, 10, 0, 0, 1]), "10.0.0.1");
    }

    #[test]
    fn ipv6_full_mask_omitted() {
        let mut raw = vec![3, 128, 0, 16];
        raw.extend_from_slice(&[0; 15]);
        raw.push(1); // ::1
        assert_eq!(decode(&raw), "::1");
    }

    #[test]
    fn ipv6_with_mask_canonical_compression() {
        // 2001:db8::1/64
        let mut raw = vec![3, 64, 0, 16];
        raw.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8]);
        raw.extend_from_slice(&[0; 11]);
        raw.push(1);
        assert_eq!(decode(&raw), "2001:db8::1/64");
    }

    #[test]
    fn cidr_keeps_full_mask() {
        // is_cidr=1 -> always show /bits, even a full host mask
        assert_eq!(decode(&[2, 32, 1, 4, 10, 0, 0, 1]), "10.0.0.1/32");
        assert_eq!(decode(&[2, 24, 1, 4, 192, 168, 0, 0]), "192.168.0.0/24");
    }

    #[test]
    fn rejects_truncated() {
        assert!(InetValue::from_sql(&Type::INET, &[2, 24, 0]).is_err());
        assert!(InetValue::from_sql(&Type::INET, &[2, 24, 0, 4, 192, 168]).is_err());
    }
}

#[cfg(test)]
mod float_tests {
    use super::*;

    #[test]
    fn float4_like_postgres() {
        assert_eq!(float4_to_str(&999999.0), "999999");
        assert_eq!(float4_to_str(&1e6), "1e+06");
        assert_eq!(float4_to_str(&-1223768.0), "-1.223768e+06");
        assert_eq!(float4_to_str(&0.0001), "0.0001");
        assert_eq!(float4_to_str(&0.00001), "1e-05");
        assert_eq!(float4_to_str(&1.5), "1.5");
        assert_eq!(float4_to_str(&-0.0), "-0");
        assert_eq!(float4_to_str(&3.4e38), "3.4e+38");
        assert_eq!(float4_to_str(&f32::NAN), "NaN");
        assert_eq!(float4_to_str(&f32::NEG_INFINITY), "-Infinity");
    }

    #[test]
    fn float8_like_postgres() {
        assert_eq!(float8_to_str(&999999999999999.0), "999999999999999");
        assert_eq!(float8_to_str(&1e15), "1e+15");
        assert_eq!(float8_to_str(&1223768.0), "1223768");
        assert_eq!(float8_to_str(&0.1), "0.1");
        assert_eq!(float8_to_str(&1.5e-7), "1.5e-07");
        assert_eq!(float8_to_str(&-0.0), "-0");
        assert_eq!(
            float8_to_str(&1.7976931348623157e308),
            "1.7976931348623157e+308"
        );
        assert_eq!(float8_to_str(&f64::INFINITY), "Infinity");
    }
}
