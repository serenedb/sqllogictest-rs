use std::fmt::Write;
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use chrono::NaiveTime;
use futures::{pin_mut, StreamExt};
use pg_interval::Interval;
use postgres_types::{FromSql, Kind, PgLsn, ToSql, Type};
use sqllogictest::{DBOutput, DefaultColumnType};

use crate::postgres::error::PgDriverError;

use super::{Extended, Postgres, Result};

// A value of any type as the server sent it: tokio-postgres asks for the binary format.
struct Raw<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(raw))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

// The text of a non-NULL value of type `ty` from its binary form, as the simple engine prints
// it: one decoder for top-level values, array elements, range bounds and record fields, so a
// type prints the same wherever it appears. None when the bytes are no value of the type, or
// the type's text is unknown here.
fn render(ty: &Type, raw: &[u8]) -> Option<String> {
    let text = match *ty {
        Type::BOOL => match raw {
            [0] => "f".into(),
            [_] => "t".into(),
            _ => return None,
        },
        Type::CHAR => match raw {
            [0] => String::new(),
            [byte @ 0x01..=0x7f] => char::from(*byte).to_string(),
            [byte] => format!("\\{byte:03o}"),
            _ => return None,
        },
        Type::INT2 => i16::from_be_bytes(raw.try_into().ok()?).to_string(),
        Type::INT4 => i32::from_be_bytes(raw.try_into().ok()?).to_string(),
        Type::INT8 => i64::from_be_bytes(raw.try_into().ok()?).to_string(),
        Type::OID => u32::from_be_bytes(raw.try_into().ok()?).to_string(),
        Type::FLOAT4 => float4_to_str(&f32::from_be_bytes(raw.try_into().ok()?)),
        Type::FLOAT8 => float8_to_str(&f64::from_be_bytes(raw.try_into().ok()?)),
        Type::NUMERIC => numeric_to_string(raw)?,
        Type::TEXT
        | Type::VARCHAR
        | Type::BPCHAR
        | Type::NAME
        | Type::JSON
        | Type::XML
        | Type::REFCURSOR
        | Type::UNKNOWN => std::str::from_utf8(raw).ok()?.into(),
        Type::JSONB => serde_json::Value::from_sql(ty, raw).ok()?.to_string(),
        // It assumes that 'BYTEA_OUTPUT' variable is set to 'hex' (default value)
        Type::BYTEA => raw.iter().fold("\\x".to_string(), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        }),
        // i32::MAX and i32::MIN days are ±infinity.
        Type::DATE => match i32::from_be_bytes(raw.try_into().ok()?) {
            i32::MAX => "infinity".into(),
            i32::MIN => "-infinity".into(),
            days => format_date(days.into()),
        },
        Type::TIME => format_time(read_micros(raw)?),
        Type::TIMETZ => render_timetz(raw)?,
        Type::TIMESTAMP => format_timestamp(read_micros(raw)?, ""),
        // TIMESTAMPTZ renders in UTC with the fixed "+00" suffix.
        Type::TIMESTAMPTZ => format_timestamp(read_micros(raw)?, "+00"),
        Type::INTERVAL => Interval::from_sql(ty, raw).ok()?.to_postgres(),
        Type::UUID => {
            match <&[u8; 16]>::try_from(raw) {
                Ok(bytes) => bytes.iter().enumerate().fold(
                    String::with_capacity(36),
                    |mut out, (i, byte)| {
                        if matches!(i, 4 | 6 | 8 | 10) {
                            out.push('-');
                        }
                        let _ = write!(out, "{byte:02x}");
                        out
                    },
                ),
                Err(_) => std::str::from_utf8(raw).ok()?.into(),
            }
        }
        Type::BIT | Type::VARBIT => render_bits(raw)?,
        Type::INET | Type::CIDR => render_inet(raw)?,
        Type::MACADDR | Type::MACADDR8 => raw
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(":"),
        Type::MONEY => render_money(i64::from_be_bytes(raw.try_into().ok()?)),
        Type::TID => {
            let (block, offset) = raw.split_at_checked(4)?;
            format!(
                "({},{})",
                u32::from_be_bytes(block.try_into().ok()?),
                u16::from_be_bytes(offset.try_into().ok()?)
            )
        }
        // int2vector and oidvector travel in the binary array format but print as
        // space-separated numbers.
        Type::INT2_VECTOR | Type::OID_VECTOR => {
            let element = if *ty == Type::INT2_VECTOR {
                Type::INT2
            } else {
                Type::OID
            };
            parse_array(raw)?
                .elements
                .into_iter()
                .map(|bytes| render(&element, bytes?))
                .collect::<Option<Vec<_>>>()?
                .join(" ")
        }
        Type::PG_LSN => PgLsn::from_sql(ty, raw).ok()?.to_string(),
        Type::VOID => String::new(),
        Type::POINT
        | Type::LINE
        | Type::LSEG
        | Type::BOX
        | Type::PATH
        | Type::POLYGON
        | Type::CIRCLE => render_geometric(ty, raw)?,
        Type::RECORD => render_record_or_text(raw)?,
        // tokio-postgres classifies RECORD_ARRAY as Kind::Pseudo, not Kind::Array(Record).
        Type::RECORD_ARRAY => match render_array(&Type::RECORD, raw) {
            Some(text) => text,
            None => std::str::from_utf8(raw).ok()?.into(),
        },
        _ => match ty.kind() {
            // An enum label travels as its UTF-8 text in both formats.
            Kind::Enum(_) => std::str::from_utf8(raw).ok()?.into(),
            Kind::Composite(_) => render_record_or_text(raw)?,
            Kind::Array(element) => render_array(element, raw)?,
            Kind::Range(element) => render_range(element, raw)?,
            Kind::Multirange(element) => render_multirange(element, raw)?,
            // The binary format carries the WKB bytes; the text format spells the same bytes as
            // upper-case hex, so render them identically.
            _ if ty.name() == "geometry" => raw.iter().map(|byte| format!("{byte:02X}")).collect(),
            _ => return None,
        },
    };
    Some(text)
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn read_micros(raw: &[u8]) -> Option<i64> {
    Some(i64::from_be_bytes(raw.try_into().ok()?))
}

fn read_be_i32(buf: &[u8], pos: &mut usize) -> Option<i32> {
    let v = i32::from_be_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?);
    *pos += 4;
    Some(v)
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

// TIME WITH TIME ZONE: 12-byte binary (i64 microseconds-of-day + i32 zone in
// seconds west of UTC), rendered as serened does, e.g. "11:30:00.123456+00".
fn render_timetz(raw: &[u8]) -> Option<String> {
    let (micros, zone) = raw.split_at_checked(8)?;
    let micros = i64::from_be_bytes(micros.try_into().ok()?);
    let zone = i32::from_be_bytes(zone.try_into().ok()?);
    let secs = (micros / 1_000_000) as u32;
    let nanos = ((micros % 1_000_000) * 1_000) as u32;
    let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)?;
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
    Some(format!("{time}{suffix}"))
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

// BIT / BIT VARYING: a 4-byte big-endian bit length followed by ceil(len/8)
// bytes, MSB-first, rendered as a '0'/'1' string (BIT(3) '101' -> "101").
fn render_bits(raw: &[u8]) -> Option<String> {
    let (length, bytes) = raw.split_at_checked(4)?;
    let length = usize::try_from(i32::from_be_bytes(length.try_into().ok()?)).ok()?;
    (bytes.len() >= length.div_ceil(8)).then(|| {
        (0..length)
            .map(|i| match (bytes[i / 8] >> (7 - i % 8)) & 1 {
                1 => '1',
                _ => '0',
            })
            .collect()
    })
}

// INET / CIDR (see PG `network_send`): u8 family, u8 netmask bits, u8 is_cidr,
// u8 address byte count (4 or 16), then the address in network byte order.
// Rendered via std::net for canonical text, with the `/bits` suffix omitted
// for a full host mask on `inet` (matching PG's text output), always kept for
// `cidr`.
fn render_inet(raw: &[u8]) -> Option<String> {
    let [_, bits, is_cidr, length, address @ ..] = raw else {
        return None;
    };
    let (ip, full_bits): (std::net::IpAddr, u8) = match length {
        4 => (<[u8; 4]>::try_from(address.get(..4)?).ok()?.into(), 32),
        16 => (<[u8; 16]>::try_from(address.get(..16)?).ok()?.into(), 128),
        _ => return None,
    };
    Some(if *is_cidr != 0 || *bits != full_bits {
        format!("{ip}/{bits}")
    } else {
        ip.to_string()
    })
}

fn render_money(value: i64) -> String {
    let cents = value.unsigned_abs();
    let units = (cents / 100).to_string();
    let mut grouped = String::with_capacity(units.len() + units.len() / 3);
    for (i, digit) in units.chars().enumerate() {
        if i > 0 && (units.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    let sign = if value < 0 { "-" } else { "" };
    format!("{sign}${grouped}.{:02}", cents % 100)
}

// Range values (see PG `range_send`): a flags byte, then the lower and upper
// bounds that are present, each an int32 length followed by the element's
// binary value. Rendered like PG's `range_out`.
const RANGE_EMPTY: u8 = 0x01;
const RANGE_LB_INC: u8 = 0x02;
const RANGE_UB_INC: u8 = 0x04;
const RANGE_LB_INF: u8 = 0x08;
const RANGE_UB_INF: u8 = 0x10;

fn render_range(element: &Type, raw: &[u8]) -> Option<String> {
    let (&flags, mut rest) = raw.split_first()?;
    if flags & RANGE_EMPTY != 0 {
        return (rest.is_empty()).then(|| "empty".into());
    }
    let mut bound = |infinite: u8| -> Option<String> {
        if flags & infinite != 0 {
            return Some(String::new());
        }
        let mut pos = 0;
        let len = usize::try_from(read_be_i32(rest, &mut pos)?).ok()?;
        let bytes = rest.get(pos..pos + len)?;
        rest = &rest[pos + len..];
        Some(escape_range_bound(&render(element, bytes)?))
    };
    let lower = bound(RANGE_LB_INF)?;
    let upper = bound(RANGE_UB_INF)?;
    let open = if flags & RANGE_LB_INC != 0 { '[' } else { '(' };
    let close = if flags & RANGE_UB_INC != 0 { ']' } else { ')' };
    rest.is_empty()
        .then(|| format!("{open}{lower},{upper}{close}"))
}

// Multirange values (see PG `multirange_send`): an int32 range count, then
// each range as an int32 length followed by its `range_send` bytes. Rendered
// like PG's `multirange_out`.
fn render_multirange(element: &Type, raw: &[u8]) -> Option<String> {
    let mut pos = 0;
    let count = read_be_i32(raw, &mut pos)?;
    let mut out = String::from('{');
    for i in 0..count {
        if i > 0 {
            out.push(',');
        }
        let len = usize::try_from(read_be_i32(raw, &mut pos)?).ok()?;
        out.push_str(&render_range(element, raw.get(pos..pos + len)?)?);
        pos += len;
    }
    out.push('}');
    (pos == raw.len()).then_some(out)
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

// Geometric values (see PG `point_send` and its siblings): float8 coordinates,
// a closed-flag byte and an int32 point count in front of a path's points, an
// int32 point count in front of a polygon's. Rendered like PG's `point_out`,
// `line_out`, `lseg_out`, `box_out`, `path_out`, `poly_out` and `circle_out`.
fn render_geometric(ty: &Type, raw: &[u8]) -> Option<String> {
    let coordinates = |bytes: &[u8], count: usize| -> Option<Vec<f64>> {
        (bytes.len() == count * 8).then(|| {
            bytes
                .chunks_exact(8)
                .map(|chunk| f64::from_be_bytes(chunk.try_into().unwrap()))
                .collect()
        })
    };
    let points = |values: &[f64]| -> String {
        values
            .chunks_exact(2)
            .map(|point| {
                format!(
                    "({},{})",
                    float8out_to_str(&point[0]),
                    float8out_to_str(&point[1])
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    let counted = |bytes: &[u8]| -> Option<Vec<f64>> {
        let mut pos = 0;
        let count = usize::try_from(read_be_i32(bytes, &mut pos)?).ok()?;
        coordinates(&bytes[pos..], 2 * count)
    };
    match *ty {
        Type::POINT => Some(points(&coordinates(raw, 2)?)),
        Type::LINE => {
            let values = coordinates(raw, 3)?;
            Some(format!(
                "{{{},{},{}}}",
                float8out_to_str(&values[0]),
                float8out_to_str(&values[1]),
                float8out_to_str(&values[2])
            ))
        }
        Type::LSEG => Some(format!("[{}]", points(&coordinates(raw, 4)?))),
        Type::BOX => Some(points(&coordinates(raw, 4)?)),
        Type::CIRCLE => {
            let values = coordinates(raw, 3)?;
            Some(format!(
                "<{},{}>",
                points(&values[..2]),
                float8out_to_str(&values[2])
            ))
        }
        Type::PATH => {
            let (&closed, rest) = raw.split_first()?;
            let (open, close) = if closed != 0 { ('(', ')') } else { ('[', ']') };
            Some(format!("{open}{}{close}", points(&counted(rest)?)))
        }
        Type::POLYGON => Some(format!("({})", points(&counted(raw)?))),
        _ => None,
    }
}

// Composite/record values (see PG `record_send`): an int32 field count, then
// per field { int32 type_oid, int32 length (-1 = NULL), `length` bytes }.
// Rendered like PG's `record_out`: a field is quoted if empty, or holding `,`,
// `(`, `)`, `"`, `\` or whitespace; inside quotes `"` and `\` are doubled.
fn render_record(raw: &[u8]) -> Option<String> {
    let mut pos = 0;
    let count = usize::try_from(read_be_i32(raw, &mut pos)?).ok()?;
    let mut out = String::from('(');
    for i in 0..count {
        if i > 0 {
            out.push(',');
        }
        let oid = read_be_i32(raw, &mut pos)? as u32;
        let len = read_be_i32(raw, &mut pos)?;
        if len == -1 {
            continue;
        }
        let bytes = raw.get(pos..pos + usize::try_from(len).ok()?)?;
        pos += bytes.len();
        let field = match Type::from_oid(oid) {
            Some(ty) => render(&ty, bytes)?,
            // A type tokio-postgres doesn't know by its OID: a user-defined
            // composite or enum.
            None => render_record_or_text(bytes)?,
        };
        let needs_quote = field.is_empty()
            || field
                .chars()
                .any(|c| matches!(c, ',' | '(' | ')' | '"' | '\\') || c.is_ascii_whitespace());
        if !needs_quote {
            out.push_str(&field);
            continue;
        }
        out.push('"');
        for c in field.chars() {
            if c == '"' || c == '\\' {
                out.push(c);
            }
            out.push(c);
        }
        out.push('"');
    }
    out.push(')');
    (pos == raw.len()).then_some(out)
}

// The server may send a composite as text even when the client asked for the
// binary format, and an enum label inside a record reads as text, so the
// bytes that aren't a binary record are their text.
fn render_record_or_text(raw: &[u8]) -> Option<String> {
    match render_record(raw) {
        Some(text) => Some(text),
        None => Some(std::str::from_utf8(raw).ok()?.into()),
    }
}

// PG binary array format (see PG `array_send`): int32 ndim, int32 flags,
// int32 elemtype, then per-dim {int32 size, int32 lbound}, then per-element
// {int32 len (-1=NULL), bytes}.
struct ArrayValue<'a> {
    dims: Vec<usize>,
    elements: Vec<Option<&'a [u8]>>,
}

fn parse_array(raw: &[u8]) -> Option<ArrayValue<'_>> {
    let mut pos = 0;
    let ndim = usize::try_from(read_be_i32(raw, &mut pos)?).ok()?;
    let _flags = read_be_i32(raw, &mut pos)?;
    let _elemtype = read_be_i32(raw, &mut pos)?;
    let mut dims = Vec::with_capacity(ndim);
    for _ in 0..ndim {
        dims.push(usize::try_from(read_be_i32(raw, &mut pos)?).ok()?);
        let _lbound = read_be_i32(raw, &mut pos)?;
    }
    let count = if dims.is_empty() {
        0
    } else {
        dims.iter().product()
    };
    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_be_i32(raw, &mut pos)?;
        if len == -1 {
            elements.push(None);
            continue;
        }
        let bytes = raw.get(pos..pos + usize::try_from(len).ok()?)?;
        pos += bytes.len();
        elements.push(Some(bytes));
    }
    (pos == raw.len()).then_some(ArrayValue { dims, elements })
}

fn render_array(element: &Type, raw: &[u8]) -> Option<String> {
    let array = parse_array(raw)?;
    let texts = array
        .elements
        .iter()
        .map(|bytes| match bytes {
            Some(bytes) => render(element, bytes).map(Some),
            None => Some(None),
        })
        .collect::<Option<Vec<_>>>()?;
    // The type's delimiter: a semicolon for box, a colon for PostGIS's geometry.
    let delim = if *element == Type::BOX {
        ';'
    } else if element.name() == "geometry" {
        ':'
    } else {
        ','
    };
    let mut out = String::new();
    format_array(&mut out, &array.dims, &mut texts.iter(), delim);
    Some(out)
}

// Prints an array like PG's `array_out`, with ndim levels of nesting
// (`{{1,2},{3,4}}`). An element is quoted if empty, equal to "NULL" ignoring
// case, or holding the delimiter, `{`, `}`, `"`, `\` or whitespace; inside
// quotes `"` and `\` are escaped with a backslash.
fn format_array<'a>(
    out: &mut String,
    dims: &[usize],
    texts: &mut impl Iterator<Item = &'a Option<String>>,
    delim: char,
) {
    out.push('{');
    if let Some((&size, inner)) = dims.split_first() {
        for i in 0..size {
            if i > 0 {
                out.push(delim);
            }
            if !inner.is_empty() {
                format_array(out, inner, texts, delim);
                continue;
            }
            let Some(Some(text)) = texts.next() else {
                out.push_str("NULL");
                continue;
            };
            let needs_quote = text.is_empty()
                || text.eq_ignore_ascii_case("null")
                || text.chars().any(|c| {
                    matches!(c, '{' | '}' | '"' | '\\') || c == delim || c.is_ascii_whitespace()
                });
            if !needs_quote {
                out.push_str(text);
                continue;
            }
            out.push('"');
            for c in text.chars() {
                if c == '"' || c == '\\' {
                    out.push('\\');
                }
                out.push(c);
            }
            out.push('"');
        }
    }
    out.push('}');
}

// A value the engine can't print fails its own record, not the run.
fn decode_error(ty: &Type) -> PgDriverError {
    PgDriverError::Decode(format!(
        "the extended engine can't decode type {}",
        ty.name()
    ))
}

fn placeholder_if_empty(value: String) -> String {
    if value.is_empty() {
        "(empty)".into()
    } else {
        value
    }
}

fn split_exponential(exponential: &str) -> (&str, i32) {
    let (mantissa, exponent) = exponential
        .split_once('e')
        .expect("exponential float formatting has an exponent");
    (
        mantissa,
        exponent.parse().expect("float exponent is a number"),
    )
}

fn scientific_to_str(mantissa: &str, exponent: i32) -> String {
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}

fn shortest_float_to_str(exponential: String, fixed: String, fixed_below: i32) -> String {
    let (mantissa, exponent) = split_exponential(&exponential);
    if (-4..fixed_below).contains(&exponent) {
        fixed
    } else {
        scientific_to_str(mantissa, exponent)
    }
}

fn shortest_or_integral_to_str(fixed: String, exponential: String, integral: bool) -> String {
    if integral {
        return fixed;
    }
    let (mantissa, exponent) = split_exponential(&exponential);
    let scientific = scientific_to_str(mantissa, exponent);
    if scientific.len() < fixed.len() {
        scientific
    } else {
        fixed
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
        shortest_or_integral_to_str(
            value.to_string(),
            format!("{value:e}"),
            value.abs() < u64::MAX as f32 && value.fract() == 0.0,
        )
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
        shortest_or_integral_to_str(
            value.to_string(),
            format!("{value:e}"),
            value.abs() < u64::MAX as f64 && value.fract() == 0.0,
        )
    }
}

fn float8out_to_str(value: &f64) -> String {
    if value.is_finite() {
        shortest_float_to_str(format!("{value:e}"), value.to_string(), 15)
    } else {
        float8_to_str(value)
    }
}

impl Postgres<Extended> {
    // Intervals and reg* values print as the server prints them: pg_interval's
    // to_postgres() is not PG's interval text, and only the server knows the
    // names behind reg* OIDs.
    async fn server_text(&mut self, sql: &str, value: &(dyn ToSql + Sync)) -> Result<String> {
        Ok(self.client()?.query_one(sql, &[value]).await?.get(0))
    }

    async fn interval_text(&mut self, raw: &[u8]) -> Result<String> {
        let interval =
            Interval::from_sql(&Type::INTERVAL, raw).map_err(|_| decode_error(&Type::INTERVAL))?;
        self.server_text("select ($1::INTERVAL)::varchar", &interval)
            .await
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
                let ty = column.type_();
                let value: Option<Raw> = row.get(idx);
                let Some(Raw(raw)) = value else {
                    row_vec.push("NULL".to_string());
                    continue;
                };
                let text = match *ty {
                    Type::INTERVAL => self.interval_text(raw).await?,
                    Type::INTERVAL_ARRAY => {
                        let array = parse_array(raw).ok_or_else(|| decode_error(ty))?;
                        let mut texts = Vec::with_capacity(array.elements.len());
                        for bytes in array.elements {
                            texts.push(match bytes {
                                Some(bytes) => Some(self.interval_text(bytes).await?),
                                None => None,
                            });
                        }
                        let mut out = String::new();
                        format_array(&mut out, &array.dims, &mut texts.iter(), ',');
                        out
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
                        let oid = u32::from_sql(ty, raw).map_err(|_| decode_error(ty))?;
                        let sql = format!("select ($1::oid)::{}::varchar", ty.name());
                        self.server_text(&sql, &oid).await?
                    }
                    _ => render(ty, raw).ok_or_else(|| decode_error(ty))?,
                };
                row_vec.push(placeholder_if_empty(text));
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
        render(&Type::INET, raw).unwrap()
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
        assert!(render(&Type::INET, &[2, 24, 0]).is_none());
        assert!(render(&Type::INET, &[2, 24, 0, 4, 192, 168]).is_none());
    }
}

#[cfg(test)]
mod float_tests {
    use super::*;

    #[test]
    fn float4_integral_or_shortest() {
        assert_eq!(float4_to_str(&999999.0), "999999");
        assert_eq!(float4_to_str(&1e6), "1000000");
        assert_eq!(float4_to_str(&-1223768.0), "-1223768");
        assert_eq!(float4_to_str(&0.0001), "1e-04");
        assert_eq!(float4_to_str(&0.00001), "1e-05");
        assert_eq!(float4_to_str(&1.5), "1.5");
        assert_eq!(float4_to_str(&123456.7), "123456.7");
        assert_eq!(float4_to_str(&f32::NAN), "NaN");
        assert_eq!(float4_to_str(&f32::NEG_INFINITY), "-Infinity");
    }

    #[test]
    fn float8_integral_or_shortest() {
        assert_eq!(float8_to_str(&1e15), "1000000000000000");
        assert_eq!(float8_to_str(&1223768.0), "1223768");
        assert_eq!(float8_to_str(&0.1), "0.1");
        assert_eq!(float8_to_str(&1000000000000000.5), "1000000000000000.5");
        assert_eq!(
            float8_to_str(&1.0000000005838672e-7),
            "1.0000000005838672e-07"
        );
        assert_eq!(float8_to_str(&1e20), "1e+20");
        assert_eq!(float8_to_str(&f64::INFINITY), "Infinity");
    }

    #[test]
    fn float8out_like_postgres() {
        assert_eq!(float8out_to_str(&999999999999999.0), "999999999999999");
        assert_eq!(float8out_to_str(&1e15), "1e+15");
        assert_eq!(float8out_to_str(&1223768.0), "1223768");
        assert_eq!(float8out_to_str(&0.1), "0.1");
        assert_eq!(float8out_to_str(&1.5e-7), "1.5e-07");
        assert_eq!(float8out_to_str(&-0.0), "-0");
        assert_eq!(
            float8out_to_str(&1.7976931348623157e308),
            "1.7976931348623157e+308"
        );
        assert_eq!(float8out_to_str(&f64::INFINITY), "Infinity");
        assert_eq!(float8out_to_str(&f64::NAN), "NaN");
    }
}

#[cfg(test)]
mod geometric_tests {
    use super::*;

    fn bytes(values: &[f64]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_be_bytes()).collect()
    }

    fn counted(prefix: &[u8], values: &[f64]) -> Vec<u8> {
        let mut raw = prefix.to_vec();
        raw.extend_from_slice(&((values.len() / 2) as i32).to_be_bytes());
        raw.extend(bytes(values));
        raw
    }

    fn decode(ty: &Type, raw: &[u8]) -> String {
        render(ty, raw).unwrap()
    }

    #[test]
    fn fixed_shapes() {
        assert_eq!(decode(&Type::POINT, &bytes(&[1.5, -2.0])), "(1.5,-2)");
        assert_eq!(decode(&Type::LINE, &bytes(&[1.0, -1.0, 0.0])), "{1,-1,0}");
        assert_eq!(
            decode(&Type::LSEG, &bytes(&[0.0, 0.0, 1.0, 2.0])),
            "[(0,0),(1,2)]"
        );
        assert_eq!(
            decode(&Type::BOX, &bytes(&[2.0, 2.0, 0.0, 0.0])),
            "(2,2),(0,0)"
        );
        assert_eq!(
            decode(&Type::CIRCLE, &bytes(&[1.0, 1.0, f64::INFINITY])),
            "<(1,1),Infinity>"
        );
    }

    #[test]
    fn paths_and_polygons() {
        let points = [0.0, 0.0, 1.0, 1e20];
        assert_eq!(
            decode(&Type::PATH, &counted(&[1], &points)),
            "((0,0),(1,1e+20))"
        );
        assert_eq!(
            decode(&Type::PATH, &counted(&[0], &points)),
            "[(0,0),(1,1e+20)]"
        );
        assert_eq!(
            decode(&Type::POLYGON, &counted(&[], &points)),
            "((0,0),(1,1e+20))"
        );
    }

    #[test]
    fn rejects_malformed() {
        assert!(render(&Type::POINT, &bytes(&[1.0])).is_none());
        assert!(render(&Type::POLYGON, &counted(&[], &[1.0, 2.0])[..10]).is_none());
    }
}
