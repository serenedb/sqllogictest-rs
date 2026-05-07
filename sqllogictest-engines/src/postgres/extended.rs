use std::fmt;
use std::fmt::Write;
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use futures::{pin_mut, StreamExt};
use pg_interval::Interval;
use postgres_types::{accepts, FromSql, Kind, ToSql, Type};
use rust_decimal::Decimal;
use sqllogictest::{DBOutput, DefaultColumnType};

use crate::postgres::error::PgDriverError;

use super::{Extended, Postgres, Result};

// Inspired by postgres_type::Array implementation of Display trait
fn print_array<T: std::fmt::Display>(
    arr: &postgres_array::Array<Option<T>>,
    fmt: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    print_array_helper(0, arr.dimensions(), &mut arr.iter(), fmt)
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
            write!(fmt, ",")?;
        }
        print_array_helper(depth + 1, dims, data, fmt)?;
    }
    write!(fmt, "}}")
}

struct ArrayFmt<'a, T>(&'a postgres_array::Array<Option<T>>);

impl<'a, T: std::fmt::Display> std::fmt::Display for ArrayFmt<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        print_array(self.0, f)
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
        if matches!(t.kind(), Kind::Array(_)) {
            if let Some(rendered) = parse_array_binary(bytes) {
                return Some(rendered);
            }
        }
    }
    if let Some(t) = ty {
        match t {
            Type::BOOL if bytes.len() == 1 => {
                return Some(if bytes[0] != 0 { "t".into() } else { "f".into() });
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
                return Some(f32::from_be_bytes(bytes.try_into().ok()?).to_string());
            }
            Type::FLOAT8 if bytes.len() == 8 => {
                return Some(f64::from_be_bytes(bytes.try_into().ok()?).to_string());
            }
            Type::TEXT | Type::VARCHAR | Type::NAME | Type::BPCHAR => {
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
                let d = Decimal::from_sql(&Type::NUMERIC, bytes).ok()?;
                return Some(d.to_string());
            }
            Type::TIMESTAMP => {
                let dt = NaiveDateTime::from_sql(&Type::TIMESTAMP, bytes).ok()?;
                return Some(format_naive_timestamp(dt));
            }
            Type::TIMESTAMPTZ => {
                let dt = DateTime::<chrono::Utc>::from_sql(&Type::TIMESTAMPTZ, bytes)
                    .ok()?;
                return Some(format_naive_timestamp(dt.naive_utc()) + "+00");
            }
            Type::DATE => {
                let d = NaiveDate::from_sql(&Type::DATE, bytes).ok()?;
                return Some(d.format("%Y-%m-%d").to_string());
            }
            Type::TIME => {
                let t = NaiveTime::from_sql(&Type::TIME, bytes).ok()?;
                return Some(format_naive_time(t));
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
    // Unknown OID (custom composite, enum, etc.): fall back to raw UTF-8
    // (covers enums sent as label bytes). Don't speculatively try array/
    // record binary parsers -- they're loose enough to match interval/
    // numeric/uuid bytes by accident.
    std::str::from_utf8(bytes).ok().map(|s| s.to_string())
}

// Format a NaiveDateTime as PG would (e.g. "2025-03-15 14:30:00", with
// fractional seconds appended only when nonzero, trailing zeros trimmed).
fn format_naive_timestamp(dt: NaiveDateTime) -> String {
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    let nanos = dt.nanosecond();
    if nanos == 0 {
        base
    } else {
        let frac = format!("{:09}", nanos);
        let frac = frac.trim_end_matches('0');
        format!("{base}.{frac}")
    }
}

fn format_naive_time(t: NaiveTime) -> String {
    let base = t.format("%H:%M:%S").to_string();
    let nanos = t.nanosecond();
    if nanos == 0 {
        base
    } else {
        let frac = format!("{:09}", nanos);
        let frac = frac.trim_end_matches('0');
        format!("{base}.{frac}")
    }
}

// Append a record field to `out`, applying PG's composite quoting rules
// (matches `record_out`). A field is quoted if empty, or contains `,`,
// `(`, `)`, `"`, `\`, or whitespace. Inside quotes, `"` becomes `""` and
// `\` becomes `\\` (each special char is doubled).
fn append_composite_field(out: &mut String, field: &str) {
    let needs_quote = field.is_empty()
        || field.chars().any(|c| {
            matches!(c, ',' | '(' | ')' | '"' | '\\') || c.is_whitespace()
        });
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

fn render_array_dim(
    out: &mut String,
    elems: &[Option<String>],
    dims: &[usize],
    idx: &mut usize,
) {
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
        unreachable!("Void type is not printable");
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
                write!(f, "{:o}", self.0)
            }
        }
    }
}

#[derive(Debug)]
struct Regtype(String);

impl<'a> FromSql<'a> for Regtype {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let oid = <u32 as FromSql>::from_sql(ty, raw)?;
        let name = match oid {
            16 => "boolean",
            17 => "bytea",
            18 => "character",
            20 => "bigint",
            21 => "smallint",
            23 => "integer",
            25 => "text",
            114 => "json",
            700 => "real",
            701 => "double precision",
            705 => "unknown",
            1043 => "character varying",
            1082 => "date",
            1114 => "timestamp without time zone",
            1184 => "timestamp with time zone",
            1186 => "interval",
            1700 => "numeric",
            2205 => "regclass",
            2206 => "regtype",
            2950 => "uuid",
            3802 => "jsonb",
            // Array types
            199 => "json[]",
            1000 => "boolean[]",
            1001 => "bytea[]",
            1002 => "character[]",
            1005 => "smallint[]",
            1007 => "integer[]",
            1009 => "text[]",
            1015 => "character varying[]",
            1016 => "bigint[]",
            1021 => "real[]",
            1022 => "double precision[]",
            1115 => "timestamp without time zone[]",
            1182 => "date[]",
            1185 => "timestamp with time zone[]",
            1187 => "interval[]",
            1231 => "numeric[]",
            2951 => "uuid[]",
            other => return Ok(Regtype(other.to_string())),
        };
        Ok(Regtype(name.to_string()))
    }

    accepts!(REGTYPE);
}

impl fmt::Display for Regtype {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// It's required to use postgres_array::Array instead of Vec.
// See: https://github.com/rust-postgres/rust-postgres/issues/1186
macro_rules! array_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<postgres_array::Array<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let dimensions: Vec<postgres_array::Dimension> = value.dimensions().into();
                let data: Vec<Option<String>> = value
                    .into_iter()
                    .map(|opt| opt.map(|v| format!("{}", v)))
                    .collect();
                let value = postgres_array::Array::from_parts(data, dimensions);
                let value = ArrayFmt(&value);
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
                let value = ArrayFmt(&value);
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
                let value = ArrayFmt(&value);
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

fn float4_to_str(value: &f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if *value == f32::INFINITY {
        "Infinity".to_string()
    } else if *value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        value.to_string()
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
        value.to_string()
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
                        single_process!(row, row_vec, idx, Decimal);
                    }
                    Type::NUMERIC_ARRAY => {
                        array_process!(row, row_vec, idx, Decimal);
                    }
                    Type::DATE => {
                        single_process!(row, row_vec, idx, NaiveDate);
                    }
                    Type::DATE_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveDate);
                    }
                    Type::TIME => {
                        single_process!(row, row_vec, idx, NaiveTime);
                    }
                    Type::TIME_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveTime);
                    }
                    Type::TIMESTAMP => {
                        single_process!(row, row_vec, idx, NaiveDateTime);
                    }
                    Type::TIMESTAMP_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveDateTime);
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
                    Type::REGTYPE => {
                        single_process!(row, row_vec, idx, Regtype);
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
                        single_process!(
                            self,
                            row,
                            row_vec,
                            idx,
                            DateTime<chrono::Utc>,
                            TIMESTAMPTZ
                        );
                    }
                    Type::TIMESTAMPTZ_ARRAY => {
                        array_process!(self, row, row_vec, idx, DateTime<chrono::Utc>, TIMESTAMPTZ);
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
                    Type::UUID => {
                        single_process!(row, row_vec, idx, UuidValue);
                    }
                    Type::UUID_ARRAY => {
                        array_process!(row, row_vec, idx, UuidValue);
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
                    _ => {
                        todo!("Don't support {} type now.", column.type_().name())
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
}
