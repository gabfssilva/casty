//! msgpack as casty writes it, on the wire and in the store.
//!
//! The markers are part of the format, not an implementation detail: a value is always written with the smallest
//! marker that holds it and a float always as a double, so the same value has the same bytes on every node.

// Every narrowing cast here writes a value its match arm has already bounded to the width it is cast to.
#![allow(clippy::cast_possible_truncation)]

use core::fmt;

/// What a value is, which is all a union needs to pick the alternative that reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    None,
    Bool,
    Int,
    Float,
    Str,
    Bytes,
    List,
    Map,
}

impl Kind {
    /// What a union calls it when no alternative reads it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bool => "bool",
            Self::Int => "int",
            Self::Float => "float",
            Self::Str => "str",
            Self::Bytes => "bytes",
            Self::List => "list",
            Self::Map => "map",
        }
    }

    /// The Python type a payload of this kind decodes to, which is what a mismatch names.
    #[must_use]
    pub fn python(self) -> &'static str {
        match self {
            Self::None => "NoneType",
            Self::Bool => "bool",
            Self::Int => "int",
            Self::Float => "float",
            Self::Str => "str",
            Self::Bytes => "bytes",
            Self::List => "list",
            Self::Map => "dict",
        }
    }
}

/// A Python `int` on the wire: `packb` writes the smallest marker, unsigned for zero and up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Int {
    Unsigned(u64),
    Signed(i64),
}

impl Int {
    #[must_use]
    pub fn as_i64(self) -> Option<i64> {
        match self {
            Self::Unsigned(value) => i64::try_from(value).ok(),
            Self::Signed(value) => Some(value),
        }
    }

    #[must_use]
    pub fn as_f64(self) -> f64 {
        match self {
            #[allow(clippy::cast_precision_loss)]
            Self::Unsigned(value) => value as f64,
            #[allow(clippy::cast_precision_loss)]
            Self::Signed(value) => value as f64,
        }
    }
}

/// A payload that is not the msgpack this library writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Malformed {
    /// The payload ended in the middle of a value.
    Truncated,
    /// A marker no value of this library uses, such as an extension type.
    Marker(u8),
    /// A string that is not UTF-8, which `unpackb` would refuse as well.
    Utf8,
    /// Bytes left over after the value, which means the payload is not one value.
    Trailing,
}

impl fmt::Display for Malformed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("the payload ends in the middle of a value"),
            Self::Marker(marker) => write!(
                formatter,
                "msgpack marker {marker:#04x} is not a value of this library"
            ),
            Self::Utf8 => formatter.write_str("a string is not UTF-8"),
            Self::Trailing => formatter.write_str("the payload holds more than one value"),
        }
    }
}

impl core::error::Error for Malformed {}

pub type Result<T> = core::result::Result<T, Malformed>;

// --- writing ---

pub fn write_nil(out: &mut Vec<u8>) {
    out.push(0xc0);
}

pub fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 0xc3 } else { 0xc2 });
}

pub fn write_int(out: &mut Vec<u8>, value: Int) {
    match value {
        Int::Unsigned(value) => write_uint(out, value),
        Int::Signed(value) if value >= 0 => {
            #[allow(clippy::cast_sign_loss)]
            write_uint(out, value as u64);
        }
        Int::Signed(value) => write_negative(out, value),
    }
}

/// `packb` writes every float as a double, so a value read back is the value written.
pub fn write_f64(out: &mut Vec<u8>, value: f64) {
    out.push(0xcb);
    out.extend_from_slice(&value.to_be_bytes());
}

pub fn write_str(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    match bytes.len() {
        len @ 0..=31 => out.push(0xa0 | len as u8),
        len @ 32..=0xff => {
            out.push(0xd9);
            out.push(len as u8);
        }
        len @ 0x100..=0xffff => {
            out.push(0xda);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            out.push(0xdb);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }
    out.extend_from_slice(bytes);
}

pub fn write_bin(out: &mut Vec<u8>, value: &[u8]) {
    match value.len() {
        len @ 0..=0xff => {
            out.push(0xc4);
            out.push(len as u8);
        }
        len @ 0x100..=0xffff => {
            out.push(0xc5);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            out.push(0xc6);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }
    out.extend_from_slice(value);
}

pub fn write_array_len(out: &mut Vec<u8>, len: usize) {
    write_count(out, len, 0x90, 0xdc, 0xdd);
}

pub fn write_map_len(out: &mut Vec<u8>, len: usize) {
    write_count(out, len, 0x80, 0xde, 0xdf);
}

/// The length of an array or a map, which have a fixed form of up to fifteen and then 16 and 32 bit ones.
fn write_count(out: &mut Vec<u8>, len: usize, fixed: u8, sixteen: u8, thirty_two: u8) {
    match len {
        len @ 0..=15 => out.push(fixed | len as u8),
        len @ 16..=0xffff => {
            out.push(sixteen);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            out.push(thirty_two);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }
}

fn write_uint(out: &mut Vec<u8>, value: u64) {
    match value {
        0..=0x7f => out.push(value as u8),
        0x80..=0xff => {
            out.push(0xcc);
            out.push(value as u8);
        }
        0x100..=0xffff => {
            out.push(0xcd);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(0xce);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            out.push(0xcf);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}

fn write_negative(out: &mut Vec<u8>, value: i64) {
    match value {
        -0x20..=-1 => {
            #[allow(clippy::cast_sign_loss)]
            out.push(value as i8 as u8);
        }
        -0x80..=-0x21 => {
            out.push(0xd0);
            #[allow(clippy::cast_sign_loss)]
            out.push(value as i8 as u8);
        }
        -0x8000..=-0x81 => {
            out.push(0xd1);
            out.extend_from_slice(&(value as i16).to_be_bytes());
        }
        -0x8000_0000..=-0x8001 => {
            out.push(0xd2);
            out.extend_from_slice(&(value as i32).to_be_bytes());
        }
        _ => {
            out.push(0xd3);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}

// --- reading ---

/// A payload read one value at a time, guided by the schema instead of by the bytes.
///
/// Nothing is built that the schema did not ask for: a field the reader does not know is skipped over the bytes, and
/// a value it knows goes straight to whoever asked for it.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    #[must_use]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }

    /// What the next value is, without consuming it.
    pub fn kind(&self) -> Result<Kind> {
        Ok(match self.marker()? {
            0x00..=0x7f | 0xcc..=0xd3 | 0xe0..=0xff => Kind::Int,
            0x80..=0x8f | 0xde | 0xdf => Kind::Map,
            0x90..=0x9f | 0xdc | 0xdd => Kind::List,
            0xa0..=0xbf | 0xd9..=0xdb => Kind::Str,
            0xc0 => Kind::None,
            0xc2 | 0xc3 => Kind::Bool,
            0xc4..=0xc6 => Kind::Bytes,
            0xca | 0xcb => Kind::Float,
            marker => return Err(Malformed::Marker(marker)),
        })
    }

    /// Whether the payload has been read to the end.
    #[must_use]
    pub fn done(&self) -> bool {
        self.at >= self.data.len()
    }

    pub fn finish(self) -> Result<()> {
        if self.done() {
            Ok(())
        } else {
            Err(Malformed::Trailing)
        }
    }

    pub fn read_nil(&mut self) -> Result<()> {
        match self.take_marker()? {
            0xc0 => Ok(()),
            marker => Err(Malformed::Marker(marker)),
        }
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        match self.take_marker()? {
            0xc2 => Ok(false),
            0xc3 => Ok(true),
            marker => Err(Malformed::Marker(marker)),
        }
    }

    pub fn read_int(&mut self) -> Result<Int> {
        let marker = self.take_marker()?;
        Ok(match marker {
            0x00..=0x7f => Int::Unsigned(u64::from(marker)),
            #[allow(clippy::cast_possible_wrap)]
            0xe0..=0xff => Int::Signed(i64::from(marker as i8)),
            0xcc => Int::Unsigned(u64::from(self.take::<1>()?[0])),
            0xcd => Int::Unsigned(u64::from(u16::from_be_bytes(*self.take::<2>()?))),
            0xce => Int::Unsigned(u64::from(u32::from_be_bytes(*self.take::<4>()?))),
            0xcf => Int::Unsigned(u64::from_be_bytes(*self.take::<8>()?)),
            #[allow(clippy::cast_possible_wrap)]
            0xd0 => Int::Signed(i64::from(self.take::<1>()?[0] as i8)),
            0xd1 => Int::Signed(i64::from(i16::from_be_bytes(*self.take::<2>()?))),
            0xd2 => Int::Signed(i64::from(i32::from_be_bytes(*self.take::<4>()?))),
            0xd3 => Int::Signed(i64::from_be_bytes(*self.take::<8>()?)),
            marker => return Err(Malformed::Marker(marker)),
        })
    }

    /// A float of either width: `packb` writes doubles, and a peer that writes singles is still read.
    pub fn read_f64(&mut self) -> Result<f64> {
        match self.take_marker()? {
            0xca => Ok(f64::from(f32::from_be_bytes(*self.take::<4>()?))),
            0xcb => Ok(f64::from_be_bytes(*self.take::<8>()?)),
            marker => Err(Malformed::Marker(marker)),
        }
    }

    pub fn read_str(&mut self) -> Result<&'a str> {
        let len = match self.take_marker()? {
            marker @ 0xa0..=0xbf => usize::from(marker & 0x1f),
            0xd9 => usize::from(self.take::<1>()?[0]),
            0xda => usize::from(u16::from_be_bytes(*self.take::<2>()?)),
            0xdb => u32::from_be_bytes(*self.take::<4>()?) as usize,
            marker => return Err(Malformed::Marker(marker)),
        };
        core::str::from_utf8(self.slice(len)?).map_err(|_| Malformed::Utf8)
    }

    pub fn read_bin(&mut self) -> Result<&'a [u8]> {
        let len = match self.take_marker()? {
            0xc4 => usize::from(self.take::<1>()?[0]),
            0xc5 => usize::from(u16::from_be_bytes(*self.take::<2>()?)),
            0xc6 => u32::from_be_bytes(*self.take::<4>()?) as usize,
            marker => return Err(Malformed::Marker(marker)),
        };
        self.slice(len)
    }

    pub fn read_array_len(&mut self) -> Result<usize> {
        let len = match self.take_marker()? {
            marker @ 0x90..=0x9f => usize::from(marker & 0x0f),
            0xdc => usize::from(u16::from_be_bytes(*self.take::<2>()?)),
            0xdd => u32::from_be_bytes(*self.take::<4>()?) as usize,
            marker => return Err(Malformed::Marker(marker)),
        };
        self.holding(len)
    }

    pub fn read_map_len(&mut self) -> Result<usize> {
        let len = match self.take_marker()? {
            marker @ 0x80..=0x8f => usize::from(marker & 0x0f),
            0xde => usize::from(u16::from_be_bytes(*self.take::<2>()?)),
            0xdf => u32::from_be_bytes(*self.take::<4>()?) as usize,
            marker => return Err(Malformed::Marker(marker)),
        };
        self.holding(len)
    }

    /// A count of values the rest of the payload can hold, at a byte each at least.
    ///
    /// The count is what a reader sizes its buffer by, so five bytes that claim four billion values would otherwise
    /// ask for gigabytes before the first missing value is noticed.
    fn holding(&self, len: usize) -> Result<usize> {
        if len > self.data.len().saturating_sub(self.at) {
            return Err(Malformed::Truncated);
        }
        Ok(len)
    }

    /// Step over the next value, whatever it is: a field this version of the type does not know.
    pub fn skip(&mut self) -> Result<()> {
        let mut pending = 1usize;
        while pending > 0 {
            pending -= 1;
            match self.kind()? {
                Kind::None => self.read_nil()?,
                Kind::Bool => {
                    self.read_bool()?;
                }
                Kind::Int => {
                    self.read_int()?;
                }
                Kind::Float => {
                    self.read_f64()?;
                }
                Kind::Str => {
                    self.read_str()?;
                }
                Kind::Bytes => {
                    self.read_bin()?;
                }
                Kind::List => pending += self.read_array_len()?,
                Kind::Map => pending += self.read_map_len()? * 2,
            }
        }
        Ok(())
    }

    fn marker(&self) -> Result<u8> {
        self.data.get(self.at).copied().ok_or(Malformed::Truncated)
    }

    fn take_marker(&mut self) -> Result<u8> {
        let marker = self.marker()?;
        self.at += 1;
        Ok(marker)
    }

    fn take<const N: usize>(&mut self) -> Result<&'a [u8; N]> {
        let taken = self.slice(N)?;
        taken.try_into().map_err(|_| Malformed::Truncated)
    }

    fn slice(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(len).ok_or(Malformed::Truncated)?;
        let taken = self.data.get(self.at..end).ok_or(Malformed::Truncated)?;
        self.at = end;
        Ok(taken)
    }
}

#[cfg(test)]
mod tests {
    use super::{Int, Kind, Malformed, Reader};

    /// What a write puts out, in hex, which the tests hold against what `msgpack.packb` writes for the same value.
    fn written(write: impl FnOnce(&mut Vec<u8>)) -> String {
        let mut out = Vec::new();
        write(&mut out);
        hex(&out)
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut text, byte| {
            use core::fmt::Write;
            let _ = write!(text, "{byte:02x}");
            text
        })
    }

    #[test]
    fn writes_integers_at_every_boundary_as_packb_does() {
        let cases: [(Int, &str); 22] = [
            (Int::Unsigned(0), "00"),
            (Int::Unsigned(127), "7f"),
            (Int::Unsigned(128), "cc80"),
            (Int::Unsigned(255), "ccff"),
            (Int::Unsigned(256), "cd0100"),
            (Int::Unsigned(65535), "cdffff"),
            (Int::Unsigned(65536), "ce00010000"),
            (Int::Unsigned(0xffff_ffff), "ceffffffff"),
            (Int::Unsigned(0x1_0000_0000), "cf0000000100000000"),
            (Int::Unsigned(u64::MAX), "cfffffffffffffffff"),
            (Int::Signed(0), "00"),
            (Int::Signed(127), "7f"),
            (Int::Signed(128), "cc80"),
            (Int::Signed(-1), "ff"),
            (Int::Signed(-32), "e0"),
            (Int::Signed(-33), "d0df"),
            (Int::Signed(-128), "d080"),
            (Int::Signed(-129), "d1ff7f"),
            (Int::Signed(-32768), "d18000"),
            (Int::Signed(-32769), "d2ffff7fff"),
            (Int::Signed(-2_147_483_648), "d280000000"),
            (Int::Signed(i64::MIN), "d38000000000000000"),
        ];
        for (value, expected) in cases {
            assert_eq!(
                written(|out| super::write_int(out, value)),
                expected,
                "{value:?}"
            );
        }
    }

    #[test]
    fn writes_the_other_scalars_as_packb_does() {
        assert_eq!(written(super::write_nil), "c0");
        assert_eq!(written(|out| super::write_bool(out, true)), "c3");
        assert_eq!(written(|out| super::write_bool(out, false)), "c2");
        assert_eq!(
            written(|out| super::write_f64(out, 0.0)),
            "cb0000000000000000"
        );
        assert_eq!(
            written(|out| super::write_f64(out, core::f64::consts::PI)),
            "cb400921fb54442d18"
        );
        assert_eq!(
            written(|out| super::write_f64(out, -1.5)),
            "cbbff8000000000000"
        );
    }

    #[test]
    fn writes_lengths_at_every_boundary_as_packb_does() {
        let prefix = |out: &Vec<u8>, take: usize| hex(&out[..take]);
        for (len, expected, take) in [
            (0, "a0", 1),
            (3, "a3", 1),
            (31, "bf", 1),
            (32, "d920", 2),
            (255, "d9ff", 2),
        ] {
            let mut out = Vec::new();
            super::write_str(&mut out, &"x".repeat(len));
            assert_eq!(prefix(&out, take), expected, "str of {len}");
            assert_eq!(out.len(), take + len);
        }
        let mut out = Vec::new();
        super::write_str(&mut out, &"x".repeat(256));
        assert_eq!(prefix(&out, 3), "da0100");

        for (len, expected, take) in [(0, "c400", 2), (255, "c4ff", 2), (256, "c50100", 3)] {
            let mut out = Vec::new();
            super::write_bin(&mut out, &vec![b'x'; len]);
            assert_eq!(prefix(&out, take), expected, "bin of {len}");
            assert_eq!(out.len(), take + len);
        }

        for (len, expected) in [
            (0usize, "90"),
            (15, "9f"),
            (16, "dc0010"),
            (65535, "dcffff"),
            (65536, "dd00010000"),
        ] {
            assert_eq!(
                written(|out| super::write_array_len(out, len)),
                expected,
                "array of {len}"
            );
        }
        for (len, expected) in [
            (0usize, "80"),
            (15, "8f"),
            (16, "de0010"),
            (65536, "df00010000"),
        ] {
            assert_eq!(
                written(|out| super::write_map_len(out, len)),
                expected,
                "map of {len}"
            );
        }
    }

    #[test]
    fn reads_back_what_it_writes() {
        let mut out = Vec::new();
        super::write_array_len(&mut out, 6);
        super::write_nil(&mut out);
        super::write_bool(&mut out, true);
        super::write_int(&mut out, Int::Signed(-129));
        super::write_f64(&mut out, 1.25);
        super::write_str(&mut out, "olá");
        super::write_bin(&mut out, b"\x00\xff");

        let mut reader = Reader::new(&out);
        assert_eq!(reader.kind(), Ok(Kind::List));
        assert_eq!(reader.read_array_len(), Ok(6));
        assert_eq!(reader.read_nil(), Ok(()));
        assert_eq!(reader.read_bool(), Ok(true));
        assert_eq!(reader.read_int(), Ok(Int::Signed(-129)));
        assert_eq!(reader.read_f64(), Ok(1.25));
        assert_eq!(reader.read_str(), Ok("olá"));
        assert_eq!(reader.read_bin(), Ok(&b"\x00\xff"[..]));
        assert_eq!(reader.finish(), Ok(()));
    }

    #[test]
    fn skips_a_value_of_any_shape() {
        let mut out = Vec::new();
        super::write_map_len(&mut out, 2);
        super::write_str(&mut out, "unknown");
        super::write_array_len(&mut out, 2);
        super::write_map_len(&mut out, 1);
        super::write_str(&mut out, "deep");
        super::write_bin(&mut out, b"payload");
        super::write_int(&mut out, Int::Unsigned(7));
        super::write_str(&mut out, "known");
        super::write_int(&mut out, Int::Unsigned(1));

        let mut reader = Reader::new(&out);
        assert_eq!(reader.read_map_len(), Ok(2));
        assert_eq!(reader.read_str(), Ok("unknown"));
        assert_eq!(reader.skip(), Ok(()));
        assert_eq!(reader.read_str(), Ok("known"));
        assert_eq!(reader.read_int(), Ok(Int::Unsigned(1)));
        assert_eq!(reader.finish(), Ok(()));
    }

    #[test]
    fn refuses_what_is_not_one_value_of_this_library() {
        assert_eq!(Reader::new(&[]).kind(), Err(Malformed::Truncated));
        assert_eq!(Reader::new(&[0xc1]).kind(), Err(Malformed::Marker(0xc1)));
        assert_eq!(Reader::new(&[0xd4]).kind(), Err(Malformed::Marker(0xd4)));
        let mut truncated = Reader::new(&[0xcd, 0x01]);
        assert_eq!(truncated.read_int(), Err(Malformed::Truncated));

        let mut reader = Reader::new(&[0xa3, 0xff, 0xff, 0xff]);
        assert_eq!(reader.read_str(), Err(Malformed::Utf8));

        let mut trailing = Reader::new(&[0xc0, 0xc0]);
        assert_eq!(trailing.read_nil(), Ok(()));
        assert_eq!(trailing.finish(), Err(Malformed::Trailing));
    }

    #[test]
    fn refuses_a_count_the_rest_of_the_payload_cannot_hold() {
        let mut huge = Reader::new(&[0xdd, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(huge.read_array_len(), Err(Malformed::Truncated));
        let mut huge = Reader::new(&[0xdf, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(huge.read_map_len(), Err(Malformed::Truncated));
        let mut short = Reader::new(&[0x92, 0xc0]);
        assert_eq!(short.read_array_len(), Err(Malformed::Truncated));

        let mut full = Reader::new(&[0x92, 0xc0, 0xc0]);
        assert_eq!(full.read_array_len(), Ok(2));
        let mut empty = Reader::new(&[0x80]);
        assert_eq!(empty.read_map_len(), Ok(0));
    }
}
