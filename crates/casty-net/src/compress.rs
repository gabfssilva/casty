//! The compressors a connection may negotiate, compiled into the wheel.
//!
//! Each one writes the format the Python implementation writes with `zstandard`, `lz4.frame` and `zlib`, so a node
//! of either side reads what the other compressed.

use std::io::{Read, Write};

use crate::frame::ProtocolError;

/// The compressors this build has, in the order they are preferred.
pub const PREFERENCE: [Name; 3] = [Name::Zstd, Name::Lz4, Name::Zlib];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Name {
    Zstd,
    Lz4,
    Zlib,
}

impl Name {
    #[must_use]
    pub fn of(written: &str) -> Option<Self> {
        match written {
            "zstd" => Some(Self::Zstd),
            "lz4" => Some(Self::Lz4),
            "zlib" => Some(Self::Zlib),
            _ => None,
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
            Self::Zlib => "zlib",
        }
    }

    #[must_use]
    pub fn compress(self, data: &[u8]) -> Vec<u8> {
        match self {
            // The frame declares the size it holds, which is what the other side checks before it allocates.
            Self::Zstd => {
                zstd::bulk::compress(data, 3).expect("zstd compression of a slice cannot fail")
            }
            Self::Lz4 => {
                let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
                encoder
                    .write_all(data)
                    .expect("writing to a vector cannot fail");
                encoder.finish().expect("finishing a vector cannot fail")
            }
            Self::Zlib => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(1));
                encoder
                    .write_all(data)
                    .expect("writing to a vector cannot fail");
                encoder.finish().expect("finishing a vector cannot fail")
            }
        }
    }

    /// `data` decompressed, refused when it is corrupt or holds more than `limit` bytes.
    pub fn decompress(self, data: &[u8], limit: usize) -> Result<Vec<u8>, ProtocolError> {
        match self {
            // The size is read from the frame header before anything is allocated for it.
            Self::Zstd => {
                let held = zstd::zstd_safe::get_frame_content_size(data).ok().flatten();
                if held.is_none_or(|held| held > limit as u64) {
                    return Err(ProtocolError::new(format!(
                        "compressed frame without a size up to {limit} bytes"
                    )));
                }
                zstd::bulk::decompress(data, limit)
                    .map_err(|error| ProtocolError::new(format!("corrupt zstd frame: {error}")))
            }
            Self::Lz4 => {
                let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
                let mut out = Vec::new();
                bounded(&mut decoder, &mut out, limit, "lz4")?;
                Ok(out)
            }
            Self::Zlib => {
                let mut decoder = flate2::read::ZlibDecoder::new(data);
                let mut out = Vec::new();
                bounded(&mut decoder, &mut out, limit, "zlib")?;
                Ok(out)
            }
        }
    }
}

/// Read `from` into `out`, refusing a stream that holds more than `limit` bytes without reading it all.
fn bounded(
    from: &mut impl std::io::Read,
    out: &mut Vec<u8>,
    limit: usize,
    named: &str,
) -> Result<(), ProtocolError> {
    let mut held = from.take(limit as u64 + 1);
    held.read_to_end(out)
        .map_err(|error| ProtocolError::new(format!("corrupt {named} frame: {error}")))?;
    if out.len() > limit {
        return Err(ProtocolError::new(format!(
            "{named} frame truncated or larger than {limit} bytes"
        )));
    }
    Ok(())
}

/// The first compressor of the peer's offer that this side also offers.
#[must_use]
pub fn chosen(offer: &[Name], ours: &[Name]) -> Option<Name> {
    offer.iter().copied().find(|name| ours.contains(name))
}

#[cfg(test)]
mod tests {
    use super::{Name, PREFERENCE, chosen};

    fn payload() -> Vec<u8> {
        b"the same line over and over, which is what makes a payload worth compressing. "
            .repeat(2_000)
    }

    #[test]
    fn every_compressor_reads_back_what_it_wrote() {
        for name in PREFERENCE {
            let data = payload();
            let small = name.compress(&data);
            assert!(small.len() < data.len() / 4, "{name:?} barely compressed");
            assert_eq!(
                name.decompress(&small, data.len()).unwrap(),
                data,
                "{name:?}"
            );
            assert_eq!(
                name.decompress(&name.compress(&[]), 16).unwrap(),
                Vec::<u8>::new(),
                "{name:?}"
            );
        }
    }

    #[test]
    fn a_frame_that_holds_more_than_the_limit_is_refused_before_it_is_built() {
        for name in PREFERENCE {
            let data = payload();
            let small = name.compress(&data);
            let error = name.decompress(&small, data.len() - 1).unwrap_err();
            assert!(error.to_string().contains("bytes"), "{name:?}: {error}");
        }
    }

    #[test]
    fn a_corrupt_frame_is_refused() {
        for name in PREFERENCE {
            let mut small = name.compress(&payload());
            let at = small.len() / 2;
            small[at] ^= 0xff;
            small.truncate(at + 1);
            assert!(name.decompress(&small, 1 << 20).is_err(), "{name:?}");
        }
    }

    #[test]
    fn the_peers_first_common_choice_wins() {
        let offer = [Name::Lz4, Name::Zstd];
        assert_eq!(chosen(&offer, &PREFERENCE), Some(Name::Lz4));
        assert_eq!(chosen(&offer, &[Name::Zstd]), Some(Name::Zstd));
        assert_eq!(chosen(&offer, &[Name::Zlib]), None);
        assert_eq!(chosen(&[], &PREFERENCE), None);
    }
}
