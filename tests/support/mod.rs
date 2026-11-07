//! Helpers the test binaries share: a wait, and the length-prefix cases every test of the framing runs.
#![allow(dead_code, reason = "each test binary uses some of these")]

use coqui_transport::frame::LengthPrefix;

/// A wait long enough for any loopback exchange, short enough to fail a hung test.
pub const WAIT: Option<core::time::Duration> = Some(core::time::Duration::from_secs(10));

/// One length-prefix configuration, with an oracle for its header that shares no code with the codec.
pub struct Preset {
    pub name: String,
    pub codec: LengthPrefix,
    pub width: usize,
    pub empty_allowed: bool,
    prefix: fn(usize) -> Vec<u8>,
}

impl Preset {
    /// The header of a frame whose body is `length` bytes.
    pub fn header(&self, length: usize) -> Vec<u8> {
        (self.prefix)(length)
    }

    /// A header followed by `body`.
    pub fn frame(&self, body: &[u8]) -> Vec<u8> {
        [self.header(body.len()), body.to_vec()].concat()
    }
}

fn u8_header(length: usize) -> Vec<u8> {
    vec![u8::try_from(length).unwrap()]
}
fn u16_be(length: usize) -> Vec<u8> {
    u16::try_from(length).unwrap().to_be_bytes().to_vec()
}
fn u16_le(length: usize) -> Vec<u8> {
    u16::try_from(length).unwrap().to_le_bytes().to_vec()
}
fn u32_be(length: usize) -> Vec<u8> {
    u32::try_from(length).unwrap().to_be_bytes().to_vec()
}
fn u32_le(length: usize) -> Vec<u8> {
    u32::try_from(length).unwrap().to_le_bytes().to_vec()
}

/// A preset's name, codec, prefix width and header oracle, before the empty rule is applied.
type Configuration = (&'static str, LengthPrefix, usize, fn(usize) -> Vec<u8>);

/// Every width, both byte orders, and both empty rules.
pub fn presets() -> Vec<Preset> {
    let mut all = Vec::new();
    for empty_allowed in [true, false] {
        let rule = if empty_allowed {
            "empty allowed"
        } else {
            "empty refused"
        };
        let set = |codec: LengthPrefix| {
            if empty_allowed {
                codec.allow_empty()
            } else {
                codec.refuse_empty()
            }
        };
        let configurations: [Configuration; 5] = [
            ("u8", LengthPrefix::u8(), 1, u8_header),
            ("u16 big-endian", LengthPrefix::u16_be(), 2, u16_be),
            ("u16 little-endian", LengthPrefix::u16_le(), 2, u16_le),
            ("u32 big-endian", LengthPrefix::u32_be(), 4, u32_be),
            ("u32 little-endian", LengthPrefix::u32_le(), 4, u32_le),
        ];
        for (name, codec, width, prefix) in configurations {
            all.push(Preset {
                name: format!("{name}, {rule}"),
                codec: set(codec),
                width,
                empty_allowed,
                prefix,
            });
        }
    }
    all
}

/// A body sent under a limit, and the frame that must result, or `None` when it must be refused.
pub struct EncodeCase {
    pub name: &'static str,
    pub max: usize,
    pub body: Vec<u8>,
    pub wire: Option<Vec<u8>>,
}

/// Bytes written in chunks to a decoder under a limit, and what must come out.
pub struct DecodeCase {
    pub name: &'static str,
    pub max: usize,
    pub chunks: Vec<Vec<u8>>,
    pub messages: Vec<Vec<u8>>,
    /// The stream is refused after `messages`.
    pub error: bool,
    /// The bytes end inside a frame.
    pub truncated: bool,
}

pub fn encode_cases(preset: &Preset) -> Vec<EncodeCase> {
    let case = |name, max, body: &[u8], fits: bool| EncodeCase {
        name,
        max,
        body: body.to_vec(),
        wire: fits.then(|| preset.frame(body)),
    };
    let mut cases = vec![
        case("a short body", 10, b"hello", true),
        case("exactly the limit", 5, b"hello", true),
        case("one byte over the limit", 4, b"hello", false),
        case("binary bytes", 10, &[0, 0xff, 10, 13, 26], true),
        case("an empty body", 10, b"", preset.empty_allowed),
        case(
            "an empty body under a limit of 0",
            0,
            b"",
            preset.empty_allowed,
        ),
    ];
    // The widest body the prefix can declare, and one more, under a limit that allows both.
    let widest = match preset.width {
        1 => 255,
        2 => 65_535,
        _ => 300,
    };
    cases.push(case(
        "the widest body the prefix declares",
        1 << 20,
        &vec![7; widest],
        true,
    ));
    if preset.width < 4 {
        cases.push(case(
            "one more than the prefix declares",
            1 << 20,
            &vec![7; widest + 1],
            false,
        ));
    }
    cases
}

pub fn decode_cases(preset: &Preset) -> Vec<DecodeCase> {
    let hello = preset.frame(b"hello");
    let world = preset.frame(b"world");
    let both = [hello.clone(), world].concat();
    let width = preset.width;
    let oversized = preset.header(11);
    let one = |bytes: &[u8]| vec![bytes.to_vec()];
    let case = |name, max, chunks: Vec<Vec<u8>>, messages: &[&[u8]], error, truncated| DecodeCase {
        name,
        max,
        chunks,
        messages: messages.iter().map(|m| m.to_vec()).collect(),
        error,
        truncated,
    };
    let mut cases = vec![
        case(
            "one frame whole",
            10,
            one(&hello),
            &[b"hello"],
            false,
            false,
        ),
        case(
            "split inside the prefix",
            10,
            vec![hello[..width - 1].to_vec(), hello[width - 1..].to_vec()],
            &[b"hello"],
            false,
            false,
        ),
        case(
            "split inside the body",
            10,
            vec![hello[..width + 2].to_vec(), hello[width + 2..].to_vec()],
            &[b"hello"],
            false,
            false,
        ),
        case(
            "one byte at a time",
            10,
            hello.iter().map(|byte| vec![*byte]).collect(),
            &[b"hello"],
            false,
            false,
        ),
        case(
            "two frames coalesced",
            10,
            one(&both),
            &[b"hello", b"world"],
            false,
            false,
        ),
        case(
            "coalesced then split",
            10,
            vec![both[..width + 7].to_vec(), both[width + 7..].to_vec()],
            &[b"hello", b"world"],
            false,
            false,
        ),
        case(
            "an incomplete frame waits",
            10,
            one(&hello[..width + 2]),
            &[],
            false,
            true,
        ),
        case(
            "exactly the limit",
            5,
            one(&hello),
            &[b"hello"],
            false,
            false,
        ),
        case("an oversized header", 10, one(&oversized), &[], true, false),
        case(
            "a good frame before a bad header",
            10,
            one(&[hello.clone(), oversized.clone()].concat()),
            &[b"hello"],
            true,
            false,
        ),
        case(
            "a good frame before a bad header, in a later chunk",
            10,
            vec![hello.clone(), oversized.clone()],
            &[b"hello"],
            true,
            false,
        ),
        case(
            "a bad header stops what follows it",
            10,
            one(&[oversized, hello.clone()].concat()),
            &[],
            true,
            false,
        ),
        case(
            "a header of all ones",
            10,
            one(&vec![0xff; width]),
            &[],
            true,
            false,
        ),
        case(
            "invalid UTF-8 is a message like any other",
            10,
            one(&preset.frame(&[0xff])),
            &[&[0xff]],
            false,
            false,
        ),
    ];
    if width > 1 {
        // One byte of a prefix is a torn frame; with a one-byte prefix there is no such thing.
        cases.push(case(
            "an incomplete prefix waits",
            10,
            one(&hello[..width - 1]),
            &[],
            false,
            true,
        ));
    }
    let empty = preset.frame(b"");
    if preset.empty_allowed {
        cases.push(case(
            "empty messages between others",
            10,
            one(&[empty.clone(), preset.frame(b"a"), empty.clone()].concat()),
            &[b"", b"a", b""],
            false,
            false,
        ));
        cases.push(case(
            "an empty message under a limit of 0",
            0,
            one(&empty),
            &[b""],
            false,
            false,
        ));
    } else {
        cases.push(case("an empty frame", 10, one(&empty), &[], true, false));
        cases.push(case(
            "a good frame before an empty one",
            10,
            one(&[hello, empty].concat()),
            &[b"hello"],
            true,
            false,
        ));
    }
    cases
}
