//! The versioned frame: one message of a [`Channel`](crate::Channel) that carries a logical
//! channel's message, a datagram or a close, with a sequence number and a checksum.
//!
//! Both ends are configured for it; nothing negotiates it (see `docs/frame.md`). All integers are
//! little-endian.
//!
//! ```text
//! offset  size  field
//! 0       1     version        1
//! 1       1     kind           0 message, 1 datagram, 2 close
//! 2       2     channel        the logical channel's number
//! 4       4     sequence       see below
//! 8       n     payload        empty for a close
//! 8+n     4     checksum       CRC-32C of bytes 0 to 8+n
//! ```
//!
//! The frame has no length of its own: the message that carries it ends where it ends. Over a byte
//! stream the stream's codec ([`LengthPrefix`](super::LengthPrefix), or a serial line's COBS)
//! delimits it.
//!
//! **Sequence.** Per logical channel and per direction, the sender counts its message and close
//! frames from 0, wrapping at 2^32, so a receiver finds a reliable frame that went missing. Datagram
//! frames carry a separate count, of the datagrams written on that channel, which only measures loss.

use alloc::vec::Vec;
use core::fmt;

/// The version this module writes and reads.
pub const VERSION: u8 = 1;
/// The bytes before the payload.
pub const HEADER: usize = 8;
/// The bytes after the payload.
pub const TRAILER: usize = 4;
/// The bytes a frame adds to its payload.
pub const OVERHEAD: usize = HEADER + TRAILER;

/// What a frame carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Kind {
    /// One message of the logical channel, reliable and in order.
    Message,
    /// One datagram of the logical channel.
    Datagram,
    /// The sender closed the logical channel: no more of its messages follow.
    Close,
}

impl Kind {
    /// The byte that names this kind on the wire.
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            Self::Message => 0,
            Self::Datagram => 1,
            Self::Close => 2,
        }
    }

    /// The kind a wire byte names, or `None` for one this version does not define.
    #[must_use]
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Message),
            1 => Some(Self::Datagram),
            2 => Some(Self::Close),
            _ => None,
        }
    }
}

/// One frame, decoded or about to be encoded.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Frame {
    /// What it carries.
    pub kind: Kind,
    /// The logical channel's number.
    pub channel: u16,
    /// The sender's count, as the module's table says.
    pub sequence: u32,
    /// The message or datagram; empty for a close.
    pub payload: Vec<u8>,
}

/// Why bytes are not a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// Fewer bytes than the header and the checksum.
    Short,
    /// A version other than [`VERSION`]: a peer that is not framed, or framed otherwise.
    Version(u8),
    /// The checksum does not match: the frame was damaged on the way.
    Checksum,
    /// A kind byte this version does not define, in a frame whose checksum matched.
    Kind(u8),
    /// A close that carries a payload.
    Payload,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Short => f.write_str("a frame is shorter than its header and checksum"),
            Self::Version(version) => write!(f, "a frame of version {version}, not {VERSION}"),
            Self::Checksum => f.write_str("a frame's checksum does not match"),
            Self::Kind(kind) => write!(f, "a frame of unknown kind {kind}"),
            Self::Payload => f.write_str("a close frame carries a payload"),
        }
    }
}

impl core::error::Error for DecodeError {}

impl Frame {
    /// A frame to encode.
    #[must_use]
    pub fn new(kind: Kind, channel: u16, sequence: u32, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            channel,
            sequence,
            payload: payload.into(),
        }
    }

    /// The bytes of this frame: header, payload, checksum.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(OVERHEAD + self.payload.len());
        bytes.push(VERSION);
        bytes.push(self.kind.byte());
        bytes.extend_from_slice(&self.channel.to_le_bytes());
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        let checksum = crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// The frame `bytes` hold, which must be exactly one.
    ///
    /// The checks run in this order: length, version, checksum, kind, payload. A version is
    /// judged before the checksum because a frame of another version may be laid out otherwise.
    ///
    /// # Errors
    ///
    /// [`DecodeError`], by the first check that fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < OVERHEAD {
            return Err(DecodeError::Short);
        }
        if bytes[0] != VERSION {
            return Err(DecodeError::Version(bytes[0]));
        }
        let (body, trailer) = bytes.split_at(bytes.len() - TRAILER);
        let declared = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        if crc32c(body) != declared {
            return Err(DecodeError::Checksum);
        }
        let kind = Kind::from_byte(body[1]).ok_or(DecodeError::Kind(body[1]))?;
        let payload = &body[HEADER..];
        if kind == Kind::Close && !payload.is_empty() {
            return Err(DecodeError::Payload);
        }
        Ok(Self {
            kind,
            channel: u16::from_le_bytes([body[2], body[3]]),
            sequence: u32::from_le_bytes([body[4], body[5], body[6], body[7]]),
            payload: payload.to_vec(),
        })
    }
}

const TABLE: [u32; 256] = {
    let mut table = [0_u32; 256];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ 0x82F6_3B78
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
};

/// CRC-32C (Castagnoli: polynomial `0x1EDC6F41`, reflected, initial value and final xor
/// `0xFFFFFFFF`). The check value of `b"123456789"` is `0xE3069283`.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc = TABLE[usize::from((crc as u8) ^ byte)] ^ (crc >> 8);
    }
    !crc
}

/// The tagged form: one byte before a message or a datagram, nothing else, for a wire whose codec
/// is a legacy one (see `docs/frame.md`).
pub mod tagged {
    /// The tag of a message.
    pub const MESSAGE: u8 = 0;
    /// The tag of a datagram.
    pub const DATAGRAM: u8 = 1;
}
