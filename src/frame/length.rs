//! The length-prefix codec: a fixed-width length, then the message.

use alloc::vec::Vec;

use super::{Codec, Decode, FrameError};

/// The most room a decoder keeps once it holds nothing, so one large message does not keep its
/// buffer for the decoder's life.
const RETAINED: usize = 64 * 1024;

/// How many bytes the length takes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Width {
    One = 1,
    Two = 2,
    Four = 4,
}

impl Width {
    const fn bytes(self) -> usize {
        self as usize
    }

    /// The largest length the prefix can declare.
    const fn largest(self) -> u32 {
        match self {
            Self::One => u8::MAX as u32,
            Self::Two => u16::MAX as u32,
            Self::Four => u32::MAX,
        }
    }
}

/// A framing of a fixed-width length followed by the message: the width, the byte order and
/// whether an empty message is a message are the codec's settings, each named by a constructor,
/// never guessed.
///
/// A codec built by [`u32_be`](Self::u32_be) allows empty messages; [`refuse_empty`](Self::refuse_empty)
/// refuses them both ways: [`encode`](Codec::encode) fails and a header declaring one fails the
/// decoder. A message the width cannot declare (256 bytes on a `u8` prefix) is refused as one over the
/// limit. Both ends of a connection are configured for the same codec; nothing in the bytes says
/// which.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[must_use]
pub struct LengthPrefix {
    width: Width,
    big_endian: bool,
    empty_allowed: bool,
}

impl LengthPrefix {
    const fn new(width: Width, big_endian: bool) -> Self {
        Self {
            width,
            big_endian,
            empty_allowed: true,
        }
    }

    /// A one-byte length: messages of at most 255 bytes.
    pub const fn u8() -> Self {
        Self::new(Width::One, true)
    }

    /// A two-byte big-endian length: messages of at most 65535 bytes.
    pub const fn u16_be() -> Self {
        Self::new(Width::Two, true)
    }

    /// A two-byte little-endian length: messages of at most 65535 bytes.
    pub const fn u16_le() -> Self {
        Self::new(Width::Two, false)
    }

    /// A four-byte big-endian length.
    pub const fn u32_be() -> Self {
        Self::new(Width::Four, true)
    }

    /// A four-byte little-endian length.
    pub const fn u32_le() -> Self {
        Self::new(Width::Four, false)
    }

    /// This codec with empty messages allowed (as every constructor leaves it).
    pub const fn allow_empty(mut self) -> Self {
        self.empty_allowed = true;
        self
    }

    /// This codec refusing empty messages: [`encode`](Codec::encode) fails on one and a header
    /// declaring one fails the decoder.
    pub const fn refuse_empty(mut self) -> Self {
        self.empty_allowed = false;
        self
    }

    /// How many bytes the length takes: 1, 2 or 4.
    #[must_use]
    pub const fn width(&self) -> usize {
        self.width.bytes()
    }

    /// Whether the length is written most significant byte first.
    #[must_use]
    pub const fn is_big_endian(&self) -> bool {
        self.big_endian
    }

    /// Whether an empty message is a message.
    #[must_use]
    pub const fn empty_allowed(&self) -> bool {
        self.empty_allowed
    }

    /// The length a header declares.
    fn declared(&self, header: &[u8]) -> u32 {
        let mut four = [0_u8; 4];
        if self.big_endian {
            four[4 - header.len()..].copy_from_slice(header);
            u32::from_be_bytes(four)
        } else {
            four[..header.len()].copy_from_slice(header);
            u32::from_le_bytes(four)
        }
    }
}

impl Codec for LengthPrefix {
    type Decoder = LengthDecoder;

    fn encode(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
        if (message.is_empty() && !self.empty_allowed) || message.len() > maximum {
            return Err(FrameError::Size);
        }
        let size = u32::try_from(message.len())
            .ok()
            .filter(|size| *size <= self.width.largest())
            .ok_or(FrameError::Size)?;
        let width = self.width();
        let mut out = Vec::with_capacity(width + message.len());
        if self.big_endian {
            out.extend_from_slice(&size.to_be_bytes()[4 - width..]);
        } else {
            out.extend_from_slice(&size.to_le_bytes()[..width]);
        }
        out.extend_from_slice(message);
        Ok(out)
    }

    fn decoder(&self, maximum: usize) -> LengthDecoder {
        LengthDecoder::new(*self, maximum)
    }
}

/// The incremental decoder of a [`LengthPrefix`].
#[derive(Debug)]
pub struct LengthDecoder {
    prefix: LengthPrefix,
    /// `held[start..]` is what no returned frame accounts for.
    held: Vec<u8>,
    start: usize,
    maximum: usize,
    failed: bool,
}

impl LengthDecoder {
    /// A decoder of `prefix`'s frames accepting messages of at most `maximum` bytes.
    #[must_use]
    pub const fn new(prefix: LengthPrefix, maximum: usize) -> Self {
        Self {
            prefix,
            held: Vec::new(),
            start: 0,
            maximum,
            failed: false,
        }
    }
}

impl Decode for LengthDecoder {
    fn set_maximum(&mut self, maximum: usize) {
        self.maximum = maximum;
    }

    fn push(&mut self, part: &[u8]) {
        if self.failed {
            return;
        }
        // Returned frames are dropped before more arrive, so what is held is at most one partial
        // frame plus what was pushed since.
        if self.start > 0 {
            self.held.drain(..self.start);
            self.start = 0;
        }
        self.held.extend_from_slice(part);
    }

    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        if self.failed {
            return Err(FrameError::Size);
        }
        let width = self.prefix.width();
        let available = &self.held[self.start..];
        let Some(header) = available.get(..width) else {
            return Ok(None);
        };
        let declared = self.prefix.declared(header);
        let size = usize::try_from(declared).unwrap_or(usize::MAX);
        if (size == 0 && !self.prefix.empty_allowed) || size > self.maximum {
            self.failed = true;
            self.held = Vec::new();
            self.start = 0;
            return Err(FrameError::Size);
        }
        let Some(body) = available[width..].get(..size) else {
            return Ok(None);
        };
        let message = body.to_vec();
        self.start += width + size;
        if self.start == self.held.len() {
            self.held.clear();
            self.held.shrink_to(RETAINED);
            self.start = 0;
        }
        Ok(Some(message))
    }

    fn buffered(&self) -> usize {
        self.held.len() - self.start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_decoder_gives_back_the_room_a_large_message_took() {
        let codec = LengthPrefix::u32_be();
        let mut decoder = LengthDecoder::new(codec, 1 << 20);
        decoder.push(&codec.encode(&[0; 1 << 20], 1 << 20).unwrap());
        assert_eq!(
            decoder.next_frame().unwrap().map(|m| m.len()),
            Some(1 << 20)
        );
        assert!(decoder.held.capacity() <= RETAINED);
    }

    #[test]
    fn a_declared_length_reserves_nothing_until_its_bytes_arrive() {
        let mut decoder = LengthDecoder::new(LengthPrefix::u32_be(), usize::MAX);
        decoder.push(&u32::MAX.to_be_bytes());
        assert_eq!(decoder.next_frame(), Ok(None));
        assert!(decoder.held.capacity() < 64);
    }
}
