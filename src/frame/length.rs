//! The two legacy wires: a `u32` length, then the message.

use alloc::vec::Vec;

use super::{Codec, Decode, FrameError};

const PREFIX: usize = 4;

/// What tells the two wires apart.
#[derive(Clone, Copy, Debug)]
struct Layout {
    big_endian: bool,
    empty_allowed: bool,
}

const PEER: Layout = Layout {
    big_endian: true,
    empty_allowed: true,
};

const WEBTRANSPORT: Layout = Layout {
    big_endian: false,
    empty_allowed: false,
};

fn encode(layout: Layout, body: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
    if (body.is_empty() && !layout.empty_allowed) || body.len() > maximum {
        return Err(FrameError::Size);
    }
    let size = u32::try_from(body.len()).map_err(|_| FrameError::Size)?;
    let prefix = if layout.big_endian {
        size.to_be_bytes()
    } else {
        size.to_le_bytes()
    };
    let mut out = Vec::with_capacity(PREFIX + body.len());
    out.extend_from_slice(&prefix);
    out.extend_from_slice(body);
    Ok(out)
}

/// The decoding both wires share. `held[start..]` is what no returned frame accounts for.
#[derive(Debug)]
struct Lengths {
    layout: Layout,
    held: Vec<u8>,
    start: usize,
    maximum: usize,
    failed: bool,
}

impl Lengths {
    const fn new(layout: Layout, maximum: usize) -> Self {
        Self {
            layout,
            held: Vec::new(),
            start: 0,
            maximum,
            failed: false,
        }
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
        let available = &self.held[self.start..];
        let Some(prefix) = available.first_chunk::<PREFIX>() else {
            return Ok(None);
        };
        let declared = if self.layout.big_endian {
            u32::from_be_bytes(*prefix)
        } else {
            u32::from_le_bytes(*prefix)
        };
        let size = usize::try_from(declared).unwrap_or(usize::MAX);
        if (size == 0 && !self.layout.empty_allowed) || size > self.maximum {
            self.failed = true;
            self.held = Vec::new();
            self.start = 0;
            return Err(FrameError::Size);
        }
        let Some(body) = available[PREFIX..].get(..size) else {
            return Ok(None);
        };
        let message = body.to_vec();
        self.start += PREFIX + size;
        if self.start == self.held.len() {
            self.held.clear();
            self.start = 0;
        }
        Ok(Some(message))
    }

    fn buffered(&self) -> usize {
        self.held.len() - self.start
    }
}

/// The frame of `coqui_peer::write_frame` and of `coqui-peer-pipe`'s messages: the message's
/// length as four big-endian bytes, then the message. An empty message is a message.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct PeerFrame;

impl Codec for PeerFrame {
    type Decoder = PeerDecoder;

    fn encode(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
        encode(PEER, message, maximum)
    }

    fn decoder(&self, maximum: usize) -> PeerDecoder {
        PeerDecoder::new(maximum)
    }
}

/// The incremental decoder of [`PeerFrame`].
#[derive(Debug)]
pub struct PeerDecoder(Lengths);

impl PeerDecoder {
    /// A decoder accepting messages of at most `maximum` bytes.
    #[must_use]
    pub const fn new(maximum: usize) -> Self {
        Self(Lengths::new(PEER, maximum))
    }

    /// As [`Decode::set_maximum`].
    pub const fn set_maximum(&mut self, maximum: usize) {
        self.0.maximum = maximum;
    }

    /// As [`Decode::push`].
    pub fn push(&mut self, part: &[u8]) {
        self.0.push(part);
    }

    /// As [`Decode::next_frame`].
    ///
    /// # Errors
    ///
    /// As [`Decode::next_frame`].
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        self.0.next_frame()
    }

    /// As [`Decode::buffered`].
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.0.buffered()
    }
}

impl Decode for PeerDecoder {
    fn set_maximum(&mut self, maximum: usize) {
        Self::set_maximum(self, maximum);
    }

    fn push(&mut self, part: &[u8]) {
        Self::push(self, part);
    }

    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        Self::next_frame(self)
    }

    fn buffered(&self) -> usize {
        Self::buffered(self)
    }
}

/// The frame of `coqui-web-channel`'s WebTransport streams: the message's length as four
/// little-endian bytes, then the message. A message is never empty: one is refused when sent, and a
/// frame declaring one ends the stream.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct WebTransportFrame;

impl Codec for WebTransportFrame {
    type Decoder = WebTransportDecoder;

    fn encode(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
        encode(WEBTRANSPORT, message, maximum)
    }

    fn decoder(&self, maximum: usize) -> WebTransportDecoder {
        WebTransportDecoder::new(maximum)
    }
}

/// The incremental decoder of [`WebTransportFrame`].
#[derive(Debug)]
pub struct WebTransportDecoder(Lengths);

impl WebTransportDecoder {
    /// A decoder accepting messages of at most `maximum` bytes.
    #[must_use]
    pub const fn new(maximum: usize) -> Self {
        Self(Lengths::new(WEBTRANSPORT, maximum))
    }

    /// As [`Decode::set_maximum`].
    pub const fn set_maximum(&mut self, maximum: usize) {
        self.0.maximum = maximum;
    }

    /// As [`Decode::push`].
    pub fn push(&mut self, part: &[u8]) {
        self.0.push(part);
    }

    /// As [`Decode::next_frame`].
    ///
    /// # Errors
    ///
    /// As [`Decode::next_frame`].
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        self.0.next_frame()
    }

    /// As [`Decode::buffered`].
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.0.buffered()
    }
}

impl Decode for WebTransportDecoder {
    fn set_maximum(&mut self, maximum: usize) {
        Self::set_maximum(self, maximum);
    }

    fn push(&mut self, part: &[u8]) {
        Self::push(self, part);
    }

    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        Self::next_frame(self)
    }

    fn buffered(&self) -> usize {
        Self::buffered(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_survive_fragmentation_and_coalescing() {
        let mut bytes = WebTransportFrame.encode(b"hello", 10).unwrap();
        bytes.extend(WebTransportFrame.encode(b"world", 10).unwrap());
        let mut decoder = WebTransportDecoder::new(10);
        decoder.push(&bytes[..3]);
        assert_eq!(decoder.next_frame(), Ok(None));
        decoder.push(&bytes[3..]);
        assert_eq!(decoder.next_frame(), Ok(Some(b"hello".to_vec())));
        assert_eq!(decoder.next_frame(), Ok(Some(b"world".to_vec())));
        assert_eq!(decoder.next_frame(), Ok(None));
        assert_eq!(decoder.buffered(), 0);
    }

    #[test]
    fn complete_frames_come_before_a_bad_header_and_the_failure_sticks() {
        let mut bytes = WebTransportFrame.encode(b"good", 10).unwrap();
        bytes.extend([11, 0, 0, 0]);
        let mut decoder = WebTransportDecoder::new(10);
        decoder.push(&bytes);
        assert_eq!(decoder.next_frame(), Ok(Some(b"good".to_vec())));
        assert_eq!(decoder.next_frame(), Err(FrameError::Size));
        decoder.push(&WebTransportFrame.encode(b"later", 10).unwrap());
        assert_eq!(decoder.next_frame(), Err(FrameError::Size));
        assert_eq!(decoder.buffered(), 0);
    }

    #[test]
    fn an_empty_body_is_refused_both_ways_on_webtransport_and_a_message_on_the_peer_wire() {
        assert_eq!(WebTransportFrame.encode(b"", 10), Err(FrameError::Size));
        let mut decoder = WebTransportDecoder::new(10);
        decoder.push(&[0, 0, 0, 0]);
        assert_eq!(decoder.next_frame(), Err(FrameError::Size));

        assert_eq!(PeerFrame.encode(b"", 10), Ok(vec![0, 0, 0, 0]));
        let mut decoder = PeerDecoder::new(0);
        decoder.push(&[0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(decoder.next_frame(), Ok(Some(Vec::new())));
        assert_eq!(decoder.next_frame(), Ok(Some(Vec::new())));
        assert_eq!(decoder.next_frame(), Ok(None));
    }

    #[test]
    fn a_declared_length_reserves_nothing_until_its_bytes_arrive() {
        let mut decoder = PeerDecoder::new(usize::MAX);
        decoder.push(&u32::MAX.to_be_bytes());
        assert_eq!(decoder.next_frame(), Ok(None));
        assert!(decoder.0.held.capacity() < 64);
        assert_eq!(decoder.buffered(), PREFIX);
    }

    #[test]
    fn the_two_wires_differ_only_in_byte_order_and_the_empty_message() {
        assert_eq!(
            PeerFrame.encode(b"ab", 10).unwrap(),
            [0, 0, 0, 2, b'a', b'b']
        );
        assert_eq!(
            WebTransportFrame.encode(b"ab", 10).unwrap(),
            [2, 0, 0, 0, b'a', b'b']
        );
    }

    #[test]
    fn a_lowered_maximum_applies_to_the_next_header_checked() {
        let mut decoder = PeerDecoder::new(10);
        decoder.push(&PeerFrame.encode(b"12345", 10).unwrap());
        decoder.set_maximum(4);
        assert_eq!(decoder.next_frame(), Err(FrameError::Size));
    }
}
