//! Framing: whole messages on a byte stream, with no I/O.
//!
//! A [`Codec`] turns a message into the bytes of one frame, and makes the [`Decode`] that finds
//! frames again in bytes as they arrive, whatever their fragmentation or coalescing. Codecs are
//! never guessed: both ends are configured for the same one.
//!
//! [`LengthPrefix`] is the codec this crate provides: a one-, two- or four-byte length, big- or
//! little-endian, and an empty message either allowed or refused, each chosen by name:
//!
//! ```
//! use coqui_transport::frame::{Codec, LengthPrefix};
//!
//! let codec = LengthPrefix::u32_le().refuse_empty();
//! assert_eq!(codec.encode(b"ab", 10).unwrap(), [2, 0, 0, 0, b'a', b'b']);
//! ```
//!
//! It carries nothing but messages: a datagram, a second logical channel or a version would reach a
//! peer as an ordinary message. Those need a framing both ends agree to, which is a codec of its own
//! beside this one. A protocol's own framing is a preset of a codec, defined where the protocol is.

mod length;

use alloc::vec::Vec;
use core::fmt;

pub use length::{LengthDecoder, LengthPrefix};

/// The largest message a codec's user accepts unless the application chooses otherwise: 1 MiB.
pub const DEFAULT_MAX_MESSAGE: usize = 1024 * 1024;

/// A frame outside the bounds this side accepts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FrameError {
    /// The body is empty where the codec refuses empty ones, or over the limit. Fatal on a stream:
    /// the framing cannot resynchronize.
    Size,
    /// A tag this does not know.
    Tag,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Size => "a frame is empty or over the size limit",
            Self::Tag => "a message has a tag this does not know",
        })
    }
}

impl core::error::Error for FrameError {}

/// A framing: how one message becomes one frame, and the decoder that reads them back.
pub trait Codec: Send + Sync + 'static {
    /// The decoder for this framing.
    type Decoder: Decode + Send + 'static;

    /// One frame holding `message`, if it is within `maximum` bytes and the framing can carry it.
    ///
    /// # Errors
    ///
    /// [`FrameError::Size`] for a message over `maximum`, longer than the frame can declare, or
    /// empty where the framing refuses empty messages.
    fn encode(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError>;

    /// A decoder accepting messages of at most `maximum` bytes.
    fn decoder(&self, maximum: usize) -> Self::Decoder;
}

/// An incremental decoder: bytes go in as they arrived, whole messages come out.
///
/// [`push`](Self::push) what arrived, then call [`next_frame`](Self::next_frame) until it answers
/// `None`. Frames that were complete before a bad header are returned before the error. Once it
/// fails, the decoder stays failed and holds nothing. It never allocates for a length a header
/// declares, only for bytes that arrived, so its memory is what was pushed and not yet returned.
pub trait Decode {
    /// Changes the largest message accepted, from the next frame whose header is checked on.
    fn set_maximum(&mut self, maximum: usize);

    /// Adds bytes as they arrived. A failed decoder discards them.
    fn push(&mut self, part: &[u8]);

    /// The next complete message, or `None` until more bytes arrive.
    ///
    /// # Errors
    ///
    /// [`FrameError::Size`] when a header declares a message over the maximum, or one the framing
    /// refuses; the decoder then stays failed and every later call fails the same way.
    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError>;

    /// How many bytes are held that no returned message accounts for: 0 between frames, so a stream
    /// that ends there ended in order, and more inside one.
    fn buffered(&self) -> usize;
}
