//! Logical channels and datagrams over one [`Channel`]: the versioned frame at work.
//!
//! [`Mux::start`] takes a channel whose messages are frames of [`frame::versioned`](crate::frame::versioned)
//! and gives back logical channels, numbered by the application and the same at both ends
//! ([`Mux::channel`]). Each is a [`Channel`] of its own, and each has [`Datagrams`] that travel
//! inside the carrier, [`Delivery::OverStream`].
//!
//! **Both ends opt in.** Nothing in the bytes negotiates the frame: both ends are configured for it,
//! as they are for a [`LengthPrefix`](crate::frame::LengthPrefix). A peer that is not framed is
//! found by its first messages: a wrong version byte, a short message, or a checksum that fails
//! before any frame has passed it is [`Failure::Protocol`], and so is a run of
//! [`max_damaged`](MuxSettings::max_damaged) failures. A damaged frame among good ones is found by
//! its checksum.
//!
//! **Head-of-line blocking.** Every logical channel and every datagram shares the carrier's one
//! ordered stream. A large message delays everything behind it, and a channel whose queue is full
//! (its reader is not reading) holds up the reader of the carrier, so every other channel waits
//! until it is read. A caller that opens several channels reads all of them. The reader never waits
//! for a number nobody has taken: those hold a small budget between them
//! ([`unopened_bytes`](MuxSettings::unopened_bytes)) and a number past it fails.
//!
//! **Memory.** What a peer can make this side hold is bounded by settings. Every held message or
//! datagram counts its bytes and 64 more, so an empty one is not free. Three budgets are kept apart:
//! [`received_bytes`](MuxSettings::received_bytes) for the messages of the channels that were taken,
//! the same again for their datagrams, and [`unopened_bytes`](MuxSettings::unopened_bytes), at most a
//! quarter of `received_bytes`, for the numbers nobody has taken, which take no room from a taken
//! channel while untaken; taking one moves what it held into the first two, over their bound by at
//! most `unopened_bytes` until the channels are read. A channel's close or drop gives back what it
//! held unread. Add [`queued_bytes`](MuxSettings::queued_bytes) and the datagram queue to send, one
//! slot of under 256 bytes for each number a peer touches (at most `channels`), and what the carrier
//! itself buffers, which a `StreamChannel` bounds by its own `pending` and `queued_bytes`.
//!
//! **Datagrams** never wait: [`Datagrams::send`] only enqueues, and when the queue is full drops the
//! oldest or the newest (a setting), counted by [`Datagrams::dropped`]. They share the carrier with
//! messages in turn, and arrive in order or not at all.
//!
//! **Damage.** The frame's checksum catches damage a carrier let through. A frame that fails it is
//! dropped and counted ([`Counters::corrupt`]); a reliable channel then sees its sequence skip and
//! ends with [`Failure::Protocol`] rather than hand over a stream with a hole in it. A carrier that
//! already drops damaged frames (a serial line's COBS) makes the same thing happen.
//!
//! [`Mux::legacy`] is the explicit wrapper for a legacy codec's wire: see [`Framing::Tagged`].
//!
//! ```
//! use std::time::Duration;
//! use coqui_transport::frame::LengthPrefix;
//! use coqui_transport::mux::Mux;
//! use coqui_transport::{Channel, Delivery, StreamChannel, memory};
//!
//! let (one, other) = memory::pipe(4096);
//! let near = Mux::start(StreamChannel::start(one, LengthPrefix::u32_le())?)?;
//! let far = Mux::start(StreamChannel::start(other, LengthPrefix::u32_le())?)?;
//! let (near, far) = (near.channel(1)?, far.channel(1)?);
//!
//! near.send(b"a message")?;
//! near.datagrams().expect("every channel has them").send(b"a datagram")?;
//! let wait = Some(Duration::from_secs(5));
//! assert_eq!(far.receive(wait)?, Some(b"a message".to_vec()));
//! let datagrams = far.datagrams().expect("datagrams");
//! assert_eq!(datagrams.delivery(), Delivery::OverStream);
//! assert_eq!(datagrams.receive(wait)?, Some(b"a datagram".to_vec()));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod engine;

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;
use std::io;
use std::sync::Mutex;
use std::thread::JoinHandle;

use coqui_builder::{Builder, Describe, Leaf, NonDefault};

use self::engine::{Config, Engine, overhead};
use crate::capabilities::{Capabilities, Carrier};
use crate::channel::{Channel, Datagrams, Delivery};
use crate::deadline;
use crate::failure::Failure;
use crate::frame::versioned::OVERHEAD;

/// How frames are laid out in the carrier's messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Leaf)]
#[map(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Framing {
    /// The versioned frame ([`frame::versioned`](crate::frame::versioned)): logical channels, a
    /// sequence and a checksum on every message.
    Versioned,
    /// One byte before each message or datagram and nothing else: one channel, no sequence, no
    /// checksum, no close. For a wire whose codec is a legacy one and whose both ends the operator
    /// controls: a peer that does not wrap its messages the same way misreads every one.
    Tagged,
}

/// Which datagram goes when a datagram queue is full.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Leaf)]
#[map(rename_all = "lowercase")]
#[non_exhaustive]
pub enum DatagramDrop {
    /// The oldest waiting: the newest data wins, which suits state that goes stale.
    Oldest,
    /// The one just offered: what is queued goes out first.
    Newest,
}

/// What a [`MuxBuilder`] takes whole through `fill`, in the form a file, a form or a peer fills.
///
/// Every limit is a setting with a finite default. Every key is optional and an unknown key is
/// refused; a duration is a number with an optional unit (`30`, `250ms`). A queue length of 0 is 1.
/// The carrier's own bounds decide what fits: a frame is `max_message` plus
/// [`OVERHEAD`] bytes, so a carrier that reports a lower bound
/// ([`Channel::capabilities`]) lowers `max_message` to what it admits, and one that cannot hold a
/// frame is refused at the start. A carrier's bound is never raised. A peer's message over
/// `max_message` that the carrier let through fails only its channel; one over the carrier's own
/// bound ends the carrier, which ends the multiplexer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Describe)]
#[map(deny_unknown_fields)]
#[builder(setters_on = <T: Channel + 'static> MuxBuilder<T>)]
#[non_exhaustive]
pub struct MuxSettings {
    /// How frames are laid out. Default `versioned`.
    #[map(default = MuxSettings::new().framing)]
    pub framing: Framing,
    /// How many logical channel numbers are accepted, 0 to `channels - 1`; a frame for another is a
    /// breach. At most 65536; [`Framing::Tagged`] has one. Default [`Mux::DEFAULT_CHANNELS`].
    #[map(default = MuxSettings::new().channels)]
    pub channels: usize,
    /// The largest message of a logical channel, both ways; a channel's `set_max_` calls can lower
    /// it, never raise it. Lowered to what the carrier admits. Default
    /// [`Mux::DEFAULT_MAX_MESSAGE`], which fits a default `StreamChannel`.
    #[map(default = MuxSettings::new().max_message)]
    pub max_message: usize,
    /// How many received messages a logical channel holds before the reader of the carrier waits
    /// for it. Default [`Mux::DEFAULT_PENDING`].
    #[map(default = MuxSettings::new().pending)]
    pub pending: usize,
    /// The most bytes of messages queued to send, each counting its frame and 64 more; a message that
    /// does not fit waits or is refused, as [`Channel::send`] and [`Channel::send_timeout`] say.
    /// Default [`Mux::DEFAULT_QUEUED_BYTES`].
    #[map(default = MuxSettings::new().queued_bytes)]
    pub queued_bytes: usize,
    /// The most that received messages held for `receive` by the channels that were taken may
    /// count, all together, and again the most for received datagrams, so datagrams never keep a
    /// message out. A held message or datagram counts its bytes and 64 more, so an empty one is not
    /// free. A channel's closing, or its handle's drop, gives back what it held unread. When messages reach it the
    /// reader of the carrier waits until a channel is read (head-of-line blocking); a datagram that
    /// does not fit is dropped by `datagram_drop`. Default
    /// [`Mux::DEFAULT_RECEIVED_BYTES`].
    #[map(default = MuxSettings::new().received_bytes)]
    pub received_bytes: usize,
    /// The most that messages and datagrams held for channel numbers nobody has taken yet may
    /// count, all of them together and apart from `received_bytes`, counted as there; at most a
    /// quarter of `received_bytes`. A number that passes it fails (and reports the failure when it is
    /// taken) instead of making the reader wait. Take the numbers before a peer can send on them.
    /// Default [`Mux::DEFAULT_UNOPENED_BYTES`].
    #[map(default = MuxSettings::new().unopened_bytes)]
    pub unopened_bytes: usize,
    /// The multiplexer ends with [`Failure::Protocol`] at the `max_damaged`th frame in a row that
    /// fails its checksum (the 8th by default); one that fails before any frame has passed ends it
    /// at once. 0 is 1.
    /// Default [`Mux::DEFAULT_MAX_DAMAGED`].
    #[map(default = MuxSettings::new().max_damaged)]
    pub max_damaged: usize,
    /// How long the carrier may take none of a frame before the multiplexer ends with
    /// [`Failure::NoProgress`]; also the most a close waits for what is queued. Default
    /// [`Mux::DEFAULT_SEND_LIMIT`].
    #[map(default = MuxSettings::new().send_limit)]
    pub send_limit: Duration,
    /// The largest datagram payload sent, and the largest accepted: a longer one that arrives is
    /// dropped and counted. Both ends should agree. Lowered, like `max_message`, to what the
    /// carrier admits less the frame's overhead; a longer send is refused locally, whatever the
    /// carrier. Default [`Mux::DEFAULT_MAX_DATAGRAM`].
    #[map(default = MuxSettings::new().max_datagram)]
    pub max_datagram: usize,
    /// How many datagrams wait to be written, all channels together. Default
    /// [`Mux::DEFAULT_DATAGRAM_QUEUE`].
    #[map(default = MuxSettings::new().datagram_queue)]
    pub datagram_queue: usize,
    /// How many received datagrams a logical channel holds for `receive`. Default
    /// [`Mux::DEFAULT_DATAGRAM_PENDING`].
    #[map(default = MuxSettings::new().datagram_pending)]
    pub datagram_pending: usize,
    /// Which datagram goes when either datagram queue is full. Default `oldest`.
    #[map(default = MuxSettings::new().datagram_drop)]
    pub datagram_drop: DatagramDrop,
}

impl MuxSettings {
    /// Every setting at its default.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            framing: Framing::Versioned,
            channels: Mux::DEFAULT_CHANNELS,
            max_message: Mux::DEFAULT_MAX_MESSAGE,
            pending: Mux::DEFAULT_PENDING,
            queued_bytes: Mux::DEFAULT_QUEUED_BYTES,
            received_bytes: Mux::DEFAULT_RECEIVED_BYTES,
            unopened_bytes: Mux::DEFAULT_UNOPENED_BYTES,
            max_damaged: Mux::DEFAULT_MAX_DAMAGED,
            send_limit: Mux::DEFAULT_SEND_LIMIT,
            max_datagram: Mux::DEFAULT_MAX_DATAGRAM,
            datagram_queue: Mux::DEFAULT_DATAGRAM_QUEUE,
            datagram_pending: Mux::DEFAULT_DATAGRAM_PENDING,
            datagram_drop: DatagramDrop::Oldest,
        }
    }

    /// Every setting that differs from its default, named by key path.
    #[must_use]
    pub fn non_defaults(&self) -> Vec<NonDefault> {
        coqui_builder::non_defaults(self)
    }
}

impl Default for MuxSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// What a [`MuxBuilder`] holds: the settings and the carrier. Build with `MuxBuilder::new()`, set
/// the limits through its setters (or `fill`), give `carrier`, then `build`, which starts the
/// reader and the writer thread.
#[derive(Builder, Debug)]
#[builder(build = Mux, fallible, entry = "new")]
pub struct MuxParts<T: Channel + 'static> {
    settings: MuxSettings,
    #[builder(input)]
    carrier: T,
}

impl<T: Channel + 'static> TryFrom<MuxParts<T>> for Mux {
    type Error = io::Error;

    fn try_from(parts: MuxParts<T>) -> io::Result<Self> {
        Self::launch(parts.carrier, parts.settings)
    }
}

/// What a multiplexer counts of damage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Counters {
    /// Frames dropped for a checksum that did not match.
    pub corrupt: u64,
    /// Datagrams the peer wrote that never arrived, by the gaps in their count.
    pub lost_datagrams: u64,
}

/// Why a logical channel could not be taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenError {
    /// The number is not below the `channels` setting.
    Number,
    /// The number was taken before; a number is used once.
    InUse,
    /// The multiplexer has ended.
    Closed,
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Number => "the channel number is not below the number of channels",
            Self::InUse => "the channel number was taken before",
            Self::Closed => "the multiplexer has ended",
        })
    }
}

impl core::error::Error for OpenError {}

/// The owner of the two threads: when the last handle drops, everything closes.
struct Owner {
    engine: Arc<Engine>,
    reader: Mutex<Option<JoinHandle<()>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl Owner {
    /// Closes everything within the send limit; a thread still running after that is left to end by
    /// itself.
    fn close(&self) {
        self.engine.shutdown();
        for slot in [&self.writer, &self.reader] {
            if let Some(handle) = crate::sync::locked(slot).take()
                && handle.is_finished()
            {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.close();
    }
}

/// Logical channels over one carrier. See the [module](self).
///
/// Dropping the last handle (this, or any [`MuxChannel`]) closes every channel, writes what is
/// queued and closes the carrier.
#[must_use]
pub struct Mux {
    owner: Arc<Owner>,
}

impl Mux {
    /// The default number of logical channel numbers: 16.
    pub const DEFAULT_CHANNELS: usize = 16;
    /// The default largest message: 1 MiB less the frame's overhead, so it fits a carrier whose
    /// bound is 1 MiB, as a default `StreamChannel`'s is.
    pub const DEFAULT_MAX_MESSAGE: usize = crate::frame::DEFAULT_MAX_MESSAGE - OVERHEAD;
    /// The default number of received messages a channel holds: 64.
    pub const DEFAULT_PENDING: usize = 64;
    /// The default bound on bytes queued to send: 16 MiB.
    pub const DEFAULT_QUEUED_BYTES: usize = 16 * 1024 * 1024;
    /// The default bound on received bytes held for every channel together: 16 MiB.
    pub const DEFAULT_RECEIVED_BYTES: usize = 16 * 1024 * 1024;
    /// The default bound on bytes held for numbers nobody has taken: 64 KiB.
    pub const DEFAULT_UNOPENED_BYTES: usize = 64 * 1024;
    /// The default number of frames in a row that may fail their checksum: 8.
    pub const DEFAULT_MAX_DAMAGED: usize = 8;
    /// The default time the carrier may take none of a frame: 30 s.
    pub const DEFAULT_SEND_LIMIT: Duration = Duration::from_secs(30);
    /// The default largest datagram: 1200 bytes, which also fits an IPv6 minimum-MTU packet, so
    /// code written against over-stream datagrams works unchanged on native ones.
    pub const DEFAULT_MAX_DATAGRAM: usize = 1200;
    /// The default number of datagrams waiting to be written: 64.
    pub const DEFAULT_DATAGRAM_QUEUE: usize = 64;
    /// The default number of received datagrams a channel holds: 64.
    pub const DEFAULT_DATAGRAM_PENDING: usize = 64;

    /// Starts a multiplexer on `carrier` with the default settings.
    ///
    /// # Errors
    ///
    /// As [`MuxBuilder::build`].
    pub fn start<T: Channel + 'static>(carrier: T) -> io::Result<Self> {
        Self::launch(carrier, MuxSettings::new())
    }

    /// Starts a multiplexer on `carrier` and takes its channel 0: the usual case of one logical
    /// channel with datagrams.
    ///
    /// # Errors
    ///
    /// As [`start`](Self::start).
    pub fn single<T: Channel + 'static>(carrier: T) -> io::Result<MuxChannel> {
        let mut settings = MuxSettings::new();
        settings.channels = 1;
        let mux = Self::launch(carrier, settings)?;
        mux.channel(0)
            .map_err(|error| io::Error::other(error.to_string()))
    }

    /// The explicit wrapper for a legacy codec: `carrier` is a channel whose peer is unframed, and
    /// this adds datagrams to it by [`Framing::Tagged`], one byte before every message. Use it only
    /// where the operator controls both ends and both wrap: the peer's messages are otherwise
    /// misread, one byte short.
    ///
    /// # Errors
    ///
    /// As [`start`](Self::start).
    pub fn legacy<T: Channel + 'static>(carrier: T) -> io::Result<MuxChannel> {
        let mut settings = MuxSettings::new();
        settings.framing = Framing::Tagged;
        let mux = Self::launch(carrier, settings)?;
        mux.channel(0)
            .map_err(|error| io::Error::other(error.to_string()))
    }

    fn launch<T: Channel + 'static>(carrier: T, settings: MuxSettings) -> io::Result<Self> {
        // The carrier's own bounds decide: a frame must fit them, so the largest message is lowered to
        // what they admit, and a carrier that cannot hold a frame is refused.
        let capabilities = carrier.capabilities();
        let reserved = overhead(settings.framing);
        let mut max_message = settings.max_message;
        let mut max_datagram = settings.max_datagram;
        for bound in [capabilities.max_send, capabilities.max_receive]
            .into_iter()
            .flatten()
        {
            // A bound of exactly the frame's overhead holds an empty message.
            if bound < reserved {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the carrier's message bound cannot hold a frame",
                ));
            }
            max_message = max_message.min(bound - reserved);
            max_datagram = max_datagram.min(bound - reserved);
        }
        let engine = Arc::new(Engine::new(
            Box::new(carrier),
            Config::new(settings, max_message, max_datagram),
        ));
        let writing = Arc::clone(&engine);
        let writer = std::thread::Builder::new()
            .name("coqui-transport mux writer".into())
            .spawn(move || writing.write_loop())?;
        let reading = Arc::clone(&engine);
        let reader = std::thread::Builder::new()
            .name("coqui-transport mux reader".into())
            .spawn(move || reading.read_loop());
        let reader = match reader {
            Ok(reader) => reader,
            Err(error) => {
                engine.mark_done(true);
                engine.shutdown();
                let _ = writer.join();
                return Err(error);
            }
        };
        Ok(Self {
            owner: Arc::new(Owner {
                engine,
                reader: Mutex::new(Some(reader)),
                writer: Mutex::new(Some(writer)),
            }),
        })
    }

    /// Takes logical channel `number`.
    ///
    /// # Errors
    ///
    /// [`OpenError`] for a number out of range, taken before or after the end.
    pub fn channel(&self, number: u16) -> Result<MuxChannel, OpenError> {
        self.owner.engine.open(number)?;
        Ok(MuxChannel {
            owner: Arc::clone(&self.owner),
            number,
            datagrams: MuxDatagrams {
                engine: Arc::clone(&self.owner.engine),
                number,
            },
        })
    }

    /// What has been counted of damage so far.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.owner.engine.counters()
    }

    /// Closes every channel, writes what is queued, closes the carrier and joins the threads.
    /// Repeatable.
    pub fn close(&self) {
        self.owner.close();
    }
}

impl fmt::Debug for Mux {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mux")
            .field("counters", &self.counters())
            .finish_non_exhaustive()
    }
}

/// One logical channel of a [`Mux`], with its datagrams.
///
/// `receive` returns the messages the peer sent on this number, in order; `Ok(None)` once the peer
/// closed it (or this side did), after what arrived. Messages already received stay readable after
/// a failure. Dropping it closes it. It is not authenticated: run the peer's proof on it.
#[must_use]
pub struct MuxChannel {
    owner: Arc<Owner>,
    number: u16,
    datagrams: MuxDatagrams,
}

impl MuxChannel {
    /// This channel's number.
    #[must_use]
    pub const fn number(&self) -> u16 {
        self.number
    }

    /// What the multiplexer has counted of damage so far; the same as [`Mux::counters`], for a
    /// holder of only this channel.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.owner.engine.counters()
    }

    fn engine(&self) -> &Engine {
        &self.owner.engine
    }
}

impl fmt::Debug for MuxChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MuxChannel")
            .field("number", &self.number)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl Channel for MuxChannel {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        self.engine().send(self.number, message, None)
    }

    fn send_timeout(&self, message: &[u8], timeout: Option<Duration>) -> Result<(), Failure> {
        self.engine()
            .send(self.number, message, Some(deadline::within(timeout)))
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        self.engine().receive(self.number, timeout)
    }

    fn close(&self) {
        if self.engine().config().framing == Framing::Tagged {
            // The tagged framing has no close frame: the end of the carrier is the end, so the
            // carrier is flushed and closed.
            self.owner.close();
        } else {
            self.engine().close_channel(self.number);
        }
    }

    fn set_max_message(&self, bytes: usize) {
        self.engine()
            .set_limits(self.number, Some(bytes), Some(bytes));
    }

    fn set_max_receive(&self, bytes: usize) {
        self.engine().set_limits(self.number, None, Some(bytes));
    }

    fn set_max_send(&self, bytes: usize) {
        self.engine().set_limits(self.number, Some(bytes), None);
    }

    fn set_pending(&self, messages: usize) -> Result<(), Failure> {
        self.engine().set_pending(self.number, messages);
        Ok(())
    }

    fn datagrams(&self) -> Option<&dyn Datagrams> {
        Some(&self.datagrams)
    }

    fn capabilities(&self) -> Capabilities {
        let (send, receive) = self.engine().limits(self.number);
        Capabilities::new()
            .carrier(Carrier::Stream)
            .limits(send, receive)
            .datagrams(Some((
                Delivery::OverStream,
                Some(self.engine().config().max_datagram),
            )))
    }

    fn is_closed(&self) -> bool {
        self.engine().is_closed(self.number)
    }
}

impl Drop for MuxChannel {
    fn drop(&mut self) {
        self.engine().close_channel(self.number);
    }
}

/// The datagrams of a [`MuxChannel`]: [`Delivery::OverStream`].
struct MuxDatagrams {
    engine: Arc<Engine>,
    number: u16,
}

impl Datagrams for MuxDatagrams {
    fn max_size(&self) -> Option<usize> {
        Some(self.engine.config().max_datagram)
    }

    fn send(&self, payload: &[u8]) -> Result<(), Failure> {
        self.engine.send_datagram(self.number, payload)
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        self.engine.receive_datagram(self.number, timeout)
    }

    fn dropped(&self) -> u64 {
        self.engine.dropped(self.number)
    }

    fn delivery(&self) -> Delivery {
        Delivery::OverStream
    }
}
