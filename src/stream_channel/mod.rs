//! A [`Channel`] over any byte stream: one framing, a reader thread and a writer thread.

mod shared;

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;
use std::io;
use std::sync::Mutex;
use std::thread::JoinHandle;

use self::shared::{End, Exit, FRAME_COST, READER, Shared, WRITER};
use crate::capabilities::{Capabilities, Carrier};
use crate::channel::Channel;
use crate::deadline;
use crate::failure::Failure;
use crate::frame::{Codec, DEFAULT_MAX_MESSAGE, FrameError};
use crate::stream::Split;
use crate::sync::{locked, wait_until};

/// A [`Channel`] over a byte stream, every message one frame of the [`Codec`] it was given.
///
/// It works on anything that [`Split`]s: a TCP stream, this process's standard input and output, a
/// pipe, a serial port, an in-memory stream, a caller's own `Read` and `Write`. It is not
/// authenticated: run the peer's proof on it before anything else.
///
/// **Reading.** A reader thread of its own reads the stream, decodes it incrementally and holds up
/// to [`pending`](StreamChannelBuilder::pending) messages for `receive`; past that it reads nothing,
/// so a slow reader holds the peer's sends back. Each message is judged by the receive bound for its
/// position ([`Channel::set_max_receive_after`]). A header over the bound ends the channel with
/// [`Failure::Protocol`] before any of its body is read, and the stream is ended so the peer sees
/// it. The stream ending between frames is the peer closing in order (`Ok(None)`); ending inside a
/// frame is [`Failure::PathLost`]. A `receive` that times out loses nothing.
///
/// **Writing.** A send encodes the message at once (a message outside the send bound is
/// [`Failure::Protocol`], and the channel goes on), then queues it for a writer thread of its own,
/// which writes every frame whole however the stream splits the writes, and flushes when the queue
/// empties. A queued message is not taken back; one refused for want of room was not sent. Queued
/// bytes are bounded by [`queued_bytes`](StreamChannelBuilder::queued_bytes). A frame the stream
/// takes nothing of for the [`send_limit`](StreamChannelBuilder::send_limit) ends the channel with
/// [`Failure::NoProgress`], seen by the next call that waits on the writer, or by the writer itself
/// on a stream with a write timeout.
///
/// `send` answers [`Failure::NoProgress`] in two cases, as the legacy channels do: with
/// [`is_closed`](Channel::is_closed) still `false`, the queue was full, nothing was sent and the
/// channel goes on; with it `true`, the writer stalled and the channel ended. After a local `close`
/// a send is [`Failure::PathLost`]. [`capabilities`](Channel::capabilities) reports as
/// `max_receive` the bound for the next message the reader thread reads, which with read-ahead can
/// be past what `receive` has returned.
///
/// **Closing.** `close`, which dropping also does, refuses further sends and waits up to the send
/// limit for what is queued to be written; the writer half is then dropped, so the peer reads the
/// end. Until the peer ends its side too, or has sent nothing for the
/// [`linger`](StreamChannelBuilder::linger), the reader discards what still arrives, since a socket
/// ended with bytes unread is reset and its peer loses what was sent last. Then the stream's
/// [`Interrupt`](crate::Interrupt) wakes a blocked read, and both
/// threads are joined, each waited for at most the send limit. Messages already read stay
/// readable, then `receive` answers `Ok(None)`. A thread the stream cannot wake (any read on a
/// stream with no interrupt, such as this process's standard input) is left to end when the stream
/// next yields bytes or ends, and does nothing further.
pub struct StreamChannel {
    shared: Arc<Shared>,
    encoder: Box<dyn Encoder>,
    reader: Mutex<Option<JoinHandle<()>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

/// The codec's encoding, without its decoder type.
trait Encoder: Send + Sync {
    fn frame(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError>;
}

impl<C: Codec> Encoder for C {
    fn frame(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
        self.encode(message, maximum)
    }
}

impl StreamChannel {
    /// The default largest message, both ways: 1 MiB.
    pub const DEFAULT_MAX_MESSAGE: usize = DEFAULT_MAX_MESSAGE;
    /// The default number of messages read ahead of the reader: 64.
    pub const DEFAULT_PENDING: usize = 64;
    /// The default bound on bytes queued to send: 64 MiB.
    pub const DEFAULT_QUEUED_BYTES: usize = 64 * 1024 * 1024;
    /// The default time a frame may wait for the stream to take any of it: 30 s.
    pub const DEFAULT_SEND_LIMIT: Duration = Duration::from_secs(30);
    /// The default size of one read from the stream: 64 KiB.
    pub const DEFAULT_READ_BUFFER: usize = 64 * 1024;
    /// The default time closing waits, after the peer last sent anything, for its end: 500 ms.
    pub const DEFAULT_LINGER: Duration = Duration::from_millis(500);

    /// Settings for a channel framed by `codec`, at their defaults.
    pub const fn builder<C: Codec>(codec: C) -> StreamChannelBuilder<C> {
        StreamChannelBuilder {
            codec,
            max_message: Self::DEFAULT_MAX_MESSAGE,
            pending: Self::DEFAULT_PENDING,
            queued_bytes: Self::DEFAULT_QUEUED_BYTES,
            send_limit: Self::DEFAULT_SEND_LIMIT,
            read_buffer: Self::DEFAULT_READ_BUFFER,
            linger: Self::DEFAULT_LINGER,
        }
    }

    /// Starts a channel on `stream`, framed by `codec`, with the default settings.
    ///
    /// # Errors
    ///
    /// As [`StreamChannelBuilder::start`].
    pub fn start<S: Split, C: Codec>(stream: S, codec: C) -> io::Result<Self> {
        Self::builder(codec).start(stream)
    }

    /// Waits for the reader to see the peer's end, while it discards what the peer still sends:
    /// until the peer has been quiet for the linger, and never past the send limit.
    fn linger(&self) {
        let cap = deadline::after(self.shared.send_limit);
        let mut done = locked(&self.shared.done);
        while !done[READER] {
            let heard = locked(&self.shared.inbound).heard;
            let quiet = heard.and_then(|at| at.checked_add(self.shared.linger));
            let until = deadline::earlier(cap, quiet);
            if deadline::passed(until) {
                break;
            }
            done = wait_until(&self.shared.exited, done, until);
        }
    }

    /// Joins one thread once it has finished, waiting for that up to the send limit when the stream
    /// was interrupted, and not at all when it could not be: a thread still blocked on the stream
    /// after that is left to end by itself.
    fn join(&self, slot: &Mutex<Option<JoinHandle<()>>>, thread: usize) {
        let Some(handle) = locked(slot).take() else {
            return;
        };
        let until = if self.shared.interrupt.is_some() {
            deadline::after(self.shared.send_limit)
        } else {
            Some(std::time::Instant::now())
        };
        let mut done = locked(&self.shared.done);
        while !done[thread] && !deadline::passed(until) {
            done = wait_until(&self.shared.exited, done, until);
        }
        let finished = done[thread];
        drop(done);
        if finished {
            let _ = handle.join();
        }
    }
}

impl fmt::Debug for StreamChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamChannel")
            .field("capabilities", &self.capabilities())
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// The settings of a [`StreamChannel`], from [`StreamChannel::builder`].
///
/// Every limit is a setting with a finite default; none has a floor beyond what makes it mean
/// anything (a read-ahead of 0 and a read buffer of 0 are 1). [`weakened`](Self::weakened) lists
/// those set looser than their defaults.
#[derive(Clone, Debug)]
#[must_use]
pub struct StreamChannelBuilder<C> {
    codec: C,
    max_message: usize,
    pending: usize,
    queued_bytes: usize,
    send_limit: Duration,
    read_buffer: usize,
    linger: Duration,
}

impl<C: Codec> StreamChannelBuilder<C> {
    /// The largest message, both ways, until a `set_max_` call changes it. Default
    /// [`StreamChannel::DEFAULT_MAX_MESSAGE`].
    pub const fn max_message(mut self, bytes: usize) -> Self {
        self.max_message = bytes;
        self
    }

    /// How many messages the reader thread holds ahead of `receive` before it stops reading, until
    /// [`Channel::set_pending`] changes it. 0 is 1. Default [`StreamChannel::DEFAULT_PENDING`].
    pub const fn pending(mut self, messages: usize) -> Self {
        self.pending = messages;
        self
    }

    /// The most bytes queued to send, each message counting its frame and 64 bytes more; a message
    /// that does not fit waits or is refused, as [`Channel::send`] and [`Channel::send_timeout`]
    /// say. Default [`StreamChannel::DEFAULT_QUEUED_BYTES`].
    pub const fn queued_bytes(mut self, bytes: usize) -> Self {
        self.queued_bytes = bytes;
        self
    }

    /// How long a frame being written may go with the stream taking none of it, after which the
    /// channel ends with [`Failure::NoProgress`]; also the most `close` waits for what is queued.
    /// Default [`StreamChannel::DEFAULT_SEND_LIMIT`].
    pub const fn send_limit(mut self, limit: Duration) -> Self {
        self.send_limit = limit;
        self
    }

    /// The most bytes one read takes from the stream, which is also how far past one message the
    /// decoder can hold. 0 is 1. Default [`StreamChannel::DEFAULT_READ_BUFFER`].
    pub const fn read_buffer(mut self, bytes: usize) -> Self {
        self.read_buffer = bytes;
        self
    }

    /// How long `close`, once it has written what was queued, waits after the peer last sent
    /// anything for the peer's end, reading and discarding what still arrives, before it ends the
    /// stream; never longer than the send limit in all. A socket ended with bytes unread is reset,
    /// and the peer then loses what this side sent last. Default
    /// [`StreamChannel::DEFAULT_LINGER`].
    pub const fn linger(mut self, quiet: Duration) -> Self {
        self.linger = quiet;
        self
    }

    /// The names of the settings looser than their defaults: `max_message`, `pending`,
    /// `queued_bytes`, `send_limit`, `read_buffer` and `linger` above theirs.
    #[must_use]
    pub fn weakened(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self.max_message > StreamChannel::DEFAULT_MAX_MESSAGE {
            names.push("max_message");
        }
        if self.pending > StreamChannel::DEFAULT_PENDING {
            names.push("pending");
        }
        if self.queued_bytes > StreamChannel::DEFAULT_QUEUED_BYTES {
            names.push("queued_bytes");
        }
        if self.send_limit > StreamChannel::DEFAULT_SEND_LIMIT {
            names.push("send_limit");
        }
        if self.read_buffer > StreamChannel::DEFAULT_READ_BUFFER {
            names.push("read_buffer");
        }
        if self.linger > StreamChannel::DEFAULT_LINGER {
            names.push("linger");
        }
        names
    }

    /// Starts the channel on `stream`: splits it, then starts the reader thread and the writer
    /// thread, which the channel owns and joins when closed.
    ///
    /// # Errors
    ///
    /// When the stream cannot be split or a thread cannot be started, as on a target with no
    /// threads.
    pub fn start<S: Split>(self, stream: S) -> io::Result<StreamChannel> {
        let halves = stream.split()?;
        let shared = Arc::new(Shared::new(
            self.max_message,
            self.pending,
            self.queued_bytes,
            self.send_limit,
            self.linger,
            halves.interrupt,
        ));
        let mut writer = halves.writer;
        let writing = Arc::clone(&shared);
        let writer = std::thread::Builder::new()
            .name("coqui-transport writer".into())
            .spawn(move || {
                let _exit = Exit {
                    shared: &writing,
                    thread: WRITER,
                };
                writing.write_stream(&mut writer);
                // The write direction ends here, before the thread is seen finished.
                drop(writer);
            })?;
        let mut reader = halves.reader;
        let mut decoder = self.codec.decoder(self.max_message);
        let chunk = self.read_buffer;
        let reading = Arc::clone(&shared);
        let reader = std::thread::Builder::new()
            .name("coqui-transport reader".into())
            .spawn(move || {
                let _exit = Exit {
                    shared: &reading,
                    thread: READER,
                };
                reading.read_stream(&mut reader, &mut decoder, chunk);
                drop(reader);
            });
        let reader = match reader {
            Ok(reader) => reader,
            Err(error) => {
                shared.fail(Failure::Cancelled);
                let _ = writer.join();
                return Err(error);
            }
        };
        Ok(StreamChannel {
            shared,
            encoder: Box::new(self.codec),
            reader: Mutex::new(Some(reader)),
            writer: Mutex::new(Some(writer)),
        })
    }
}

impl StreamChannel {
    fn frame(&self, message: &[u8]) -> Result<Vec<u8>, Failure> {
        let bound = locked(&self.shared.outbound).max_send;
        self.encoder
            .frame(message, bound)
            .map_err(|_| Failure::Protocol)
    }

    /// Ends the channel for a writer that stalled, saying so.
    fn stall(&self) -> Failure {
        self.shared.fail(Failure::NoProgress);
        Failure::NoProgress
    }
}

impl Channel for StreamChannel {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        let mut frame = Some(self.frame(message)?);
        let mut outbound = locked(&self.shared.outbound);
        if outbound.ended.is_none() && !outbound.closing && self.shared.stalled(&outbound) {
            drop(outbound);
            return Err(self.stall());
        }
        if self.shared.try_queue(&mut outbound, &mut frame)? {
            Ok(())
        } else {
            Err(Failure::NoProgress)
        }
    }

    fn send_timeout(&self, message: &[u8], timeout: Option<Duration>) -> Result<(), Failure> {
        let mut frame = Some(self.frame(message)?);
        // Room for more than the whole budget never comes.
        if frame
            .as_ref()
            .map_or(0, Vec::len)
            .saturating_add(FRAME_COST)
            > self.shared.queued_limit
        {
            return Err(Failure::NoProgress);
        }
        let deadline = deadline::within(timeout);
        let mut outbound = locked(&self.shared.outbound);
        loop {
            if outbound.ended.is_none() && !outbound.closing && self.shared.stalled(&outbound) {
                drop(outbound);
                return Err(self.stall());
            }
            if self.shared.try_queue(&mut outbound, &mut frame)? {
                return Ok(());
            }
            if deadline::passed(deadline) {
                return Err(Failure::Timeout);
            }
            let wake = deadline::earlier(deadline, self.shared.stall_deadline(&outbound));
            outbound = wait_until(&self.shared.room, outbound, wake);
        }
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        let deadline = deadline::within(timeout);
        let mut inbound = locked(&self.shared.inbound);
        loop {
            if let Some(message) = inbound.messages.pop_front() {
                inbound.taken += 1;
                drop(inbound);
                self.shared.taken.notify_all();
                return Ok(Some(message));
            }
            match inbound.end {
                Some(End::Clean) => return Ok(None),
                Some(End::Failed(failure)) => return Err(failure),
                None if inbound.closed => return Ok(None),
                None => {}
            }
            if deadline::passed(deadline) {
                return Err(Failure::Timeout);
            }
            inbound = wait_until(&self.shared.arrived, inbound, deadline);
        }
    }

    fn close(&self) {
        let drained = {
            let mut outbound = locked(&self.shared.outbound);
            outbound.closing = true;
            self.shared.queued.notify_all();
            let until = deadline::after(self.shared.send_limit);
            while !outbound.drained && outbound.ended.is_none() && !deadline::passed(until) {
                outbound = wait_until(&self.shared.room, outbound, until);
            }
            outbound.frames.clear();
            outbound.bytes = 0;
            outbound.drained
        };
        {
            let mut inbound = locked(&self.shared.inbound);
            if !inbound.closed {
                inbound.closed = true;
                inbound.heard = Some(std::time::Instant::now());
            }
        }
        self.shared.wake_all();
        if drained {
            self.linger();
        }
        locked(&self.shared.inbound).linger_over = true;
        self.shared.end_stream();
        self.join(&self.writer, WRITER);
        self.join(&self.reader, READER);
    }

    fn set_max_message(&self, bytes: usize) {
        {
            let mut inbound = locked(&self.shared.inbound);
            inbound.receive_now = bytes;
            inbound.staged = None;
        }
        locked(&self.shared.outbound).max_send = bytes;
    }

    fn set_max_receive(&self, bytes: usize) {
        let mut inbound = locked(&self.shared.inbound);
        inbound.receive_now = bytes;
        inbound.staged = None;
    }

    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> {
        let mut inbound = locked(&self.shared.inbound);
        if messages == 0 {
            inbound.receive_now = bytes;
            inbound.staged = None;
        } else {
            let from = inbound
                .taken
                .saturating_add(u64::try_from(messages).unwrap_or(u64::MAX));
            inbound.staged = Some((from, bytes));
        }
        Ok(())
    }

    fn set_pending(&self, messages: usize) -> Result<(), Failure> {
        locked(&self.shared.inbound).pending = messages.max(1);
        self.shared.taken.notify_all();
        Ok(())
    }

    fn set_max_send(&self, bytes: usize) {
        locked(&self.shared.outbound).max_send = bytes;
    }

    fn capabilities(&self) -> Capabilities {
        let max_receive = locked(&self.shared.inbound).receive_now;
        let max_send = locked(&self.shared.outbound).max_send;
        Capabilities::new()
            .with_carrier(Carrier::Stream)
            .with_limits(max_send, max_receive)
    }

    fn is_closed(&self) -> bool {
        let inbound = locked(&self.shared.inbound);
        if inbound.closed || inbound.end.is_some() {
            return true;
        }
        drop(inbound);
        locked(&self.shared.outbound).ended.is_some()
    }
}

impl Drop for StreamChannel {
    fn drop(&mut self) {
        self.close();
    }
}
