//! An [`AsyncChannel`] over any tokio byte stream, read and written by the calls themselves.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::Poll;
use core::time::Duration;
use std::io;
use std::sync::Mutex;

use coqui_builder::{Builder, Describe, NonDefault};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;

use super::{AsyncChannel, BoxFuture, within};
use crate::capabilities::{Capabilities, Carrier};
use crate::failure::Failure;
use crate::frame::{Codec, DEFAULT_MAX_MESSAGE, Decode, FrameError};
use crate::sync::locked;

/// An [`AsyncChannel`] over an async byte stream, every message one frame of the [`Codec`] it was
/// given: the async twin of [`crate::StreamChannel`].
///
/// It spawns nothing: `receive` reads and decodes the stream itself, and `send` writes it, so it
/// runs on whatever runtime polls it. Nothing is read ahead of the reader, so a slow reader holds
/// the peer's sends back, and each message is judged by the receive bound for its position.
///
/// **Cancellation.** A `receive` dropped or timed out loses nothing: bytes already read stay in the
/// decoder for the next call. A `send` dropped before its first byte was written sent nothing; one
/// dropped after has its frame finished by the next `send` or `close`, ahead of anything else, so a
/// frame is never left torn on the wire, and that message is sent.
///
/// **Limits.** A header over the receive bound ends the channel with [`Failure::Protocol`] before
/// its body is read; the stream ending inside a frame is [`Failure::PathLost`], between frames the
/// peer closing in order. A write the stream takes nothing of for the
/// [`send_limit`](StreamChannelSettings::send_limit) ends the channel with [`Failure::NoProgress`].
///
/// **Closing.** `close` finishes a frame under way, shuts the writing half down (the peer reads the
/// end) and wakes any `receive`, which then returns what is already decoded, then `Ok(None)`.
/// Dropping the channel without `close` drops the stream as it stands.
pub struct StreamChannel {
    reading: tokio::sync::Mutex<Reading>,
    writing: tokio::sync::Mutex<Writing>,
    bounds: Mutex<Bounds>,
    stop: watch::Sender<Option<Stop>>,
    ended: AtomicBool,
    encoder: Box<dyn Encoder>,
    send_limit: Duration,
}

trait Encoder: Send + Sync {
    fn frame(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError>;
}

impl<C: Codec> Encoder for C {
    fn frame(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
        self.encode(message, maximum)
    }
}

#[derive(Clone, Copy, Debug)]
enum Stop {
    Closed,
    Failed(Failure),
}

#[derive(Clone, Copy, Debug)]
enum End {
    Clean,
    Failed(Failure),
}

struct Reading {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    decoder: Box<dyn Decode + Send>,
    buffer: Vec<u8>,
    end: Option<End>,
}

struct Writing {
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    /// A frame and how much of it the stream has taken, kept across a cancelled send.
    partial: Option<(Vec<u8>, usize)>,
    ended: Option<Failure>,
    shut: bool,
}

#[derive(Debug)]
struct Bounds {
    receive_now: usize,
    staged: Option<(u64, usize)>,
    taken: u64,
    max_send: usize,
}

impl Bounds {
    /// The bound for the next message `receive` returns.
    fn next(&mut self) -> usize {
        if let Some((from, bytes)) = self.staged
            && self.taken >= from
        {
            self.receive_now = bytes;
            self.staged = None;
        }
        self.receive_now
    }
}

/// Which of two futures finished first.
enum First<A, B> {
    One(A),
    Other(B),
}

async fn first<A: Future, B: Future>(one: A, other: B) -> First<A::Output, B::Output> {
    let mut one = pin!(one);
    let mut other = pin!(other);
    poll_fn(|cx| {
        if let Poll::Ready(value) = one.as_mut().poll(cx) {
            return Poll::Ready(First::One(value));
        }
        if let Poll::Ready(value) = other.as_mut().poll(cx) {
            return Poll::Ready(First::Other(value));
        }
        Poll::Pending
    })
    .await
}

impl StreamChannel {
    /// The default largest message, both ways: 1 MiB.
    pub const DEFAULT_MAX_MESSAGE: usize = DEFAULT_MAX_MESSAGE;
    /// The default time a write may go with the stream taking none of it: 30 s.
    pub const DEFAULT_SEND_LIMIT: Duration = Duration::from_secs(30);
    /// The default size of one read from the stream: 64 KiB.
    pub const DEFAULT_READ_BUFFER: usize = 64 * 1024;

    /// A channel on `stream`, framed by `codec`, with the default settings. It does no I/O until
    /// called.
    pub fn new<S, C>(stream: S, codec: C) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
        C: Codec,
    {
        Self::launch(stream, codec, StreamChannelSettings::new())
    }

    fn stop(&self, stop: Stop) {
        self.stop.send_if_modified(|now| {
            if now.is_none() {
                *now = Some(stop);
                true
            } else {
                false
            }
        });
        if matches!(stop, Stop::Failed(_)) {
            self.ended.store(true, Ordering::SeqCst);
        }
    }

    async fn receive_now(&self) -> Result<Option<Vec<u8>>, Failure> {
        let mut stop = self.stop.subscribe();
        let mut guard = self.reading.lock().await;
        let reading = &mut *guard;
        loop {
            match reading.end {
                Some(End::Clean) => return Ok(None),
                Some(End::Failed(failure)) => return Err(failure),
                None => {}
            }
            let bound = locked(&self.bounds).next();
            reading.decoder.set_maximum(bound);
            match reading.decoder.next_frame() {
                Ok(Some(message)) => {
                    locked(&self.bounds).taken += 1;
                    return Ok(Some(message));
                }
                Ok(None) => {}
                Err(_) => return Err(self.end_reading(reading, Failure::Protocol)),
            }
            match *stop.borrow_and_update() {
                Some(Stop::Closed) => return Ok(None),
                Some(Stop::Failed(failure)) => {
                    reading.end = Some(End::Failed(failure));
                    return Err(failure);
                }
                None => {}
            }
            let read = {
                let Reading { reader, buffer, .. } = &mut *reading;
                match first(reader.read(buffer), stop.changed()).await {
                    First::One(read) => Some(read),
                    First::Other(_) => None,
                }
            };
            match read {
                None => {}
                Some(Ok(0)) if reading.decoder.buffered() == 0 => {
                    reading.end = Some(End::Clean);
                    self.ended.store(true, Ordering::SeqCst);
                    return Ok(None);
                }
                Some(Ok(0)) => return Err(self.end_reading(reading, Failure::PathLost)),
                Some(Ok(count)) => reading.decoder.push(&reading.buffer[..count]),
                Some(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                Some(Err(_)) => return Err(self.end_reading(reading, Failure::PathLost)),
            }
        }
    }

    /// Ends the channel from the reading side, and the writing half with it if no send holds it.
    fn end_reading(&self, reading: &mut Reading, failure: Failure) -> Failure {
        reading.end = Some(End::Failed(failure));
        self.stop(Stop::Failed(failure));
        if let Ok(mut writing) = self.writing.try_lock() {
            writing.ended.get_or_insert(failure);
        }
        failure
    }

    async fn send_now(&self, message: &[u8], turn: Option<Duration>) -> Result<(), Failure> {
        let bound = locked(&self.bounds).max_send;
        let frame = self
            .encoder
            .frame(message, bound)
            .map_err(|_| Failure::Protocol)?;
        let mut guard = match turn {
            None => self.writing.lock().await,
            Some(limit) => within(limit, self.writing.lock())
                .await
                .ok_or(Failure::Timeout)?,
        };
        let writing = &mut *guard;
        if let Some(failure) = writing.ended {
            return Err(failure);
        }
        match *self.stop.borrow() {
            Some(Stop::Closed) => return Err(Failure::PathLost),
            Some(Stop::Failed(failure)) => return Err(failure),
            None => {}
        }
        self.finish(writing).await?;
        // The frame is kept as the one under way only once the stream took part of it, so a send
        // dropped before then sent nothing.
        loop {
            let failure = match within(self.send_limit, writing.writer.write(&frame)).await {
                None => Failure::NoProgress,
                Some(Ok(0)) => Failure::PathLost,
                Some(Ok(count)) => {
                    writing.partial = Some((frame, count));
                    return self.finish(writing).await;
                }
                Some(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
                Some(Err(_)) => Failure::PathLost,
            };
            writing.ended = Some(failure);
            self.stop(Stop::Failed(failure));
            return Err(failure);
        }
    }

    /// Writes what is left of the frame under way, then flushes.
    async fn finish(&self, writing: &mut Writing) -> Result<(), Failure> {
        let Writing {
            writer,
            partial,
            ended,
            ..
        } = writing;
        while let Some((frame, taken)) = partial.as_mut() {
            if *taken >= frame.len() {
                *partial = None;
                break;
            }
            let failure = match within(self.send_limit, writer.write(&frame[*taken..])).await {
                None => Failure::NoProgress,
                Some(Ok(0)) => Failure::PathLost,
                Some(Ok(count)) => {
                    *taken += count;
                    continue;
                }
                Some(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
                Some(Err(_)) => Failure::PathLost,
            };
            *partial = None;
            *ended = Some(failure);
            self.stop(Stop::Failed(failure));
            return Err(failure);
        }
        let failure = match within(self.send_limit, writer.flush()).await {
            Some(Ok(())) => return Ok(()),
            None => Failure::NoProgress,
            Some(Err(_)) => Failure::PathLost,
        };
        *ended = Some(failure);
        self.stop(Stop::Failed(failure));
        Err(failure)
    }

    async fn close_now(&self) {
        self.stop(Stop::Closed);
        self.ended.store(true, Ordering::SeqCst);
        let Some(mut guard) = within(self.send_limit, self.writing.lock()).await else {
            return;
        };
        let writing = &mut *guard;
        if writing.shut {
            return;
        }
        writing.shut = true;
        if writing.ended.is_none() && self.finish(writing).await.is_err() {
            return;
        }
        let _ = within(self.send_limit, writing.writer.shutdown()).await;
        writing.ended.get_or_insert(Failure::PathLost);
    }
}

impl fmt::Debug for StreamChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamChannel")
            .field("bounds", &*locked(&self.bounds))
            .field("send_limit", &self.send_limit)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl AsyncChannel for StreamChannel {
    fn send<'a>(&'a self, message: &'a [u8]) -> BoxFuture<'a, Result<(), Failure>> {
        Box::pin(self.send_now(message, None))
    }

    /// Waits at most `timeout` for sends already under way; once its own frame starts, the send
    /// limit governs it.
    fn send_timeout<'a>(
        &'a self,
        message: &'a [u8],
        timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Failure>> {
        Box::pin(self.send_now(message, timeout))
    }

    fn receive(
        &self,
        timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Failure>> {
        Box::pin(async move {
            match timeout {
                None => self.receive_now().await,
                Some(limit) => within(limit, self.receive_now())
                    .await
                    .unwrap_or(Err(Failure::Timeout)),
            }
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.close_now())
    }

    fn set_max_message(&self, bytes: usize) {
        let mut bounds = locked(&self.bounds);
        bounds.receive_now = bytes;
        bounds.staged = None;
        bounds.max_send = bytes;
    }

    fn set_max_receive(&self, bytes: usize) {
        let mut bounds = locked(&self.bounds);
        bounds.receive_now = bytes;
        bounds.staged = None;
    }

    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> {
        let mut bounds = locked(&self.bounds);
        if messages == 0 {
            bounds.receive_now = bytes;
            bounds.staged = None;
        } else {
            let from = bounds
                .taken
                .saturating_add(u64::try_from(messages).unwrap_or(u64::MAX));
            bounds.staged = Some((from, bytes));
        }
        Ok(())
    }

    fn set_max_send(&self, bytes: usize) {
        locked(&self.bounds).max_send = bytes;
    }

    fn capabilities(&self) -> Capabilities {
        let bounds = locked(&self.bounds);
        Capabilities::new()
            .carrier(Carrier::Stream)
            .limits(bounds.max_send, bounds.receive_now)
    }

    fn is_closed(&self) -> bool {
        self.ended.load(Ordering::SeqCst)
    }
}

/// What an async [`StreamChannelBuilder`] takes whole through `fill`, in the form a file, a form
/// or a peer fills.
///
/// Every key is optional (an absent one is its default) and an unknown key is refused; a duration
/// is a number with an optional unit (`30`, `250ms`, `1.5h`). The type is `coqui_builder::Describe`:
/// a `Partial` fills it, and its setters are methods of [`StreamChannelBuilder`], and [`StreamChannelSettingsSetters`] for a builder elsewhere. It is
/// `#[non_exhaustive]`: start from [`Default`] and assign the fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Describe)]
#[map(deny_unknown_fields)]
#[builder(setters_on = <C: Codec, S: AsyncRead + AsyncWrite + Send + 'static> StreamChannelBuilder<C, S>)]
#[non_exhaustive]
pub struct StreamChannelSettings {
    /// The largest message, both ways, until a `set_max_` call changes it. Default
    /// [`StreamChannel::DEFAULT_MAX_MESSAGE`].
    #[map(default = StreamChannelSettings::new().max_message)]
    pub max_message: usize,
    /// How long one write or flush may wait with the stream taking nothing, after which the channel
    /// ends with [`Failure::NoProgress`]; also the most `close` waits for each step. Default
    /// [`StreamChannel::DEFAULT_SEND_LIMIT`].
    #[map(default = StreamChannelSettings::new().send_limit)]
    pub send_limit: Duration,
    /// The most bytes one read takes from the stream. 0 is 1. Default
    /// [`StreamChannel::DEFAULT_READ_BUFFER`].
    #[map(default = StreamChannelSettings::new().read_buffer)]
    pub read_buffer: usize,
}

impl StreamChannelSettings {
    /// Every setting at its default.
    pub const fn new() -> Self {
        Self {
            max_message: StreamChannel::DEFAULT_MAX_MESSAGE,
            send_limit: StreamChannel::DEFAULT_SEND_LIMIT,
            read_buffer: StreamChannel::DEFAULT_READ_BUFFER,
        }
    }

    /// Every setting that differs from its default, named by key path.
    #[must_use]
    pub fn non_defaults(&self) -> Vec<NonDefault> {
        coqui_builder::non_defaults(self)
    }
}

impl Default for StreamChannelSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// What a [`StreamChannelBuilder`] holds: the settings, the codec that frames the messages and the
/// stream they travel on. Build with `StreamChannelBuilder::new()`, set the limits through its
/// setters (or `fill`), give `codec` and `stream`, then `build`.
///
/// `build` splits the stream into a reading and a writing half and does no I/O. The builder's
/// `non_defaults()` lists the settings that differ from their defaults.
#[derive(Builder, Debug)]
#[builder(build = StreamChannel, entry = "new")]
pub struct StreamChannelParts<C, S>
where
    C: Codec,
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    settings: StreamChannelSettings,
    #[builder(input)]
    codec: C,
    #[builder(input)]
    stream: S,
}

impl<C, S> From<StreamChannelParts<C, S>> for StreamChannel
where
    C: Codec,
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    fn from(parts: StreamChannelParts<C, S>) -> Self {
        Self::launch(parts.stream, parts.codec, parts.settings)
    }
}

impl StreamChannel {
    /// Splits `stream` into its halves and wraps them with `codec` under `settings`.
    fn launch<S, C>(stream: S, codec: C, settings: StreamChannelSettings) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
        C: Codec,
    {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reading: tokio::sync::Mutex::new(Reading {
                reader: Box::new(reader),
                decoder: Box::new(codec.decoder(settings.max_message)),
                buffer: alloc::vec![0; settings.read_buffer.max(1)],
                end: None,
            }),
            writing: tokio::sync::Mutex::new(Writing {
                writer: Box::new(writer),
                partial: None,
                ended: None,
                shut: false,
            }),
            bounds: Mutex::new(Bounds {
                receive_now: settings.max_message,
                staged: None,
                taken: 0,
                max_send: settings.max_message,
            }),
            stop: watch::Sender::new(None),
            ended: AtomicBool::new(false),
            encoder: Box::new(codec),
            send_limit: settings.send_limit,
        }
    }
}
