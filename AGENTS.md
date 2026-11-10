# `coqui-transport` — API header

Header-file-style reference for the `coqui-transport` crate: every public item with its signature, contract and gotchas, so the
crate can be used without reading its source. It ships inside the package and is self-contained. Development documentation lives
with the source at <https://github.com/Boriken-Dev/coqui-transport>.

| Feature | Adds | Needed for | Default |
| --- | --- | --- | --- |
| `tokio` | `asynch::{AsyncChannel, AsyncConnector, AsyncListener, BoxFuture, StreamChannel, StreamChannelBuilder, StreamChannelParts, StreamChannelSettings}`, `asynch::prelude::StreamChannelSettingsSetters`, `Duplex` as `AsyncRead`/`AsyncWrite`, `AsyncConnector for tcp::Connector`, `AsyncListener for tokio::net::TcpListener`; on `tokio` with `io-util`, `net`, `sync`, `time` | async transports | off |

MSRV Rust 1.89. A default build depends on `coqui-builder` (by revision, with its `derive` feature: the settings types' description, fill, setters and builders, and `NonDefault`) and nothing beyond `std`. Version `0.0.0-alpha.0`, not published; the API may change in
any release before 1.0. The default build compiles for `wasm32-unknown-unknown`; `--features tokio` does not (tokio's `net`).

Imports: the root holds `Channel`, `Datagrams`, `Delivery`, `Failure`, `Capabilities`, `Carrier`, `Connector`, `Listener`, `NonDefault`,
`Duplex`, `Halves`, `Interrupt`, `Split`, `StreamChannel`, `StreamChannelBuilder`, `StreamChannelParts`, `StreamChannelSettings`. Public modules: `prelude`, `frame`, `memory`, `stdio`,
`tcp`, `asynch` (feature `tokio`).

No `unsafe` code (`#![forbid(unsafe_code)]`). No process-wide state. The library prints nothing and starts no runtime; the only
threads it starts are a `StreamChannel`'s two.

## Channels (crate root)

```rust
pub trait Channel: Send + Sync {
    fn send(&self, message: &[u8]) -> Result<(), Failure>;
    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure>;   // None: the peer closed in order
    fn close(&self);                                                                       // repeatable, also after a failure
    fn set_max_message(&self, _bytes: usize) {}                                            // both ways, from now on; default ignores it
    fn set_max_receive(&self, _bytes: usize) {}
    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> { /* 0: set_max_receive; else Err(Unsupported) */ }
    fn set_pending(&self, _messages: usize) -> Result<(), Failure> { Err(Failure::Unsupported) }   // read-ahead; 0 is 1
    fn set_max_send(&self, _bytes: usize) {}
    fn send_timeout(&self, message: &[u8], _timeout: Option<Duration>) -> Result<(), Failure> { self.send(message) }
    fn datagrams(&self) -> Option<&dyn Datagrams> { None }
    fn peer_certificates(&self) -> Option<&[Vec<u8>]> { None }                           // DER, peer's first: transport metadata only
    fn capabilities(&self) -> Capabilities { /* only what datagrams() says */ }
    fn is_closed(&self) -> bool { false }
}
impl Channel for Box<dyn Channel>                                                          // forwards every method
```

A reliable, ordered channel of whole messages, not authenticated: whoever opened it runs the peer's proof before trusting what
arrives. `receive`: `Failure::Timeout` when nothing arrived in time (the channel goes on); `None` waits without limit; any other
failure ended the channel. `send`: `Failure::Protocol` for a message outside its bounds (the channel goes on).

`set_max_receive_after(n, bytes)` stages the read bound by position: the next `n` messages `receive` has not returned are held to
the bound in force, every one after them to `bytes`, however far the driver reads ahead. Positions count from what `receive` has
returned at the call. A later `set_max_receive_after`, `set_max_receive` or `set_max_message` replaces a stage not reached.

`set_pending(n)`: the driver holds at most `n` unreturned messages and reads nothing more until the reader takes one, so the
peer's sends wait. A lower limit drops nothing; a higher one lets the driver read on at once.

`send_timeout`: waits up to `timeout` for room; `Failure::Timeout` means nothing was sent and the channel goes on.

`capabilities()` is a snapshot when called: ask again after anything that can change it. `is_closed()`: `true` once this side
knows the channel ended (closed here, a failure, or the peer's end); `false` promises nothing.

```rust
pub enum Delivery { Unreliable, OverStream }                                               // non_exhaustive

pub trait Datagrams: Send + Sync {
    fn max_size(&self) -> Option<usize>;                                                   // fits now; None: peer takes none
    fn send(&self, payload: &[u8]) -> Result<(), Failure>;                                 // Protocol if too large; Unsupported; PathLost
    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure>;      // None once the connection ended
    fn dropped(&self) -> u64;                                                              // discarded on this side
    fn delivery(&self) -> Delivery;                                                        // how they travel now
    fn path_changes(&self) -> Option<std::sync::mpsc::Receiver<Delivery>> { None }         // each later change; None: never changes
}
```

`Unreliable`: real network datagrams, lost, reordered or repeated, never held up. `OverStream`: inside the channel's ordered
stream, in order and late rather than lost, lost only where a queue is full (counted by `dropped`). Emulated datagrams are never
`Unreliable`. Datagrams are outside any identity proof run on the messages.

## Failures (crate root)

```rust
pub enum Failure {                                                                         // non_exhaustive, Copy, Error
    Unsupported, Timeout, PathLost, NoProgress, SuspectedMtu,
    Certificate, PeerProof, Protocol, Permission, Cancelled,
}
impl Failure { pub const fn is_path(self) -> bool }                                        // the first five
```

## Capabilities (crate root)

```rust
pub enum Carrier { Unknown /* default */, Messages, Stream }                               // non_exhaustive
pub struct Capabilities {                                                                  // non_exhaustive, Copy, Default
    pub carrier: Carrier,
    pub max_send: Option<usize>,
    pub max_receive: Option<usize>,
    pub datagrams: Option<Delivery>,
    pub max_datagram: Option<usize>,
}
impl Capabilities {
    pub const fn new() -> Self;                                                            // nothing reported
    pub const fn carrier(self, carrier: Carrier) -> Self;
    pub const fn limits(self, max_send: usize, max_receive: usize) -> Self;
    pub const fn datagrams(self, datagrams: Option<(Delivery, Option<usize>)>) -> Self;
}
```

`Carrier::Stream`: frames on one ordered byte stream, so one late message holds up every message behind it.

## Connecting (crate root)

```rust
pub trait Connector { type Connection; type Error: core::error::Error;
    fn connect(&self, timeout: Duration) -> Result<Self::Connection, Self::Error>; }
pub trait Listener { type Connection; type Error: core::error::Error;
    fn accept(&self) -> Result<Self::Connection, Self::Error>; }
```

Kept apart from channels: a transport implements only what it has. A connection is not an authenticated peer.

## Streams (crate root)

```rust
pub struct Duplex<R, W>;                       // Read from R, Write to W; Clone, Debug, Default
impl<R, W> Duplex<R, W> {
    pub const fn new(reader: R, writer: W) -> Self;
    pub const fn reader(&self) -> &R;  pub const fn writer(&self) -> &W;
    pub const fn reader_mut(&mut self) -> &mut R;  pub const fn writer_mut(&mut self) -> &mut W;
    pub fn into_parts(self) -> (R, W);
}
pub struct Interrupt;                          // Clone; ends both directions from any thread, may be called again
impl Interrupt { pub fn new(end: impl Fn() + Send + Sync + 'static) -> Self; pub fn interrupt(&self); }
pub struct Halves<R, W> { pub reader: R, pub writer: W, pub interrupt: Option<Interrupt> }   // non_exhaustive
impl<R, W> Halves<R, W> { pub const fn new(reader: R, writer: W) -> Self; pub fn interrupt(self, i: Interrupt) -> Self; }
pub trait Split {
    type Reader: Read + Send + 'static;
    type Writer: Write + Send + 'static;       // dropping it ends the write direction
    fn split(self) -> io::Result<Halves<Self::Reader, Self::Writer>>;
}
impl Split for Halves<R, W>; impl Split for Duplex<R, W>;      // no interrupt from a Duplex
impl From<Duplex<R, W>> for Halves<R, W>;
```

With `tokio`, `Duplex<R, W>` is `AsyncRead` when `R: AsyncRead + Unpin, W: Unpin` and `AsyncWrite` when `W: AsyncWrite + Unpin`.

## Framing (`coqui_transport::frame`)

```rust
pub const DEFAULT_MAX_MESSAGE: usize = 1024 * 1024;
pub enum FrameError { Size, Tag }                                                          // non_exhaustive
pub trait Codec: Send + Sync + 'static {
    type Decoder: Decode + Send + 'static;
    fn encode(&self, message: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError>;      // Size: over maximum, too long, or refused empty
    fn decoder(&self, maximum: usize) -> Self::Decoder;
}
pub trait Decode {                                                                         // object-safe
    fn set_maximum(&mut self, maximum: usize);                                             // from the next header checked
    fn push(&mut self, part: &[u8]);                                                       // a failed decoder discards it
    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError>;                       // Size, and on every later call
    fn buffered(&self) -> usize;                                                           // 0 between frames
}

pub struct LengthPrefix;                       // Clone, Copy, Debug, Eq, Hash, must_use: a fixed-width length, then the message
impl LengthPrefix {
    pub const fn u8() -> Self;                 // 1 byte: messages of at most 255 bytes
    pub const fn u16_be() -> Self;  pub const fn u16_le() -> Self;      // 2 bytes: at most 65535
    pub const fn u32_be() -> Self;  pub const fn u32_le() -> Self;      // 4 bytes
    pub const fn allow_empty(self) -> Self;    // every constructor leaves it so
    pub const fn refuse_empty(self) -> Self;   // encode fails on an empty message, a header declaring one fails the decoder
    pub const fn width(&self) -> usize;  pub const fn is_big_endian(&self) -> bool;  pub const fn empty_allowed(&self) -> bool;
}
impl Codec for LengthPrefix { type Decoder = LengthDecoder; }
pub struct LengthDecoder;                      // Debug; LengthDecoder::new(prefix: LengthPrefix, maximum: usize); implements Decode
```

There is no default codec and none is ever guessed: both ends are configured for the same one, and nothing in the bytes says
which. `encode` refuses (`FrameError::Size`) a message over `maximum`, one the width cannot declare (256 bytes on `u8`), or an
empty one where refused. A decoder returns frames complete before a bad header first, then fails for good and holds nothing. It
never allocates for a declared length, only for bytes pushed. `LengthPrefix` carries messages only: no datagrams, logical
channels or version. A protocol's own framing is a preset of it, defined where the protocol is.

## StreamChannel (crate root)

```rust
impl StreamChannel {
    pub const DEFAULT_MAX_MESSAGE: usize;      // 1 MiB
    pub const DEFAULT_PENDING: usize;          // 64
    pub const DEFAULT_QUEUED_BYTES: usize;     // 64 MiB
    pub const DEFAULT_SEND_LIMIT: Duration;    // 30 s
    pub const DEFAULT_READ_BUFFER: usize;      // 64 KiB
    pub const DEFAULT_LINGER: Duration;        // 500 ms
    pub fn start<S: Split, C: Codec>(stream: S, codec: C) -> io::Result<Self>;   // default settings
}
// Derived by coqui_builder::Builder from StreamChannelParts<C: Codec, S: Split> (settings + inputs `codec`, `stream`).
// Not Clone. The limits' setters are its inherent methods (`setters_on`): no import.
impl<C: Codec, S: Split> StreamChannelBuilder<C, S> {
    pub fn new() -> Self;                                         // also Default
    pub fn codec(self, codec: C) -> Self; pub fn stream(self, stream: S) -> Self;   // both required
    pub fn max_message(self, bytes: usize) -> Self; pub fn pending(self, messages: usize) -> Self;   // 0 is 1
    pub fn queued_bytes(self, bytes: usize) -> Self;             // each message counts its frame + 64
    pub fn send_limit(self, limit: impl Into<Duration>) -> Self; pub fn read_buffer(self, bytes: usize) -> Self;   // 0 is 1
    pub fn linger(self, quiet: impl Into<Duration>) -> Self;     // close's wait for the peer's end
    pub fn fill(self, settings: Partial<StreamChannelSettings>) -> Self;            // only the keys set; the last write wins
    pub fn fill_from(self, source: &mut impl Source) -> Result<Self, FillError>;
    pub fn non_defaults(&self) -> Vec<NonDefault>;                // every setting that differs from its default, by key path
    pub fn parts(self) -> Result<StreamChannelParts<C, S>, Missing>;
    pub fn build(self) -> Result<StreamChannel, BuildError<io::Error>>;   // splits, starts two threads; Missing names every unset key and input
}
// prelude::StreamChannelSettingsSetters: the same setters as a trait over any `Holds<StreamChannelSettings>`, for a builder in
// another crate (an integer setting takes `usize` itself, a duration `impl Into<Duration>`).
pub trait StreamChannelSettingsSetters: Sized { /* the five setters above, by the same names */ }
impl Channel for StreamChannel; impl Drop for StreamChannel;     // drop closes
// coqui_builder::Describe: every key optional, unknown keys refused, a duration is a number with an optional unit (`30`, `250ms`, `1.5h`).
// Read from text with `Partial<StreamChannelSettings>` (feature `serde` of coqui-builder), then `build()`.
#[non_exhaustive] pub struct StreamChannelSettings { pub max_message: usize, pub pending: usize, pub queued_bytes: usize,
    pub send_limit: Duration, pub read_buffer: usize, pub linger: Duration }   // Default: the DEFAULT_ constants
impl StreamChannelSettings { pub const fn new() -> Self; pub fn non_defaults(&self) -> Vec<NonDefault>; }   // also Default

pub use coqui_builder::NonDefault;   // { setting: String, value: Shown, default: Shown }
```

`non_defaults()` returns data and nothing is logged or refused: the application that knows the use case decides what a
different limit means. `value` and `default` print as the type and its text (`usize(6)`, `Duration(31s)`), and `setting` is the
key path in `StreamChannelSettings`. `coqui_builder::non_defaults(&settings)` makes the list for any described settings type.

Implements every `Channel` method: staging and `set_pending` return `Ok`, `datagrams()` is `None`, `capabilities()` is
`Carrier::Stream` with the current limits. A reader thread decodes and holds up to `pending` messages, judging each by the bound
for its position; a header over it is `Failure::Protocol` (before its body is read) at both ends, since the stream is then
ended. The stream ending between frames is `Ok(None)`; inside one, `Failure::PathLost`. `send` encodes at once and queues;
past `queued_bytes` it is `Failure::NoProgress` with `is_closed()` still `false` (nothing sent, the channel goes on), and
`send_timeout` waits for room. A frame the stream takes nothing of for `send_limit` ends the channel with `Failure::NoProgress`
(`is_closed()` then `true`), seen by the next send waiting on the writer, or by the writer on a stream whose writes time out.
Each write call is given at most 64 KiB, so a slow but steady stream shows progress. `close` refuses further sends (then
`Failure::PathLost`), waits up to `send_limit` for the queue, drops the writer (the peer reads the end), then lets the reader
read and discard until the peer's end or until the peer has sent nothing for `linger` (never past `send_limit` in all), since
a socket ended with bytes unread is reset and the peer loses what was sent last; then it calls the `Interrupt` and joins each
thread, waiting at most `send_limit` for one, or not at all without an `Interrupt`. Messages already read stay readable, then
`Ok(None)`. Read and write errors `TimedOut`, `WouldBlock` and `Interrupted` are retried. `capabilities().max_receive` is the
bound for the next message the reader thread reads, which with read-ahead can be past what `receive` returned.

`start` errors when the stream cannot split or a thread cannot start (any target without threads).

## Adapters

```rust
// coqui_transport::memory
pub fn pipe(capacity: usize) -> (Stream, Stream);              // each way at most capacity unread bytes; 0 is 1
pub struct Stream;  impl Read, Write, Split for Stream;         // split gives an Interrupt
impl Stream { pub fn into_halves(self) -> (ReadHalf, WriteHalf); }   // no Interrupt
pub struct ReadHalf;  impl Read;   // dropped: the other end's writes fail BrokenPipe
pub struct WriteHalf; impl Write;  // dropped: the other end reads the end after what was written

// coqui_transport::stdio
pub fn current() -> Duplex<Stdin, Stdout>;

// coqui_transport::tcp
pub const WAKE: Duration;                                      // 250 ms: the read timeout split gives a socket with none
impl Split for TcpStream { type Reader = TcpStream; type Writer = tcp::WriteHalf; }   // Interrupt: shutdown(Both)
pub struct WriteHalf; impl Write; impl WriteHalf { pub const fn get_ref(&self) -> &TcpStream; }   // drop: shutdown(Write)
pub struct Connector;                                          // Clone, must_use
impl Connector {
    pub fn new(addresses: impl IntoIterator<Item = SocketAddr>) -> Self;
    pub const fn nodelay(self, nodelay: bool) -> Self;          // default true
    pub fn addresses(&self) -> &[SocketAddr];
    pub fn connect(&self, timeout: Duration) -> io::Result<TcpStream>;
}
impl coqui_transport::Connector for tcp::Connector { type Connection = TcpStream; type Error = io::Error; }
impl Listener for TcpListener { type Connection = TcpStream; type Error = io::Error; }
```

A memory blocked read woken by the interrupt fails `ConnectionAborted`, a blocked write `BrokenPipe`. Splitting a
`TcpStream` with no read timeout sets `WAKE` on it, so a split reading half's reads end with `WouldBlock`/`TimedOut` that often
(a `StreamChannel` retries them): a Windows socket's `shutdown` does not wake a read blocked in another thread. `tcp::Connector::connect`
tries each address in order within one `timeout`: the last address's error, `TimedOut`, or `InvalidInput` for no address or a
zero timeout; a timeout too long for the clock is none. It resolves no names.

## Async (`coqui_transport::asynch`, feature `tokio`)

```rust
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub trait AsyncChannel: Send + Sync {                                                     // object-safe
    fn send<'a>(&'a self, message: &'a [u8]) -> BoxFuture<'a, Result<(), Failure>>;
    fn send_timeout<'a>(&'a self, message: &'a [u8], timeout: Option<Duration>) -> BoxFuture<'a, Result<(), Failure>> { self.send(message) }
    fn receive(&self, timeout: Option<Duration>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Failure>>;
    fn close(&self) -> BoxFuture<'_, ()>;
    fn set_max_message(&self, _: usize) {}  fn set_max_receive(&self, _: usize) {}  fn set_max_send(&self, _: usize) {}
    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> { /* as Channel */ }
    fn capabilities(&self) -> Capabilities { Capabilities::new() }
    fn is_closed(&self) -> bool { false }
}
pub trait AsyncConnector { type Connection: Send; type Error: Error + Send;
    fn connect(&self, timeout: Duration) -> BoxFuture<'_, Result<Self::Connection, Self::Error>>; }
pub trait AsyncListener { type Connection: Send; type Error: Error + Send;
    fn accept(&self) -> BoxFuture<'_, Result<Self::Connection, Self::Error>>; }

impl StreamChannel {                               // asynch::StreamChannel
    pub const DEFAULT_MAX_MESSAGE: usize; pub const DEFAULT_SEND_LIMIT: Duration; pub const DEFAULT_READ_BUFFER: usize;
    pub fn new<S: AsyncRead + AsyncWrite + Send + 'static, C: Codec>(stream: S, codec: C) -> Self;   // no I/O
}
// Derived from asynch::StreamChannelParts<C: Codec, S: AsyncRead + AsyncWrite + Send + 'static>; not Clone.
impl<C, S> StreamChannelBuilder<C, S> {
    pub fn new() -> Self;                                         // also Default
    pub fn codec(self, codec: C) -> Self; pub fn stream(self, stream: S) -> Self;   // both required
    pub fn fill(self, settings: Partial<asynch::StreamChannelSettings>) -> Self; pub fn fill_from(self, source: &mut impl Source) -> Result<Self, FillError>;
    pub fn non_defaults(&self) -> Vec<NonDefault>;
    pub fn parts(self) -> Result<asynch::StreamChannelParts<C, S>, Missing>;
    pub fn build(self) -> Result<StreamChannel, Missing>;         // no I/O
}
// asynch::StreamChannelBuilder has inherent max_message, send_limit and read_buffer; asynch::prelude::StreamChannelSettingsSetters is the trait, as for the sync settings.
#[non_exhaustive] pub struct asynch::StreamChannelSettings { pub max_message: usize, pub send_limit: Duration, pub read_buffer: usize }   // Describe, as above
impl AsyncChannel for asynch::StreamChannel;
```

The async `StreamChannel` spawns nothing and reads nothing ahead: `receive` reads and decodes, `send` writes, on the caller's
runtime, which needs tokio's I/O and time drivers. A dropped or timed-out `receive` loses nothing. A `send` dropped before the
stream took its first byte sent nothing; one dropped after has its frame finished by the next `send` or `close`, so that
message is sent and no frame is torn. `send_timeout` bounds only the wait for sends already under way. A write or flush the
stream takes nothing of for `send_limit` is `Failure::NoProgress`. `close` finishes a frame under way, shuts the writer down and
wakes `receive`, which returns what is already decoded, then `Ok(None)`; if a send under way holds the writer past
`send_limit`, `close` returns without shutting it down. Dropping it without `close` drops the stream as it stands.

## Gotchas

- A channel is not an authenticated peer, and `peer_certificates` is transport metadata, not application identity.
- Over `stdio::current()`, nothing else in the process may write to standard output. A Windows console refuses bytes that are
  not UTF-8; a pipe or file carries them unchanged. Neither stream can be interrupted, so after `close` a read already waiting on
  standard input ends only when input arrives or ends, and the peer reads the end when this process exits.
- A `Split` stream with no `Interrupt` (any `Duplex`) cannot wake a blocked read on `close`: its reader thread is left to end on
  its own and is not joined; it discards what it reads next. A thread an `Interrupt` does not wake (a write blocked for good on
  a custom stream) is left too, after `send_limit`, holding its half of the stream.
- What a peer can make one channel hold before it is proven: up to `pending` messages of `max_message` read ahead (64 MiB by
  default) plus one partial frame. Hold an unproven peer to `set_pending(1)` and a small `set_max_receive`, then raise them.
- `close` waits up to `linger` after the peer last sent anything for the peer's end: a peer that keeps its side open makes each
  `close` take that long. `linger(Duration::ZERO)` ends the stream at once, at the risk of a reset.
- A stream in non-blocking mode makes the reader thread spin: give `StreamChannel` a blocking stream (a read timeout is fine).
- `close` waits up to `send_limit` for the queue; with `Duration::MAX` it waits until the peer takes everything.
- `LengthPrefix` carries no datagrams: a datagram would reach a peer as an ordinary message.
- `Failure::recovery`, `Kind`, `Phase`, `Recovery`, `Dialer`, `connect`, `Preflight` and endpoints are not in this crate, and no
  protocol's own frame is (no `coqui-peer` or `coqui-web-channel` name appears here).
