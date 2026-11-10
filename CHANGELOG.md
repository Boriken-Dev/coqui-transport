# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Channel`, `Datagrams` and `Delivery` (`Unreliable`, `OverStream`), with the methods and meaning
  they have in `coqui-web-channel` 0.0.0-alpha.0, and two provided methods on `Channel`:
  `capabilities`, which reports what carries the messages, the limits as they stand and the
  datagrams as they travel now, and `is_closed`, `true` once this side knows the channel ended.
  `Box<dyn Channel>` forwards every method.
- `Failure`, with every variant of `coqui-web-channel`'s and `is_path`. `Failure::recovery`, which
  decides a WebTransport-to-WebSocket fallback, is not part of this crate.
- `Capabilities` and `Carrier` (`Unknown`, `Messages`, `Stream`).
- `Connector` and `Listener`, which open and take connections apart from any channel.
- `Duplex<R, W>`, a reader and a writer as one stream (also `AsyncRead`/`AsyncWrite` with
  `tokio`); `Split`, `Halves` and `Interrupt`, a stream split for two threads and how to wake them.
- `frame`: the `Codec` and `Decode` traits, `FrameError`, `DEFAULT_MAX_MESSAGE` (1 MiB), and one
  codec, `LengthPrefix` with its `LengthDecoder`: a one-, two- or four-byte length, big- or
  little-endian (`u8`, `u16_be`, `u16_le`, `u32_be`, `u32_le`), with empty messages allowed
  (`allow_empty`, the constructors' state) or refused (`refuse_empty`). No project's framing is
  in the crate; a protocol defines its own as a preset of `LengthPrefix`.
- `StreamChannelSettings` (and `asynch::StreamChannelSettings`): the builders' limits as a `coqui_builder::Describe` type a file
  fills (a `Partial` of it, with coqui-builder's `serde` feature), set by methods of the builders (and by `prelude::StreamChannelSettingsSetters` for a builder elsewhere), with
  `non_defaults()` named by key path. `Capabilities` sets by `carrier`, `limits` and `datagrams`, and `Halves` by
  `interrupt`. coqui-builder is a dependency; nothing else is.
- `StreamChannel`, `StreamChannelParts` and `StreamChannelBuilder`: a `Channel` over any `Split` stream, with settings
  `max_message` (1 MiB), `pending` (64 messages read ahead), `queued_bytes` (64 MiB), `send_limit`
  (30 s), `read_buffer` (64 KiB) and `linger` (500 ms: how long `close` reads and discards, after
  the peer last sent anything, waiting for the peer's end, so a socket is not reset with bytes
  unread), and `non_defaults` returning those above their defaults as `NonDefault` data (the
  setting, its value and its default), never logged or refused.
- `memory::pipe`, two connected in-memory streams with a bounded buffer each way.
- `stdio::current`, this process's standard input and output as one `Duplex`.
- `tcp`: `Split` for `std::net::TcpStream` (with `tcp::WriteHalf`; a socket with no read timeout
  gets `tcp::WAKE`, 250 ms, since a Windows socket's `shutdown` does not wake a read blocked on it),
  `tcp::Connector`, and `Listener` for `std::net::TcpListener`.
- Feature `tokio`: `asynch::{AsyncChannel, AsyncConnector, AsyncListener, BoxFuture}`, object-safe;
  `asynch::StreamChannel` and its builder (`asynch::StreamChannelParts`); `AsyncConnector` for `tcp::Connector` and
  `AsyncListener` for `tokio::net::TcpListener`.

### Changed

- The stream channel builders are derived by coqui-builder from `StreamChannelParts<C, S>` (the settings, the `codec` and
  the `stream` as inputs): `StreamChannelBuilder::new().codec(c).max_message(1024usize).stream(s).build()`, which
  gives `Result<StreamChannel, BuildError<io::Error>>`, and for `asynch` `Result<StreamChannel, Missing>`. The
  limits' setters are inherent methods of the builder (an integer setting takes `usize`, a duration `impl Into<Duration>`),
  also as `prelude::StreamChannelSettingsSetters` (`asynch::prelude` for the async one). `fill(Partial<..>)` and `fill_from(source)` layer settings, and `parts()` gives the parts.
  `StreamChannel::start(stream, codec)` and `asynch::StreamChannel::new(stream, codec)` stay.
- `NonDefault` is `coqui_builder::NonDefault`: `value` and `default` are `Shown`, printed as the type and its text
  (`usize(6)`, `Duration(31s)`) where they were plain text (`6`, `[31,0]`).
- A duration setting reads a number with an optional unit (`30`, `250ms`, `1.5h`), no longer `[secs, nanos]` or
  `{secs, nanos}`.

### Removed

- `StreamChannel::builder(codec)` and `asynch::StreamChannel::builder(codec)`.
- The builders' `start(stream)` and `settings(settings)` (`build()` after `stream(..)`; `fill(Partial::from(settings))`),
  their `const` setters, and their `Clone`.
- `NonDefault::new`, `NonDefault::from_difference` and `NonDefault::of_settings` (`coqui_builder::non_defaults(&settings)`).
- The `guatiao` and `guatiao-serde` dependencies, and the guatiao value form of the settings types (`ToValue`, `FromValue`,
  `Schema`), so a settings value is read from text but no longer written to it.
