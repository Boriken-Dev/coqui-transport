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
- `StreamChannel` and `StreamChannelBuilder`: a `Channel` over any `Split` stream, with settings
  `max_message` (1 MiB), `pending` (64 messages read ahead), `queued_bytes` (64 MiB), `send_limit`
  (30 s), `read_buffer` (64 KiB) and `linger` (500 ms: how long `close` reads and discards, after
  the peer last sent anything, waiting for the peer's end, so a socket is not reset with bytes
  unread), and `weakened` listing those above their defaults.
- `memory::pipe`, two connected in-memory streams with a bounded buffer each way.
- `stdio::current`, this process's standard input and output as one `Duplex`.
- `tcp`: `Split` for `std::net::TcpStream` (with `tcp::WriteHalf`; a socket with no read timeout
  gets `tcp::WAKE`, 250 ms, since a Windows socket's `shutdown` does not wake a read blocked on it),
  `tcp::Connector`, and `Listener` for `std::net::TcpListener`.
- Feature `tokio`: `asynch::{AsyncChannel, AsyncConnector, AsyncListener, BoxFuture}`, object-safe;
  `asynch::StreamChannel` and its builder; `AsyncConnector` for `tcp::Connector` and
  `AsyncListener` for `tokio::net::TcpListener`.
