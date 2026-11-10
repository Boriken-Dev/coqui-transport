# coqui-transport

[![Version](https://img.shields.io/crates/v/coqui-transport.svg)](https://crates.io/crates/coqui-transport)
[![Rust version](https://img.shields.io/crates/msrv/coqui-transport.svg)](https://crates.io/crates/coqui-transport)
[![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](https://github.com/Boriken-Dev/coqui-transport/blob/main/LICENSE)
[![Docs](https://img.shields.io/docsrs/coqui-transport)](https://docs.rs/coqui-transport)
[![CI](https://img.shields.io/github/actions/workflow/status/Boriken-Dev/coqui-transport/test.yml)](https://github.com/Boriken-Dev/coqui-transport/actions/workflows/test.yml)

**Applications talk through streams, channels or datagrams; the transport decides how they are
carried.** This crate holds the contracts every Coqui transport shares (a reliable message
`Channel`, the `Datagrams` beside it and how they are delivered, connectors and listeners), the
framing codecs that turn a byte stream into messages without guessing a wire, a `StreamChannel` that
does it over any stream, and adapters for memory, this process's standard streams and TCP. It knows
nothing of peers, trust or applications. Documentation:
<https://boriken-dev.github.io/coqui-transport/>.

## Features

- **Three models, none forced.** A stream is `std::io::Read` and `Write` (or tokio's `AsyncRead`
  and `AsyncWrite`); a channel is reliable, ordered, whole messages; datagrams are whole packets
  whose `Delivery` says whether they are really unreliable (`Unreliable`) or ride inside an ordered
  stream (`OverStream`). A transport implements only the ones it has.
- **One stream-to-channel adapter.** `StreamChannel` frames any stream that splits into a reading
  and a writing half: incremental decoding, a bound on every buffer, limits that are settings,
  send and receive timeouts, backpressure from a slow reader to the peer, partial reads and writes,
  concurrent sending and receiving, and an end that is either clean or a failure, never a guess.
- **One configurable framing, never guessed.** `frame::LengthPrefix` is a one-, two- or four-byte
  length, big- or little-endian, with empty messages allowed or refused, each chosen by a named
  constructor (`LengthPrefix::u32_be()`, `u32_le().refuse_empty()`). There is no default: both ends
  name the one they speak. A protocol's own framing is a preset of it, defined by that protocol.
- **Capabilities asked, not assumed.** `Channel::capabilities` reports what carries the messages,
  the limits as they stand and the datagrams as they travel now; it is asked again whenever it
  matters, since a connection can change.
- **Connecting apart from using.** `Connector` and `Listener` open and take connections; a
  channel never connects or listens, and a transport with neither (this process's standard
  streams) needs neither.
- **Async without hidden threads.** With `tokio`, `asynch::AsyncChannel`, `AsyncConnector` and
  `AsyncListener` are object-safe, and `asynch::StreamChannel` reads and writes in the calls
  themselves: nothing blocking runs on the runtime, and nothing is spawned.
- **No dependency beyond `std`** in the default build, and no `unsafe`.

## Installation

```sh
cargo add coqui-transport
```

| Feature | Adds | Needed for | Default |
| --- | --- | --- | --- |
| `tokio` | `asynch::{AsyncChannel, AsyncConnector, AsyncListener, StreamChannel}`, `Duplex` as `AsyncRead`/`AsyncWrite`, async TCP, on `tokio` (`io-util`, `net`, `sync`, `time`) | async transports | off |

## Quick start

A TCP stream as a message channel: both ends frame it with the codec they agreed on.

```rust
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use coqui_transport::{Channel, StreamChannel};
use coqui_transport::frame::LengthPrefix;

let listener = TcpListener::bind("127.0.0.1:0")?;
let address = listener.local_addr()?;
let server = std::thread::spawn(move || -> std::io::Result<Option<Vec<u8>>> {
    let (stream, _) = listener.accept()?;
    let channel = StreamChannel::start(stream, LengthPrefix::u32_be())?;
    Ok(channel.receive(Some(Duration::from_secs(5))).ok().flatten())
});

let channel = StreamChannel::start(TcpStream::connect(address)?, LengthPrefix::u32_be())?;
channel.send(b"hello")?;
assert_eq!(server.join().unwrap()?, Some(b"hello".to_vec()));
# Ok::<(), Box<dyn std::error::Error>>(())
```

Limits are settings with finite defaults, and a message over a bound is refused, not truncated:

```rust
use coqui_transport::{Channel, Failure, StreamChannel, StreamChannelBuilder};
use coqui_transport::frame::LengthPrefix;
use coqui_transport::memory;

let (one, other) = memory::pipe(4096);
let codec = LengthPrefix::u32_le().refuse_empty();
let small = StreamChannelBuilder::new().codec(codec).max_message(8).stream(one).build()?;
let _other = StreamChannel::start(other, codec)?;
assert_eq!(small.send(b"a message too long"), Err(Failure::Protocol));
// This codec refuses empty messages.
assert_eq!(small.send(b""), Err(Failure::Protocol));
assert_eq!(small.capabilities().max_send, Some(8));
# Ok::<(), Box<dyn std::error::Error>>(())
```

A stream of your own is any reader and writer, joined by `Duplex` (or `Halves`, with a way to
interrupt them):

```rust
use std::io::Cursor;
use std::time::Duration;

use coqui_transport::{Channel, Duplex, StreamChannel};
use coqui_transport::frame::LengthPrefix;

// Two frames already on the wire, and a writer that keeps what is sent.
let incoming = Cursor::new(vec![0, 0, 0, 2, b'h', b'i', 0, 0, 0, 0]);
let channel = StreamChannel::start(Duplex::new(incoming, Vec::new()), LengthPrefix::u32_be())?;
assert_eq!(channel.receive(Some(Duration::from_secs(5)))?, Some(b"hi".to_vec()));
assert_eq!(channel.receive(Some(Duration::from_secs(5)))?, Some(Vec::new()));
// The stream ended between frames: the peer closed in order.
assert_eq!(channel.receive(Some(Duration::from_secs(5)))?, None);
# Ok::<(), Box<dyn std::error::Error>>(())
```

## API overview

| Module | Purpose |
| --- | --- |
| root | `Channel`, `Datagrams`, `Delivery`, `Failure`, `Capabilities`, `Connector`, `Listener`, `Duplex`, `Split`, `StreamChannel` |
| `frame` | the `Codec` and `Decode` traits and the `LengthPrefix` codec, with no I/O |
| `memory` | an in-memory stream pair with bounded buffers |
| `stdio` | this process's standard input and output as one stream |
| `tcp` | TCP streams, a connector, and `TcpListener` as a listener |
| `asynch` | the async traits and the async `StreamChannel` (feature `tokio`) |

## Streams, channels and datagrams

A **stream** is ordered bytes with no boundaries. A **channel** is reliable, ordered, whole
messages: a stream becomes one through a framing, and a transport that already has messages (a
WebSocket) is one natively. **Datagrams** are whole packets beside a channel's messages, delivered
whole or not at all. `Delivery::Unreliable` means the network carries them as datagrams: lost,
reordered or repeated, and never held up. `Delivery::OverStream` means they ride inside the channel's
ordered stream: in order, late rather than lost, and lost only where a queue is full, which
`Datagrams::dropped` counts. Emulated datagrams are never reported as `Unreliable`.

`LengthPrefix` carries messages and nothing else: a datagram, a logical channel or a version would
reach a peer as an ordinary message. A framing that carries them is a codec of its own that both
ends opt into; a connection is never upgraded silently.

## Security boundaries

A channel is not an authenticated peer. Whatever opened it runs the peer's identity proof on it
before trusting a message, and datagrams are outside that proof. `Channel::peer_certificates` is
what the transport's TLS accepted, which is transport metadata: it says nothing about the
application's identity of the peer. Everything an unauthenticated peer controls is bounded: a
declared length over the receive bound ends the channel before its body is read, a decoder never
allocates for a length it has only been told, and read-ahead, queued bytes and read size each have
a limit.

## Migrating from coqui-web-channel and coqui-peer-pipe

`Channel`, `Datagrams`, `Delivery` and `Failure` are `coqui-web-channel`'s, with the same methods
and meaning, so a crate implementing them keeps compiling against these. Two methods are new,
`capabilities` and `is_closed`, each with a default. `Failure::recovery`, `Kind`, `Phase`,
`Recovery`, `Dialer`, `connect`, `Preflight` and the endpoints belong to the web transports and are
not here. `coqui-peer-pipe`'s message framing is `LengthPrefix::u32_be()` (empty messages allowed),
and its channel is `StreamChannel` with that codec; `coqui-web-channel`'s WebTransport stream frame is
`LengthPrefix::u32_le().refuse_empty()`.

## Development

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
cargo test --all-features
cargo test --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo check --target wasm32-unknown-unknown
```

### Releasing

A `v*` tag runs the tests and the strict docs build, then creates the release from `CHANGELOG.md`.
Nothing is published to a registry yet: `publish = false` until a release is chosen.

## License

MPL-2.0; see [LICENSE](https://github.com/Boriken-Dev/coqui-transport/blob/main/LICENSE).
