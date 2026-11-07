# coqui-transport

Applications talk through streams, channels or datagrams; the transport decides how they are
carried. `coqui-transport` holds what every Coqui transport shares:

- the **channel contract**: `Channel` (reliable, ordered, whole messages), the `Datagrams` beside
  it, and `Delivery`, which says whether those datagrams are really unreliable or ride inside an
  ordered stream;
- **capabilities** a channel reports as they stand, asked again whenever they matter;
- **connectors and listeners**, apart from any established channel;
- **a framing codec** that turns a byte stream into messages: `LengthPrefix`, a one-, two- or
  four-byte length, big- or little-endian, empty messages allowed or refused;
- **`StreamChannel`**, one stream-to-channel adapter for every byte stream, blocking or on tokio;
- adapters for **memory**, this process's **standard streams** and **TCP**.

It knows nothing of peers, trust or applications: a channel is never an authenticated peer.

!!! note "Alpha"
    `0.0.0-alpha.0` is unreleased: the API may change in any release before 1.0.

```rust
use std::net::TcpStream;
use coqui_transport::{Channel, StreamChannel};
use coqui_transport::frame::LengthPrefix;

let channel = StreamChannel::start(TcpStream::connect("127.0.0.1:4000")?, LengthPrefix::u32_be())?;
channel.send(b"hello")?;
let answer = channel.receive(Some(std::time::Duration::from_secs(5)))?;
```

The API reference is under
[Rust API](https://boriken-dev.github.io/coqui-transport/rust/index.html) for `main`. The source,
the API header (`AGENTS.md`) and the changelog are in the
[repository](https://github.com/Boriken-Dev/coqui-transport).
