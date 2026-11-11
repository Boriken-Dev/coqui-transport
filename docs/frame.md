# The versioned frame

This page specifies the frame of `coqui_transport::frame::versioned` and what the multiplexer
(`coqui_transport::mux`) does with it. Another implementation needs nothing else. The bytes are
pinned by `tests/vectors/versioned_frame.json`, which `tools/frame_vectors.py` makes from this page,
in Python, independently of the Rust code.

The key words MUST and MUST NOT are used as in RFC 2119.

## What it is for

A legacy codec (`LengthPrefix`) carries messages and nothing else: a second logical channel, a
datagram or a version would reach the peer as an ordinary message. The versioned frame adds those,
over any `Channel`: a TCP stream, a pipe, a child process's standard streams or a serial line.

## Opting in

**Both ends are configured for the frame; nothing negotiates it.** A negotiation would itself need a
framing both ends already agree on, and the legacy peers this crate must not break would read the
offer as a message. So an operator (or a protocol's specification) says "this link speaks version 1",
as it says which `LengthPrefix` a link speaks, and nothing in the bytes selects it. The version byte
in every frame makes a mistake loud instead of silent. A peer on another version is a
`Failure::Protocol` on its first frame for certain. A peer that is not framed at all is one on its
first message unless that message happens to start with the byte `1` and be at least 12 bytes
long; then its checksum fails, and a checksum that fails before any frame has passed it is a
breach too, so the first such message ends the link. A legacy peer therefore never goes on
unnoticed, whatever it sends.

## The frame

A frame is exactly one message of the carrier. It has no length of its own: the carrier's codec
delimits it (a length prefix on a byte stream, COBS on a serial line, a WebSocket message). All
integers are little-endian.

| Offset | Size | Field | Meaning |
| --- | --- | --- | --- |
| 0 | 1 | version | `1` |
| 1 | 1 | kind | `0` message, `1` datagram, `2` close |
| 2 | 2 | channel | the logical channel's number, `0` to `channels - 1` |
| 4 | 4 | sequence | see below |
| 8 | n | payload | the message or datagram; MUST be empty for a close |
| 8+n | 4 | checksum | CRC-32C of bytes `0` to `8+n` |

The overhead is 12 bytes. The checksum is CRC-32C (Castagnoli): polynomial `0x1EDC6F41`, reflected
(`0x82F63B78`), initial value and final xor `0xFFFFFFFF`; the check value of `123456789` is
`0xE3069283`. It is the same CRC the serial framing uses.

**Sequence.** For each logical channel and direction, a sender numbers its *message* and *close*
frames from 0, adding one per frame and wrapping at 2^32. A receiver expects the next number.
*Datagram* frames carry a second count, of the datagrams the sender wrote on that channel, which only
measures loss (a datagram queued and dropped before it is written is never counted).

### A receiver checks, in this order

1. Fewer than 12 bytes: breach.
2. Version is not `1`: breach. This precedes the checksum because another version may lay a frame out
   otherwise.
3. The checksum does not match: the frame is **damaged**. It is dropped and counted
   (`Counters::corrupt`) and the carrier goes on, with two exceptions that are breaches: a damaged
   frame before any frame has passed its checksum (the peer is not speaking this frame), and
   `max_damaged` damaged frames in a row (default 8; the link is not carrying the stream).
4. Kind is not `0`, `1` or `2`, or a close has a payload: breach.
5. Channel is not below the number of channels this side accepts: breach.

A *breach* ends the whole multiplexer with `Failure::Protocol` and closes the carrier: the peer is
not speaking this frame, and nothing it says can be trusted.

## Logical channels

A logical channel is a number, chosen by the application and the same at both ends; there is no
open handshake. A number is taken once: after it is closed (or failed) it cannot be taken again.

* **A number nobody has taken.** A peer may send on a number before this side takes it, so frames
  for it are held, but never at the cost of making the reader wait: all such numbers together hold
  at most `unopened_bytes` (default 64 KiB, and at most a quarter of `received_bytes`), and a number
  that passes it fails. A number that failed before it was taken is not used up: it can still be
  taken, and then reports `Failure::Protocol` from `receive` after what it held. So take the numbers
  at start, before a peer can send on them. `Mux::single` accepts only channel 0 (`channels` 1);
  a frame for another number is a breach.
* **After a channel has ended** (the peer's close, this side's close, or a failure) every later frame
  for that number is ignored: an end stays an end, and `receive` does not turn from `Ok(None)` into
  an error.

* **Messages** are reliable and ordered per channel. A frame whose sequence is not the expected one
  means a frame was lost between the peer and this multiplexer (a damaged frame dropped, or a carrier
  that drops). The channel **fails with `Failure::Protocol`** rather than hand over a stream with a
  hole in it; messages received before the hole stay readable, then `receive` returns the failure,
  and a close frame tells the peer. Other channels are unaffected.
* **Close.** A close frame says the sender sends no more on that channel. The receiver reads what
  arrived, then `receive` returns `Ok(None)`; sending to it is `Failure::PathLost`. Closing every
  channel and the carrier is the orderly end; the carrier ending in order ends every channel with
  `Ok(None)`.
* **A message over the receiver's bound** (`max_message`, or what `set_max_receive` says) fails
  that channel with `Failure::Protocol`, as a gap does. A message over the *carrier's* own bound
  never reaches the multiplexer: the carrier ends, and so does the multiplexer. `max_message` is
  lowered to what the carrier admits (its reported bound less 12), so a frame holding a message of
  the largest size fits; a carrier whose bound cannot hold a frame is refused at the start, and a
  carrier's bound is never raised. A channel's `set_max_` calls can lower its bound, never raise it
  above that.

### Head-of-line blocking

There is one ordered stream under all channels, so:

* a large message delays everything behind it, on every channel and the datagrams too;
* a channel whose received messages are not being read fills its bound (`pending` messages, and
  `received_bytes` for all channels together), and then the multiplexer stops reading the carrier
  until it is read, so **every other channel waits behind it**. A number nobody has taken never does
  this (see above). This is deliberate: the alternative is to drop a reliable message or to reset the channel,
  and there is no per-channel flow control in version 1. An application that opens several channels
  reads all of them, or closes the ones it does not read;
* a lost frame on a carrier that drops (serial) is a gap on whichever channel it belonged to.

A caller that needs a channel which never delays another needs a carrier per channel.

## Datagrams

Every logical channel offers `Channel::datagrams()` by default, reporting `Delivery::OverStream`:
inside the ordered stream, in order and late rather than lost on the way, lost only where a queue is
full (counted by `Datagrams::dropped`). They are never reported as `Unreliable`.

* **Sending never waits.** `Datagrams::send` copies the payload into a queue and returns. The queue
  holds `datagram_queue` datagrams for all channels; when it is full the setting `datagram_drop`
  decides: `oldest` (default) drops the oldest waiting and queues this one; `newest` drops this one.
  Either way the call succeeds and the drop is counted against the channel the dropped datagram
  belonged to.
* **Payload bound.** `max_datagram` (default 1200 bytes, which fits an IPv6 minimum-MTU packet, so
  code written against over-stream datagrams runs unchanged on native ones). A larger payload is
  `Failure::Protocol` on send; a larger one that arrives is dropped and counted.
* **Turns.** The writer takes a message frame and a datagram frame in turn when both are waiting, so
  a stream of messages does not starve datagrams nor the reverse.
* **Receiving.** Each channel holds `datagram_pending` datagrams; past that the same `datagram_drop`
  rule applies at the receiver. A datagram never makes the reader of the carrier wait.
* **Security.** Datagrams are outside any identity proof run on a channel's messages, as for native
  datagrams.

## Limits (all settings, with finite defaults)

| Setting | Default | Meaning |
| --- | --- | --- |
| `framing` | `versioned` | `versioned`, or `tagged` for the legacy wrapper |
| `channels` | 16 | how many channel numbers are accepted (at most 65536) |
| `max_message` | 1 MiB less 12 | largest message, both ways; lowered to what the carrier admits |
| `pending` | 64 | received messages a channel holds before the reader waits |
| `queued_bytes` | 16 MiB | queued messages awaiting the writer; each counts its frame and 64 |
| `received_bytes` | 16 MiB | received messages held for `receive`, all channels together; and again for datagrams |
| `unopened_bytes` | 64 KiB | held for numbers nobody has taken, all together; at most a quarter of `received_bytes` |
| `max_damaged` | 8 | frames in a row with a bad checksum that end the multiplexer |
| `send_limit` | 30 s | the carrier taking none of a frame ends the multiplexer with `NoProgress` |
| `max_datagram` | 1200 | largest datagram payload |
| `datagram_queue` | 64 | datagrams waiting to be written, all channels |
| `datagram_pending` | 64 | datagrams a channel holds for `receive` |
| `datagram_drop` | `oldest` | `oldest` or `newest` |

Memory a peer can make this side hold is bounded by the settings, in bytes: `received_bytes` for
messages of every channel together (a message that would pass it waits, so it is exceeded by
nothing but one message alone in an empty queue) and, separately, as much for datagrams (one that
would pass it is dropped, so datagrams never keep a message out), at most a quarter of that again
for the numbers nobody has taken, `queued_bytes` and `datagram_queue` datagrams to write. Counts
(`pending`, `datagram_pending`) bound each channel's queue within that. Added to this is what the
carrier buffers itself: for a `StreamChannel`, `pending` messages read ahead of up to its
`max_message` each, and its own `queued_bytes` to send; set those with the rest. Memory is not
bounded by the number of channels beyond `channels` slots of a few hundred bytes each.

## Damage and serial

The checksum catches damage a carrier let through. A damaged frame is dropped and counted
(`Counters::corrupt`) and is never delivered. Over a **serial line** the COBS-and-CRC framing of
`coqui-transport-serial` already drops a damaged frame before the multiplexer sees it, so the
multiplexer sees the same thing as a gap: a message channel fails with `Failure::Protocol`, a
datagram is lost and counted (`Counters::lost_datagrams`, by the gaps in the datagram count). The
frame's checksum is then redundant on that carrier and costs 4 bytes; it is kept so the frame means
the same on every carrier.

A link that cannot lose a message uses a carrier that does not lose (TCP, a pipe), or acknowledges
and repeats above the channel.

## Closing and time

`Mux::close`, and dropping the last handle, close every channel (a close frame each), write what is
queued, close the carrier and wait for the two threads, all within `send_limit` from the call. The
carrier is closed once, on a thread of its own, because a carrier's close can wait for its peer for
as long as the carrier's own limit. A thread or a carrier close still running when `send_limit`
passes is left to end by itself. A panic in either thread ends the multiplexer with
`Failure::Cancelled`.

## The legacy wrapper

`Mux::legacy(channel)` (`Framing::Tagged`) is for an operator who controls both ends of a link whose
codec is a legacy one and who wants datagrams on it. It adds **one byte** before every message and
datagram, and nothing else:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 1 | tag: `0` message, `1` datagram |
| 1 | n | payload |

One channel (`0`), no sequence, no checksum, no close frame (the carrier's end is the end), the
same datagram queues and bounds. Closing the channel flushes what is queued and closes the carrier,
which is how the peer learns of it; a failure of the channel (a message over its bound) ends the
link the same way. **Both ends MUST wrap.** A peer that does not misreads every message by
one byte, and a wrapped end reads a peer that does not as a breach (an unknown tag) or, worse,
silently drops a first byte; nothing in the bytes tells a wrapped link from an unwrapped one. That is
why it is a function you call and never a default, and why the versioned frame, which names its
version, is preferred wherever both ends can be updated.

## Compatibility

Version 1 is fixed once released. A change to the layout is version 2, which an older peer reads as
a breach on the first frame. Unknown kinds are a breach, not ignored: a new kind is a new version.
