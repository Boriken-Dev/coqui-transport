//! Memory a multiplexer holds for channels that end unread, for numbers nobody took, and for empty
//! frames; what it admits of a datagram on a small carrier; and the edges of the carrier's bound.

use std::time::Duration;

use coqui_transport::frame::LengthPrefix;
use coqui_transport::frame::versioned::{Frame, Kind};
use coqui_transport::memory;
use coqui_transport::mux::{Mux, MuxBuilder};
use coqui_transport::{Channel, Failure, StreamChannel, StreamChannelBuilder};

const WAIT: Option<Duration> = Some(Duration::from_secs(5));
const SHORT: Option<Duration> = Some(Duration::from_millis(300));

fn carriers() -> (StreamChannel, StreamChannel) {
    let (one, other) = memory::pipe(1 << 16);
    (
        StreamChannel::start(one, LengthPrefix::u32_le()).unwrap(),
        StreamChannel::start(other, LengthPrefix::u32_le()).unwrap(),
    )
}

/// A carrier whose message bound is `bound`, and the plain one at its other end.
fn bounded(bound: usize) -> (StreamChannel, StreamChannel) {
    let (x, y) = memory::pipe(1 << 16);
    (
        StreamChannelBuilder::new()
            .codec(LengthPrefix::u32_le())
            .stream(x)
            .max_message(bound)
            .build()
            .unwrap(),
        StreamChannel::start(y, LengthPrefix::u32_le()).unwrap(),
    )
}

fn frame(kind: Kind, channel: u16, sequence: u32, payload: &[u8]) -> Vec<u8> {
    Frame::new(kind, channel, sequence, payload).encode()
}

#[test]
fn what_a_dropped_channel_held_unread_is_released() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .received_bytes(10_000)
        .pending(1000)
        .build()
        .unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    for n in 0..10_u32 {
        raw.send(&frame(Kind::Message, 0, n, &[1; 1000])).unwrap();
    }
    raw.send(&frame(Kind::Message, 1, 0, b"behind")).unwrap();
    // The budget is full of channel 0's bytes, which nobody will read.
    assert_eq!(a1.receive(SHORT), Err(Failure::Timeout));
    drop(a0);
    assert_eq!(a1.receive(WAIT).unwrap().unwrap(), b"behind");
}

#[test]
fn what_a_closed_channel_held_unread_is_released() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .received_bytes(10_000)
        .pending(1000)
        .build()
        .unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    for n in 0..10_u32 {
        raw.send(&frame(Kind::Message, 0, n, &[1; 1000])).unwrap();
    }
    raw.send(&frame(Kind::Message, 1, 0, b"behind")).unwrap();
    assert_eq!(a1.receive(SHORT), Err(Failure::Timeout));
    a0.close();
    assert_eq!(a1.receive(WAIT).unwrap().unwrap(), b"behind");
}

#[test]
fn what_a_failed_channel_held_is_released_when_it_is_dropped() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .received_bytes(10_000)
        .pending(1000)
        .build()
        .unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    for n in 0..9_u32 {
        raw.send(&frame(Kind::Message, 0, n, &[1; 1000])).unwrap();
    }
    // A gap fails channel 0 with its nine messages still unread.
    raw.send(&frame(Kind::Message, 0, 20, b"gap")).unwrap();
    raw.send(&frame(Kind::Message, 1, 0, &[2; 3000])).unwrap();
    assert_eq!(a1.receive(SHORT), Err(Failure::Timeout));
    drop(a0);
    assert_eq!(a1.receive(WAIT).unwrap().unwrap().len(), 3000);
}

#[test]
fn bytes_held_for_a_number_nobody_took_never_starve_an_open_channel() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .received_bytes(4000)
        .build()
        .unwrap();
    let a0 = a.channel(0).unwrap();
    raw.send(&frame(Kind::Message, 7, 0, &[5; 1000])).unwrap();
    raw.send(&frame(Kind::Message, 0, 0, &[6; 3001])).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap().len(), 3001);
}

#[test]
fn empty_frames_for_a_number_nobody_took_are_counted_too() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let a0 = a.channel(0).unwrap();
    for n in 0..3000_u32 {
        raw.send(&frame(Kind::Message, 9, n, b"")).unwrap();
    }
    raw.send(&frame(Kind::Message, 0, 0, b"sync")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"sync");
    let nine = a.channel(9).unwrap();
    let mut held = 0;
    while nine.receive(SHORT).is_ok_and(|message| message.is_some()) {
        held += 1;
    }
    // 64 KiB of budget at 64 bytes an item.
    assert!(held <= 1100, "{held} empty messages were held");
}

#[test]
fn the_datagram_size_fits_the_carrier_and_an_oversize_send_ends_nothing() {
    let (one, other) = bounded(512);
    let near = Mux::single(one).unwrap();
    let far = Mux::single(other).unwrap();
    let datagrams = near.datagrams().unwrap();
    assert!(datagrams.max_size().unwrap() <= 512 - 12);
    assert_eq!(datagrams.send(&[0; 1000]), Err(Failure::Protocol));
    near.send(b"still up").unwrap();
    assert_eq!(far.receive(WAIT).unwrap().unwrap(), b"still up");
    datagrams.send(&[1; 500]).unwrap();
    assert_eq!(
        far.datagrams()
            .unwrap()
            .receive(WAIT)
            .unwrap()
            .unwrap()
            .len(),
        500
    );
}

#[test]
fn a_carrier_that_holds_exactly_an_empty_frame_is_accepted() {
    let (one, other) = bounded(12);
    let near = Mux::single(one).unwrap();
    let far = Mux::single(other).unwrap();
    assert_eq!(near.capabilities().max_send, Some(0));
    assert_eq!(near.send(&[0; 1]), Err(Failure::Protocol));
    near.send(b"").unwrap();
    assert_eq!(far.receive(WAIT).unwrap().unwrap(), b"");
}

#[test]
fn a_lost_close_is_not_noticed_until_the_carrier_ends() {
    let (one, raw) = carriers();
    let a = Mux::single(one).unwrap();
    raw.send(&frame(Kind::Message, 0, 0, b"m")).unwrap();
    let mut close = frame(Kind::Close, 0, 1, b"");
    close[8 - 1] ^= 1;
    raw.send(&close).unwrap();
    assert_eq!(a.receive(WAIT).unwrap().unwrap(), b"m");
    // Documented: a lost close leaves the channel waiting for another frame or the carrier's end.
    assert_eq!(a.receive(SHORT), Err(Failure::Timeout));
    raw.close();
    assert_eq!(a.receive(WAIT), Ok(None));
}
