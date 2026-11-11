//! What a hostile or unlucky peer, a slow carrier and a panicking one must not be able to do to a
//! multiplexer: park its reader, burn its numbers, make it hold memory without bound, hang its close.

mod support;

use std::time::{Duration, Instant};

use coqui_transport::frame::LengthPrefix;
use coqui_transport::frame::versioned::{Frame, Kind};
use coqui_transport::memory;
use coqui_transport::mux::{Mux, MuxBuilder, OpenError};
use coqui_transport::{Channel, Failure, StreamChannel, StreamChannelBuilder};

/// A bound for a wait that must succeed; a failing run waits this long.
const WAIT: Option<Duration> = Some(Duration::from_secs(5));

fn carriers() -> (StreamChannel, StreamChannel) {
    let (one, other) = memory::pipe(1 << 16);
    (
        StreamChannel::start(one, LengthPrefix::u32_le()).unwrap(),
        StreamChannel::start(other, LengthPrefix::u32_le()).unwrap(),
    )
}

fn frame(kind: Kind, channel: u16, sequence: u32, payload: &[u8]) -> Vec<u8> {
    Frame::new(kind, channel, sequence, payload).encode()
}

/// A carrier whose close takes `delay`, as one waiting on a peer that reads nothing does.
struct SlowClose {
    inner: StreamChannel,
    delay: Duration,
}

impl Channel for SlowClose {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        self.inner.send(message)
    }

    fn send_timeout(&self, message: &[u8], timeout: Option<Duration>) -> Result<(), Failure> {
        self.inner.send_timeout(message, timeout)
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        self.inner.receive(timeout)
    }

    fn close(&self) {
        std::thread::sleep(self.delay);
        self.inner.close();
    }
}

/// A carrier that panics when read.
struct Panicking;

impl Channel for Panicking {
    fn send(&self, _: &[u8]) -> Result<(), Failure> {
        Ok(())
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        std::thread::sleep(timeout.map_or(Duration::from_millis(50), |t| {
            t.min(Duration::from_millis(50))
        }));
        panic!("the carrier broke");
    }

    fn close(&self) {}
}

#[test]
fn frames_for_a_number_nobody_took_never_park_the_reader() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let a0 = a.channel(0).unwrap();
    // 200 KB for number 5, which this side never takes: more than any budget for it, and more
    // messages than a queue holds.
    for n in 0..200_u32 {
        raw.send(&frame(Kind::Message, 5, n, &[7; 1000])).unwrap();
    }
    raw.send(&frame(Kind::Message, 0, 0, b"after")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"after");
}

#[test]
fn single_has_only_channel_zero() {
    let (one, raw) = carriers();
    let only = Mux::single(one).unwrap();
    raw.send(&frame(Kind::Message, 1, 0, b"x")).unwrap();
    assert_eq!(only.receive(WAIT), Err(Failure::Protocol));
}

#[test]
fn a_number_the_peer_broke_before_it_was_taken_can_still_be_taken() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let a0 = a.channel(0).unwrap();
    // Sequence 4 first: a gap on a number nobody has taken.
    raw.send(&frame(Kind::Message, 3, 4, b"early")).unwrap();
    raw.send(&frame(Kind::Message, 0, 0, b"sync")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"sync");
    let three = a.channel(3);
    assert_eq!(
        three.as_ref().err(),
        None,
        "the number was burnt: {:?}",
        three.as_ref().err()
    );
    assert_eq!(three.unwrap().receive(WAIT), Err(Failure::Protocol));
    assert_ne!(a.channel(3).err(), Some(OpenError::Number));
}

#[test]
fn dropping_a_multiplexer_is_bounded_by_its_send_limit_whatever_the_carrier_close_takes() {
    let (one, _other) = carriers();
    let slow = SlowClose {
        inner: one,
        delay: Duration::from_secs(4),
    };
    let mux = MuxBuilder::new()
        .carrier(slow)
        .send_limit(Duration::from_millis(300))
        .build()
        .unwrap();
    let channel = mux.channel(0).unwrap();
    channel.send(b"queued").unwrap();
    let started = Instant::now();
    drop(channel);
    drop(mux);
    assert!(
        started.elapsed() < Duration::from_millis(2500),
        "dropping took {:?}",
        started.elapsed()
    );
}

#[test]
fn closing_the_legacy_channel_flushes_and_closes_the_carrier() {
    let (one, other) = carriers();
    let a = Mux::legacy(one).unwrap();
    let b = Mux::legacy(other).unwrap();
    a.send(b"last").unwrap();
    a.close();
    assert_eq!(b.receive(WAIT).unwrap().unwrap(), b"last");
    assert_eq!(b.receive(WAIT), Ok(None));
}

#[test]
fn a_legacy_channel_that_fails_ends_the_link_for_the_peer() {
    let (one, other) = carriers();
    let a = Mux::legacy(one).unwrap();
    let b = Mux::legacy(other).unwrap();
    a.set_max_receive(10);
    b.send(&[0; 11]).unwrap();
    assert_eq!(a.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(b.receive(WAIT), Ok(None));
}

#[test]
fn a_peer_that_is_not_framed_is_a_breach_not_a_counter() {
    // A legacy peer whose messages happen to start with the version byte.
    let (one, raw) = carriers();
    let a = Mux::single(one).unwrap();
    for _ in 0..40 {
        raw.send(b"\x01\x00 an unframed peer's message, long enough for a frame")
            .unwrap();
    }
    assert_eq!(a.receive(WAIT), Err(Failure::Protocol));
}

#[test]
fn a_reader_that_panics_ends_the_multiplexer_instead_of_dying_silently() {
    let mux = MuxBuilder::new()
        .carrier(Panicking)
        .send_limit(Duration::from_millis(500))
        .build()
        .unwrap();
    let channel = mux.channel(0).unwrap();
    assert_eq!(channel.receive(WAIT), Err(Failure::Cancelled));
}

#[test]
fn frames_after_the_peers_close_are_ignored_and_the_end_stays_an_end() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    raw.send(&frame(Kind::Message, 0, 0, b"a")).unwrap();
    raw.send(&frame(Kind::Close, 0, 1, b"")).unwrap();
    raw.send(&frame(Kind::Close, 0, 5, b"")).unwrap();
    raw.send(&frame(Kind::Message, 0, 9, b"stray")).unwrap();
    raw.send(&frame(Kind::Message, 1, 0, b"sync")).unwrap();
    assert_eq!(a1.receive(WAIT).unwrap().unwrap(), b"sync");
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"a");
    assert_eq!(a0.receive(WAIT), Ok(None));
    assert_eq!(a0.receive(WAIT), Ok(None));
}

#[test]
fn a_carriers_tighter_bound_is_kept_and_no_channel_can_raise_it() {
    let (one, other) = {
        let (x, y) = memory::pipe(1 << 16);
        (
            StreamChannelBuilder::new()
                .codec(LengthPrefix::u32_le())
                .stream(x)
                .max_message(1000)
                .build()
                .unwrap(),
            StreamChannel::start(y, LengthPrefix::u32_le()).unwrap(),
        )
    };
    let a = Mux::start(one).unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    let capabilities = a0.capabilities();
    assert!(
        capabilities.max_send.unwrap() <= 1000 - 12,
        "{capabilities:?}"
    );
    assert!(
        capabilities.max_receive.unwrap() <= 1000 - 12,
        "{capabilities:?}"
    );
    a0.set_max_message(1 << 20);
    a1.set_max_receive(1 << 20);
    assert!(a0.capabilities().max_send.unwrap() <= 1000 - 12);
    assert!(a1.capabilities().max_receive.unwrap() <= 1000 - 12);
    assert_eq!(a0.send(&[0; 1000]), Err(Failure::Protocol));
    drop(other);
}

#[test]
fn a_carrier_that_cannot_hold_a_frame_is_refused() {
    let (x, _y) = memory::pipe(1 << 16);
    let tiny = StreamChannelBuilder::new()
        .codec(LengthPrefix::u32_le())
        .stream(x)
        .max_message(8)
        .build()
        .unwrap();
    assert!(Mux::start(tiny).is_err());
}

#[test]
fn a_message_over_the_multiplexers_setting_fails_only_its_channel() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .max_message(100)
        .build()
        .unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    raw.send(&frame(Kind::Message, 0, 0, &[0; 101])).unwrap();
    raw.send(&frame(Kind::Message, 1, 0, b"fine")).unwrap();
    assert_eq!(a0.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(a1.receive(WAIT).unwrap().unwrap(), b"fine");
}
