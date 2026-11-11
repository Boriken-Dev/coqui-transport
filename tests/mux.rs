//! The multiplexer: logical channels, datagrams over the stream, head-of-line blocking, bounds,
//! damage and the legacy wrapper, over `StreamChannel` carriers on in-memory streams.

mod support;

use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use coqui_builder::Partial;
use coqui_transport::frame::LengthPrefix;
use coqui_transport::frame::versioned::{Frame, Kind, OVERHEAD};
use coqui_transport::memory;
use coqui_transport::mux::{
    DatagramDrop, Framing, Mux, MuxBuilder, MuxChannel, MuxSettings, OpenError,
};
use coqui_transport::{Capabilities, Carrier, Channel, Delivery, Failure, StreamChannel};
use support::WAIT;

const SHORT: Option<Duration> = Some(Duration::from_millis(100));

fn codec() -> LengthPrefix {
    LengthPrefix::u32_le()
}

/// Two carriers joined to each other.
fn carriers() -> (StreamChannel, StreamChannel) {
    let (one, other) = memory::pipe(1 << 16);
    (
        StreamChannel::start(one, codec()).unwrap(),
        StreamChannel::start(other, codec()).unwrap(),
    )
}

fn pair() -> (Mux, Mux) {
    let (one, other) = carriers();
    (Mux::start(one).unwrap(), Mux::start(other).unwrap())
}

fn builder(settings: MuxSettings, carrier: impl Channel + 'static) -> Mux {
    MuxBuilder::new()
        .carrier(carrier)
        .framing(settings.framing)
        .channels(settings.channels)
        .max_message(settings.max_message)
        .pending(settings.pending)
        .queued_bytes(settings.queued_bytes)
        .send_limit(settings.send_limit)
        .max_datagram(settings.max_datagram)
        .datagram_queue(settings.datagram_queue)
        .datagram_pending(settings.datagram_pending)
        .datagram_drop(settings.datagram_drop)
        .build()
        .unwrap()
}

/// A carrier that writes only while its gate is open, to hold the writer where a test needs it.
struct Gated {
    inner: StreamChannel,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

fn gated(inner: StreamChannel) -> (Gated, Arc<(Mutex<bool>, Condvar)>) {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    (
        Gated {
            inner,
            gate: Arc::clone(&gate),
        },
        gate,
    )
}

fn open(gate: &(Mutex<bool>, Condvar)) {
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
}

impl Channel for Gated {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        self.send_timeout(message, None)
    }

    fn send_timeout(&self, message: &[u8], timeout: Option<Duration>) -> Result<(), Failure> {
        let until = Instant::now() + timeout.unwrap_or(Duration::from_secs(60));
        let mut passable = self.gate.0.lock().unwrap();
        while !*passable {
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Failure::Timeout);
            }
            passable = self.gate.1.wait_timeout(passable, left).unwrap().0;
        }
        drop(passable);
        self.inner.send(message)
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        self.inner.receive(timeout)
    }

    fn close(&self) {
        open(&self.gate);
        self.inner.close();
    }
}

fn frame(kind: Kind, channel: u16, sequence: u32, payload: &[u8]) -> Vec<u8> {
    Frame::new(kind, channel, sequence, payload).encode()
}

/// Reads frames off a raw carrier until `count` arrived.
fn frames(raw: &StreamChannel, count: usize) -> Vec<Frame> {
    (0..count)
        .map(|_| Frame::decode(&raw.receive(WAIT).unwrap().unwrap()).unwrap())
        .collect()
}

#[test]
fn logical_channels_carry_their_own_messages_in_order_both_ways() {
    let (a, b) = pair();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    let (b0, b1) = (b.channel(0).unwrap(), b.channel(1).unwrap());
    for n in 0..20_u8 {
        a0.send(&[0, n]).unwrap();
        a1.send(&[1, n]).unwrap();
    }
    b1.send(b"back").unwrap();
    for n in 0..20_u8 {
        assert_eq!(b1.receive(WAIT).unwrap().unwrap(), [1, n]);
    }
    for n in 0..20_u8 {
        assert_eq!(b0.receive(WAIT).unwrap().unwrap(), [0, n]);
    }
    assert_eq!(a1.receive(WAIT).unwrap().unwrap(), b"back");
    assert_eq!(a0.receive(SHORT), Err(Failure::Timeout));
}

#[test]
fn empty_and_largest_messages_cross_whatever_the_carriers_default_bound() {
    let (a, b) = pair();
    let (a, b) = (a.channel(0).unwrap(), b.channel(0).unwrap());
    let largest: Vec<u8> = (0..Mux::DEFAULT_MAX_MESSAGE)
        .map(|n| (n % 251) as u8)
        .collect();
    a.send(b"").unwrap();
    a.send(&largest).unwrap();
    assert_eq!(b.receive(WAIT).unwrap().unwrap(), b"");
    assert_eq!(b.receive(WAIT).unwrap().unwrap(), largest);
    assert_eq!(
        a.send(&[0; Mux::DEFAULT_MAX_MESSAGE + 1]),
        Err(Failure::Protocol)
    );
}

#[test]
fn numbers_are_taken_once_and_bounded() {
    let (a, _b) = pair();
    let _zero = a.channel(0).unwrap();
    assert_eq!(a.channel(0).err(), Some(OpenError::InUse));
    assert!(a.channel(15).is_ok());
    assert_eq!(a.channel(16).err(), Some(OpenError::Number));
    assert_eq!(a.channel(u16::MAX).err(), Some(OpenError::Number));
    let closed = a.channel(3).unwrap();
    closed.close();
    drop(closed);
    assert_eq!(a.channel(3).err(), Some(OpenError::InUse));
    a.close();
    assert_eq!(a.channel(4).err(), Some(OpenError::Closed));
}

#[test]
fn a_close_ends_the_peers_channel_after_what_was_sent() {
    let (a, b) = pair();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    let (b0, b1) = (b.channel(0).unwrap(), b.channel(1).unwrap());
    a0.send(b"one").unwrap();
    a0.send(b"two").unwrap();
    a0.close();
    assert_eq!(a0.send(b"late"), Err(Failure::PathLost));
    assert!(a0.is_closed());
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), b"one");
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), b"two");
    assert_eq!(b0.receive(WAIT), Ok(None));
    assert_eq!(b0.send(b"to a closed one"), Err(Failure::PathLost));
    // The other channel is untouched.
    a1.send(b"still").unwrap();
    assert_eq!(b1.receive(WAIT).unwrap().unwrap(), b"still");
    assert!(!b1.is_closed());
}

#[test]
fn the_end_of_the_carrier_ends_every_channel_in_order() {
    let (a, b) = pair();
    let (a0, b0) = (a.channel(0).unwrap(), b.channel(0).unwrap());
    a0.send(b"last").unwrap();
    a.close();
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), b"last");
    assert_eq!(b0.receive(WAIT), Ok(None));
    assert!(b0.is_closed());
}

#[test]
fn dropping_the_last_handle_closes_the_carrier() {
    let (a, b) = pair();
    let a0 = a.channel(0).unwrap();
    let b0 = b.channel(0).unwrap();
    a0.send(b"bye").unwrap();
    drop(a);
    drop(a0);
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), b"bye");
    assert_eq!(b0.receive(WAIT), Ok(None));
}

#[test]
fn datagrams_say_they_travel_over_the_stream_and_cross() {
    let (a, b) = pair();
    let (a, b) = (a.channel(0).unwrap(), b.channel(0).unwrap());
    let datagrams = a.datagrams().expect("datagrams by default");
    assert_eq!(datagrams.delivery(), Delivery::OverStream);
    assert_eq!(datagrams.max_size(), Some(Mux::DEFAULT_MAX_DATAGRAM));
    assert!(datagrams.path_changes().is_none());
    assert_eq!(
        a.capabilities(),
        Capabilities::new()
            .carrier(Carrier::Stream)
            .limits(Mux::DEFAULT_MAX_MESSAGE, Mux::DEFAULT_MAX_MESSAGE)
            .datagrams(Some((
                Delivery::OverStream,
                Some(Mux::DEFAULT_MAX_DATAGRAM)
            )))
    );
    for n in 0..10_u8 {
        datagrams.send(&[n; 8]).unwrap();
    }
    let received = b.datagrams().unwrap();
    for n in 0..10_u8 {
        assert_eq!(received.receive(WAIT).unwrap().unwrap(), [n; 8]);
    }
    assert_eq!(received.receive(SHORT), Err(Failure::Timeout));
    assert_eq!(received.dropped(), 0);
}

#[test]
fn a_datagram_over_the_maximum_is_refused_and_the_channel_goes_on() {
    let (a, b) = pair();
    let (a, b) = (a.channel(0).unwrap(), b.channel(0).unwrap());
    let datagrams = a.datagrams().unwrap();
    assert_eq!(
        datagrams.send(&vec![0; Mux::DEFAULT_MAX_DATAGRAM + 1]),
        Err(Failure::Protocol)
    );
    datagrams.send(&vec![7; Mux::DEFAULT_MAX_DATAGRAM]).unwrap();
    assert_eq!(
        b.datagrams().unwrap().receive(WAIT).unwrap().unwrap().len(),
        Mux::DEFAULT_MAX_DATAGRAM
    );
}

#[test]
fn a_datagram_travels_beside_the_messages_of_its_channel_only() {
    let (a, b) = pair();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    let (b0, b1) = (b.channel(0).unwrap(), b.channel(1).unwrap());
    a1.datagrams().unwrap().send(b"for one").unwrap();
    a0.send(b"message").unwrap();
    assert_eq!(
        b1.datagrams().unwrap().receive(WAIT).unwrap().unwrap(),
        b"for one"
    );
    assert_eq!(
        b0.datagrams().unwrap().receive(SHORT),
        Err(Failure::Timeout)
    );
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), b"message");
    assert_eq!(b1.receive(SHORT), Err(Failure::Timeout));
}

#[test]
fn sending_a_datagram_never_waits_and_the_oldest_goes_first_by_default() {
    let (one, other) = carriers();
    let (held, gate) = gated(one);
    let mut settings = MuxSettings::new();
    settings.datagram_queue = 4;
    let a = builder(settings, held).channel(0).unwrap();
    let b = Mux::start(other).unwrap().channel(0).unwrap();
    // The writer takes this and stops at the gate.
    a.send(b"holds the writer").unwrap();
    let datagrams = a.datagrams().unwrap();
    let started = Instant::now();
    for n in 0..20_u8 {
        datagrams.send(&[n]).unwrap();
    }
    assert!(started.elapsed() < Duration::from_secs(2), "a send waited");
    assert_eq!(datagrams.dropped(), 16);
    open(&gate);
    assert_eq!(b.receive(WAIT).unwrap().unwrap(), b"holds the writer");
    let received = b.datagrams().unwrap();
    for n in 16..20_u8 {
        assert_eq!(received.receive(WAIT).unwrap().unwrap(), [n]);
    }
    assert_eq!(received.receive(SHORT), Err(Failure::Timeout));
}

#[test]
fn the_newest_can_go_first_instead() {
    let (one, other) = carriers();
    let (held, gate) = gated(one);
    let mut settings = MuxSettings::new();
    settings.datagram_queue = 4;
    settings.datagram_drop = DatagramDrop::Newest;
    let a = builder(settings, held).channel(0).unwrap();
    let b = Mux::start(other).unwrap().channel(0).unwrap();
    a.send(b"holds the writer").unwrap();
    let datagrams = a.datagrams().unwrap();
    for n in 0..20_u8 {
        datagrams.send(&[n]).unwrap();
    }
    assert_eq!(datagrams.dropped(), 16);
    open(&gate);
    let received = b.datagrams().unwrap();
    for n in 0..4_u8 {
        assert_eq!(received.receive(WAIT).unwrap().unwrap(), [n]);
    }
    assert_eq!(received.receive(SHORT), Err(Failure::Timeout));
}

#[test]
fn a_receive_queue_of_datagrams_is_bounded_and_counts_what_it_drops() {
    let (one, other) = carriers();
    let a = Mux::start(one).unwrap().channel(0).unwrap();
    let mut settings = MuxSettings::new();
    settings.datagram_pending = 3;
    let b = builder(settings, other).channel(0).unwrap();
    for n in 0..10_u8 {
        a.datagrams().unwrap().send(&[n]).unwrap();
    }
    let received = b.datagrams().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    while received.dropped() < 7 && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(received.dropped(), 7);
    for n in 7..10_u8 {
        assert_eq!(received.receive(WAIT).unwrap().unwrap(), [n]);
    }
}

#[test]
fn datagrams_and_messages_take_turns_on_the_carrier() {
    let (one, raw) = carriers();
    let (held, gate) = gated(one);
    let a = Mux::start(held).unwrap().channel(0).unwrap();
    for n in 0..30_u8 {
        a.send(&[n]).unwrap();
    }
    a.datagrams().unwrap().send(b"d").unwrap();
    open(&gate);
    let seen = frames(&raw, 31);
    let position = seen
        .iter()
        .position(|frame| frame.kind == Kind::Datagram)
        .expect("the datagram was written");
    assert!(
        position <= 2,
        "the datagram waited behind {position} messages"
    );
    // The messages stay in order.
    let sequences: Vec<u32> = seen
        .iter()
        .filter(|frame| frame.kind == Kind::Message)
        .map(|frame| frame.sequence)
        .collect();
    assert_eq!(sequences, (0..30).collect::<Vec<u32>>());
}

#[test]
fn a_channel_nobody_reads_holds_up_the_others() {
    let (one, other) = carriers();
    let a = Mux::start(one).unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    let mut settings = MuxSettings::new();
    settings.pending = 2;
    let b = builder(settings, other);
    let (b0, b1) = (b.channel(0).unwrap(), b.channel(1).unwrap());
    for n in 0..3_u8 {
        a0.send(&[n]).unwrap();
    }
    a1.send(b"behind").unwrap();
    // Channel 0 holds two and the reader waits on the third: channel 1 hears nothing.
    assert_eq!(b1.receive(SHORT), Err(Failure::Timeout));
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), [0]);
    assert_eq!(b1.receive(WAIT).unwrap().unwrap(), b"behind");
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), [1]);
    assert_eq!(b0.receive(WAIT).unwrap().unwrap(), [2]);
}

#[test]
fn sends_are_refused_or_wait_when_the_queue_is_full_and_the_channel_goes_on() {
    let (one, other) = carriers();
    let (held, gate) = gated(one);
    let mut settings = MuxSettings::new();
    settings.queued_bytes = 300;
    let a = builder(settings, held).channel(0).unwrap();
    let b = Mux::start(other).unwrap().channel(0).unwrap();
    let payload = [9_u8; 100];
    // The first the writer takes; then room for one more of 100 + frame + cost.
    a.send(&payload).unwrap();
    thread::sleep(Duration::from_millis(200));
    let mut accepted = 0;
    let refused = loop {
        match a.send(&payload) {
            Ok(()) => accepted += 1,
            Err(failure) => break failure,
        }
        assert!(accepted < 10, "the queue never filled");
    };
    assert_eq!(refused, Failure::NoProgress);
    assert!(!a.is_closed());
    assert_eq!(a.send_timeout(&payload, SHORT), Err(Failure::Timeout));
    open(&gate);
    for _ in 0..=accepted {
        assert_eq!(b.receive(WAIT).unwrap().unwrap(), payload);
    }
    a.send_timeout(&payload, WAIT).unwrap();
}

#[test]
fn a_message_over_the_bound_is_refused_and_the_channel_goes_on() {
    let (a, b) = pair();
    let (a, b) = (a.channel(0).unwrap(), b.channel(0).unwrap());
    a.set_max_send(10);
    assert_eq!(a.send(&[0; 11]), Err(Failure::Protocol));
    a.send(&[0; 10]).unwrap();
    assert_eq!(b.receive(WAIT).unwrap().unwrap().len(), 10);
    a.set_max_message(20);
    assert_eq!(
        a.capabilities().max_send.zip(a.capabilities().max_receive),
        Some((20, 20))
    );
}

#[test]
fn a_message_over_what_this_side_accepts_ends_that_channel_only() {
    let (a, b) = pair();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    let (b0, b1) = (b.channel(0).unwrap(), b.channel(1).unwrap());
    b0.set_max_receive(10);
    a0.send(&[0; 11]).unwrap();
    assert_eq!(b0.receive(WAIT), Err(Failure::Protocol));
    assert!(b0.is_closed());
    a1.send(b"fine").unwrap();
    assert_eq!(b1.receive(WAIT).unwrap().unwrap(), b"fine");
    // The peer was told: its channel closes.
    assert_eq!(a0.receive(WAIT), Ok(None));
}

#[test]
fn a_missing_frame_ends_a_reliable_channel_rather_than_skip_it() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    raw.send(&frame(Kind::Message, 0, 0, b"first")).unwrap();
    // Sequence 1 is missing.
    raw.send(&frame(Kind::Message, 0, 2, b"third")).unwrap();
    raw.send(&frame(Kind::Message, 1, 0, b"other")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"first");
    assert_eq!(a0.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(a1.receive(WAIT).unwrap().unwrap(), b"other");
    // The peer is told by a close.
    let told = frames(&raw, 1);
    assert_eq!((told[0].kind, told[0].channel), (Kind::Close, 0));
}

#[test]
fn a_damaged_frame_is_dropped_counted_and_found_by_the_gap_it_leaves() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let a0 = a.channel(0).unwrap();
    raw.send(&frame(Kind::Message, 0, 0, b"first")).unwrap();
    let mut damaged = frame(Kind::Message, 0, 1, b"second");
    damaged[HEADER_PAYLOAD] ^= 0x04;
    raw.send(&damaged).unwrap();
    raw.send(&frame(Kind::Message, 0, 2, b"third")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"first");
    assert_eq!(a0.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(a.counters().corrupt, 1);
}

/// The first byte of a payload in a frame.
const HEADER_PAYLOAD: usize = 8;

#[test]
fn a_lost_datagram_is_counted_by_the_gap_and_the_channel_goes_on() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let a0 = a.channel(0).unwrap();
    raw.send(&frame(Kind::Datagram, 0, 0, b"a")).unwrap();
    let mut damaged = frame(Kind::Datagram, 0, 1, b"b");
    damaged[HEADER_PAYLOAD] ^= 0x01;
    raw.send(&damaged).unwrap();
    raw.send(&frame(Kind::Datagram, 0, 2, b"c")).unwrap();
    raw.send(&frame(Kind::Datagram, 0, 5, b"f")).unwrap();
    let datagrams = a0.datagrams().unwrap();
    for expected in [b"a", b"c", b"f"] {
        assert_eq!(datagrams.receive(WAIT).unwrap().unwrap(), expected);
    }
    let counters = a.counters();
    assert_eq!((counters.corrupt, counters.lost_datagrams), (1, 3));
    // Messages still work.
    raw.send(&frame(Kind::Message, 0, 0, b"m")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"m");
}

#[test]
fn a_frame_of_another_version_ends_everything_with_a_protocol_failure() {
    for wrong in [
        vec![2_u8; 40],
        vec![0_u8; 12],
        b"{\"jsonrpc\":\"2.0\",\"id\":1}".to_vec(),
    ] {
        let (one, raw) = carriers();
        let a = Mux::start(one).unwrap();
        let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
        raw.send(&wrong).unwrap();
        assert_eq!(a0.receive(WAIT), Err(Failure::Protocol));
        assert_eq!(a1.receive(WAIT), Err(Failure::Protocol));
        assert_eq!(a0.send(b"x"), Err(Failure::Protocol));
        assert_eq!(a0.datagrams().unwrap().send(b"x"), Err(Failure::Protocol));
        assert!(a1.is_closed());
        // The carrier was closed, so the unframed peer sees its end.
        assert!(matches!(raw.receive(WAIT), Ok(None) | Err(_)));
    }
}

#[test]
fn a_short_message_a_bad_kind_and_a_channel_out_of_range_are_breaches() {
    let cases: Vec<Vec<u8>> = vec![
        b"short".to_vec(),
        Vec::new(),
        frame(Kind::Message, 16, 0, b"x"),
        frame(Kind::Message, u16::MAX, 0, b"x"),
        {
            // Kind 9 with a valid checksum.
            let mut body = frame(Kind::Message, 0, 0, b"x");
            let at = body.len() - 4;
            body[1] = 9;
            let crc = coqui_transport::frame::versioned::crc32c(&body[..at]);
            body[at..].copy_from_slice(&crc.to_le_bytes());
            body
        },
    ];
    for case in cases {
        let (one, raw) = carriers();
        let a = Mux::start(one).unwrap().channel(0).unwrap();
        raw.send(&case).unwrap();
        assert_eq!(a.receive(WAIT), Err(Failure::Protocol), "{case:?}");
    }
}

#[test]
fn a_carrier_that_ends_inside_a_frame_ends_every_channel_with_its_failure() {
    use std::io::Write;
    let (stream, mut far) = memory::pipe(1024);
    let mux = Mux::start(StreamChannel::start(stream, codec()).unwrap()).unwrap();
    let (c0, c1) = (mux.channel(0).unwrap(), mux.channel(1).unwrap());
    // A length prefix promising 100 bytes, three of them, then the end.
    far.write_all(&[100, 0, 0, 0, 1, 2, 3]).unwrap();
    drop(far);
    assert_eq!(c0.receive(WAIT), Err(Failure::PathLost));
    assert_eq!(c1.receive(WAIT), Err(Failure::PathLost));
    assert_eq!(c0.send(b"x"), Err(Failure::PathLost));
    assert!(c0.is_closed());
}

#[test]
fn several_threads_on_several_channels_keep_each_channels_order() {
    let (a, b) = pair();
    let senders: Vec<MuxChannel> = (0..4).map(|n| a.channel(n).unwrap()).collect();
    let receivers: Vec<MuxChannel> = (0..4).map(|n| b.channel(n).unwrap()).collect();
    let mut threads = Vec::new();
    for sender in senders {
        threads.push(thread::spawn(move || {
            for n in 0..300_u32 {
                sender.send(&n.to_le_bytes()).unwrap();
                sender.datagrams().unwrap().send(&n.to_le_bytes()).unwrap();
            }
            sender
        }));
    }
    let mut readers = Vec::new();
    for receiver in receivers {
        readers.push(thread::spawn(move || {
            for n in 0..300_u32 {
                let got = receiver.receive(WAIT).unwrap().unwrap();
                assert_eq!(got, n.to_le_bytes());
            }
            receiver
        }));
    }
    let _kept: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    let _kept: Vec<_> = readers.into_iter().map(|t| t.join().unwrap()).collect();
}

#[test]
fn the_single_channel_shortcut_is_channel_zero() {
    let (one, other) = carriers();
    let a = Mux::single(one).unwrap();
    let b = Mux::single(other).unwrap();
    assert_eq!(a.number(), 0);
    a.send(b"x").unwrap();
    assert_eq!(b.receive(WAIT).unwrap().unwrap(), b"x");
    assert!(b.datagrams().is_some());
}

// ----- the legacy wrapper -----

#[test]
fn the_legacy_wrapper_adds_datagrams_to_a_plain_codec_and_both_ends_wrap() {
    let (one, other) = carriers();
    let a = Mux::legacy(one).unwrap();
    let b = Mux::legacy(other).unwrap();
    a.send(b"message").unwrap();
    a.datagrams().unwrap().send(b"datagram").unwrap();
    assert_eq!(b.receive(WAIT).unwrap().unwrap(), b"message");
    assert_eq!(
        b.datagrams().unwrap().receive(WAIT).unwrap().unwrap(),
        b"datagram"
    );
    assert_eq!(b.datagrams().unwrap().delivery(), Delivery::OverStream);
}

#[test]
fn the_legacy_wrapper_puts_one_tag_byte_on_the_wire_and_nothing_else() {
    let (one, raw) = carriers();
    let a = Mux::legacy(one).unwrap();
    a.send(b"hi").unwrap();
    a.datagrams().unwrap().send(b"yo").unwrap();
    let mut seen = vec![
        raw.receive(WAIT).unwrap().unwrap(),
        raw.receive(WAIT).unwrap().unwrap(),
    ];
    seen.sort();
    assert_eq!(seen, vec![b"\x00hi".to_vec(), b"\x01yo".to_vec()]);
    // And it reads the same.
    raw.send(b"\x00plain").unwrap();
    raw.send(b"\x01gram").unwrap();
    assert_eq!(a.receive(WAIT).unwrap().unwrap(), b"plain");
    assert_eq!(
        a.datagrams().unwrap().receive(WAIT).unwrap().unwrap(),
        b"gram"
    );
}

#[test]
fn the_legacy_wrapper_reads_a_peer_that_does_not_wrap_as_a_breach() {
    for first in [&b"\x07abc"[..], b""] {
        let (one, raw) = carriers();
        let a = Mux::legacy(one).unwrap();
        raw.send(first).unwrap();
        assert_eq!(a.receive(WAIT), Err(Failure::Protocol));
    }
}

#[test]
fn the_legacy_wrapper_has_one_channel() {
    let (one, _other) = carriers();
    let mut settings = MuxSettings::new();
    settings.framing = Framing::Tagged;
    settings.channels = 8;
    let mux = builder(settings, one);
    assert!(mux.channel(0).is_ok());
    assert_eq!(mux.channel(1).err(), Some(OpenError::Number));
}

// ----- settings -----

#[test]
fn the_defaults_are_the_documented_ones() {
    let settings = MuxSettings::new();
    assert_eq!(settings, MuxSettings::default());
    assert!(settings.non_defaults().is_empty());
    assert_eq!(settings.framing, Framing::Versioned);
    assert_eq!(settings.channels, 16);
    assert_eq!(settings.max_message, 1024 * 1024 - 12);
    assert_eq!(settings.pending, 64);
    assert_eq!(settings.queued_bytes, 16 * 1024 * 1024);
    assert_eq!(settings.send_limit, Duration::from_secs(30));
    assert_eq!(settings.max_datagram, 1200);
    assert_eq!(settings.datagram_queue, 64);
    assert_eq!(settings.datagram_pending, 64);
    assert_eq!(settings.datagram_drop, DatagramDrop::Oldest);
}

#[test]
fn settings_are_read_from_text_and_unknown_keys_are_refused() {
    let read = |text: &str| -> Result<MuxSettings, String> {
        toml::from_str::<Partial<MuxSettings>>(text)
            .map_err(|error| error.to_string())?
            .build()
            .map_err(|error| error.to_string())
    };
    let settings = read("framing = \"tagged\"\nmax_datagram = 500\ndatagram_drop = \"newest\"\nsend_limit = \"250ms\"").unwrap();
    assert_eq!(settings.framing, Framing::Tagged);
    assert_eq!(settings.max_datagram, 500);
    assert_eq!(settings.datagram_drop, DatagramDrop::Newest);
    assert_eq!(settings.send_limit, Duration::from_millis(250));
    let mut keys: Vec<String> = settings
        .non_defaults()
        .into_iter()
        .map(|n| n.setting)
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        ["datagram_drop", "framing", "max_datagram", "send_limit"]
    );
    assert!(read("windows = 4").is_err());
    assert!(read("framing = \"cobs\"").is_err());
    assert_eq!(read("").unwrap(), MuxSettings::new());
}

#[test]
fn the_builder_setters_reach_the_settings() {
    let (one, _other) = carriers();
    let builder = MuxBuilder::new()
        .carrier(one)
        .channels(2)
        .max_message(1000)
        .pending(3)
        .queued_bytes(5000)
        .send_limit(Duration::from_secs(4))
        .max_datagram(100)
        .datagram_queue(5)
        .datagram_pending(6)
        .datagram_drop(DatagramDrop::Newest)
        .framing(Framing::Versioned);
    let mut keys: Vec<String> = builder
        .non_defaults()
        .into_iter()
        .map(|n| n.setting)
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "channels",
            "datagram_drop",
            "datagram_pending",
            "datagram_queue",
            "max_datagram",
            "max_message",
            "pending",
            "queued_bytes",
            "send_limit"
        ]
    );
    let mux = builder.build().unwrap();
    let channel = mux.channel(1).unwrap();
    assert_eq!(mux.channel(2).err(), Some(OpenError::Number));
    assert_eq!(channel.datagrams().unwrap().max_size(), Some(100));
    assert_eq!(channel.capabilities().max_send, Some(1000));
}

#[test]
fn a_builder_without_a_carrier_names_what_is_missing() {
    let result = MuxBuilder::<StreamChannel>::new().build();
    assert!(result.is_err());
}

#[test]
fn the_frame_overhead_is_what_the_carrier_must_admit() {
    let (one, raw) = carriers();
    let a = Mux::single(one).unwrap();
    a.send(&[0; 100]).unwrap();
    assert_eq!(raw.receive(WAIT).unwrap().unwrap().len(), 100 + OVERHEAD);
}
