//! The multiplexer's received-memory bounds and its damage rules: settings and counters a hostile
//! peer meets.

use std::time::Duration;

use coqui_transport::frame::LengthPrefix;
use coqui_transport::frame::versioned::{Frame, Kind};
use coqui_transport::memory;
use coqui_transport::mux::{DatagramDrop, Mux, MuxBuilder, MuxSettings};
use coqui_transport::{Channel, Failure, StreamChannel};

const WAIT: Option<Duration> = Some(Duration::from_secs(5));
const SHORT: Option<Duration> = Some(Duration::from_millis(300));

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

#[test]
fn received_bytes_bound_what_all_channels_hold_and_hold_the_reader_at_the_bound() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .received_bytes(10_000)
        .pending(1000)
        .build()
        .unwrap();
    let (a0, a1) = (a.channel(0).unwrap(), a.channel(1).unwrap());
    for n in 0..30_u32 {
        raw.send(&frame(Kind::Message, 0, n, &[1; 1000])).unwrap();
    }
    raw.send(&frame(Kind::Message, 1, 0, b"behind")).unwrap();
    // Ten messages of 1000 bytes fill the budget: the reader waits, so channel 1 hears nothing.
    assert_eq!(a1.receive(SHORT), Err(Failure::Timeout));
    for _ in 0..30 {
        assert_eq!(a0.receive(WAIT).unwrap().unwrap().len(), 1000);
    }
    assert_eq!(a1.receive(WAIT).unwrap().unwrap(), b"behind");
}

#[test]
fn received_bytes_also_bound_datagrams_which_never_hold_the_reader() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .received_bytes(5_000)
        .datagram_pending(1000)
        .build()
        .unwrap();
    let a0 = a.channel(0).unwrap();
    for n in 0..20_u32 {
        let mut payload = [0_u8; 1000];
        payload[0] = u8::try_from(n).unwrap();
        raw.send(&frame(Kind::Datagram, 0, n, &payload)).unwrap();
    }
    raw.send(&frame(Kind::Message, 0, 0, b"sync")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"sync");
    let datagrams = a0.datagrams().unwrap();
    // Each counts its 1000 bytes and 64 more: four fit, and the oldest go first.
    assert_eq!(datagrams.dropped(), 16);
    for n in 16..20_u8 {
        assert_eq!(datagrams.receive(WAIT).unwrap().unwrap()[0], n);
    }
    assert_eq!(datagrams.receive(SHORT), Err(Failure::Timeout));
}

#[test]
fn datagrams_for_a_number_nobody_took_are_dropped_past_its_budget() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .unopened_bytes(3_000)
        .build()
        .unwrap();
    let a0 = a.channel(0).unwrap();
    for n in 0..10_u32 {
        raw.send(&frame(Kind::Datagram, 4, n, &[9; 1000])).unwrap();
    }
    raw.send(&frame(Kind::Message, 0, 0, b"sync")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"sync");
    let four = a.channel(4).unwrap();
    let datagrams = four.datagrams().unwrap();
    assert!(datagrams.dropped() >= 7, "dropped {}", datagrams.dropped());
}

#[test]
fn unopened_numbers_share_a_small_budget_and_the_rest_waits_for_nobody() {
    let (one, raw) = carriers();
    let a = Mux::start(one).unwrap();
    let a0 = a.channel(0).unwrap();
    // Every number but 0 and 15, a little each: together far more than the default share.
    for number in 1..15_u16 {
        for n in 0..8_u32 {
            raw.send(&frame(Kind::Message, number, n, &[3; 2000]))
                .unwrap();
        }
    }
    raw.send(&frame(Kind::Message, 0, 0, b"after")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"after");
}

#[test]
fn the_defaults_for_the_new_bounds_are_the_documented_ones() {
    let settings = MuxSettings::new();
    assert_eq!(settings.received_bytes, 16 * 1024 * 1024);
    assert_eq!(settings.unopened_bytes, 64 * 1024);
    assert_eq!(settings.max_damaged, 8);
    assert_eq!(Mux::DEFAULT_MAX_MESSAGE, 1024 * 1024 - 12);
    assert_eq!(settings.datagram_drop, DatagramDrop::Oldest);
}

#[test]
fn the_counters_are_readable_from_a_single_channel() {
    let (one, raw) = carriers();
    let only = Mux::single(one).unwrap();
    raw.send(&frame(Kind::Message, 0, 0, b"good")).unwrap();
    let mut damaged = frame(Kind::Message, 0, 1, b"bad");
    damaged[8] ^= 1;
    raw.send(&damaged).unwrap();
    assert_eq!(only.receive(WAIT).unwrap().unwrap(), b"good");
    raw.send(&frame(Kind::Datagram, 0, 3, b"d")).unwrap();
    assert_eq!(
        only.datagrams().unwrap().receive(WAIT).unwrap().unwrap(),
        b"d"
    );
    let counters = only.counters();
    assert_eq!((counters.corrupt, counters.lost_datagrams), (1, 3));
}

#[test]
fn damage_before_any_good_frame_is_a_breach() {
    let (one, raw) = carriers();
    let only = Mux::single(one).unwrap();
    let mut damaged = frame(Kind::Message, 0, 0, b"bad");
    damaged[8] ^= 1;
    raw.send(&damaged).unwrap();
    assert_eq!(only.receive(WAIT), Err(Failure::Protocol));
}

#[test]
fn a_run_of_damaged_frames_is_a_breach_but_scattered_damage_is_not() {
    let (one, raw) = carriers();
    let a = MuxBuilder::new()
        .carrier(one)
        .max_damaged(3)
        .build()
        .unwrap();
    let a0 = a.channel(0).unwrap();
    let damaged = |sequence: u32| {
        let mut bytes = frame(Kind::Datagram, 0, sequence, b"x");
        bytes[8] ^= 1;
        bytes
    };
    raw.send(&frame(Kind::Datagram, 0, 0, b"ok")).unwrap();
    raw.send(&damaged(1)).unwrap();
    raw.send(&damaged(2)).unwrap();
    raw.send(&frame(Kind::Datagram, 0, 3, b"ok")).unwrap();
    raw.send(&damaged(4)).unwrap();
    raw.send(&damaged(5)).unwrap();
    raw.send(&frame(Kind::Message, 0, 0, b"still up")).unwrap();
    assert_eq!(a0.receive(WAIT).unwrap().unwrap(), b"still up");
    for sequence in 6..9 {
        raw.send(&damaged(sequence)).unwrap();
    }
    assert_eq!(a0.receive(WAIT), Err(Failure::Protocol));
}
