//! `StreamChannel` on in-memory streams: framing, bounds, partial I/O, timeouts, backpressure,
//! concurrency, the end of the stream and closing.

mod support;

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use coqui_transport::frame::{Codec, PeerFrame, WebTransportFrame};
use coqui_transport::memory::{self, ReadHalf, WriteHalf};
use coqui_transport::{
    Capabilities, Carrier, Channel, Failure, Halves, StreamChannel, StreamChannelBuilder,
};
use support::WAIT;

const SHORT: Option<Duration> = Some(Duration::from_millis(50));

fn assert_send_sync<T: Send + Sync>() {}

fn channels<C: Codec + Clone>(codec: C) -> (StreamChannel, StreamChannel) {
    let (one, other) = memory::pipe(4096);
    (
        StreamChannel::start(one, codec.clone()).unwrap(),
        StreamChannel::start(other, codec).unwrap(),
    )
}

/// A channel on one end of a pipe, and the other end's halves to read and write bytes by hand.
fn raw<C: Codec>(
    builder: StreamChannelBuilder<C>,
    capacity: usize,
) -> (StreamChannel, ReadHalf, WriteHalf) {
    let (one, other) = memory::pipe(capacity);
    let channel = builder.start(one).unwrap();
    let (reader, writer) = other.into_halves();
    (channel, reader, writer)
}

fn frame(message: &[u8]) -> Vec<u8> {
    PeerFrame.encode(message, usize::MAX).unwrap()
}

#[test]
fn a_channel_is_send_and_sync_and_a_boxed_one_is_a_channel() {
    assert_send_sync::<StreamChannel>();
    let (one, other) = channels(PeerFrame);
    let boxed: Box<dyn Channel> = Box::new(one);
    boxed.send(b"boxed").unwrap();
    assert_eq!(other.receive(WAIT).unwrap(), Some(b"boxed".to_vec()));
}

#[test]
fn messages_cross_in_order_both_ways() {
    let (one, other) = channels(WebTransportFrame);
    for at in 0..100_u32 {
        one.send(&at.to_be_bytes()).unwrap();
        other.send(&(at + 1000).to_be_bytes()).unwrap();
    }
    for at in 0..100_u32 {
        assert_eq!(
            other.receive(WAIT).unwrap(),
            Some(at.to_be_bytes().to_vec())
        );
        assert_eq!(
            one.receive(WAIT).unwrap(),
            Some((at + 1000).to_be_bytes().to_vec())
        );
    }
}

#[test]
fn an_empty_message_and_a_large_one_cross_on_the_peer_wire() {
    let (one, other) = memory::pipe(4096);
    let builder = || StreamChannel::builder(PeerFrame).max_message(256 * 1024);
    let one = builder().start(one).unwrap();
    let other = builder().start(other).unwrap();
    let large: Vec<u8> = (0..200 * 1024).map(|at| (at % 251) as u8).collect();
    one.send(b"").unwrap();
    one.send(&large).unwrap();
    one.send(b"").unwrap();
    assert_eq!(other.receive(WAIT).unwrap(), Some(Vec::new()));
    assert_eq!(other.receive(WAIT).unwrap(), Some(large));
    assert_eq!(other.receive(WAIT).unwrap(), Some(Vec::new()));
}

#[test]
fn the_webtransport_wire_refuses_an_empty_message_and_goes_on() {
    let (one, other) = channels(WebTransportFrame);
    assert_eq!(one.send(b""), Err(Failure::Protocol));
    assert_eq!(one.send_timeout(b"", WAIT), Err(Failure::Protocol));
    one.send(b"after").unwrap();
    assert_eq!(other.receive(WAIT).unwrap(), Some(b"after".to_vec()));
}

#[test]
fn a_message_above_the_send_bound_is_refused_and_the_channel_goes_on() {
    let (one, other) = channels(PeerFrame);
    one.set_max_send(4);
    assert_eq!(one.send(b"12345"), Err(Failure::Protocol));
    assert_eq!(one.send_timeout(b"12345", WAIT), Err(Failure::Protocol));
    one.send(b"1234").unwrap();
    assert_eq!(other.receive(WAIT).unwrap(), Some(b"1234".to_vec()));
    assert!(!one.is_closed());
}

#[test]
fn a_header_above_the_receive_bound_ends_the_channel_at_both_ends() {
    let (channel, mut reader, mut writer) =
        raw(StreamChannel::builder(PeerFrame).max_message(10), 4096);
    writer.write_all(&frame(b"ok")).unwrap();
    // A header declaring 11 bytes, whose body never comes.
    writer.write_all(&11_u32.to_be_bytes()).unwrap();
    assert_eq!(channel.receive(WAIT).unwrap(), Some(b"ok".to_vec()));
    assert_eq!(channel.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(channel.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(channel.send(b"x"), Err(Failure::Protocol));
    assert!(channel.is_closed());
    // The peer reads the end of the stream.
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
}

#[test]
fn partial_reads_and_writes_carry_whole_messages() {
    let (one, other) = memory::pipe(1);
    let builder = || StreamChannel::builder(PeerFrame).read_buffer(1);
    let one = builder().start(one).unwrap();
    let other = builder().start(other).unwrap();
    let messages: Vec<Vec<u8>> = (0..20)
        .map(|length| vec![length as u8; length * 7])
        .collect();
    for message in &messages {
        one.send(message).unwrap();
    }
    for message in &messages {
        assert_eq!(other.receive(WAIT).unwrap().as_ref(), Some(message));
    }
}

#[test]
fn a_receive_that_times_out_keeps_what_arrived_and_the_channel_goes_on() {
    let (channel, _reader, mut writer) = raw(StreamChannel::builder(PeerFrame), 4096);
    let bytes = frame(b"hello");
    writer.write_all(&bytes[..6]).unwrap();
    assert_eq!(channel.receive(SHORT), Err(Failure::Timeout));
    assert_eq!(channel.receive(Some(Duration::ZERO)), Err(Failure::Timeout));
    writer.write_all(&bytes[6..]).unwrap();
    assert_eq!(channel.receive(WAIT).unwrap(), Some(b"hello".to_vec()));
    assert!(!channel.is_closed());
}

#[test]
fn a_stream_ending_inside_a_frame_is_a_lost_path() {
    for cut in [1, 3, 4, 6] {
        let (channel, _reader, mut writer) = raw(StreamChannel::builder(PeerFrame), 4096);
        writer.write_all(&frame(b"hello")[..cut]).unwrap();
        drop(writer);
        assert_eq!(
            channel.receive(WAIT),
            Err(Failure::PathLost),
            "cut at {cut}"
        );
        assert_eq!(
            channel.receive(WAIT),
            Err(Failure::PathLost),
            "cut at {cut}"
        );
        assert!(channel.is_closed());
    }
}

#[test]
fn a_stream_ending_between_frames_is_an_orderly_end_and_stays_one() {
    let (channel, _reader, mut writer) = raw(StreamChannel::builder(PeerFrame), 4096);
    writer.write_all(&frame(b"last")).unwrap();
    drop(writer);
    assert_eq!(channel.receive(WAIT).unwrap(), Some(b"last".to_vec()));
    assert_eq!(channel.receive(WAIT), Ok(None));
    assert_eq!(channel.receive(WAIT), Ok(None));
    assert!(channel.is_closed());
}

#[test]
fn a_peer_closing_its_channel_is_an_orderly_end() {
    let (one, other) = channels(PeerFrame);
    one.send(b"bye").unwrap();
    one.close();
    assert_eq!(other.receive(WAIT).unwrap(), Some(b"bye".to_vec()));
    assert_eq!(other.receive(WAIT), Ok(None));
}

/// Writes `count` frames of 32 bytes in a thread, counting each one the stream took whole.
fn flood(mut writer: WriteHalf, count: usize) -> (Arc<AtomicUsize>, thread::JoinHandle<()>) {
    let written = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&written);
    let thread = thread::spawn(move || {
        for _ in 0..count {
            if writer.write_all(&frame(&[9; 32])).is_err() {
                return;
            }
            counting.fetch_add(1, Ordering::SeqCst);
        }
    });
    (written, thread)
}

#[test]
fn a_slow_reader_holds_the_peers_sends_back() {
    let builder = StreamChannel::builder(PeerFrame).pending(1).read_buffer(36);
    let (channel, _reader, writer) = raw(builder, 36);
    let (written, flooding) = flood(writer, 50);
    thread::sleep(Duration::from_millis(300));
    // One message held for the reader, one in the decoder's read, one in the pipe, one in the write.
    let held = written.load(Ordering::SeqCst);
    assert!(
        held <= 4,
        "{held} frames went out to a reader that took none"
    );

    // A higher read-ahead lets the channel read on at once, with nothing received.
    channel.set_pending(10).unwrap();
    thread::sleep(Duration::from_millis(300));
    let more = written.load(Ordering::SeqCst);
    assert!(
        more > held && more <= 14,
        "{more} after raising the read-ahead from {held}"
    );

    for _ in 0..50 {
        assert_eq!(channel.receive(WAIT).unwrap(), Some(vec![9; 32]));
    }
    flooding.join().unwrap();
    assert_eq!(written.load(Ordering::SeqCst), 50);
}

#[test]
fn a_read_ahead_of_0_is_1() {
    let builder = StreamChannel::builder(PeerFrame).pending(0).read_buffer(36);
    let (channel, _reader, writer) = raw(builder, 36);
    let (written, flooding) = flood(writer, 10);
    thread::sleep(Duration::from_millis(300));
    assert!(written.load(Ordering::SeqCst) <= 4);
    for _ in 0..10 {
        assert_eq!(channel.receive(WAIT).unwrap(), Some(vec![9; 32]));
    }
    flooding.join().unwrap();
}

#[test]
fn a_full_queue_refuses_or_waits_and_nothing_refused_is_sent() {
    // Room for two queued 10-byte messages, on a pipe that holds one byte and is never read.
    let cost = 4 + 10 + 64;
    let builder = StreamChannel::builder(PeerFrame)
        .queued_bytes(2 * cost)
        .send_limit(Duration::from_secs(60));
    let (channel, mut reader, _writer) = raw(builder, 1);
    channel.send(&[1; 10]).unwrap();
    channel.send(&[2; 10]).unwrap();
    assert_eq!(channel.send(&[3; 10]), Err(Failure::NoProgress));
    let started = Instant::now();
    assert_eq!(channel.send_timeout(&[4; 10], SHORT), Err(Failure::Timeout));
    assert!(started.elapsed() >= Duration::from_millis(40));
    assert!(!channel.is_closed());

    // Reading frees room, and a send waiting for it goes through.
    let reading = thread::spawn(move || {
        let mut wire = vec![0; 3 * 14];
        reader.read_exact(&mut wire).unwrap();
        wire
    });
    channel.send_timeout(&[5; 10], WAIT).unwrap();
    let wire = reading.join().unwrap();
    let mut expected = frame(&[1; 10]);
    expected.extend(frame(&[2; 10]));
    expected.extend(frame(&[5; 10]));
    assert_eq!(wire, expected);
}

#[test]
fn a_message_larger_than_the_whole_queue_is_refused_at_once() {
    let builder = StreamChannel::builder(PeerFrame).queued_bytes(100);
    let (channel, _reader, _writer) = raw(builder, 4096);
    let started = Instant::now();
    assert_eq!(
        channel.send_timeout(&[0; 40], None),
        Err(Failure::NoProgress)
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(channel.send(&[0; 40]), Err(Failure::NoProgress));
    channel.send(&[0; 30]).unwrap();
}

#[test]
fn every_queued_message_counts_more_than_its_bytes_so_empty_ones_are_bounded_too() {
    let builder = StreamChannel::builder(PeerFrame)
        .queued_bytes(3 * (4 + 64))
        .send_limit(Duration::from_secs(60));
    let (channel, _reader, _writer) = raw(builder, 1);
    for _ in 0..3 {
        channel.send(b"").unwrap();
    }
    assert_eq!(channel.send(b""), Err(Failure::NoProgress));
}

#[test]
fn a_peer_that_takes_nothing_for_the_send_limit_ends_the_channel() {
    let builder = StreamChannel::builder(PeerFrame).send_limit(Duration::from_millis(100));
    let (channel, _reader, _writer) = raw(builder, 8);
    channel.send(&[0; 64]).unwrap();
    thread::sleep(Duration::from_millis(250));
    assert_eq!(channel.send(b"more"), Err(Failure::NoProgress));
    assert_eq!(channel.send(b"more"), Err(Failure::NoProgress));
    assert_eq!(channel.receive(WAIT), Err(Failure::NoProgress));
    assert!(channel.is_closed());
}

#[test]
fn a_send_waiting_for_room_sees_the_send_limit_pass() {
    let builder = StreamChannel::builder(PeerFrame)
        .send_limit(Duration::from_millis(100))
        .queued_bytes(100);
    let (channel, _reader, _writer) = raw(builder, 8);
    channel.send(&[0; 30]).unwrap();
    let started = Instant::now();
    assert_eq!(
        channel.send_timeout(&[0; 30], WAIT),
        Err(Failure::NoProgress)
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// A writer that takes nothing and says it timed out, as a stream with a write timeout does.
struct TimingOut;

impl Write for TimingOut {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        thread::sleep(Duration::from_millis(10));
        Err(io::ErrorKind::TimedOut.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_stream_with_a_write_timeout_ends_the_channel_by_itself_at_the_send_limit() {
    let (mine, _theirs) = memory::pipe(16);
    let (reader, _writer) = mine.into_halves();
    let channel = StreamChannel::builder(PeerFrame)
        .send_limit(Duration::from_millis(100))
        .start(Halves::new(reader, TimingOut))
        .unwrap();
    channel.send(b"never taken").unwrap();
    // No further send: the writer itself gives up, and a waiting receive hears it.
    assert_eq!(channel.receive(WAIT), Err(Failure::NoProgress));
}

#[test]
fn a_peer_that_stopped_reading_for_good_is_a_lost_path() {
    let (channel, reader, _writer) = raw(StreamChannel::builder(PeerFrame), 4096);
    drop(reader);
    channel.send(b"into nothing").unwrap();
    let started = Instant::now();
    while channel.send(b"again").is_ok() {
        assert!(started.elapsed() < Duration::from_secs(10));
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(channel.send(b"again"), Err(Failure::PathLost));
}

#[test]
fn many_threads_send_and_receive_at_once() {
    let (one, other) = channels(PeerFrame);
    let one = Arc::new(one);
    let other = Arc::new(other);
    let senders: Vec<_> = (0..4_u8)
        .map(|sender| {
            let one = Arc::clone(&one);
            thread::spawn(move || {
                for at in 0..200_u16 {
                    let mut message = vec![sender];
                    message.extend(at.to_be_bytes());
                    one.send_timeout(&message, WAIT).unwrap();
                }
            })
        })
        .collect();
    // The other way at the same time, on the same two channels.
    let answering = {
        let other = Arc::clone(&other);
        thread::spawn(move || {
            for at in 0..200_u16 {
                other.send_timeout(&at.to_be_bytes(), WAIT).unwrap();
            }
        })
    };
    let receivers: Vec<_> = (0..2)
        .map(|_| {
            let other = Arc::clone(&other);
            thread::spawn(move || {
                let mut got = Vec::new();
                while let Ok(Some(message)) = other.receive(Some(Duration::from_millis(500))) {
                    got.push(message);
                }
                got
            })
        })
        .collect();
    for at in 0..200_u16 {
        assert_eq!(one.receive(WAIT).unwrap(), Some(at.to_be_bytes().to_vec()));
    }
    for sender in senders {
        sender.join().unwrap();
    }
    answering.join().unwrap();
    let mut all: Vec<Vec<u8>> = receivers
        .into_iter()
        .flat_map(|receiver| receiver.join().unwrap())
        .collect();
    assert_eq!(all.len(), 800);
    // Each sender's messages were queued in its order, so they arrive in it.
    for sender in 0..4_u8 {
        let mine: Vec<u16> = all
            .iter()
            .filter(|message| message[0] == sender)
            .map(|message| u16::from_be_bytes([message[1], message[2]]))
            .collect();
        assert_eq!(mine.len(), 200);
    }
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 800);
}

#[test]
fn one_receiver_sees_each_senders_messages_in_order() {
    let (one, other) = channels(PeerFrame);
    let one = Arc::new(one);
    let senders: Vec<_> = (0..4_u8)
        .map(|sender| {
            let one = Arc::clone(&one);
            thread::spawn(move || {
                for at in 0..200_u16 {
                    let mut message = vec![sender];
                    message.extend(at.to_be_bytes());
                    one.send_timeout(&message, WAIT).unwrap();
                }
            })
        })
        .collect();
    let mut next = [0_u16; 4];
    for _ in 0..800 {
        let message = other.receive(WAIT).unwrap().unwrap();
        let sender = usize::from(message[0]);
        assert_eq!(u16::from_be_bytes([message[1], message[2]]), next[sender]);
        next[sender] += 1;
    }
    for sender in senders {
        sender.join().unwrap();
    }
}

#[test]
fn a_second_receiver_waits_only_as_long_as_its_own_timeout() {
    let (one, other) = channels(PeerFrame);
    let other = Arc::new(other);
    let waiting = {
        let other = Arc::clone(&other);
        thread::spawn(move || other.receive(None))
    };
    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();
    assert_eq!(other.receive(SHORT), Err(Failure::Timeout));
    assert!(started.elapsed() < Duration::from_secs(2));
    one.send(b"for the first").unwrap();
    assert_eq!(waiting.join().unwrap(), Ok(Some(b"for the first".to_vec())));
}

#[test]
fn closing_sends_what_is_queued() {
    let (one, other) = memory::pipe(64);
    let one = StreamChannel::start(one, PeerFrame).unwrap();
    let other = StreamChannel::start(other, PeerFrame).unwrap();
    for at in 0..100_u32 {
        one.send(&at.to_be_bytes()).unwrap();
    }
    // More than the peer reads ahead, so closing waits for the peer's reader to take them.
    let receiving = thread::spawn(move || {
        for at in 0..100_u32 {
            assert_eq!(
                other.receive(WAIT).unwrap(),
                Some(at.to_be_bytes().to_vec())
            );
        }
        assert_eq!(other.receive(WAIT), Ok(None));
    });
    one.close();
    receiving.join().unwrap();
}

#[test]
fn closing_wakes_a_blocked_receive_and_can_be_repeated() {
    let (one, _other) = channels(PeerFrame);
    let one = Arc::new(one);
    let waiting = {
        let one = Arc::clone(&one);
        thread::spawn(move || one.receive(None))
    };
    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();
    one.close();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(waiting.join().unwrap(), Ok(None));
    one.close();
    assert_eq!(one.send(b"late"), Err(Failure::PathLost));
    assert_eq!(one.send_timeout(b"late", WAIT), Err(Failure::PathLost));
    assert_eq!(one.receive(WAIT), Ok(None));
    assert!(one.is_closed());
}

#[test]
fn messages_read_before_closing_stay_readable() {
    let (one, other) = channels(PeerFrame);
    for at in 0..3_u8 {
        other.send(&[at]).unwrap();
    }
    thread::sleep(Duration::from_millis(200));
    one.close();
    for at in 0..3_u8 {
        assert_eq!(one.receive(WAIT).unwrap(), Some(vec![at]));
    }
    assert_eq!(one.receive(WAIT), Ok(None));
}

#[test]
fn closing_while_the_peer_takes_nothing_gives_up_after_the_send_limit() {
    let builder = StreamChannel::builder(PeerFrame).send_limit(Duration::from_millis(100));
    let (channel, _reader, _writer) = raw(builder, 4);
    channel.send(&[0; 64]).unwrap();
    let started = Instant::now();
    channel.close();
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(90) && took < Duration::from_secs(5),
        "{took:?}"
    );
}

#[test]
fn a_failure_wakes_a_writer_blocked_on_the_stream_and_the_peer_reads_the_end() {
    let builder = StreamChannel::builder(PeerFrame)
        .max_message(10)
        .send_limit(Duration::from_secs(60));
    let (channel, mut reader, mut writer) = raw(builder, 8);
    // The writer blocks on a pipe the peer has not read, then the peer breaks the wire.
    channel.send(&[0; 10]).unwrap();
    thread::sleep(Duration::from_millis(50));
    writer.write_all(&11_u32.to_be_bytes()).unwrap();
    assert_eq!(channel.receive(WAIT), Err(Failure::Protocol));
    let (done, ended) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let mut rest = Vec::new();
        let _ = done.send(reader.read_to_end(&mut rest).map(|_| rest.len()));
    });
    let read = ended
        .recv_timeout(Duration::from_secs(5))
        .expect("the peer reads the end");
    assert!(read.unwrap() <= 14);
}

/// Accepts everything, taking time in proportion to what one call is given, as a blocking socket
/// send does until it has queued the whole buffer.
struct Paced;

impl Write for Paced {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        thread::sleep(Duration::from_micros(buf.len() as u64 / 2));
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_large_frame_to_a_slow_but_steady_stream_is_not_taken_for_a_stall() {
    let (mine, _theirs) = memory::pipe(16);
    let (reader, _writer) = mine.into_halves();
    let channel = StreamChannel::builder(PeerFrame)
        .send_limit(Duration::from_millis(200))
        .start(Halves::new(reader, Paced))
        .unwrap();
    // About half a second for the whole frame, at two bytes a microsecond.
    channel.send(&vec![0; 1 << 20]).unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_eq!(channel.send(b"next"), Ok(()));
    assert!(!channel.is_closed());
}

#[test]
fn closing_a_channel_whose_writer_cannot_be_woken_returns_after_the_send_limit() {
    let (mine, _theirs) = memory::pipe(1);
    let (reader, writer) = mine.into_halves();
    // An interrupt that wakes nothing, as a platform whose shutdown leaves a blocked write blocked.
    let halves = Halves::new(reader, writer).with_interrupt(coqui_transport::Interrupt::new(|| {}));
    let channel = StreamChannel::builder(PeerFrame)
        .send_limit(Duration::from_millis(100))
        .start(halves)
        .unwrap();
    channel.send(&[0; 64]).unwrap();
    let started = Instant::now();
    channel.close();
    let took = started.elapsed();
    assert!(took < Duration::from_secs(5), "{took:?}");
    assert_eq!(channel.send(b"late"), Err(Failure::PathLost));
}

#[test]
fn a_staged_bound_takes_effect_by_position_however_far_ahead_it_is_read() {
    let (one, other) = memory::pipe(4096);
    let one = StreamChannel::builder(PeerFrame)
        .max_message(10)
        .start(one)
        .unwrap();
    let other = StreamChannel::start(other, PeerFrame).unwrap();
    one.set_max_receive_after(1, 100).unwrap();
    other.send(&[1; 10]).unwrap();
    other.send(&[2; 50]).unwrap();
    other.send(&[3; 100]).unwrap();
    // All three are read ahead of the reader before it takes any.
    thread::sleep(Duration::from_millis(100));
    assert_eq!(one.receive(WAIT).unwrap(), Some(vec![1; 10]));
    assert_eq!(one.receive(WAIT).unwrap(), Some(vec![2; 50]));
    assert_eq!(one.receive(WAIT).unwrap(), Some(vec![3; 100]));

    // A large message where the small bound still holds is refused.
    let (one, other) = memory::pipe(4096);
    let one = StreamChannel::builder(PeerFrame)
        .max_message(10)
        .start(one)
        .unwrap();
    let other = StreamChannel::start(other, PeerFrame).unwrap();
    one.set_max_receive_after(1, 100).unwrap();
    other.send(&[2; 50]).unwrap();
    assert_eq!(one.receive(WAIT), Err(Failure::Protocol));
}

#[test]
fn a_stage_of_nothing_is_the_bound_at_once_and_a_later_call_replaces_a_stage() {
    let (one, other) = memory::pipe(4096);
    let one = StreamChannel::builder(PeerFrame)
        .max_message(10)
        .start(one)
        .unwrap();
    let other = StreamChannel::start(other, PeerFrame).unwrap();
    one.set_max_receive_after(0, 20).unwrap();
    assert_eq!(one.capabilities().max_receive, Some(20));
    one.set_max_receive_after(1, 100).unwrap();
    one.set_max_receive(20);
    other.send(&[1; 20]).unwrap();
    other.send(&[2; 50]).unwrap();
    assert_eq!(one.receive(WAIT).unwrap(), Some(vec![1; 20]));
    assert_eq!(one.receive(WAIT), Err(Failure::Protocol));
}

#[test]
fn capabilities_report_the_stream_and_its_limits_as_they_stand() {
    let (one, _other) = channels(PeerFrame);
    assert_eq!(
        one.capabilities(),
        Capabilities::new()
            .with_carrier(Carrier::Stream)
            .with_limits(
                StreamChannel::DEFAULT_MAX_MESSAGE,
                StreamChannel::DEFAULT_MAX_MESSAGE
            )
    );
    one.set_max_send(5);
    one.set_max_receive(7);
    let now = one.capabilities();
    assert_eq!((now.max_send, now.max_receive), (Some(5), Some(7)));
    one.set_max_message(9);
    let now = one.capabilities();
    assert_eq!((now.max_send, now.max_receive), (Some(9), Some(9)));
    // A legacy wire carries no datagrams: one would reach an old peer as a message.
    assert!(one.datagrams().is_none());
    assert_eq!((now.datagrams, now.max_datagram), (None, None));
    assert!(!one.is_closed());
    one.close();
    assert!(one.is_closed());
}

#[test]
fn bounds_too_long_for_the_clock_wait_as_none_would() {
    let (one, other) = memory::pipe(4096);
    let one = StreamChannel::builder(PeerFrame)
        .send_limit(Duration::MAX)
        .start(one)
        .unwrap();
    let other = StreamChannel::start(other, PeerFrame).unwrap();
    one.send_timeout(b"far", Some(Duration::MAX)).unwrap();
    assert_eq!(
        other.receive(Some(Duration::MAX)).unwrap(),
        Some(b"far".to_vec())
    );
    other.send(b"back").unwrap();
    assert_eq!(
        one.receive(Some(Duration::MAX)).unwrap(),
        Some(b"back".to_vec())
    );
}

#[test]
fn only_settings_above_their_defaults_are_listed_as_weaker() {
    let defaults = StreamChannel::builder(PeerFrame);
    assert!(defaults.weakened().is_empty());
    let tighter = StreamChannel::builder(PeerFrame)
        .max_message(1)
        .pending(1)
        .queued_bytes(1)
        .send_limit(Duration::from_millis(1))
        .read_buffer(1)
        .linger(Duration::ZERO);
    assert!(tighter.weakened().is_empty());
    let looser = StreamChannel::builder(PeerFrame)
        .max_message(StreamChannel::DEFAULT_MAX_MESSAGE + 1)
        .pending(StreamChannel::DEFAULT_PENDING + 1)
        .queued_bytes(StreamChannel::DEFAULT_QUEUED_BYTES + 1)
        .send_limit(StreamChannel::DEFAULT_SEND_LIMIT + Duration::from_secs(1))
        .read_buffer(StreamChannel::DEFAULT_READ_BUFFER + 1)
        .linger(StreamChannel::DEFAULT_LINGER + Duration::from_millis(1));
    assert_eq!(
        looser.weakened(),
        [
            "max_message",
            "pending",
            "queued_bytes",
            "send_limit",
            "read_buffer",
            "linger"
        ]
    );
}

#[test]
fn dropping_a_channel_closes_it_and_the_peer_reads_the_end() {
    let (one, other) = channels(PeerFrame);
    one.send(b"kept").unwrap();
    drop(one);
    assert_eq!(other.receive(WAIT).unwrap(), Some(b"kept".to_vec()));
    assert_eq!(other.receive(WAIT), Ok(None));
}
