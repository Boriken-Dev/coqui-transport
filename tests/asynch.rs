//! The async channel over tokio streams: the same wires and bounds as the blocking one, and what a
//! cancelled call leaves behind.

mod support;

use std::sync::Arc;
use std::time::Duration;

use coqui_transport::asynch::{AsyncChannel, AsyncConnector, AsyncListener, StreamChannel};
use coqui_transport::frame::{Codec, PeerFrame, WebTransportFrame};
use coqui_transport::{Carrier, Duplex, Failure, tcp};
use serde_json::Value;
use support::{WAIT, body, cases, chunks, flag, hex, max, messages, name, vectors};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

const SHORT: Option<Duration> = Some(Duration::from_millis(50));

fn channels<C: Codec + Clone>(codec: C) -> (StreamChannel, StreamChannel) {
    let (one, other) = duplex(4096);
    (
        StreamChannel::new(one, codec.clone()),
        StreamChannel::new(other, codec),
    )
}

fn frame(message: &[u8]) -> Vec<u8> {
    PeerFrame.encode(message, usize::MAX).unwrap()
}

#[tokio::test]
async fn messages_cross_in_order_both_ways() {
    let (one, other) = channels(WebTransportFrame);
    for at in 0..50_u32 {
        one.send(&at.to_be_bytes()).await.unwrap();
        other.send(&(at + 1000).to_be_bytes()).await.unwrap();
    }
    for at in 0..50_u32 {
        assert_eq!(
            other.receive(WAIT).await.unwrap(),
            Some(at.to_be_bytes().to_vec())
        );
        assert_eq!(
            one.receive(WAIT).await.unwrap(),
            Some((at + 1000).to_be_bytes().to_vec())
        );
    }
}

#[tokio::test]
async fn a_boxed_channel_is_an_async_channel() {
    let (one, other) = channels(PeerFrame);
    let one: Box<dyn AsyncChannel> = Box::new(one);
    let other: Arc<dyn AsyncChannel> = Arc::new(other);
    one.send(b"").await.unwrap();
    one.send_timeout(b"dyn", WAIT).await.unwrap();
    assert_eq!(other.receive(WAIT).await.unwrap(), Some(Vec::new()));
    assert_eq!(other.receive(WAIT).await.unwrap(), Some(b"dyn".to_vec()));
    assert_eq!(one.capabilities().carrier, Carrier::Stream);
    one.close().await;
    assert!(one.is_closed());
    assert_eq!(other.receive(WAIT).await, Ok(None));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_carries_a_channel_whose_handles_are_send_across_awaits() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let stream = AsyncListener::accept(&listener).await.unwrap();
        let channel = StreamChannel::new(stream, PeerFrame);
        while let Ok(Some(message)) = channel.receive(WAIT).await {
            channel.send(&message.repeat(3)).await.unwrap();
        }
        channel.close().await;
    });
    // Taken from an await and used across others, on a runtime that may move it between threads.
    let client = tokio::spawn(async move {
        let stream =
            AsyncConnector::connect(&tcp::Connector::new([address]), Duration::from_secs(5))
                .await
                .unwrap();
        assert!(stream.nodelay().unwrap());
        let channel = StreamChannel::new(stream, PeerFrame);
        for message in [&b"ab"[..], &[5; 100_000][..]] {
            channel.send(message).await.unwrap();
            assert_eq!(
                channel.receive(WAIT).await.unwrap(),
                Some(message.repeat(3))
            );
        }
        channel.close().await;
    });
    client.await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn an_async_connector_refuses_a_zero_timeout_and_no_address() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let zero = AsyncConnector::connect(&tcp::Connector::new([address]), Duration::ZERO).await;
    assert_eq!(zero.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    let none = AsyncConnector::connect(&tcp::Connector::new([]), Duration::from_secs(1)).await;
    assert_eq!(none.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    AsyncConnector::connect(&tcp::Connector::new([address]), Duration::MAX)
        .await
        .unwrap();
}

async fn vectors_hold<C: Codec + Clone>(codec: C, vectors: &Value) {
    for case in cases(vectors, "encode") {
        let (mine, mut theirs) = duplex(1 << 16);
        let channel = StreamChannel::builder(codec.clone())
            .max_message(max(case))
            .build(mine);
        let sent = channel.send(&body(case)).await;
        channel.close().await;
        let mut wire = Vec::new();
        theirs.read_to_end(&mut wire).await.unwrap();
        if flag(case, "error") {
            assert_eq!(sent, Err(Failure::Protocol), "{}", name(case));
            assert!(wire.is_empty(), "{}", name(case));
        } else {
            assert_eq!(hex(&wire), case["hex"].as_str().unwrap(), "{}", name(case));
        }
    }
    for case in cases(vectors, "decode") {
        let (mine, mut theirs) = duplex(1 << 16);
        let channel = StreamChannel::builder(codec.clone())
            .max_message(max(case))
            .build(mine);
        for chunk in chunks(case) {
            theirs.write_all(&chunk).await.unwrap();
        }
        theirs.shutdown().await.unwrap();
        let mut got = Vec::new();
        let end = loop {
            match channel.receive(WAIT).await {
                Ok(Some(message)) => got.push(message),
                other => break other,
            }
        };
        assert_eq!(got, messages(case), "{}", name(case));
        let expected = if flag(case, "error") {
            Err(Failure::Protocol)
        } else if flag(case, "truncated") {
            Err(Failure::PathLost)
        } else {
            Ok(None)
        };
        assert_eq!(end, expected, "{}", name(case));
    }
}

#[tokio::test]
async fn the_vectors_hold_through_the_async_channel() {
    vectors_hold(PeerFrame, &vectors("peer_frame.json")).await;
    vectors_hold(WebTransportFrame, &vectors("webtransport_frame.json")).await;
}

#[tokio::test]
async fn a_receive_that_times_out_or_is_dropped_loses_nothing() {
    let (mine, mut theirs) = duplex(4096);
    let channel = StreamChannel::new(mine, PeerFrame);
    let bytes = frame(b"hello");
    theirs.write_all(&bytes[..2]).await.unwrap();
    assert_eq!(channel.receive(SHORT).await, Err(Failure::Timeout));
    theirs.write_all(&bytes[2..6]).await.unwrap();
    // Dropped while it waits for the rest of the frame.
    tokio::select! {
        received = channel.receive(None) => panic!("received {received:?} from half a frame"),
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    theirs.write_all(&bytes[6..]).await.unwrap();
    assert_eq!(
        channel.receive(WAIT).await.unwrap(),
        Some(b"hello".to_vec())
    );
}

#[tokio::test]
async fn a_send_dropped_after_its_first_byte_still_sends_its_frame_whole() {
    let (mine, mut theirs) = duplex(8);
    let channel = StreamChannel::new(mine, PeerFrame);
    let first = vec![1; 100];
    // The peer reads nothing yet, so the send stops after 8 bytes and is dropped there.
    let outcome = tokio::time::timeout(Duration::from_millis(50), channel.send(&first)).await;
    assert!(outcome.is_err());
    let reading = tokio::spawn(async move {
        let mut wire = vec![0; 2 * 104];
        theirs.read_exact(&mut wire).await.unwrap();
        wire
    });
    channel.send(&[2; 100]).await.unwrap();
    let mut expected = frame(&first);
    expected.extend(frame(&[2; 100]));
    assert_eq!(reading.await.unwrap(), expected);
}

#[tokio::test]
async fn a_send_that_never_got_its_turn_sent_nothing() {
    let (mine, mut theirs) = duplex(8);
    let channel = Arc::new(StreamChannel::new(mine, PeerFrame));
    let holding = {
        let channel = Arc::clone(&channel);
        tokio::spawn(async move { channel.send(&[1; 100]).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        channel.send_timeout(&[9; 100], SHORT).await,
        Err(Failure::Timeout)
    );
    let mut wire = vec![0; 104];
    theirs.read_exact(&mut wire).await.unwrap();
    holding.await.unwrap().unwrap();
    assert_eq!(wire, frame(&[1; 100]));
    channel.close().await;
    let mut rest = Vec::new();
    theirs.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty(), "the refused message reached the wire");
}

#[tokio::test]
async fn a_header_above_the_bound_ends_the_channel_both_ways() {
    let (mine, mut theirs) = duplex(4096);
    let channel = StreamChannel::builder(PeerFrame)
        .max_message(10)
        .build(mine);
    theirs.write_all(&frame(b"ok")).await.unwrap();
    theirs.write_all(&11_u32.to_be_bytes()).await.unwrap();
    assert_eq!(channel.receive(WAIT).await.unwrap(), Some(b"ok".to_vec()));
    assert_eq!(channel.receive(WAIT).await, Err(Failure::Protocol));
    assert_eq!(channel.receive(WAIT).await, Err(Failure::Protocol));
    assert_eq!(channel.send(b"x").await, Err(Failure::Protocol));
    assert!(channel.is_closed());
}

#[tokio::test]
async fn the_stream_ending_inside_a_frame_is_lost_and_between_frames_is_orderly() {
    let (mine, mut theirs) = duplex(4096);
    let channel = StreamChannel::new(mine, PeerFrame);
    theirs.write_all(&frame(b"hello")[..7]).await.unwrap();
    drop(theirs);
    assert_eq!(channel.receive(WAIT).await, Err(Failure::PathLost));
    assert_eq!(channel.receive(WAIT).await, Err(Failure::PathLost));

    let (mine, mut theirs) = duplex(4096);
    let channel = StreamChannel::new(mine, PeerFrame);
    theirs.write_all(&frame(b"last")).await.unwrap();
    drop(theirs);
    assert_eq!(channel.receive(WAIT).await.unwrap(), Some(b"last".to_vec()));
    assert_eq!(channel.receive(WAIT).await, Ok(None));
    assert_eq!(channel.receive(WAIT).await, Ok(None));
    assert!(channel.is_closed());
}

#[tokio::test]
async fn a_peer_that_takes_nothing_for_the_send_limit_ends_the_channel() {
    let (mine, _theirs) = duplex(8);
    let channel = StreamChannel::builder(PeerFrame)
        .send_limit(Duration::from_millis(100))
        .build(mine);
    assert_eq!(channel.send(&[0; 64]).await, Err(Failure::NoProgress));
    assert_eq!(channel.send(b"more").await, Err(Failure::NoProgress));
    assert_eq!(channel.receive(WAIT).await, Err(Failure::NoProgress));
    assert!(channel.is_closed());
}

#[tokio::test]
async fn closing_wakes_a_waiting_receive_ends_the_peers_stream_and_can_be_repeated() {
    let (one, other) = channels(PeerFrame);
    let one = Arc::new(one);
    let waiting = {
        let one = Arc::clone(&one);
        tokio::spawn(async move { one.receive(None).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    one.send(b"before").await.unwrap();
    one.close().await;
    assert_eq!(waiting.await.unwrap(), Ok(None));
    one.close().await;
    assert_eq!(one.send(b"late").await, Err(Failure::PathLost));
    assert_eq!(one.receive(WAIT).await, Ok(None));
    assert_eq!(other.receive(WAIT).await.unwrap(), Some(b"before".to_vec()));
    assert_eq!(other.receive(WAIT).await, Ok(None));
}

#[tokio::test]
async fn sends_and_receives_run_at_once_from_many_tasks() {
    let (one, other) = channels(PeerFrame);
    let one = Arc::new(one);
    let other = Arc::new(other);
    let mut senders = Vec::new();
    for sender in 0..4_u8 {
        let one = Arc::clone(&one);
        senders.push(tokio::spawn(async move {
            for at in 0..100_u16 {
                let mut message = vec![sender];
                message.extend(at.to_be_bytes());
                one.send(&message).await.unwrap();
            }
        }));
    }
    let answering = {
        let other = Arc::clone(&other);
        tokio::spawn(async move {
            for at in 0..100_u16 {
                other.send(&at.to_be_bytes()).await.unwrap();
            }
        })
    };
    let mut next = [0_u16; 4];
    for _ in 0..400 {
        let message = other.receive(WAIT).await.unwrap().unwrap();
        let sender = usize::from(message[0]);
        assert_eq!(u16::from_be_bytes([message[1], message[2]]), next[sender]);
        next[sender] += 1;
    }
    for at in 0..100_u16 {
        assert_eq!(
            one.receive(WAIT).await.unwrap(),
            Some(at.to_be_bytes().to_vec())
        );
    }
    for sender in senders {
        sender.await.unwrap();
    }
    answering.await.unwrap();
}

#[tokio::test]
async fn a_staged_bound_takes_effect_by_position_and_limits_are_reported() {
    let (one, other) = duplex(4096);
    let one = StreamChannel::builder(PeerFrame).max_message(10).build(one);
    let other = StreamChannel::new(other, PeerFrame);
    one.set_max_receive_after(1, 100).unwrap();
    other.send(&[1; 10]).await.unwrap();
    other.send(&[2; 50]).await.unwrap();
    assert_eq!(one.receive(WAIT).await.unwrap(), Some(vec![1; 10]));
    assert_eq!(one.receive(WAIT).await.unwrap(), Some(vec![2; 50]));
    assert_eq!(one.capabilities().max_receive, Some(100));
    one.set_max_send(3);
    assert_eq!(one.send(b"four").await, Err(Failure::Protocol));
    one.set_max_message(4);
    let now = one.capabilities();
    assert_eq!((now.max_send, now.max_receive), (Some(4), Some(4)));
    one.set_max_receive_after(0, 6).unwrap();
    assert_eq!(one.capabilities().max_receive, Some(6));
    one.send(b"four").await.unwrap();
    assert_eq!(other.receive(WAIT).await.unwrap(), Some(b"four".to_vec()));
}

#[tokio::test]
async fn a_duplex_of_two_async_halves_is_one_stream() {
    let (to_child, from_parent) = duplex(64);
    let (to_parent, from_child) = duplex(64);
    let mine = StreamChannel::new(Duplex::new(from_child, to_child), PeerFrame);
    let theirs = StreamChannel::new(Duplex::new(from_parent, to_parent), PeerFrame);
    mine.send(b"over two pipes").await.unwrap();
    assert_eq!(
        theirs.receive(WAIT).await.unwrap(),
        Some(b"over two pipes".to_vec())
    );
    theirs.send(b"and back").await.unwrap();
    assert_eq!(
        mine.receive(WAIT).await.unwrap(),
        Some(b"and back".to_vec())
    );
}

#[test]
fn only_settings_above_their_defaults_are_listed_as_weaker() {
    assert!(StreamChannel::builder(PeerFrame).weakened().is_empty());
    let looser = StreamChannel::builder(PeerFrame)
        .max_message(StreamChannel::DEFAULT_MAX_MESSAGE + 1)
        .send_limit(StreamChannel::DEFAULT_SEND_LIMIT + Duration::from_secs(1))
        .read_buffer(StreamChannel::DEFAULT_READ_BUFFER + 1);
    assert_eq!(
        looser.weakened(),
        ["max_message", "send_limit", "read_buffer"]
    );
}
