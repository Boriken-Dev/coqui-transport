//! TCP: the connector, `TcpListener` as a listener, and `StreamChannel` on real sockets.

mod support;

use std::io::{self, Read};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use coqui_transport::frame::{Codec, LengthPrefix};
use coqui_transport::{
    Channel, Connector, Failure, Listener, Split, StreamChannel, StreamChannelBuilder,
    StreamChannelSettingsSetters, tcp,
};
use support::WAIT;

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

/// An address on loopback that nothing listens on: a port just released.
fn refusing() -> SocketAddr {
    let listener = TcpListener::bind(loopback()).unwrap();
    listener.local_addr().unwrap()
}

#[test]
fn a_connector_and_a_listener_carry_a_channel_both_ways() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let stream = Listener::accept(&listener).unwrap();
        let channel = StreamChannel::start(stream, LengthPrefix::u32_le()).unwrap();
        while let Ok(Some(message)) = channel.receive(WAIT) {
            channel.send(&message.repeat(2)).unwrap();
        }
    });
    let stream =
        Connector::connect(&tcp::Connector::new([address]), Duration::from_secs(5)).unwrap();
    let channel = StreamChannel::start(stream, LengthPrefix::u32_le()).unwrap();
    for message in [&b"ab"[..], &[7; 100_000][..]] {
        channel.send(message).unwrap();
        assert_eq!(channel.receive(WAIT).unwrap(), Some(message.repeat(2)));
    }
    channel.close();
    server.join().unwrap();
}

#[test]
fn closing_a_channel_ends_the_stream_for_the_peer_after_what_was_sent() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let client =
        StreamChannel::start(TcpStream::connect(address).unwrap(), LengthPrefix::u32_be()).unwrap();
    let (stream, _) = listener.accept().unwrap();
    let server = StreamChannel::start(stream, LengthPrefix::u32_be()).unwrap();
    for at in 0..50_u8 {
        client.send(&[at; 1000]).unwrap();
    }
    client.close();
    for at in 0..50_u8 {
        assert_eq!(server.receive(WAIT).unwrap(), Some(vec![at; 1000]));
    }
    assert_eq!(server.receive(WAIT), Ok(None));
}

#[test]
fn closing_while_the_peer_still_sends_loses_nothing_this_side_sent() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let client = StreamChannelBuilder::new()
        .codec(LengthPrefix::u32_be())
        .pending(1usize)
        .stream(TcpStream::connect(address).unwrap())
        .build()
        .unwrap();
    let (server, _) = listener.accept().unwrap();
    // The peer sends far more than the channel reads before it closes.
    let flooding = {
        let mut server = server.try_clone().unwrap();
        thread::spawn(move || {
            let frame = LengthPrefix::u32_be()
                .encode(&[1; 1000], usize::MAX)
                .unwrap();
            for _ in 0..2000 {
                if io::Write::write_all(&mut server, &frame).is_err() {
                    return;
                }
            }
        })
    };
    thread::sleep(Duration::from_millis(200));
    let sent = vec![7; 200_000];
    client.send(&sent).unwrap();
    client.close();
    let mut wire = Vec::new();
    let read = (&server).read_to_end(&mut wire);
    assert!(
        read.is_ok(),
        "the peer's read ended with {read:?} after {} bytes",
        wire.len()
    );
    assert_eq!(
        wire,
        LengthPrefix::u32_be().encode(&sent, usize::MAX).unwrap()
    );
    flooding.join().unwrap();
}

#[test]
fn closing_wakes_the_reader_blocked_on_the_socket() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let client =
        StreamChannel::start(TcpStream::connect(address).unwrap(), LengthPrefix::u32_be()).unwrap();
    // The peer is open and silent, so the reader thread waits in a read.
    let (_server, _) = listener.accept().unwrap();
    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();
    client.close();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(client.receive(WAIT), Ok(None));
}

#[test]
fn a_split_socket_ends_its_writing_when_the_writing_half_is_dropped() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let halves = TcpStream::connect(address).unwrap().split().unwrap();
    let (mut server, _) = listener.accept().unwrap();
    drop(halves.writer);
    let mut read = Vec::new();
    server.read_to_end(&mut read).unwrap();
    assert!(read.is_empty());
    // The interrupt ends both directions, which a blocked read sees.
    let mut reader = halves.reader;
    let interrupt = halves.interrupt.unwrap();
    let blocked = thread::spawn(move || reader.read(&mut [0; 8]));
    thread::sleep(Duration::from_millis(50));
    interrupt.interrupt();
    interrupt.interrupt();
    assert!(matches!(blocked.join().unwrap(), Ok(0) | Err(_)));
}

#[test]
fn a_connector_tries_each_address_in_order() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let connector = tcp::Connector::new([refusing(), address]);
    assert_eq!(connector.addresses().len(), 2);
    let stream = connector.connect(Duration::from_secs(5)).unwrap();
    assert_eq!(stream.peer_addr().unwrap(), address);
    assert!(stream.nodelay().unwrap());
    let stream = connector
        .clone()
        .nodelay(false)
        .connect(Duration::from_secs(5))
        .unwrap();
    assert!(!stream.nodelay().unwrap());
}

#[test]
fn a_connector_refuses_no_address_and_a_zero_timeout_and_reports_a_refusal() {
    let none = tcp::Connector::new([]);
    assert_eq!(
        none.connect(Duration::from_secs(1)).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    assert_eq!(
        tcp::Connector::new([address])
            .connect(Duration::ZERO)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(
        tcp::Connector::new([refusing()])
            .connect(Duration::from_secs(5))
            .is_err()
    );
    // A timeout too long for the clock is no timeout.
    tcp::Connector::new([address])
        .connect(Duration::MAX)
        .unwrap();
}

#[test]
fn a_reset_connection_is_a_lost_path_for_the_channel() {
    let listener = TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let client =
        StreamChannel::start(TcpStream::connect(address).unwrap(), LengthPrefix::u32_be()).unwrap();
    let (server, _) = listener.accept().unwrap();
    // Half a frame, then the connection ends.
    io::Write::write_all(&mut &server, &[0, 0, 0, 9, 1, 2]).unwrap();
    server.shutdown(Shutdown::Both).unwrap();
    assert_eq!(client.receive(WAIT), Err(Failure::PathLost));
}
