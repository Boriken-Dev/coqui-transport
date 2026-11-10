//! A TCP stream as a message channel: a server and a client on loopback, each framing the
//! connection with the codec both ends agreed on, exchanging messages both ways.
//!
//! `cargo run --example tcp_channel`

use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::time::Duration;

use coqui_transport::frame::LengthPrefix;
use coqui_transport::{Channel, Failure, Listener, StreamChannel, StreamChannelBuilder, tcp};

const WAIT: Option<Duration> = Some(Duration::from_secs(5));

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let address = listener.local_addr()?;

    // The server answers each message with its length, until the client closes.
    let server = std::thread::spawn(move || -> Result<usize, Failure> {
        let stream = Listener::accept(&listener).map_err(|_| Failure::PathLost)?;
        let channel = StreamChannel::start(stream, LengthPrefix::u32_be())
            .map_err(|_| Failure::Unsupported)?;
        let mut answered = 0;
        while let Some(message) = channel.receive(None)? {
            channel.send(format!("{} bytes", message.len()).as_bytes())?;
            answered += 1;
        }
        Ok(answered)
    });

    let stream = tcp::Connector::new([address]).connect(Duration::from_secs(5))?;
    let channel = StreamChannelBuilder::new()
        .codec(LengthPrefix::u32_be())
        .max_message(64 * 1024)
        .stream(stream)
        .build()?;
    for message in [&b"hello"[..], &[0; 1000][..], b""] {
        channel.send(message)?;
        let answer = channel
            .receive(WAIT)?
            .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
        println!("{}", String::from_utf8_lossy(&answer));
    }
    println!("{:?}", channel.capabilities());
    channel.close();

    let answered = server.join().map_err(|_| "the server panicked")??;
    println!("the server answered {answered} messages");
    Ok(())
}
