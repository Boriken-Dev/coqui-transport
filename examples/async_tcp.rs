//! A tokio TCP stream as an async message channel, through the object-safe `AsyncChannel`.
//!
//! `cargo run --example async_tcp --features tokio`

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use coqui_transport::asynch::{AsyncChannel, AsyncConnector, AsyncListener, StreamChannel};
use coqui_transport::frame::LengthPrefix;
use coqui_transport::tcp;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener =
        tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
    let address = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let stream = AsyncListener::accept(&listener).await?;
        let channel: Box<dyn AsyncChannel> =
            Box::new(StreamChannel::new(stream, LengthPrefix::u32_be()));
        while let Ok(Some(message)) = channel.receive(None).await {
            let mut answer = b"echo: ".to_vec();
            answer.extend_from_slice(&message);
            if channel.send(&answer).await.is_err() {
                break;
            }
        }
        channel.close().await;
        Ok::<(), std::io::Error>(())
    });

    let stream =
        AsyncConnector::connect(&tcp::Connector::new([address]), Duration::from_secs(5)).await?;
    let channel = StreamChannel::new(stream, LengthPrefix::u32_be());
    channel.send(b"hello").await?;
    let answer = channel.receive(Some(Duration::from_secs(5))).await?;
    println!("{:?}", answer.as_deref().map(String::from_utf8_lossy));
    channel.close().await;
    server.await??;
    Ok(())
}
