//! A stream of your own as a message channel: any `Read` and `Write` become a `Channel` through
//! `StreamChannel`, here a reader and a writer that count every byte, around an in-memory pipe, with
//! an `Interrupt` that wakes the reader when the channel closes.
//!
//! `cargo run --example custom_stream`

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use coqui_transport::frame::LengthPrefix;
use coqui_transport::{Channel, Halves, Split, StreamChannel, memory};

/// Counts the bytes that pass through `inner`.
struct Counted<T> {
    inner: T,
    bytes: Arc<AtomicUsize>,
}

impl<T: Read> Read for Counted<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buf)?;
        self.bytes.fetch_add(count, Ordering::Relaxed);
        Ok(count)
    }
}

impl<T: Write> Write for Counted<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(buf)?;
        self.bytes.fetch_add(count, Ordering::Relaxed);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mine, theirs) = memory::pipe(16);
    let read = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(AtomicUsize::new(0));

    // Wrap the halves the pipe splits into, keeping its interrupt.
    let halves = mine.split()?;
    let mut counted = Halves::new(
        Counted {
            inner: halves.reader,
            bytes: Arc::clone(&read),
        },
        Counted {
            inner: halves.writer,
            bytes: Arc::clone(&written),
        },
    );
    if let Some(interrupt) = halves.interrupt {
        counted = counted.with_interrupt(interrupt);
    }
    let channel = StreamChannel::start(counted, LengthPrefix::u32_le())?;

    // The other end echoes.
    let echo = std::thread::spawn(move || -> io::Result<()> {
        let peer = StreamChannel::start(theirs, LengthPrefix::u32_le())?;
        while let Ok(Some(message)) = peer.receive(None) {
            if peer.send(&message).is_err() {
                break;
            }
        }
        Ok(())
    });

    // The pipe holds 16 bytes, so every frame crosses in partial writes and reads.
    channel.send(b"a message longer than the pipe holds")?;
    let echoed = channel.receive(Some(Duration::from_secs(5)))?;
    println!("{:?}", echoed.as_deref().map(String::from_utf8_lossy));
    channel.close();
    echo.join().map_err(|_| "the echo panicked")??;
    println!(
        "{} bytes written, {} read",
        written.load(Ordering::Relaxed),
        read.load(Ordering::Relaxed)
    );
    Ok(())
}
