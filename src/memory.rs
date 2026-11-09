//! An in-memory byte stream between two ends of this process.
//!
//! [`pipe`] makes two connected [`Stream`]s, each holding at most `capacity` bytes the other has
//! not read: a write blocks while that is full and takes only what fits, and a read blocks until
//! something arrives and returns only what has, so partial reads and writes and backpressure behave
//! as on a socket. Dropping one end, or its writing half, ends the stream for the other.

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use std::io::{self, Read, Write};
use std::sync::{Condvar, Mutex};

use crate::stream::{Halves, Interrupt, Split};
use crate::sync::{locked, wait};

/// One direction: bytes written by one end for the other to read.
#[derive(Debug)]
struct Direction {
    state: Mutex<Buffer>,
    /// Bytes arrived, or the direction ended.
    readable: Condvar,
    /// Room freed, or the direction ended.
    writable: Condvar,
}

#[derive(Debug)]
struct Buffer {
    bytes: VecDeque<u8>,
    capacity: usize,
    /// The writing end is gone: a reader reads what is left, then the end of the stream.
    writer_gone: bool,
    /// The reading end is gone, or the direction was interrupted: a writer is refused.
    reader_gone: bool,
    /// Interrupted: a reader is refused at once.
    interrupted: bool,
}

impl Direction {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Buffer {
                bytes: VecDeque::new(),
                capacity: capacity.max(1),
                writer_gone: false,
                reader_gone: false,
                interrupted: false,
            }),
            readable: Condvar::new(),
            writable: Condvar::new(),
        })
    }

    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut state = locked(&self.state);
        loop {
            if state.interrupted {
                return Err(io::ErrorKind::ConnectionAborted.into());
            }
            if !state.bytes.is_empty() {
                let count = buf.len().min(state.bytes.len());
                for (slot, byte) in buf.iter_mut().zip(state.bytes.drain(..count)) {
                    *slot = byte;
                }
                drop(state);
                self.writable.notify_all();
                return Ok(count);
            }
            if state.writer_gone {
                return Ok(0);
            }
            state = wait(&self.readable, state);
        }
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut state = locked(&self.state);
        loop {
            if state.reader_gone || state.interrupted {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            if state.writer_gone {
                return Err(io::ErrorKind::NotConnected.into());
            }
            let room = state.capacity.saturating_sub(state.bytes.len());
            if room > 0 {
                let count = room.min(buf.len());
                state.bytes.extend(&buf[..count]);
                drop(state);
                self.readable.notify_all();
                return Ok(count);
            }
            state = wait(&self.writable, state);
        }
    }

    fn end_writing(&self) {
        locked(&self.state).writer_gone = true;
        self.readable.notify_all();
        self.writable.notify_all();
    }

    fn end_reading(&self) {
        locked(&self.state).reader_gone = true;
        self.readable.notify_all();
        self.writable.notify_all();
    }

    fn interrupt(&self) {
        locked(&self.state).interrupted = true;
        self.readable.notify_all();
        self.writable.notify_all();
    }
}

/// Two connected in-memory streams: what one writes, the other reads. Each direction holds at most
/// `capacity` unread bytes; 0 is 1.
#[must_use]
pub fn pipe(capacity: usize) -> (Stream, Stream) {
    let there = Direction::new(capacity);
    let back = Direction::new(capacity);
    (
        Stream {
            reader: ReadHalf(Arc::clone(&back)),
            writer: WriteHalf(Arc::clone(&there)),
        },
        Stream {
            reader: ReadHalf(there),
            writer: WriteHalf(back),
        },
    )
}

/// One end of a [`pipe`].
#[derive(Debug)]
pub struct Stream {
    reader: ReadHalf,
    writer: WriteHalf,
}

impl Stream {
    /// The two halves, for two threads. Unlike [`Split::split`] this gives no [`Interrupt`].
    pub fn into_halves(self) -> (ReadHalf, WriteHalf) {
        (self.reader, self.writer)
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Split for Stream {
    type Reader = ReadHalf;
    type Writer = WriteHalf;

    /// The halves, and an interrupt that ends both directions of this end: a blocked read fails with
    /// `ConnectionAborted`, a blocked write with `BrokenPipe`, and the other end reads the end of the
    /// stream once it has read what was written.
    fn split(self) -> io::Result<Halves<ReadHalf, WriteHalf>> {
        let incoming = Arc::clone(&self.reader.0);
        let outgoing = Arc::clone(&self.writer.0);
        Ok(
            Halves::new(self.reader, self.writer).interrupt(Interrupt::new(move || {
                incoming.interrupt();
                outgoing.end_writing();
            })),
        )
    }
}

/// The reading half of a [`Stream`]. Dropping it makes the other end's writes fail with
/// `BrokenPipe`.
#[derive(Debug)]
pub struct ReadHalf(Arc<Direction>);

impl Read for ReadHalf {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Drop for ReadHalf {
    fn drop(&mut self) {
        self.0.end_reading();
    }
}

/// The writing half of a [`Stream`]. Dropping it ends the stream for the other end, after what was
/// written.
#[derive(Debug)]
pub struct WriteHalf(Arc<Direction>);

impl Write for WriteHalf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for WriteHalf {
    fn drop(&mut self) {
        self.0.end_writing();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_takes_only_what_fits_and_a_read_returns_only_what_arrived() {
        let (mut one, mut other) = pipe(3);
        assert_eq!(one.write(b"hello").unwrap(), 3);
        let mut buf = [0_u8; 8];
        assert_eq!(other.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf[..3], b"hel");
        assert_eq!(one.write(b"lo").unwrap(), 2);
        assert_eq!(other.read(&mut buf[..1]).unwrap(), 1);
        assert_eq!(other.read(&mut buf).unwrap(), 1);
    }

    #[test]
    fn dropping_an_end_ends_the_stream_after_what_was_written() {
        let (mut one, mut other) = pipe(8);
        one.write_all(b"ab").unwrap();
        drop(one);
        let mut read = Vec::new();
        other.read_to_end(&mut read).unwrap();
        assert_eq!(read, b"ab");
        assert_eq!(
            other.write(b"x").unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn an_interrupt_wakes_a_blocked_read() {
        let (one, _other) = pipe(8);
        let halves = one.split().unwrap();
        let interrupt = halves.interrupt.clone().unwrap();
        let mut reader = halves.reader;
        let blocked = std::thread::spawn(move || reader.read(&mut [0_u8; 4]));
        std::thread::sleep(core::time::Duration::from_millis(20));
        interrupt.interrupt();
        let error = blocked.join().unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }
}
