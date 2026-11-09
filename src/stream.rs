//! Byte streams: `std::io::Read` and `Write` (and tokio's `AsyncRead` and `AsyncWrite`), joined or
//! split.

use alloc::sync::Arc;
use core::fmt;
use std::io::{self, IoSlice, IoSliceMut, Read, Write};

/// A reader and a writer used as one bidirectional stream: reads go to `R`, writes to `W`.
///
/// For transports whose two directions are separate handles: this process's standard input and
/// output, a child's pipes, two ends of two pipes. With the `tokio` feature it is also an
/// `AsyncRead` and `AsyncWrite` when its halves are.
#[derive(Clone, Debug, Default)]
pub struct Duplex<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> Duplex<R, W> {
    /// Joins `reader` and `writer`.
    pub const fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    /// The reading half.
    pub const fn reader(&self) -> &R {
        &self.reader
    }

    /// The writing half.
    pub const fn writer(&self) -> &W {
        &self.writer
    }

    /// The reading half, mutably.
    pub const fn reader_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// The writing half, mutably.
    pub const fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// The two halves, reader first.
    pub fn into_parts(self) -> (R, W) {
        (self.reader, self.writer)
    }
}

impl<R: Read, W> Read for Duplex<R, W> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.reader.read_vectored(bufs)
    }
}

impl<R, W: Write> Write for Duplex<R, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.writer.write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Ends both directions of a split stream from any thread, waking a read or a write blocked on it:
/// a socket's `shutdown`, a pipe's close.
///
/// It may be called more than once, and after the stream has already ended.
#[derive(Clone)]
pub struct Interrupt(Arc<dyn Fn() + Send + Sync>);

impl Interrupt {
    /// An interrupt that calls `end`.
    pub fn new(end: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Arc::new(end))
    }

    /// Ends the stream.
    pub fn interrupt(&self) {
        (self.0)();
    }
}

impl fmt::Debug for Interrupt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Interrupt")
    }
}

/// A stream's two halves, each for a thread of its own, and how to end both.
///
/// Dropping the writer ends the write direction, so the peer reads the end of the stream: a
/// socket's write half shuts down its writing, a pipe's is closed. Without an [`Interrupt`], a read
/// blocked on the reader cannot be woken by this side; it returns only when the stream yields
/// bytes or ends.
#[derive(Debug)]
#[non_exhaustive]
pub struct Halves<R, W> {
    /// Reads the stream.
    pub reader: R,
    /// Writes the stream.
    pub writer: W,
    /// Ends both directions, if the stream has a way to.
    pub interrupt: Option<Interrupt>,
}

impl<R, W> Halves<R, W> {
    /// `reader` and `writer`, with no interrupt.
    pub const fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            interrupt: None,
        }
    }

    /// Adds the way to end both directions.
    #[must_use]
    pub fn interrupt(mut self, interrupt: Interrupt) -> Self {
        self.interrupt = Some(interrupt);
        self
    }
}

/// A byte stream that can be split into a reading half and a writing half for two threads.
///
/// [`StreamChannel`](crate::StreamChannel) reads on one thread and writes on another, so it takes
/// anything that splits. A transport's stream implements it (TCP's does, by cloning its socket); a
/// stream made of separate handles is a [`Duplex`] or [`Halves`], which split as they are.
pub trait Split {
    /// The reading half.
    type Reader: Read + Send + 'static;
    /// The writing half. Dropping it ends the write direction.
    type Writer: Write + Send + 'static;

    /// Splits the stream.
    ///
    /// # Errors
    ///
    /// When the operating system refuses a second handle on it.
    fn split(self) -> io::Result<Halves<Self::Reader, Self::Writer>>;
}

impl<R, W> Split for Halves<R, W>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    type Reader = R;
    type Writer = W;

    fn split(self) -> io::Result<Self> {
        Ok(self)
    }
}

impl<R, W> Split for Duplex<R, W>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    type Reader = R;
    type Writer = W;

    fn split(self) -> io::Result<Halves<R, W>> {
        Ok(Halves::new(self.reader, self.writer))
    }
}

impl<R: Read + Send + 'static, W: Write + Send + 'static> From<Duplex<R, W>> for Halves<R, W> {
    fn from(duplex: Duplex<R, W>) -> Self {
        Self::new(duplex.reader, duplex.writer)
    }
}

#[cfg(feature = "tokio")]
mod asynch {
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use std::io;

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use super::Duplex;

    impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for Duplex<R, W> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
        }
    }

    impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for Duplex<R, W> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().writer).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
        }
    }
}
