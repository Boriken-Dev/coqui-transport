//! TCP: [`std::net::TcpStream`] as a splittable stream, a [`Connector`] to chosen addresses, and
//! [`std::net::TcpListener`] as a [`Listener`].
//!
//! A TCP stream is a byte stream, so a message channel over it is a
//! [`StreamChannel`](crate::StreamChannel) with the framing both ends use. With the `tokio`
//! feature, the same connector opens tokio streams and tokio's listener is an
//! [`AsyncListener`](crate::asynch::AsyncListener).

use alloc::vec::Vec;
use core::net::SocketAddr;
use core::time::Duration;
use std::io::{self, Write};
use std::net::{Shutdown, TcpListener, TcpStream};

use crate::connector::{Connector as ConnectorTrait, Listener};
use crate::deadline;
use crate::stream::{Halves, Interrupt, Split};

/// The read timeout a split socket's reading half gets when it has none. A Windows socket's
/// `shutdown` does not wake a read blocked on it from another thread, so the reader wakes this
/// often to see whether its channel closed.
pub const WAKE: Duration = Duration::from_millis(250);

impl Split for TcpStream {
    type Reader = Self;
    type Writer = WriteHalf;

    /// Two more handles on the socket: one reads, and the writing half shuts down writing when
    /// dropped. The interrupt shuts down both directions, which ends the connection for the peer
    /// and wakes a blocked read where the platform does. A socket with no read timeout is given
    /// [`WAKE`], which a [`StreamChannel`](crate::StreamChannel) reader retries through.
    fn split(self) -> io::Result<Halves<Self, WriteHalf>> {
        if self.read_timeout()?.is_none() {
            self.set_read_timeout(Some(WAKE))?;
        }
        let reader = self.try_clone()?;
        let control = self.try_clone()?;
        Ok(
            Halves::new(reader, WriteHalf(self)).interrupt(Interrupt::new(move || {
                let _ = control.shutdown(Shutdown::Both);
            })),
        )
    }
}

/// The writing half of a split [`TcpStream`]. Dropping it shuts down writing, so the peer reads
/// the end of the stream even while another handle on the socket is open.
#[derive(Debug)]
pub struct WriteHalf(TcpStream);

impl WriteHalf {
    /// The socket.
    #[must_use]
    pub const fn get_ref(&self) -> &TcpStream {
        &self.0
    }
}

impl Write for WriteHalf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.0.write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Drop for WriteHalf {
    fn drop(&mut self) {
        let _ = self.0.shutdown(Shutdown::Write);
    }
}

/// Opens TCP connections to the addresses it was given, trying each in order until one opens.
///
/// It resolves no names: resolving is I/O, and the caller's to do (`ToSocketAddrs`). Each stream
/// it opens has `TCP_NODELAY` set unless [`nodelay`](Self::nodelay) says otherwise, since a channel
/// writes each message whole and waits for nothing more.
#[derive(Clone, Debug)]
#[must_use]
pub struct Connector {
    addresses: Vec<SocketAddr>,
    nodelay: bool,
}

impl Connector {
    /// A connector to `addresses`, tried in order.
    pub fn new(addresses: impl IntoIterator<Item = SocketAddr>) -> Self {
        Self {
            addresses: addresses.into_iter().collect(),
            nodelay: true,
        }
    }

    /// Whether the streams it opens set `TCP_NODELAY`. Default `true`.
    pub const fn nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = nodelay;
        self
    }

    /// The addresses it tries, in order.
    #[must_use]
    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    /// Opens one connection, trying each address in order within one `timeout` for all of them.
    ///
    /// # Errors
    ///
    /// The last address's error; `TimedOut` once `timeout` has passed; `InvalidInput` with no
    /// address, or with a zero `timeout`.
    pub fn connect(&self, timeout: Duration) -> io::Result<TcpStream> {
        let until = deadline::after(timeout);
        let mut last = io::Error::new(io::ErrorKind::InvalidInput, "no address to connect to");
        for address in &self.addresses {
            let attempt = match until {
                // A timeout too long for the clock is no timeout.
                None => TcpStream::connect(address),
                Some(until) => match until.checked_duration_since(std::time::Instant::now()) {
                    Some(left) if !left.is_zero() => TcpStream::connect_timeout(address, left),
                    _ if timeout.is_zero() => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "a connect timeout of zero",
                        ));
                    }
                    _ => return Err(io::ErrorKind::TimedOut.into()),
                },
            };
            match attempt {
                Ok(stream) => {
                    stream.set_nodelay(self.nodelay)?;
                    return Ok(stream);
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }
}

impl ConnectorTrait for Connector {
    type Connection = TcpStream;
    type Error = io::Error;

    fn connect(&self, timeout: Duration) -> io::Result<TcpStream> {
        Self::connect(self, timeout)
    }
}

impl Listener for TcpListener {
    type Connection = TcpStream;
    type Error = io::Error;

    /// The next connection, as [`TcpListener::accept`]; its peer's address is on the stream.
    fn accept(&self) -> io::Result<TcpStream> {
        Self::accept(self).map(|(stream, _)| stream)
    }
}

#[cfg(feature = "tokio")]
mod asynch {
    use core::time::Duration;
    use std::io;

    use crate::asynch::{AsyncConnector, AsyncListener, BoxFuture};

    impl AsyncConnector for super::Connector {
        type Connection = tokio::net::TcpStream;
        type Error = io::Error;

        /// As the blocking [`connect`](super::Connector::connect), on tokio.
        fn connect(&self, timeout: Duration) -> BoxFuture<'_, io::Result<tokio::net::TcpStream>> {
            Box::pin(async move {
                if timeout.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "a connect timeout of zero",
                    ));
                }
                let attempts = async {
                    let mut last =
                        io::Error::new(io::ErrorKind::InvalidInput, "no address to connect to");
                    for address in &self.addresses {
                        match tokio::net::TcpStream::connect(address).await {
                            Ok(stream) => {
                                stream.set_nodelay(self.nodelay)?;
                                return Ok(stream);
                            }
                            Err(error) => last = error,
                        }
                    }
                    Err(last)
                };
                match crate::deadline::after(timeout) {
                    None => attempts.await,
                    Some(_) => tokio::time::timeout(timeout, attempts)
                        .await
                        .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into())),
                }
            })
        }
    }

    impl AsyncListener for tokio::net::TcpListener {
        type Connection = tokio::net::TcpStream;
        type Error = io::Error;

        /// The next connection, as tokio's `TcpListener::accept`.
        fn accept(&self) -> BoxFuture<'_, io::Result<tokio::net::TcpStream>> {
            Box::pin(async move { Self::accept(self).await.map(|(stream, _)| stream) })
        }
    }
}
