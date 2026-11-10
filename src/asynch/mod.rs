//! Async channels, connectors and listeners, on tokio (feature `tokio`).
//!
//! The same contracts as the blocking ones, for transports whose I/O is natively async: these
//! traits are implemented by async I/O itself, never by blocking calls moved onto the runtime's
//! threads. Every method returns a boxed future, so each trait is object-safe and a
//! `Box<dyn AsyncChannel>` works as `Box<dyn Channel>` does.
//!
//! No runtime is started or assumed: the futures run on whatever tokio runtime the caller polls them
//! from, which needs its I/O and time drivers enabled.

mod stream_channel;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::Pin;
use core::time::Duration;

pub use stream_channel::{
    StreamChannel, StreamChannelBuilder, StreamChannelParts, StreamChannelSettings,
};

/// The setters trait of the async settings type, for a builder in another crate that holds the
/// settings. A [`StreamChannelBuilder`] needs no import.
pub mod prelude {
    pub use super::stream_channel::StreamChannelSettingsSetters;
}

use crate::capabilities::Capabilities;
use crate::failure::Failure;

/// A future boxed for a trait object: `Send`, borrowing for `'a`.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A reliable, ordered channel of whole messages, as [`Channel`](crate::Channel) is, with async
/// calls.
///
/// It is not authenticated. Every method takes `&self`, so one task can read while another writes.
/// Dropping a returned future cancels the call; what that leaves is each implementation's to say,
/// and [`StreamChannel`]'s is that a cancelled receive loses nothing and a cancelled send never
/// leaves part of a frame on the wire.
pub trait AsyncChannel: Send + Sync {
    /// Sends one message, in order, waiting as long as the transport makes it.
    ///
    /// # Errors
    ///
    /// [`Failure::Protocol`] for a message outside its bounds; otherwise why the channel ended.
    fn send<'a>(&'a self, message: &'a [u8]) -> BoxFuture<'a, Result<(), Failure>>;

    /// Sends one message as [`send`](Self::send) does, but waits at most `timeout` for its turn
    /// behind sends already under way; `None` waits as long as the channel lives.
    ///
    /// # Errors
    ///
    /// [`Failure::Timeout`] when its turn did not come in time, after which the channel is still
    /// usable and the message was not sent; otherwise as [`send`](Self::send).
    fn send_timeout<'a>(
        &'a self,
        message: &'a [u8],
        timeout: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Failure>> {
        let _ = timeout;
        self.send(message)
    }

    /// The next message, or `None` when the peer closed in an orderly way.
    ///
    /// # Errors
    ///
    /// [`Failure::Timeout`] when nothing arrived within `timeout`, after which the channel is still
    /// usable; `None` waits without limit. Any other failure ended the channel.
    fn receive(&self, timeout: Option<Duration>)
    -> BoxFuture<'_, Result<Option<Vec<u8>>, Failure>>;

    /// Ends the channel: what the transport needs to finish sending, then its end. Safe to call more
    /// than once, and after a failure.
    fn close(&self) -> BoxFuture<'_, ()>;

    /// As [`Channel::set_max_message`](crate::Channel::set_max_message).
    fn set_max_message(&self, _bytes: usize) {}

    /// As [`Channel::set_max_receive`](crate::Channel::set_max_receive).
    fn set_max_receive(&self, _bytes: usize) {}

    /// As [`Channel::set_max_receive_after`](crate::Channel::set_max_receive_after).
    ///
    /// # Errors
    ///
    /// [`Failure::Unsupported`] from a channel that cannot stage one, which then changes nothing:
    /// the default, for any `messages` but 0, which is [`set_max_receive`](Self::set_max_receive).
    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> {
        if messages == 0 {
            self.set_max_receive(bytes);
            return Ok(());
        }
        Err(Failure::Unsupported)
    }

    /// As [`Channel::set_max_send`](crate::Channel::set_max_send).
    fn set_max_send(&self, _bytes: usize) {}

    /// As [`Channel::capabilities`](crate::Channel::capabilities). The default reports nothing.
    fn capabilities(&self) -> Capabilities {
        Capabilities::new()
    }

    /// As [`Channel::is_closed`](crate::Channel::is_closed): `true` once this side knows the
    /// channel ended, and `false` promising nothing. The default always answers `false`.
    fn is_closed(&self) -> bool {
        false
    }
}

/// Opens connections, as [`Connector`](crate::Connector) does, with an async call.
pub trait AsyncConnector {
    /// What a successful connect returns.
    type Connection: Send;
    /// Why a connect failed.
    type Error: core::error::Error + Send;

    /// Opens one connection, giving up after `timeout`.
    ///
    /// # Errors
    ///
    /// Why it could not open, as precisely as the transport can tell; a timeout among them.
    fn connect(&self, timeout: Duration) -> BoxFuture<'_, Result<Self::Connection, Self::Error>>;
}

/// Takes the connections others open to it, as [`Listener`](crate::Listener) does, with an async
/// call.
pub trait AsyncListener {
    /// What an accepted connection is.
    type Connection: Send;
    /// Why accepting failed.
    type Error: core::error::Error + Send;

    /// Waits for the next connection and returns it.
    ///
    /// # Errors
    ///
    /// Why no connection could be taken.
    fn accept(&self) -> BoxFuture<'_, Result<Self::Connection, Self::Error>>;
}

/// `work`, given up after `limit` with `None`; a limit too long for the clock is no limit.
pub(crate) async fn within<T>(limit: Duration, work: impl Future<Output = T>) -> Option<T> {
    match crate::deadline::after(limit) {
        None => Some(work.await),
        Some(_) => tokio::time::timeout(limit, work).await.ok(),
    }
}
