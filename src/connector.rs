//! Establishing a connection, apart from using one.
//!
//! A [`Connector`] opens a connection to one place it was configured for; a [`Listener`] takes the
//! connections others open to it. Neither is part of a [`Channel`](crate::Channel): an established
//! channel never connects or listens, and a transport implements only what it has. A TCP client
//! connects, a named-pipe server listens, a serial port is opened, and this process's standard
//! input and output are already there, needing neither.

use core::time::Duration;

/// Opens connections to the place it was configured for.
///
/// What it returns is the transport's own: a byte stream to frame with
/// [`StreamChannel`](crate::StreamChannel), or a channel when the transport carries messages
/// natively. A connection is not an authenticated peer.
pub trait Connector {
    /// What a successful connect returns.
    type Connection;
    /// Why a connect failed.
    type Error: core::error::Error;

    /// Opens one connection, giving up after `timeout`.
    ///
    /// # Errors
    ///
    /// Why it could not open, as precisely as the transport can tell; a timeout among them.
    fn connect(&self, timeout: Duration) -> Result<Self::Connection, Self::Error>;
}

/// Takes the connections others open to it.
pub trait Listener {
    /// What an accepted connection is.
    type Connection;
    /// Why accepting failed.
    type Error: core::error::Error;

    /// Waits for the next connection and returns it. A listener with admission rules applies them
    /// before returning.
    ///
    /// # Errors
    ///
    /// Why no connection could be taken. Whether the listener can go on after one is the
    /// transport's to say.
    fn accept(&self) -> Result<Self::Connection, Self::Error>;
}
