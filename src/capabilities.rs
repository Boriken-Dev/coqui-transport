//! What a channel can do, as it stands when asked.

use crate::channel::Delivery;

/// What carries a channel's messages, which decides what one late message costs the others.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum Carrier {
    /// The channel does not say.
    #[default]
    Unknown,
    /// The transport's own messages, each delivered whole by it, as WebSocket messages are.
    Messages,
    /// Frames on one ordered byte stream: a message that is late holds up every message behind it,
    /// whatever it belongs to.
    Stream,
}

/// A channel's capabilities at the moment [`Channel::capabilities`](crate::Channel::capabilities)
/// was called.
///
/// Limits and datagrams can change while the channel is connected (a `set_max_` call, a path that
/// changes), so this is a snapshot: ask again rather than keep one. Changes of datagram delivery
/// can also be watched through [`Datagrams::path_changes`](crate::Datagrams::path_changes).
///
/// It describes the transport, never the peer: nothing here says who is at the other end.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct Capabilities {
    /// What carries the messages.
    pub carrier: Carrier,
    /// The largest message this side sends now, in bytes; `None` when the channel does not say.
    pub max_send: Option<usize>,
    /// The largest message this side accepts now, in bytes; `None` when the channel does not say.
    pub max_receive: Option<usize>,
    /// How the channel's datagrams travel now, or `None` when it has none.
    pub datagrams: Option<Delivery>,
    /// The largest datagram payload that fits now, in bytes; `None` when none can be sent.
    pub max_datagram: Option<usize>,
}

impl Capabilities {
    /// Nothing reported: an unknown carrier, no limits said, no datagrams.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            carrier: Carrier::Unknown,
            max_send: None,
            max_receive: None,
            datagrams: None,
            max_datagram: None,
        }
    }

    /// Sets what carries the messages.
    #[must_use]
    pub const fn carrier(mut self, carrier: Carrier) -> Self {
        self.carrier = carrier;
        self
    }

    /// Sets the largest message sent and the largest accepted.
    #[must_use]
    pub const fn limits(mut self, max_send: usize, max_receive: usize) -> Self {
        self.max_send = Some(max_send);
        self.max_receive = Some(max_receive);
        self
    }

    /// Sets how datagrams travel and the largest that fits, or no datagrams for `None`.
    #[must_use]
    pub const fn datagrams(mut self, datagrams: Option<(Delivery, Option<usize>)>) -> Self {
        match datagrams {
            Some((delivery, max_size)) => {
                self.datagrams = Some(delivery);
                self.max_datagram = max_size;
            }
            None => {
                self.datagrams = None;
                self.max_datagram = None;
            }
        }
        self
    }
}
