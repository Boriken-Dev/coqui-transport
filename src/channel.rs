//! The channel contract: reliable, ordered, whole messages, and the datagrams a channel may carry
//! beside them.

use alloc::vec::Vec;
use core::time::Duration;

use crate::capabilities::Capabilities;
use crate::failure::Failure;

/// A reliable, ordered channel of whole messages.
///
/// It says nothing about whether the remote application acted on a message, and it is not
/// authenticated: whoever opened it has to run the peer's identity proof on it before trusting what
/// arrives. Every method takes `&self`, so one thread can read while another writes.
pub trait Channel: Send + Sync {
    /// Queues one message, in order.
    ///
    /// # Errors
    ///
    /// [`Failure::Protocol`] for a message outside its bounds; otherwise why the channel ended.
    fn send(&self, message: &[u8]) -> Result<(), Failure>;

    /// The next message, or `None` when the peer closed in an orderly way.
    ///
    /// # Errors
    ///
    /// [`Failure::Timeout`] when nothing arrived within `timeout`, after which the channel is still
    /// usable; `None` waits without limit. Any other failure ended the channel.
    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure>;

    /// Ends the channel and releases its resources. Safe to call more than once, and after a failure.
    fn close(&self);

    /// Changes the largest message this channel takes from now on, both ways, so a server can hold an unproven peer
    /// to a small limit and raise it once the peer is proven. A channel with no limit of its own ignores it.
    fn set_max_message(&self, _bytes: usize) {}

    /// Changes the largest message this channel reads from now on, leaving what it sends as it was. A peer's answer to anything
    /// sent after this call meets it. A channel with no limit of its own ignores it.
    fn set_max_receive(&self, _bytes: usize) {}

    /// Stages a change to the largest message this channel reads, fixed by position: the next `messages` that `receive` has not yet
    /// returned are held to the bound in force now, and every one after them to `bytes`. A driver reading ahead judges each message
    /// by its position, so a peer cannot slip a larger one in early, and one the bound allows is not refused for arriving early.
    ///
    /// Positions count from what `receive` has returned when this is called. A message the driver had already read before this
    /// call keeps the judgement it had. A later call to this, [`set_max_receive`](Self::set_max_receive) or
    /// [`set_max_message`](Self::set_max_message) replaces a change that has not taken effect yet.
    ///
    /// # Errors
    ///
    /// [`Failure::Unsupported`] from a channel that cannot stage one, which then changes nothing: the default, for any `messages`
    /// but 0, which is [`set_max_receive`](Self::set_max_receive).
    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> {
        if messages == 0 {
            self.set_max_receive(bytes);
            return Ok(());
        }
        Err(Failure::Unsupported)
    }

    /// Changes how far this channel reads ahead of its reader: the driver holds at most `messages` that `receive` has not yet
    /// returned, and reads nothing more from the peer until the reader takes one, so the peer's sends wait. A channel starts at its
    /// driver's own setting. 0 is 1: a message is read before `receive` can return it.
    ///
    /// It governs only reading not yet done. A message already read stays, so a lower limit drops nothing and holds the driver
    /// until the reader is below it, and a higher one lets the driver read on at once. Each message is still judged by the bound
    /// for its position, staged or not, however far ahead it is read.
    ///
    /// # Errors
    ///
    /// [`Failure::Unsupported`] from a channel that cannot bound what it reads ahead, which then changes nothing: the default.
    fn set_pending(&self, _messages: usize) -> Result<(), Failure> {
        Err(Failure::Unsupported)
    }

    /// Changes the largest message this channel sends from now on, leaving what it reads as it was, so a side that reads under a
    /// small limit can still answer up to the peer's. A channel with no limit of its own ignores it.
    fn set_max_send(&self, _bytes: usize) {}

    /// Queues one message as [`send`](Self::send) does, but waits up to `timeout` for room behind what is already queued rather
    /// than refusing at once; `None` waits for as long as the channel lives. A channel with nothing to wait for sends at once.
    ///
    /// # Errors
    ///
    /// [`Failure::Timeout`] when there was no room in time, after which the channel is still usable and the message was not sent;
    /// otherwise as [`send`](Self::send).
    fn send_timeout(&self, message: &[u8], _timeout: Option<Duration>) -> Result<(), Failure> {
        self.send(message)
    }

    /// This channel's raw datagrams, or `None` if it has none, as a WebSocket has none unless both ends agreed. There is deliberately
    /// no datagram call on the channel itself, so a caller finds out here. [`Datagrams::delivery`] says what they then are.
    fn datagrams(&self) -> Option<&dyn Datagrams> {
        None
    }

    /// The certificate chain the peer presented and this side's TLS configuration accepted, as DER, the peer's own certificate first:
    /// the server's on a client, and on a server the client's, when its configuration asked for one. `None` on a channel with no TLS
    /// or when the peer presented none. What it proves is what that configuration's verifier checked.
    fn peer_certificates(&self) -> Option<&[Vec<u8>]> {
        None
    }

    /// What this channel can do now: what carries its messages, its limits as they stand, and its datagrams as they travel at this
    /// moment. Ask again after anything that can change them; see [`Capabilities`].
    ///
    /// The default reports only what [`datagrams`](Self::datagrams) says, and an unknown carrier and limits.
    fn capabilities(&self) -> Capabilities {
        Capabilities::new().datagrams(
            self.datagrams()
                .map(|datagrams| (datagrams.delivery(), datagrams.max_size())),
        )
    }

    /// Whether this side knows the channel has ended: closed here, or ended by a failure or by the peer. `false` promises nothing,
    /// since the channel can end at any moment, and a channel that does not track its end always answers `false`: the default.
    fn is_closed(&self) -> bool {
        false
    }
}

/// How a channel's datagrams actually travel, which decides what a caller can do with them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Delivery {
    /// On the network as datagrams: lost, reordered or repeated without notice, and never held up by anything lost before them.
    Unreliable,
    /// Inside the channel's own ordered stream: in order and late rather than lost on the way, so one lost packet holds up everything
    /// behind it. One is lost only where a queue at either end is full, which [`Datagrams::dropped`] counts.
    OverStream,
}

/// Raw datagrams beside a channel's messages: delivered whole or not at all, and with no promise of arriving.
///
/// They are outside any identity proof run on the channel's messages, so a peer proof says nothing about them and the transport's
/// own security (TLS, where it has it) is all that protects them. Anything that must know who sent a datagram has to establish that
/// itself. A channel wrapping another, such as one sealing its messages, has none of these unless it says so.
pub trait Datagrams: Send + Sync {
    /// The largest payload that fits one datagram now. It can change as the path does, and `None` means none can be sent, because
    /// the peer did not agree to them.
    fn max_size(&self) -> Option<usize>;

    /// Hands one datagram to the transport, which does not wait or report whether it arrived.
    ///
    /// # Errors
    ///
    /// [`Failure::Protocol`] for one larger than [`Self::max_size`], which leaves the channel usable;
    /// [`Failure::Unsupported`] when the peer does not take them; [`Failure::PathLost`] when the connection has ended.
    fn send(&self, payload: &[u8]) -> Result<(), Failure>;

    /// The next datagram, or `None` once the connection has ended.
    ///
    /// # Errors
    ///
    /// [`Failure::Timeout`] when none arrived within `timeout`; `None` waits without limit.
    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure>;

    /// How many this side discarded: arriving for a reader behind, or, for [`Delivery::OverStream`], waiting to be sent on a link
    /// that is behind. What the network lost cannot be seen from this side.
    fn dropped(&self) -> u64;

    /// How these travel now. On a channel whose path can change it changes during the connection: see [`Self::path_changes`].
    fn delivery(&self) -> Delivery;

    /// Each change of path from this call on, as the [`Delivery`] now in force, or `None` for a channel whose path never changes.
    /// [`Self::max_size`] follows the path, and a datagram over it is refused rather than moved to another one.
    fn path_changes(&self) -> Option<std::sync::mpsc::Receiver<Delivery>> {
        None
    }
}

impl Channel for alloc::boxed::Box<dyn Channel> {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        (**self).send(message)
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        (**self).receive(timeout)
    }

    fn close(&self) {
        (**self).close();
    }

    fn set_max_message(&self, bytes: usize) {
        (**self).set_max_message(bytes);
    }

    fn set_max_receive(&self, bytes: usize) {
        (**self).set_max_receive(bytes);
    }

    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> {
        (**self).set_max_receive_after(messages, bytes)
    }

    fn set_pending(&self, messages: usize) -> Result<(), Failure> {
        (**self).set_pending(messages)
    }

    fn set_max_send(&self, bytes: usize) {
        (**self).set_max_send(bytes);
    }

    fn send_timeout(&self, message: &[u8], timeout: Option<Duration>) -> Result<(), Failure> {
        (**self).send_timeout(message, timeout)
    }

    fn datagrams(&self) -> Option<&dyn Datagrams> {
        (**self).datagrams()
    }

    fn peer_certificates(&self) -> Option<&[Vec<u8>]> {
        (**self).peer_certificates()
    }

    fn capabilities(&self) -> Capabilities {
        (**self).capabilities()
    }

    fn is_closed(&self) -> bool {
        (**self).is_closed()
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::capabilities::Carrier;

    /// A driver of its own that stages a bound and limits its read-ahead, which the default would refuse.
    struct Staging;

    impl Channel for Staging {
        fn send(&self, _: &[u8]) -> Result<(), Failure> {
            Ok(())
        }
        fn receive(&self, _: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
            Ok(None)
        }
        fn close(&self) {}
        fn set_max_receive_after(&self, _: usize, _: usize) -> Result<(), Failure> {
            Ok(())
        }
        fn set_pending(&self, _: usize) -> Result<(), Failure> {
            Ok(())
        }
        fn is_closed(&self) -> bool {
            true
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::new().carrier(Carrier::Messages)
        }
    }

    /// A channel that only says what it reads.
    struct Plain(AtomicUsize);

    impl Channel for Plain {
        fn send(&self, _: &[u8]) -> Result<(), Failure> {
            Ok(())
        }
        fn receive(&self, _: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
            Ok(None)
        }
        fn close(&self) {}
        fn set_max_receive(&self, bytes: usize) {
            self.0.store(bytes, Ordering::SeqCst);
        }
    }

    struct Over;

    impl Datagrams for Over {
        fn max_size(&self) -> Option<usize> {
            Some(9)
        }
        fn send(&self, _: &[u8]) -> Result<(), Failure> {
            Ok(())
        }
        fn receive(&self, _: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
            Ok(None)
        }
        fn dropped(&self) -> u64 {
            0
        }
        fn delivery(&self) -> Delivery {
            Delivery::OverStream
        }
    }

    struct WithDatagrams(Over);

    impl Channel for WithDatagrams {
        fn send(&self, _: &[u8]) -> Result<(), Failure> {
            Ok(())
        }
        fn receive(&self, _: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
            Ok(None)
        }
        fn close(&self) {}
        fn datagrams(&self) -> Option<&dyn Datagrams> {
            Some(&self.0)
        }
    }

    #[test]
    fn a_channel_that_cannot_stage_a_bound_refuses_and_changes_nothing() {
        let plain = Plain(5.into());
        assert_eq!(plain.set_max_receive_after(1, 9), Err(Failure::Unsupported));
        assert_eq!(plain.0.load(Ordering::SeqCst), 5);
        // No message waits under the old bound: that is the bound itself.
        assert_eq!(plain.set_max_receive_after(0, 9), Ok(()));
        assert_eq!(plain.0.load(Ordering::SeqCst), 9);
    }

    #[test]
    fn a_boxed_channel_stages_through_its_driver() {
        let boxed: Box<dyn Channel> = Box::new(Staging);
        assert_eq!(Channel::set_max_receive_after(&boxed, 2, 7), Ok(()));
    }

    #[test]
    fn a_channel_that_cannot_bound_its_read_ahead_refuses_every_limit() {
        let plain = Plain(5.into());
        for messages in [0, 1, 64] {
            assert_eq!(plain.set_pending(messages), Err(Failure::Unsupported));
        }
        let boxed: Box<dyn Channel> = Box::new(plain);
        assert_eq!(Channel::set_pending(&boxed, 1), Err(Failure::Unsupported));
        let boxed: Box<dyn Channel> = Box::new(Staging);
        assert_eq!(Channel::set_pending(&boxed, 1), Ok(()));
    }

    #[test]
    fn the_default_capabilities_report_only_the_datagrams_and_never_a_known_end() {
        let plain = Plain(5.into());
        assert_eq!(plain.capabilities(), Capabilities::new());
        assert!(!plain.is_closed());
        let with = WithDatagrams(Over);
        let reported = with.capabilities();
        assert_eq!(reported.datagrams, Some(Delivery::OverStream));
        assert_eq!(reported.max_datagram, Some(9));
        assert_eq!(reported.carrier, Carrier::Unknown);
        assert_eq!(reported.max_send, None);
    }

    #[test]
    fn a_boxed_channel_reports_its_drivers_capabilities_and_end() {
        let boxed: Box<dyn Channel> = Box::new(Staging);
        assert_eq!(boxed.capabilities().carrier, Carrier::Messages);
        assert!(boxed.is_closed());
    }
}
