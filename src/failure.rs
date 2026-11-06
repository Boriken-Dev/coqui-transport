//! What can go wrong on a channel.

use core::fmt;

/// A failure a driver can distinguish.
///
/// A transport's generic error (a browser's, an operating system's) is not proof of an MTU problem
/// or a certificate rejection, so a driver reports only what it knows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Failure {
    /// The transport is not available here.
    Unsupported,
    /// A deadline passed before the operation finished.
    Timeout,
    /// The network path was lost.
    PathLost,
    /// A live channel stopped making forward progress.
    NoProgress,
    /// A probe suggests a path that drops large packets; never proven by a browser.
    SuspectedMtu,
    /// A certificate or pin was positively rejected.
    Certificate,
    /// The peer failed its identity proof.
    PeerProof,
    /// The peer broke the wire format, or a message was outside its bounds.
    Protocol,
    /// The peer or a policy refused.
    Permission,
    /// The caller cancelled.
    Cancelled,
}

impl Failure {
    /// Whether this says something about the network path rather than about the peer.
    #[must_use]
    pub const fn is_path(self) -> bool {
        matches!(
            self,
            Self::Unsupported
                | Self::Timeout
                | Self::PathLost
                | Self::NoProgress
                | Self::SuspectedMtu
        )
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unsupported => "the transport is not available",
            Self::Timeout => "a deadline passed",
            Self::PathLost => "the network path was lost",
            Self::NoProgress => "the channel stopped making progress",
            Self::SuspectedMtu => "the path may be dropping large packets",
            Self::Certificate => "a certificate or pin was rejected",
            Self::PeerProof => "the peer failed its identity proof",
            Self::Protocol => "the peer broke the wire format",
            Self::Permission => "the operation was refused",
            Self::Cancelled => "the operation was cancelled",
        })
    }
}

impl core::error::Error for Failure {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_failures_of_the_path_are_path_failures() {
        let path = [
            Failure::Unsupported,
            Failure::Timeout,
            Failure::PathLost,
            Failure::NoProgress,
            Failure::SuspectedMtu,
        ];
        let peer = [
            Failure::Certificate,
            Failure::PeerProof,
            Failure::Protocol,
            Failure::Permission,
            Failure::Cancelled,
        ];
        assert!(path.iter().all(|failure| failure.is_path()));
        assert!(peer.iter().all(|failure| !failure.is_path()));
    }
}
