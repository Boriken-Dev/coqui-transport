//! This process's standard input and output as one stream.
//!
//! [`current`] joins them into a [`Duplex`], which a [`StreamChannel`](crate::StreamChannel)
//! frames like any other stream, so a process spoken to over its standard streams has the same
//! [`Channel`](crate::Channel) as a socket. The framing is the two ends' agreement: a program that
//! does not speak it needs an adapter of its own.
//!
//! While standard output carries frames, nothing else in the process may write to it: a stray
//! `println!` lands between frames and breaks the stream for the peer. Diagnostics go to standard
//! error. Rust's standard streams pass bytes through unchanged, with no newline translation, so a
//! frame crosses a pipe or a file byte for byte on every platform. A Windows console is not a byte
//! stream: writing bytes that are not UTF-8 to one fails.
//!
//! Neither stream can be woken from another thread, so a channel on them has no
//! [`Interrupt`](crate::Interrupt): closing it writes what is queued and stops reading, while a read
//! already waiting on standard input returns only when input arrives or ends, and what it reads
//! then is discarded. The peer sees the end of the stream when this process exits or closes its
//! standard output.

use std::io::{Stdin, Stdout, stdin, stdout};

use crate::stream::Duplex;

/// This process's standard input, for reading, and standard output, for writing.
#[must_use]
pub fn current() -> Duplex<Stdin, Stdout> {
    Duplex::new(stdin(), stdout())
}
