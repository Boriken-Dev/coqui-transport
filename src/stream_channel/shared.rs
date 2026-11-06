//! What a stream channel's caller threads, its reader thread and its writer thread share.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::time::Duration;
use std::io::{self, Read, Write};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use crate::failure::Failure;
use crate::frame::Decode;
use crate::stream::Interrupt;
use crate::sync::{locked, wait};

/// Bytes counted for each queued message beyond its frame: about what holding one in the queue
/// costs, so the bound holds for many small messages as for a few large ones.
pub(crate) const FRAME_COST: usize = 64;

/// The most bytes one write call is given.
const WRITE_CHUNK: usize = 64 * 1024;

/// How the reading direction ended.
#[derive(Clone, Copy, Debug)]
pub(crate) enum End {
    /// The stream ended between frames.
    Clean,
    Failed(Failure),
}

#[derive(Debug)]
pub(crate) struct Inbound {
    /// Read and not yet returned.
    pub(crate) messages: VecDeque<Vec<u8>>,
    pub(crate) end: Option<End>,
    /// Messages `receive` has returned: the position of the next one it returns.
    pub(crate) taken: u64,
    /// Messages read ahead of the reader, past which the reader thread reads nothing. At least 1.
    pub(crate) pending: usize,
    /// The bound in force for the next message read.
    pub(crate) receive_now: usize,
    /// From this position on, this bound.
    pub(crate) staged: Option<(u64, usize)>,
    /// `close` has been called.
    pub(crate) closed: bool,
    /// While closing: when bytes last arrived, which the reader discards until the peer's end.
    pub(crate) heard: Option<Instant>,
    /// Closing has stopped waiting for the peer's end: the reader stops discarding.
    pub(crate) linger_over: bool,
}

impl Inbound {
    /// The bound for the message at `position`. Positions only grow, so a staged change is made the
    /// bound in force once reached.
    fn bound_at(&mut self, position: u64) -> usize {
        if let Some((from, bytes)) = self.staged
            && position >= from
        {
            self.receive_now = bytes;
            self.staged = None;
        }
        self.receive_now
    }
}

#[derive(Debug, Default)]
pub(crate) struct Outbound {
    pub(crate) frames: VecDeque<Vec<u8>>,
    /// Queued and not yet wholly written, each frame counting [`FRAME_COST`] more.
    pub(crate) bytes: usize,
    pub(crate) max_send: usize,
    pub(crate) ended: Option<Failure>,
    pub(crate) closing: bool,
    /// The writer wrote everything queued before `close` and stopped.
    pub(crate) drained: bool,
    /// While a frame is being written: when the stream last took any of it.
    pub(crate) progress: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) inbound: Mutex<Inbound>,
    /// A message arrived, or the reading direction ended.
    pub(crate) arrived: Condvar,
    /// The reader took a message, a limit changed, or the channel ended.
    pub(crate) taken: Condvar,
    pub(crate) outbound: Mutex<Outbound>,
    /// A frame was queued, or the channel is closing or ended.
    pub(crate) queued: Condvar,
    /// Room was freed, the writer drained, or the channel ended.
    pub(crate) room: Condvar,
    pub(crate) interrupt: Option<Interrupt>,
    pub(crate) send_limit: Duration,
    pub(crate) queued_limit: usize,
    /// How long closing waits, after the peer last sent anything, for the peer's end.
    pub(crate) linger: Duration,
    /// Whether the reader thread and the writer thread have finished their work.
    pub(crate) done: Mutex<[bool; 2]>,
    /// A thread finished.
    pub(crate) exited: Condvar,
}

/// Which of the two threads.
pub(crate) const READER: usize = 0;
pub(crate) const WRITER: usize = 1;

/// Marks a thread finished when it returns, or unwinds.
pub(crate) struct Exit<'a> {
    pub(crate) shared: &'a Shared,
    pub(crate) thread: usize,
}

impl Drop for Exit<'_> {
    fn drop(&mut self) {
        locked(&self.shared.done)[self.thread] = true;
        self.shared.exited.notify_all();
    }
}

impl Shared {
    pub(crate) fn new(
        max_message: usize,
        pending: usize,
        queued_limit: usize,
        send_limit: Duration,
        linger: Duration,
        interrupt: Option<Interrupt>,
    ) -> Self {
        Self {
            inbound: Mutex::new(Inbound {
                messages: VecDeque::new(),
                end: None,
                taken: 0,
                pending: pending.max(1),
                receive_now: max_message,
                staged: None,
                closed: false,
                heard: None,
                linger_over: false,
            }),
            arrived: Condvar::new(),
            taken: Condvar::new(),
            outbound: Mutex::new(Outbound {
                max_send: max_message,
                ..Outbound::default()
            }),
            queued: Condvar::new(),
            room: Condvar::new(),
            interrupt,
            send_limit,
            queued_limit,
            linger,
            done: Mutex::new([false; 2]),
            exited: Condvar::new(),
        }
    }

    /// Ends the channel both ways with `failure`, keeping messages already read ahead of it, and ends
    /// the stream so the peer sees it. Nothing is recorded once `close` has begun ending the stream.
    pub(crate) fn fail(&self, failure: Failure) {
        {
            let mut inbound = locked(&self.inbound);
            if inbound.closed {
                return;
            }
            if inbound.end.is_none() {
                inbound.end = Some(End::Failed(failure));
            }
        }
        {
            let mut outbound = locked(&self.outbound);
            if outbound.ended.is_none() {
                outbound.ended = Some(failure);
            }
            outbound.frames.clear();
            outbound.bytes = 0;
        }
        self.wake_all();
        self.end_stream();
    }

    pub(crate) fn end_stream(&self) {
        if let Some(interrupt) = &self.interrupt {
            interrupt.interrupt();
        }
    }

    pub(crate) fn wake_all(&self) {
        self.arrived.notify_all();
        self.taken.notify_all();
        self.queued.notify_all();
        self.room.notify_all();
    }

    /// Whether the frame being written has made no progress for the send limit.
    pub(crate) fn stalled(&self, outbound: &Outbound) -> bool {
        outbound
            .progress
            .is_some_and(|since| since.elapsed() >= self.send_limit)
    }

    /// When the frame being written will have stalled, if one is being written and the clock can say.
    pub(crate) fn stall_deadline(&self, outbound: &Outbound) -> Option<Instant> {
        outbound
            .progress
            .and_then(|since| since.checked_add(self.send_limit))
    }

    /// Queues `frame` if it fits, saying whether it did.
    ///
    /// # Errors
    ///
    /// Why the channel no longer sends.
    pub(crate) fn try_queue(
        &self,
        outbound: &mut Outbound,
        frame: &mut Option<Vec<u8>>,
    ) -> Result<bool, Failure> {
        if let Some(failure) = outbound.ended {
            return Err(failure);
        }
        if outbound.closing {
            return Err(Failure::PathLost);
        }
        let cost = frame
            .as_ref()
            .map_or(0, Vec::len)
            .saturating_add(FRAME_COST);
        if outbound.bytes.saturating_add(cost) > self.queued_limit {
            return Ok(false);
        }
        if let Some(frame) = frame.take() {
            outbound.bytes += cost;
            outbound.frames.push_back(frame);
            self.queued.notify_all();
        }
        Ok(true)
    }

    /// Waits until the reader thread may read the message at `position`, and gives the bound it is
    /// held to; `None` once the channel has ended or closed.
    fn reserve(&self, position: u64) -> Option<usize> {
        let mut inbound = locked(&self.inbound);
        loop {
            if inbound.closed || inbound.end.is_some() {
                return None;
            }
            let held = position.saturating_sub(inbound.taken);
            if held < u64::try_from(inbound.pending).unwrap_or(u64::MAX) {
                return Some(inbound.bound_at(position));
            }
            inbound = wait(&self.taken, inbound);
        }
    }

    /// Hands a message to the readers, saying whether the channel still takes them.
    fn deliver(&self, message: Vec<u8>) -> bool {
        let mut inbound = locked(&self.inbound);
        if inbound.closed || inbound.end.is_some() {
            return false;
        }
        inbound.messages.push_back(message);
        drop(inbound);
        self.arrived.notify_all();
        true
    }

    /// The stream ended: in order between frames, or inside one, which tears it.
    fn end_reading(&self, between_frames: bool) {
        if !between_frames {
            self.fail(Failure::PathLost);
            return;
        }
        let mut inbound = locked(&self.inbound);
        if inbound.end.is_none() && !inbound.closed {
            inbound.end = Some(End::Clean);
        }
        drop(inbound);
        self.arrived.notify_all();
    }

    /// After `close`, reads and discards what the peer still sends until its end, so the stream is
    /// not ended with bytes unread: a socket closed then is reset, and the peer loses what this side
    /// sent last. `close` decides when to stop waiting.
    fn linger<R: Read>(&self, reader: &mut R, buffer: &mut [u8]) {
        {
            let inbound = locked(&self.inbound);
            if !inbound.closed || inbound.end.is_some() {
                return;
            }
        }
        loop {
            if locked(&self.inbound).linger_over {
                return;
            }
            match reader.read(buffer) {
                Ok(0) => return,
                Ok(_) => locked(&self.inbound).heard = Some(Instant::now()),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => return,
            }
        }
    }

    /// The reader thread: reads, decodes and holds messages for `receive`, never more than the
    /// pending limit ahead of it, judging each by the bound for its position.
    pub(crate) fn read_stream<R: Read, D: Decode>(
        &self,
        reader: &mut R,
        decoder: &mut D,
        chunk: usize,
    ) {
        let mut buffer = alloc::vec![0_u8; chunk.max(1)];
        let mut position: u64 = 0;
        loop {
            let Some(bound) = self.reserve(position) else {
                self.linger(reader, &mut buffer);
                return;
            };
            decoder.set_maximum(bound);
            match decoder.next_frame() {
                Ok(Some(message)) => {
                    if !self.deliver(message) {
                        self.linger(reader, &mut buffer);
                        return;
                    }
                    position += 1;
                    continue;
                }
                Ok(None) => {}
                Err(_) => {
                    self.fail(Failure::Protocol);
                    return;
                }
            }
            match reader.read(&mut buffer) {
                Ok(0) => {
                    self.end_reading(decoder.buffered() == 0);
                    return;
                }
                Ok(count) => decoder.push(&buffer[..count]),
                // A stream with a read timeout wakes here to look whether the channel closed.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => {
                    self.fail(Failure::PathLost);
                    return;
                }
            }
        }
    }

    /// The writer thread: writes queued frames in order, flushing when the queue empties, until the
    /// channel ends, or until it closes and everything queued before that was written.
    pub(crate) fn write_stream<W: Write>(&self, writer: &mut W) {
        loop {
            let frame = {
                let mut outbound = locked(&self.outbound);
                loop {
                    if outbound.ended.is_some() {
                        return;
                    }
                    if let Some(frame) = outbound.frames.pop_front() {
                        outbound.progress = Some(Instant::now());
                        drop(outbound);
                        // A send waiting for room learns when this frame would stall.
                        self.room.notify_all();
                        break frame;
                    }
                    if outbound.closing {
                        outbound.drained = true;
                        drop(outbound);
                        self.room.notify_all();
                        return;
                    }
                    outbound = wait(&self.queued, outbound);
                }
            };
            let written = self.write_frame(writer, &frame).and_then(|()| {
                if locked(&self.outbound).frames.is_empty() {
                    self.flush(writer)
                } else {
                    Ok(())
                }
            });
            if let Err(failure) = written {
                self.fail(failure);
                return;
            }
            {
                let mut outbound = locked(&self.outbound);
                outbound.bytes = outbound
                    .bytes
                    .saturating_sub(frame.len().saturating_add(FRAME_COST));
                outbound.progress = None;
            }
            self.room.notify_all();
        }
    }

    fn write_frame<W: Write>(&self, writer: &mut W, frame: &[u8]) -> Result<(), Failure> {
        let mut written = 0;
        while written < frame.len() {
            // A blocking send can hold one call until all it was given is queued, so each call is
            // given at most a chunk, and progress is seen between them.
            let end = frame.len().min(written.saturating_add(WRITE_CHUNK));
            match writer.write(&frame[written..end]) {
                Ok(0) => return Err(Failure::PathLost),
                Ok(count) => {
                    written += count;
                    locked(&self.outbound).progress = Some(Instant::now());
                }
                Err(error) => self.retry(&error)?,
            }
        }
        Ok(())
    }

    fn flush<W: Write>(&self, writer: &mut W) -> Result<(), Failure> {
        loop {
            match writer.flush() {
                Ok(()) => return Ok(()),
                Err(error) => self.retry(&error)?,
            }
        }
    }

    /// Whether a write that failed with `error` may be tried again: interrupted, or timed out on a
    /// stream with a write timeout while the send limit has not passed.
    fn retry(&self, error: &io::Error) -> Result<(), Failure> {
        match error.kind() {
            io::ErrorKind::Interrupted => Ok(()),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
                if self.stalled(&locked(&self.outbound)) {
                    Err(Failure::NoProgress)
                } else {
                    Ok(())
                }
            }
            _ => Err(Failure::PathLost),
        }
    }
}
