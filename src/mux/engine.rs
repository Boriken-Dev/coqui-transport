//! The multiplexer's shared state and its two threads.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec::Vec;
use core::time::Duration;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Instant;

use super::{Counters, DatagramDrop, Framing, MuxSettings};
use crate::channel::Channel;
use crate::deadline;
use crate::failure::Failure;
use crate::frame::versioned::{DecodeError, Frame, Kind, OVERHEAD, tagged};
use crate::sync::{locked, wait, wait_until};

/// How often the reader looks up from the carrier to see whether the multiplexer is closing.
const POLL: Duration = Duration::from_millis(250);
/// What a queued frame counts beyond its bytes.
const COST: usize = 64;

/// The settings the engine runs on, normalized.
#[derive(Clone, Copy)]
pub(super) struct Config {
    pub(super) framing: Framing,
    pub(super) channels: usize,
    pub(super) max_message: usize,
    pub(super) pending: usize,
    pub(super) queued_bytes: usize,
    pub(super) send_limit: Duration,
    pub(super) max_datagram: usize,
    pub(super) datagram_queue: usize,
    pub(super) datagram_pending: usize,
    pub(super) datagram_drop: DatagramDrop,
}

impl Config {
    pub(super) fn new(settings: MuxSettings) -> Self {
        Self {
            framing: settings.framing,
            channels: match settings.framing {
                Framing::Tagged => 1,
                _ => settings.channels.clamp(1, 1 << 16),
            },
            max_message: settings.max_message,
            pending: settings.pending.max(1),
            queued_bytes: settings.queued_bytes,
            send_limit: settings.send_limit,
            max_datagram: settings.max_datagram,
            datagram_queue: settings.datagram_queue.max(1),
            datagram_pending: settings.datagram_pending.max(1),
            datagram_drop: settings.datagram_drop,
        }
    }
}

/// One logical channel's state.
struct Slot {
    opened: bool,
    messages: VecDeque<Vec<u8>>,
    datagrams: VecDeque<Vec<u8>>,
    pending: usize,
    max_send: usize,
    max_receive: usize,
    send_sequence: u32,
    receive_sequence: u32,
    datagram_send: u32,
    datagram_receive: u32,
    /// Closed here, or failed: nothing more is sent.
    local_closed: bool,
    /// The peer's close arrived: nothing more arrives.
    peer_closed: bool,
    failed: Option<Failure>,
    dropped: u64,
}

impl Slot {
    fn new(config: &Config) -> Self {
        Self {
            opened: false,
            messages: VecDeque::new(),
            datagrams: VecDeque::new(),
            pending: config.pending,
            max_send: config.max_message,
            max_receive: config.max_message,
            send_sequence: 0,
            receive_sequence: 0,
            datagram_send: 0,
            datagram_receive: 0,
            local_closed: false,
            peer_closed: false,
            failed: None,
            dropped: 0,
        }
    }
}

struct State {
    config: Config,
    slots: BTreeMap<u16, Slot>,
    /// Frames of messages and closes, ready to write, in order.
    frames: VecDeque<Vec<u8>>,
    /// Datagrams waiting for the writer: the channel and the payload.
    datagrams: VecDeque<(u16, Vec<u8>)>,
    queued: usize,
    /// The writer's next pick is a datagram, so neither kind starves the other.
    datagram_turn: bool,
    /// This side is closing.
    closing: bool,
    /// The carrier ended: orderly (`Some(None)`) or by a failure.
    ended: Option<Option<Failure>>,
    reader_done: bool,
    writer_done: bool,
    corrupt: u64,
    lost: u64,
}

impl State {
    fn slot(&mut self, number: u16) -> &mut Slot {
        let config = self.config;
        self.slots
            .entry(number)
            .or_insert_with(|| Slot::new(&config))
    }

    /// Closes a logical channel from this side, writing the close frame in the versioned framing.
    fn close_slot(&mut self, number: u16) {
        let framing = self.config.framing;
        let slot = self.slot(number);
        if slot.local_closed {
            return;
        }
        slot.local_closed = true;
        if framing == Framing::Versioned {
            let frame = Frame::new(Kind::Close, number, slot.send_sequence, Vec::new()).encode();
            slot.send_sequence = slot.send_sequence.wrapping_add(1);
            self.queued = self.queued.saturating_add(frame.len() + COST);
            self.frames.push_back(frame);
        }
    }

    /// Ends one logical channel with a failure, telling the peer by a close.
    fn fail_slot(&mut self, number: u16, failure: Failure) {
        self.slot(number).failed = Some(failure);
        self.close_slot(number);
    }

    fn over(&self) -> bool {
        self.closing || self.ended.is_some()
    }
}

/// What the engine's two threads and the handles share.
pub(super) struct Engine {
    carrier: Box<dyn Channel>,
    state: Mutex<State>,
    /// Something arrived, or something ended: receivers wait here.
    inbox: Condvar,
    /// Room freed in a queue: the reader, when a channel is full, and senders wait here.
    space: Condvar,
    /// Something to write, or closing: the writer waits here.
    outbox: Condvar,
    /// A thread finished.
    exited: Condvar,
}

/// What a wait for a sender's room is bounded by: `None` does not wait, `Some(None)` waits for ever.
pub(super) type Wait = Option<Option<Instant>>;

impl Engine {
    pub(super) fn new(carrier: Box<dyn Channel>, config: Config) -> Self {
        Self {
            carrier,
            state: Mutex::new(State {
                config,
                slots: BTreeMap::new(),
                frames: VecDeque::new(),
                datagrams: VecDeque::new(),
                queued: 0,
                datagram_turn: false,
                closing: false,
                ended: None,
                reader_done: false,
                writer_done: false,
                corrupt: 0,
                lost: 0,
            }),
            inbox: Condvar::new(),
            space: Condvar::new(),
            outbox: Condvar::new(),
            exited: Condvar::new(),
        }
    }

    pub(super) fn config(&self) -> Config {
        locked(&self.state).config
    }

    /// Raises the carrier's own bounds to admit a frame holding the largest message, when it says
    /// what they are and they are lower. It never lowers one.
    pub(super) fn admit(&self, send: usize, receive: usize) {
        let capabilities = self.carrier.capabilities();
        let (send, receive) = (
            send.saturating_add(OVERHEAD),
            receive.saturating_add(OVERHEAD),
        );
        if capabilities.max_send.is_some_and(|bound| bound < send) {
            self.carrier.set_max_send(send);
        }
        if capabilities
            .max_receive
            .is_some_and(|bound| bound < receive)
        {
            self.carrier.set_max_receive(receive);
        }
    }

    fn wake_all(&self) {
        self.inbox.notify_all();
        self.space.notify_all();
        self.outbox.notify_all();
        self.exited.notify_all();
    }

    /// Takes a logical channel's number; `false` when it was taken before.
    pub(super) fn open(&self, number: u16) -> Result<(), super::OpenError> {
        let mut state = locked(&self.state);
        if usize::from(number) >= state.config.channels {
            return Err(super::OpenError::Number);
        }
        if state.over() {
            return Err(super::OpenError::Closed);
        }
        let slot = state.slot(number);
        if slot.opened || slot.local_closed {
            return Err(super::OpenError::InUse);
        }
        slot.opened = true;
        Ok(())
    }

    pub(super) fn counters(&self) -> Counters {
        let state = locked(&self.state);
        Counters {
            corrupt: state.corrupt,
            lost_datagrams: state.lost,
        }
    }

    // ----- the end -----

    /// The carrier failed, or a frame broke the wire: everything ends, and the carrier is closed.
    fn fail(&self, failure: Failure) {
        {
            let mut state = locked(&self.state);
            if state.ended.is_none() {
                state.ended = Some(Some(failure));
            }
            state.frames.clear();
            state.datagrams.clear();
            state.queued = 0;
        }
        self.wake_all();
        self.carrier.close();
    }

    /// The carrier ended in order.
    fn finish(&self) {
        {
            let mut state = locked(&self.state);
            if state.ended.is_none() {
                state.ended = Some(None);
            }
            state.frames.clear();
            state.datagrams.clear();
            state.queued = 0;
        }
        self.wake_all();
        self.carrier.close();
    }

    /// Closes everything from this side: every logical channel, what is queued written, the carrier
    /// closed, both threads joined by the caller afterwards.
    pub(super) fn shutdown(&self) {
        {
            let mut state = locked(&self.state);
            state.closing = true;
            let numbers: Vec<u16> = state
                .slots
                .iter()
                .filter(|(_, slot)| slot.opened)
                .map(|(number, _)| *number)
                .collect();
            for number in numbers {
                state.close_slot(number);
            }
        }
        self.wake_all();
        let until = deadline::after(self.config().send_limit);
        let mut state = locked(&self.state);
        while !state.writer_done && !deadline::passed(until) {
            state = wait_until(&self.exited, state, until);
        }
        drop(state);
        self.carrier.close();
        self.wake_all();
    }

    /// Waits for a thread to finish, for at most the send limit.
    pub(super) fn wait_exit(&self, reader: bool) -> bool {
        let until = deadline::after(self.config().send_limit);
        let mut state = locked(&self.state);
        while !(if reader {
            state.reader_done
        } else {
            state.writer_done
        }) && !deadline::passed(until)
        {
            state = wait_until(&self.exited, state, until);
        }
        if reader {
            state.reader_done
        } else {
            state.writer_done
        }
    }

    fn mark_done(&self, reader: bool) {
        {
            let mut state = locked(&self.state);
            if reader {
                state.reader_done = true;
            } else {
                state.writer_done = true;
            }
        }
        self.exited.notify_all();
    }

    // ----- the reader -----

    pub(super) fn read_loop(&self) {
        self.read_frames();
        self.mark_done(true);
    }

    fn read_frames(&self) {
        loop {
            if locked(&self.state).closing {
                return;
            }
            match self.carrier.receive(Some(POLL)) {
                Ok(Some(bytes)) => {
                    if let Err(failure) = self.accept(&bytes) {
                        self.fail(failure);
                        return;
                    }
                }
                Ok(None) => {
                    self.finish();
                    return;
                }
                Err(Failure::Timeout) => {}
                Err(failure) => {
                    self.fail(failure);
                    return;
                }
            }
        }
    }

    /// Takes one message of the carrier. An error is a breach of the wire that ends everything.
    fn accept(&self, bytes: &[u8]) -> Result<(), Failure> {
        let (kind, number, sequence, payload) = match self.config().framing {
            Framing::Tagged => match bytes.split_first() {
                Some((&tagged::MESSAGE, rest)) => (Kind::Message, 0, None, rest.to_vec()),
                Some((&tagged::DATAGRAM, rest)) => (Kind::Datagram, 0, None, rest.to_vec()),
                _ => return Err(Failure::Protocol),
            },
            _ => match Frame::decode(bytes) {
                Ok(frame) => (
                    frame.kind,
                    frame.channel,
                    Some(frame.sequence),
                    frame.payload,
                ),
                Err(DecodeError::Checksum) => {
                    locked(&self.state).corrupt += 1;
                    return Ok(());
                }
                Err(_) => return Err(Failure::Protocol),
            },
        };
        let mut state = locked(&self.state);
        if usize::from(number) >= state.config.channels {
            return Err(Failure::Protocol);
        }
        match kind {
            Kind::Message => self.accept_message(state, number, sequence, payload),
            Kind::Close => {
                let slot = state.slot(number);
                let expected = slot.receive_sequence;
                if sequence.is_some_and(|sequence| sequence != expected) {
                    state.fail_slot(number, Failure::Protocol);
                } else {
                    slot.receive_sequence = expected.wrapping_add(1);
                    slot.peer_closed = true;
                }
                drop(state);
                self.wake_all();
            }
            Kind::Datagram => {
                let config = state.config;
                let mut lost = 0;
                if let Some(sequence) = sequence {
                    let slot = state.slot(number);
                    let gap = sequence.wrapping_sub(slot.datagram_receive);
                    slot.datagram_receive = sequence.wrapping_add(1);
                    if gap < 1 << 31 {
                        lost = u64::from(gap);
                    }
                }
                state.lost += lost;
                let slot = state.slot(number);
                if slot.local_closed || slot.peer_closed || slot.failed.is_some() {
                    return Ok(());
                }
                if payload.len() > config.max_datagram {
                    slot.dropped += 1;
                } else if slot.datagrams.len() >= config.datagram_pending {
                    slot.dropped += 1;
                    if config.datagram_drop == DatagramDrop::Oldest {
                        slot.datagrams.pop_front();
                        slot.datagrams.push_back(payload);
                    }
                } else {
                    slot.datagrams.push_back(payload);
                }
                drop(state);
                self.inbox.notify_all();
            }
        }
        Ok(())
    }

    /// One reliable message: checked against the channel's sequence and bound, then held for
    /// `receive`. A channel whose queue is full holds the reader until its reader takes one, which
    /// holds back every channel on the carrier: head-of-line blocking.
    fn accept_message<'a>(
        &'a self,
        mut state: MutexGuard<'a, State>,
        number: u16,
        sequence: Option<u32>,
        payload: Vec<u8>,
    ) {
        let slot = state.slot(number);
        if slot.local_closed || slot.peer_closed || slot.failed.is_some() {
            return;
        }
        if sequence.is_some_and(|sequence| sequence != slot.receive_sequence)
            || payload.len() > slot.max_receive
        {
            state.fail_slot(number, Failure::Protocol);
            drop(state);
            self.wake_all();
            return;
        }
        slot.receive_sequence = slot.receive_sequence.wrapping_add(1);
        loop {
            if state.over() {
                return;
            }
            let slot = state.slot(number);
            if slot.local_closed || slot.failed.is_some() {
                return;
            }
            if slot.messages.len() < slot.pending {
                slot.messages.push_back(payload);
                drop(state);
                self.inbox.notify_all();
                return;
            }
            state = wait(&self.space, state);
        }
    }

    // ----- the writer -----

    pub(super) fn write_loop(&self) {
        self.write_frames();
        self.mark_done(false);
    }

    fn write_frames(&self) {
        let limit = self.config().send_limit;
        loop {
            let frame = {
                let mut state = locked(&self.state);
                loop {
                    if state.ended.is_some() {
                        return;
                    }
                    if let Some(frame) = self.next_frame(&mut state) {
                        break frame;
                    }
                    if state.closing {
                        return;
                    }
                    state = wait(&self.outbox, state);
                }
            };
            self.space.notify_all();
            match self.carrier.send_timeout(&frame, Some(limit)) {
                Ok(()) => {}
                Err(Failure::Timeout) => {
                    self.fail(Failure::NoProgress);
                    return;
                }
                Err(failure) => {
                    self.fail(failure);
                    return;
                }
            }
        }
    }

    /// The next frame to write: messages and datagrams in turn, so neither starves the other.
    fn next_frame(&self, state: &mut State) -> Option<Vec<u8>> {
        for _ in 0..2 {
            if state.datagram_turn {
                state.datagram_turn = false;
                while let Some((number, payload)) = state.datagrams.pop_front() {
                    let slot = state.slot(number);
                    if slot.local_closed || slot.failed.is_some() {
                        continue;
                    }
                    let sequence = slot.datagram_send;
                    slot.datagram_send = sequence.wrapping_add(1);
                    return Some(match state.config.framing {
                        Framing::Tagged => [&[tagged::DATAGRAM][..], &payload].concat(),
                        _ => Frame::new(Kind::Datagram, number, sequence, payload).encode(),
                    });
                }
            } else {
                state.datagram_turn = true;
                if let Some(frame) = state.frames.pop_front() {
                    state.queued = state.queued.saturating_sub(frame.len() + COST);
                    return Some(frame);
                }
            }
        }
        None
    }

    // ----- what handles do -----

    pub(super) fn send(&self, number: u16, message: &[u8], wait_for: Wait) -> Result<(), Failure> {
        let mut state = locked(&self.state);
        let config = state.config;
        let cost = message.len() + OVERHEAD + COST;
        loop {
            if let Some(failure) = Self::refused(&mut state, number) {
                return Err(failure);
            }
            if message.len() > state.slot(number).max_send {
                return Err(Failure::Protocol);
            }
            // A message that can never fit beside the budget is sent alone, behind an empty queue.
            if state.queued == 0 || state.queued.saturating_add(cost) <= config.queued_bytes {
                let slot = state.slot(number);
                let sequence = slot.send_sequence;
                slot.send_sequence = sequence.wrapping_add(1);
                let frame = match config.framing {
                    Framing::Tagged => [&[tagged::MESSAGE][..], message].concat(),
                    _ => Frame::new(Kind::Message, number, sequence, message).encode(),
                };
                state.queued = state.queued.saturating_add(frame.len() + COST);
                state.frames.push_back(frame);
                drop(state);
                self.outbox.notify_all();
                return Ok(());
            }
            let Some(until) = wait_for else {
                return Err(Failure::NoProgress);
            };
            if deadline::passed(until) {
                return Err(Failure::Timeout);
            }
            state = wait_until(&self.space, state, until);
        }
    }

    /// Why a send on this channel is refused, or `None`.
    fn refused(state: &mut State, number: u16) -> Option<Failure> {
        let ended = state.ended;
        let closing = state.closing;
        let slot = state.slot(number);
        if let Some(failure) = slot.failed {
            return Some(failure);
        }
        if slot.local_closed || slot.peer_closed || closing {
            return Some(Failure::PathLost);
        }
        match ended {
            Some(Some(failure)) => Some(failure),
            Some(None) => Some(Failure::PathLost),
            None => None,
        }
    }

    pub(super) fn receive(
        &self,
        number: u16,
        timeout: Option<Duration>,
    ) -> Result<Option<Vec<u8>>, Failure> {
        let until = deadline::within(timeout);
        let mut state = locked(&self.state);
        loop {
            let slot = state.slot(number);
            if let Some(message) = slot.messages.pop_front() {
                drop(state);
                self.space.notify_all();
                return Ok(Some(message));
            }
            if let Some(failure) = slot.failed {
                return Err(failure);
            }
            if slot.local_closed || slot.peer_closed || state.closing {
                return Ok(None);
            }
            match state.ended {
                Some(Some(failure)) => return Err(failure),
                Some(None) => return Ok(None),
                None => {}
            }
            if deadline::passed(until) {
                return Err(Failure::Timeout);
            }
            state = wait_until(&self.inbox, state, until);
        }
    }

    pub(super) fn close_channel(&self, number: u16) {
        {
            let mut state = locked(&self.state);
            state.close_slot(number);
        }
        self.wake_all();
    }

    pub(super) fn is_closed(&self, number: u16) -> bool {
        let mut state = locked(&self.state);
        let over = state.over();
        let slot = state.slot(number);
        over || slot.local_closed || slot.peer_closed || slot.failed.is_some()
    }

    pub(super) fn limits(&self, number: u16) -> (usize, usize) {
        let mut state = locked(&self.state);
        let slot = state.slot(number);
        (slot.max_send, slot.max_receive)
    }

    pub(super) fn set_limits(&self, number: u16, send: Option<usize>, receive: Option<usize>) {
        let mut state = locked(&self.state);
        let slot = state.slot(number);
        if let Some(bytes) = send {
            slot.max_send = bytes;
        }
        if let Some(bytes) = receive {
            slot.max_receive = bytes;
        }
        let (send, receive) = (slot.max_send, slot.max_receive);
        drop(state);
        self.admit(send, receive);
    }

    pub(super) fn set_pending(&self, number: u16, messages: usize) {
        locked(&self.state).slot(number).pending = messages.max(1);
        self.space.notify_all();
    }

    // ----- datagrams -----

    /// Queues a datagram without waiting: when the queue is full the configured one is dropped.
    pub(super) fn send_datagram(&self, number: u16, payload: &[u8]) -> Result<(), Failure> {
        let mut state = locked(&self.state);
        let config = state.config;
        if payload.len() > config.max_datagram {
            return Err(Failure::Protocol);
        }
        let ended = state.ended;
        let closing = state.closing;
        let slot = state.slot(number);
        if slot.local_closed || slot.peer_closed || closing {
            return Err(Failure::PathLost);
        }
        match ended {
            Some(Some(failure)) => return Err(failure),
            Some(None) => return Err(Failure::PathLost),
            None => {}
        }
        if state.datagrams.len() >= config.datagram_queue {
            match config.datagram_drop {
                DatagramDrop::Newest => {
                    state.slot(number).dropped += 1;
                    return Ok(());
                }
                _ => {
                    if let Some((oldest, _)) = state.datagrams.pop_front() {
                        state.slot(oldest).dropped += 1;
                    }
                }
            }
        }
        state.datagrams.push_back((number, payload.to_vec()));
        drop(state);
        self.outbox.notify_all();
        Ok(())
    }

    pub(super) fn receive_datagram(
        &self,
        number: u16,
        timeout: Option<Duration>,
    ) -> Result<Option<Vec<u8>>, Failure> {
        let until = deadline::within(timeout);
        let mut state = locked(&self.state);
        loop {
            let slot = state.slot(number);
            if let Some(payload) = slot.datagrams.pop_front() {
                return Ok(Some(payload));
            }
            if slot.local_closed || slot.peer_closed || slot.failed.is_some() || state.closing {
                return Ok(None);
            }
            match state.ended {
                Some(Some(failure)) => return Err(failure),
                Some(None) => return Ok(None),
                None => {}
            }
            if deadline::passed(until) {
                return Err(Failure::Timeout);
            }
            state = wait_until(&self.inbox, state, until);
        }
    }

    pub(super) fn dropped(&self, number: u16) -> u64 {
        locked(&self.state).slot(number).dropped
    }
}
