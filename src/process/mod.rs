//! A child process with piped standard streams, as a stream.
//!
//! [`Process::start`] runs a [`Launch`] with its standard input and output piped and its standard
//! error drained on a thread of its own, into a bounded tail ([`Diagnostics`]) that is never mixed
//! into the protocol stream. A [`Process`] [splits](crate::Split) into the child's output (read)
//! and input (write), so a [`StreamChannel`](crate::StreamChannel) frames messages over it like
//! over any other stream:
//!
//! ```no_run
//! use coqui_transport::frame::LengthPrefix;
//! use coqui_transport::process::{Launch, Process};
//! use coqui_transport::{Channel, StreamChannel};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let process = Process::start(Launch::new("some-agent").arg("--stdio"))?;
//! let control = process.control();
//! let channel = StreamChannel::start(process, LengthPrefix::u32_be())?;
//! // ... send and receive ...
//! channel.close();
//! let exit = control.end(std::time::Duration::from_secs(5))?;
//! # let _ = exit;
//! # Ok(()) }
//! ```
//!
//! **The whole tree.** On Windows the child and everything it starts are in a job object of their
//! own; on Unix the child leads a process group, and on Linux and Android [`Control::kill`] also
//! ends every descendant that left the group or session. [`Control::kill`] ends them all, and so
//! does dropping the last handle ([`Process`], [`Control`], or the channel made from it). A
//! Windows job also ends with this process, however it ends. A Unix child tree is left running if
//! this process is killed without running destructors; the children then read the end of their
//! standard input, which is how a well-behaved child learns its parent is gone.
//!
//! **Closing in order.** Closing the channel drops the child's input, so it reads the end and can
//! finish what it was doing; the channel then waits for the child's output to end, for as long as
//! the channel's `linger` (default 500 ms) of silence, and then calls the stream's interrupt,
//! which ends the child's whole tree and wakes the channel's blocked threads. Set the channel's
//! `linger` to the time a child may need to shut down. [`Control::end`] is the same for a child
//! used without a channel.
//!
//! **Bytes.** The pipes are binary on every platform: nothing translates a newline, stops at
//! Ctrl-Z or requires UTF-8. A *child* that writes through a C runtime's text-mode standard output
//! on Windows translates newlines itself; this crate cannot change that from outside, and such a
//! program needs `_setmode(_fileno(stdout), _O_BINARY)` or a binary-safe runtime.
//!
//! **Diagnostics.** The child's standard error is a pipe of its own, never merged into standard
//! output. [`ErrorOutput`] says whether it is captured (the default), inherited or discarded.

mod launch;
mod tree;

use alloc::collections::VecDeque;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::io::{self, Read};
use std::process::{Child, ChildStdin, ChildStdout, Stdio};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::Instant;

use coqui_builder::{Builder, Describe, Leaf, NonDefault};

pub use self::launch::Launch;
use self::tree::Poll;
use crate::connector::Connector as ConnectorTrait;
use crate::stream::{Halves, Interrupt, Split};
use crate::sync::locked;

/// How a process ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Exit {
    /// Its exit code, when it ended by itself.
    pub code: Option<i32>,
    /// The signal that ended it, on Unix.
    pub signal: Option<i32>,
}

impl Exit {
    pub(crate) const fn new(code: Option<i32>, signal: Option<i32>) -> Self {
        Self { code, signal }
    }

    /// It ended by itself with code 0.
    #[must_use]
    pub const fn success(&self) -> bool {
        matches!(self.code, Some(0))
    }
}

impl fmt::Display for Exit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.code, self.signal) {
            (Some(code), _) => write!(f, "exited with code {code}"),
            (None, Some(signal)) => write!(f, "was ended by signal {signal}"),
            (None, None) => f.write_str("exited"),
        }
    }
}

/// Where a started process's standard error goes. Never into its standard output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Leaf)]
#[map(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ErrorOutput {
    /// A pipe this crate drains on a thread of its own into a bounded tail ([`Diagnostics`]). A
    /// child that writes to standard error is never held up by a reader that is not there.
    Capture,
    /// This process's own standard error.
    Inherit,
    /// Nowhere.
    Discard,
}

impl Process {
    /// The bytes of standard error kept by default: 64 KiB.
    pub const DEFAULT_STDERR_TAIL: usize = 64 * 1024;
}

/// What a [`ProcessBuilder`] takes whole through `fill`, in the form a file, a form or a peer
/// fills.
///
/// Every limit is a setting with a finite default. Every key is optional (an absent one is its
/// default) and an unknown key is refused. The type is `coqui_builder::Describe`: a `Partial` fills
/// it, and its setters are methods of [`ProcessBuilder`]. It is `#[non_exhaustive]`: start from
/// [`Default`] and assign the fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Describe)]
#[map(deny_unknown_fields)]
#[builder(setters_on = ProcessBuilder)]
#[non_exhaustive]
pub struct ProcessSettings {
    /// Where standard error goes. Default `capture`.
    #[map(default = ProcessSettings::new().stderr)]
    pub stderr: ErrorOutput,
    /// The most recent bytes of standard error kept, when it is captured; older bytes are dropped
    /// and counted. 0 keeps none and counts all. Default [`Process::DEFAULT_STDERR_TAIL`].
    #[map(default = ProcessSettings::new().stderr_tail)]
    pub stderr_tail: usize,
    /// Whether a console process gets a console window of its own on Windows. Off by default: a
    /// graphical program starting a console child opens no window. No effect elsewhere.
    #[map(default = ProcessSettings::new().console)]
    pub console: bool,
}

impl ProcessSettings {
    /// Every setting at its default.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stderr: ErrorOutput::Capture,
            stderr_tail: Process::DEFAULT_STDERR_TAIL,
            console: false,
        }
    }

    /// Every setting that differs from its default, named by key path.
    #[must_use]
    pub fn non_defaults(&self) -> Vec<NonDefault> {
        coqui_builder::non_defaults(self)
    }
}

impl Default for ProcessSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// What a [`ProcessBuilder`] holds: the settings and the [`Launch`]. Build with
/// `ProcessBuilder::new()`, set the limits through its setters (or `fill`), give `launch`, then
/// `build`, which starts the process. The builder's `non_defaults()` lists the settings that
/// differ from their defaults.
#[derive(Builder, Debug)]
#[builder(build = Process, fallible, entry = "new")]
pub struct ProcessParts {
    settings: ProcessSettings,
    #[builder(input)]
    launch: Launch,
}

impl TryFrom<ProcessParts> for Process {
    type Error = io::Error;

    fn try_from(parts: ProcessParts) -> io::Result<Self> {
        Self::spawn(&parts.launch, parts.settings)
    }
}

/// A started process with its standard input and output not yet taken.
///
/// [`Split`] it into a channel's stream, or take a [`Control`] first to end it, wait for it and
/// read its [`Diagnostics`] afterwards. Dropping the last handle ends the whole tree.
#[derive(Debug)]
#[must_use]
pub struct Process {
    control: Control,
    stdin: ChildStdin,
    stdout: ChildStdout,
}

impl Process {
    /// Starts `launch` with the default settings.
    ///
    /// # Errors
    ///
    /// The process could not be started: the program was not found or could not run, or its
    /// tree could not be contained.
    pub fn start(launch: Launch) -> io::Result<Self> {
        Self::spawn(&launch, ProcessSettings::new())
    }

    fn spawn(launch: &Launch, settings: ProcessSettings) -> io::Result<Self> {
        let mut command = launch.command();
        command.stdin(Stdio::piped()).stdout(Stdio::piped());
        command.stderr(match settings.stderr {
            ErrorOutput::Capture => Stdio::piped(),
            ErrorOutput::Inherit => Stdio::inherit(),
            ErrorOutput::Discard => Stdio::null(),
        });
        tree::prepare(&mut command, settings.console);
        let mut child = command.spawn()?;
        // The pipes' child ends are the child's now: readers see the end once it and its tree
        // close theirs.
        drop(command);
        let abandon = |child: &mut Child| {
            let _ = child.kill();
            let _ = child.wait();
        };
        let tree = match tree::contain(&child) {
            Ok(tree) => tree,
            Err(error) => {
                abandon(&mut child);
                return Err(error);
            }
        };
        let (stdin, stdout, stderr) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take());
        let (Some(stdin), Some(stdout)) = (stdin, stdout) else {
            tree::end(&tree, &child);
            abandon(&mut child);
            return Err(io::Error::other("the process's pipes were not opened"));
        };
        let capture = Arc::new(Capture::new(settings.stderr_tail));
        match stderr {
            Some(stderr) => {
                let draining = Arc::clone(&capture);
                let started = thread::Builder::new()
                    .name("coqui-transport stderr".into())
                    .spawn(move || draining.drain(stderr));
                if let Err(error) = started {
                    tree::end(&tree, &child);
                    abandon(&mut child);
                    return Err(error);
                }
            }
            None => capture.ended.store(true, Ordering::SeqCst),
        }
        let id = child.id();
        Ok(Self {
            control: Control {
                inner: Arc::new(Inner {
                    id,
                    state: Mutex::new(State {
                        child,
                        collected: None,
                        tree,
                    }),
                    capture,
                }),
            },
            stdin,
            stdout,
        })
    }

    /// The operating system's id of the process.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.control.id()
    }

    /// A handle that outlives the streams: to end the process, wait for it and read its
    /// [`Diagnostics`]. The process and its tree end when the last handle is dropped.
    pub fn control(&self) -> Control {
        self.control.clone()
    }

    /// The process's standard error so far; see [`Control::diagnostics`].
    #[must_use]
    pub fn diagnostics(&self) -> Diagnostics {
        self.control.diagnostics()
    }
}

impl Split for Process {
    type Reader = ChildStdout;
    type Writer = ChildStdin;

    /// The child's output to read and its input to write; dropping the writing half is the end of
    /// the child's input. The interrupt ends the whole tree ([`Control::kill`]), which closes the
    /// pipes and so wakes a blocked read or write on either half.
    fn split(self) -> io::Result<Halves<ChildStdout, ChildStdin>> {
        let control = self.control;
        Ok(
            Halves::new(self.stdout, self.stdin).interrupt(Interrupt::new(move || {
                let _ = control.kill();
            })),
        )
    }
}

/// A handle on a started process, to end it, wait for it and read its standard error. Clones share
/// the process; it ends with its tree when the last handle goes.
#[derive(Clone, Debug)]
pub struct Control {
    inner: Arc<Inner>,
}

struct Inner {
    id: u32,
    state: Mutex<State>,
    capture: Arc<Capture>,
}

struct State {
    child: Child,
    /// How it ended, once it has been collected: from then on its id, and its group's, may be
    /// another process's, so nothing is signalled again.
    collected: Option<Exit>,
    tree: tree::Tree,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Process")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Drop for Inner {
    /// A process nobody holds ends with everything it started.
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

impl Inner {
    fn kill(&self) -> io::Result<()> {
        let mut state = locked(&self.state);
        if state.collected.is_none() {
            // Closing the job ends the child with code 0, which reads as success: end the child
            // itself first so its status says it was killed.
            #[cfg(windows)]
            let _ = state.child.kill();
            tree::end(&state.tree, &state.child);
            match state.child.kill() {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => {}
                Err(error) => return Err(error),
            }
            let status = state.child.wait()?;
            state.collected = Some(tree::exit_of(status));
        } else {
            // A job still holds whatever the collected process left behind; a Unix group of a
            // collected process may be another's now.
            #[cfg(windows)]
            tree::end(&state.tree, &state.child);
        }
        Ok(())
    }

    fn try_wait(&self) -> io::Result<Option<Exit>> {
        let mut state = locked(&self.state);
        if let Some(exit) = state.collected {
            return Ok(Some(exit));
        }
        match tree::poll(&mut state.child)? {
            Poll::Running => Ok(None),
            Poll::Exited(exit) => Ok(Some(exit)),
            Poll::Collected(exit) => {
                state.collected = Some(exit);
                Ok(Some(exit))
            }
        }
    }
}

/// How often a wait looks at a process that has not ended.
const POLL: Duration = Duration::from_millis(5);

impl Control {
    /// The operating system's id of the process. Not a handle: once it has ended, the system may
    /// give the id to another process.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// How it ended, or `None` while it runs. A Unix process that has ended is left uncollected
    /// until [`kill`](Self::kill), [`end`](Self::end) or the last handle's drop, so its group can
    /// still be signalled.
    ///
    /// # Errors
    ///
    /// The operating system could not say.
    pub fn try_wait(&self) -> io::Result<Option<Exit>> {
        self.inner.try_wait()
    }

    /// Waits up to `timeout` for the process to end (`None`: without limit), and tells how it did,
    /// or `None` if it still runs. Other threads may use the handle meanwhile.
    ///
    /// # Errors
    ///
    /// The operating system could not say.
    pub fn wait(&self, timeout: Option<Duration>) -> io::Result<Option<Exit>> {
        let until = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        loop {
            if let Some(exit) = self.inner.try_wait()? {
                return Ok(Some(exit));
            }
            let left = until.map(|until| until.saturating_duration_since(Instant::now()));
            match left {
                Some(Duration::ZERO) => return Ok(None),
                Some(left) => thread::sleep(POLL.min(left)),
                None => thread::sleep(POLL),
            }
        }
    }

    /// Ends the process and everything it started, and collects it. Ending one that has ended is no
    /// error, and does it for what it left behind.
    ///
    /// # Errors
    ///
    /// The operating system refused.
    pub fn kill(&self) -> io::Result<()> {
        self.inner.kill()
    }

    /// Gives the process up to `grace` to end by itself (after its input has closed, say), then
    /// ends whatever of its tree remains, and tells how it ended.
    ///
    /// # Errors
    ///
    /// The operating system refused or could not say.
    pub fn end(&self, grace: Duration) -> io::Result<Exit> {
        let by_itself = self.wait(Some(grace))?;
        self.kill()?;
        by_itself.map_or_else(
            || {
                self.try_wait()?
                    .ok_or_else(|| io::Error::other("the process did not end"))
            },
            Ok,
        )
    }

    /// The process's standard error so far.
    #[must_use]
    pub fn diagnostics(&self) -> Diagnostics {
        Diagnostics {
            capture: Arc::clone(&self.inner.capture),
        }
    }
}

/// What the process wrote to standard error: its most recent bytes and counts. Clones read the same
/// capture. Empty and ended when standard error is inherited or discarded.
#[derive(Clone, Debug)]
pub struct Diagnostics {
    capture: Arc<Capture>,
}

impl Diagnostics {
    /// The most recent bytes, at most the `stderr_tail` setting.
    #[must_use]
    pub fn tail(&self) -> Vec<u8> {
        locked(&self.capture.tail).iter().copied().collect()
    }

    /// The most recent bytes as text, invalid UTF-8 replaced.
    #[must_use]
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.tail()).into_owned()
    }

    /// All the bytes the process has written to standard error so far.
    #[must_use]
    pub fn received(&self) -> u64 {
        self.capture.received.load(Ordering::SeqCst)
    }

    /// The bytes no longer in the tail.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        let kept = locked(&self.capture.tail).len() as u64;
        self.received().saturating_sub(kept)
    }

    /// Standard error has ended: the process and everything holding its pipe have closed it.
    #[must_use]
    pub fn ended(&self) -> bool {
        self.capture.ended.load(Ordering::SeqCst)
    }
}

struct Capture {
    limit: usize,
    tail: Mutex<VecDeque<u8>>,
    received: AtomicU64,
    ended: AtomicBool,
}

impl fmt::Debug for Capture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Capture")
            .field("limit", &self.limit)
            .field("received", &self.received)
            .finish_non_exhaustive()
    }
}

impl Capture {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            tail: Mutex::new(VecDeque::new()),
            received: AtomicU64::new(0),
            ended: AtomicBool::new(false),
        }
    }

    /// Reads standard error to its end, keeping the most recent `limit` bytes.
    fn drain(&self, mut stderr: impl Read) {
        let mut chunk = [0_u8; 4096];
        loop {
            match stderr.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    self.received.fetch_add(read as u64, Ordering::SeqCst);
                    let mut tail = locked(&self.tail);
                    tail.extend(&chunk[..read]);
                    let excess = tail.len().saturating_sub(self.limit);
                    tail.drain(..excess);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
        self.ended.store(true, Ordering::SeqCst);
    }
}

/// Starts a process each time it connects.
///
/// `connect` starts the [`Launch`] with the [`ProcessSettings`]; the operating system cannot cancel
/// a start, so a `timeout` that passes first returns [`io::ErrorKind::TimedOut`] and the process,
/// if it does start, is ended at once.
#[derive(Clone, Debug)]
#[must_use]
pub struct Connector {
    launch: Launch,
    settings: ProcessSettings,
}

impl Connector {
    /// A connector for `launch`, with the default settings.
    pub fn new(launch: Launch) -> Self {
        Self {
            launch,
            settings: ProcessSettings::new(),
        }
    }

    /// Sets the settings every connection is started with.
    pub const fn settings(mut self, settings: ProcessSettings) -> Self {
        self.settings = settings;
        self
    }

    /// What it starts.
    pub const fn launch(&self) -> &Launch {
        &self.launch
    }
}

impl ConnectorTrait for Connector {
    type Connection = Process;
    type Error = io::Error;

    fn connect(&self, timeout: Duration) -> io::Result<Process> {
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a connect needs a timeout above zero",
            ));
        }
        let (sender, started) = mpsc::channel();
        let launch = self.launch.clone();
        let settings = self.settings;
        thread::Builder::new()
            .name("coqui-transport start".into())
            .spawn(move || {
                // A start nobody waits for any more is dropped here, which ends it.
                let _ = sender.send(Process::spawn(&launch, settings));
            })?;
        started.recv_timeout(timeout).map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, "the process did not start in time")
        })?
    }
}
