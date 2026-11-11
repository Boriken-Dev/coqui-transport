//! A spawned child as a channel: the test starts its own binary again as each child it needs, talks
//! to it over its standard streams, and ends it in every way a process can end.
//!
//! It has no test harness, since a harness would write its report to the children's standard
//! output between the frames. Arguments that do not start with `-` select scenarios by name.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

use coqui_transport::frame::LengthPrefix;
use coqui_transport::process::{
    Connector, ErrorOutput, Launch, Process, ProcessBuilder, ProcessSettings,
};
use coqui_transport::{
    Channel, Connector as _, Failure, StreamChannel, StreamChannelBuilder, stdio,
};

const ROLE: &str = "COQUI_TRANSPORT_TEST_ROLE";
const BEAT: &str = "COQUI_TRANSPORT_TEST_BEAT";
const NOISE: &str = "COQUI_TRANSPORT_TEST_NOISE";
const WAIT: Option<Duration> = Some(Duration::from_secs(20));

fn codec() -> LengthPrefix {
    LengthPrefix::u32_be()
}

// ---- the children --------------------------------------------------------------------------

/// Answers every message with its bytes reversed until its input ends.
fn echo() -> ExitCode {
    let Ok(channel) = StreamChannel::start(stdio::current(), codec()) else {
        return ExitCode::from(3);
    };
    loop {
        match channel.receive(None) {
            Ok(Some(mut message)) => {
                message.reverse();
                if channel.send(&message).is_err() {
                    return ExitCode::from(4);
                }
            }
            Ok(None) => break,
            Err(_) => return ExitCode::from(5),
        }
    }
    channel.close();
    ExitCode::SUCCESS
}

/// Writes `NOISE` bytes to standard error, then a last line, before it reads anything: a parent
/// that did not drain standard error would leave it blocked.
fn noisy() -> ExitCode {
    let bytes: usize = std::env::var(NOISE)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let mut stderr = std::io::stderr();
    let block = [b'x'; 1000];
    let mut written = 0;
    while written < bytes {
        let now = block.len().min(bytes - written);
        if stderr.write_all(&block[..now]).is_err() {
            return ExitCode::from(6);
        }
        written += now;
    }
    let _ = stderr.write_all(b"\nthe last line\n");
    echo()
}

/// Appends a byte to the file named by `BEAT` every 50 ms for ever: its length stops growing when
/// it is ended.
fn beat() -> ExitCode {
    let Some(path) = std::env::var_os(BEAT) else {
        return ExitCode::from(7);
    };
    loop {
        let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) else {
            return ExitCode::from(8);
        };
        let _ = file.write_all(b".");
        drop(file);
        thread::sleep(Duration::from_millis(50));
    }
}

/// Starts a heartbeat of its own, in its own process group when `escape` (Unix), says it is ready
/// and then waits for ever with its input unread.
fn tree(escape: bool) -> ExitCode {
    let Ok(program) = std::env::current_exe() else {
        return ExitCode::from(9);
    };
    let mut command = std::process::Command::new(program);
    command
        .env(ROLE, "beat")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    if escape {
        std::os::unix::process::CommandExt::process_group(&mut command, 0);
    }
    #[cfg(not(unix))]
    let _ = escape;
    if command.spawn().is_err() {
        return ExitCode::from(10);
    }
    let Ok(channel) = StreamChannel::start(stdio::current(), codec()) else {
        return ExitCode::from(3);
    };
    if channel.send(b"ready").is_err() {
        return ExitCode::from(4);
    }
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

/// Neither reads nor writes: only being ended stops it.
fn idle() -> ExitCode {
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

/// Reads until its input ends, takes 300 ms to wind down, and exits 0.
fn slow_close() -> ExitCode {
    let Ok(channel) = StreamChannel::start(stdio::current(), codec()) else {
        return ExitCode::from(3);
    };
    let _ = channel.send(b"up");
    while let Ok(Some(_)) = channel.receive(None) {}
    thread::sleep(Duration::from_millis(300));
    ExitCode::SUCCESS
}

/// Sends what it was started with, then waits for its input to end.
fn report() -> ExitCode {
    let Ok(channel) = StreamChannel::start(stdio::current(), codec()) else {
        return ExitCode::from(3);
    };
    let text = format!(
        "set={};removed={};cwd={};vars={}",
        std::env::var("COQUI_T_SET").unwrap_or_else(|_| "-".into()),
        std::env::var("COQUI_T_REMOVED").unwrap_or_else(|_| "-".into()),
        std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
        std::env::vars().count(),
    );
    let _ = channel.send(text.as_bytes());
    while let Ok(Some(_)) = channel.receive(None) {}
    ExitCode::SUCCESS
}

fn child(role: &str) -> ExitCode {
    match role {
        "echo" => echo(),
        "noisy" => noisy(),
        "beat" => beat(),
        "tree" => tree(false),
        "tree-escape" => tree(true),
        "idle" => idle(),
        "slow-close" => slow_close(),
        "report" => report(),
        "exit7" => ExitCode::from(7),
        _ => ExitCode::from(2),
    }
}

// ---- the parent ----------------------------------------------------------------------------

type Outcome = Result<(), String>;
type Scenario = (&'static str, fn() -> Outcome);

fn me(role: &str) -> Launch {
    Launch::new(std::env::current_exe().expect("this program has a path")).env(ROLE, role)
}

fn temp_file(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "coqui-transport-process-{}-{name}",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);
    path
}

fn length(path: &Path) -> u64 {
    fs::metadata(path).map_or(0, |m| m.len())
}

fn until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) -> Outcome {
    let start = Instant::now();
    while !done() {
        if start.elapsed() > limit {
            return Err(format!("timed out waiting for {what}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn ensure(condition: bool, what: &str) -> Outcome {
    if condition {
        Ok(())
    } else {
        Err(what.to_owned())
    }
}

fn failure(failure: Failure) -> String {
    failure.to_string()
}

fn io(error: std::io::Error) -> String {
    error.to_string()
}

fn channel_over(process: Process) -> Result<StreamChannel, String> {
    StreamChannel::start(process, codec()).map_err(io)
}

/// Messages a text mode would translate or stop at, an empty one and one larger than a pipe holds
/// go there and come back reversed; closing ends the child in order.
fn messages_cross_unchanged() -> Outcome {
    let process = Process::start(me("echo")).map_err(io)?;
    let control = process.control();
    let channel = channel_over(process)?;
    let large: Vec<u8> = (0..300 * 1024).map(|at| (at % 253) as u8).collect();
    let every_byte: Vec<u8> = (0..=255).collect();
    let messages: Vec<Vec<u8>> = vec![
        b"hello".to_vec(),
        b"\n\r\n\x1a\x00\xff\xfe\r".to_vec(),
        every_byte,
        Vec::new(),
        large,
    ];
    for message in &messages {
        channel.send(message).map_err(failure)?;
    }
    for message in &messages {
        let mut expected = message.clone();
        expected.reverse();
        let got = channel.receive(WAIT).map_err(failure)?;
        ensure(
            got.as_ref() == Some(&expected),
            &format!("a message of {} bytes came back changed", message.len()),
        )?;
    }
    channel.close();
    let exit = control.end(Duration::from_secs(10)).map_err(io)?;
    ensure(exit.success(), &format!("the child {exit}"))
}

/// A child writing a megabyte to standard error before it reads a byte is not held up, standard
/// error never reaches the channel, and only the newest bytes are kept.
fn standard_error_is_drained_apart_and_bounded() -> Outcome {
    let launch = me("noisy").env(NOISE, "1048576");
    let process = ProcessBuilder::new()
        .launch(launch)
        .stderr_tail(4096)
        .build()
        .map_err(|e| e.to_string())?;
    let control = process.control();
    let channel = channel_over(process)?;
    channel.send(b"abc").map_err(failure)?;
    let got = channel.receive(WAIT).map_err(failure)?;
    ensure(
        got.as_deref() == Some(b"cba".as_slice()),
        "the answer was not the reversed message",
    )?;
    channel.close();
    let exit = control.end(Duration::from_secs(10)).map_err(io)?;
    ensure(exit.success(), &format!("the child {exit}"))?;
    let diagnostics = control.diagnostics();
    until("standard error to end", Duration::from_secs(10), || {
        diagnostics.ended()
    })?;
    let written = 1_048_576 + "\nthe last line\n".len() as u64;
    ensure(
        diagnostics.received() == written,
        &format!("received {} of {written}", diagnostics.received()),
    )?;
    ensure(
        diagnostics.tail().len() == 4096,
        "the tail is not its bound",
    )?;
    ensure(
        diagnostics.dropped() == written - 4096,
        "dropped does not add up",
    )?;
    ensure(
        diagnostics.text().ends_with("\nthe last line\n"),
        "the tail is not the newest bytes",
    )
}

/// With a tail of zero everything is counted and nothing kept; inherited and discarded standard
/// error is never captured.
fn standard_error_settings() -> Outcome {
    for (setting, tail) in [
        (ErrorOutput::Capture, 0),
        (ErrorOutput::Discard, 100),
        (ErrorOutput::Inherit, 100),
    ] {
        let process = ProcessBuilder::new()
            .launch(me("noisy").env(NOISE, "5000"))
            .stderr(setting)
            .stderr_tail(tail)
            .build()
            .map_err(|e| e.to_string())?;
        let control = process.control();
        let diagnostics = process.diagnostics();
        let channel = channel_over(process)?;
        channel.close();
        control.end(Duration::from_secs(10)).map_err(io)?;
        until("standard error to end", Duration::from_secs(10), || {
            diagnostics.ended()
        })?;
        ensure(diagnostics.tail().is_empty(), "something was kept")?;
        let expected = if setting == ErrorOutput::Capture {
            5000 + 15
        } else {
            0
        };
        ensure(
            diagnostics.received() == expected,
            &format!("{setting:?} received {}", diagnostics.received()),
        )?;
    }
    Ok(())
}

/// Ending the process ends what it started, too.
fn kill_ends_the_tree() -> Outcome {
    for role in ["tree", "tree-escape"] {
        if role == "tree-escape" && !cfg!(unix) {
            continue;
        }
        let beats = temp_file(role);
        let process = Process::start(me(role).env(BEAT, &beats)).map_err(io)?;
        let control = process.control();
        let channel = channel_over(process)?;
        let ready = channel.receive(WAIT).map_err(failure)?;
        ensure(
            ready.as_deref() == Some(b"ready".as_slice()),
            "the child did not say it is ready",
        )?;
        until("the grandchild to beat", Duration::from_secs(10), || {
            length(&beats) > 3
        })?;
        control.kill().map_err(io)?;
        control.kill().map_err(io)?;
        thread::sleep(Duration::from_millis(300));
        let stopped = length(&beats);
        thread::sleep(Duration::from_millis(600));
        let after = length(&beats);
        let _ = fs::remove_file(&beats);
        // What the channel makes of it: the stream ended with the process.
        let seen = channel.receive(WAIT);
        ensure(
            after == stopped,
            &format!("{role}: the grandchild was still beating after the kill"),
        )?;
        ensure(
            matches!(seen, Ok(None) | Err(Failure::PathLost)),
            "the channel did not see the end",
        )?;
    }
    Ok(())
}

/// Dropping every handle, with no call to end anything, ends the tree.
fn dropping_the_last_handle_ends_the_tree() -> Outcome {
    let beats = temp_file("drop");
    let process = Process::start(me("tree").env(BEAT, &beats)).map_err(io)?;
    let channel = channel_over(process)?;
    channel.receive(WAIT).map_err(failure)?;
    until("the grandchild to beat", Duration::from_secs(10), || {
        length(&beats) > 3
    })?;
    drop(channel);
    thread::sleep(Duration::from_millis(400));
    let stopped = length(&beats);
    thread::sleep(Duration::from_millis(600));
    let after = length(&beats);
    let _ = fs::remove_file(&beats);
    ensure(after == stopped, "the grandchild outlived the last handle")
}

/// A child that takes its time to wind down after its input ends is waited for, not killed, when the
/// channel's linger allows it.
fn closing_waits_for_a_child_winding_down() -> Outcome {
    let process = Process::start(me("slow-close")).map_err(io)?;
    let control = process.control();
    let channel = StreamChannelBuilder::new()
        .codec(codec())
        .stream(process)
        .linger(Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?;
    ensure(
        channel.receive(WAIT).map_err(failure)?.as_deref() == Some(b"up".as_slice()),
        "no greeting",
    )?;
    let start = Instant::now();
    channel.close();
    let exit = control.wait(Some(Duration::from_secs(10))).map_err(io)?;
    ensure(
        exit.is_some_and(|e| e.success()),
        &format!("the child was not left to end by itself: {exit:?}"),
    )?;
    ensure(
        start.elapsed() >= Duration::from_millis(250),
        "close did not wait for the child",
    )
}

/// A child that never reads its input and never closes its output does not hold `close` up: after
/// the linger the interrupt ends it, and with it the threads it was blocking.
fn closing_ends_a_child_that_ignores_it() -> Outcome {
    let process = Process::start(me("idle")).map_err(io)?;
    let control = process.control();
    let channel = StreamChannelBuilder::new()
        .codec(codec())
        .stream(process)
        .linger(Duration::from_millis(200))
        .build()
        .map_err(|e| e.to_string())?;
    // A message the child never takes: nothing waits on it.
    channel.send(b"anyone there").map_err(failure)?;
    let start = Instant::now();
    channel.close();
    ensure(
        start.elapsed() < Duration::from_secs(15),
        "close was held up",
    )?;
    let exit = control.wait(Some(Duration::from_secs(10))).map_err(io)?;
    let exit = exit.ok_or("the child was still running after close")?;
    ensure(!exit.success(), "an ignoring child cannot have ended well")?;
    #[cfg(unix)]
    ensure(
        exit.signal == Some(9),
        &format!("not ended by SIGKILL: {exit:?}"),
    )?;
    Ok(())
}

/// The exit status, and the channel's end with it.
fn the_exit_status_is_reported() -> Outcome {
    let process = Process::start(me("exit7")).map_err(io)?;
    let control = process.control();
    let channel = channel_over(process)?;
    ensure(
        channel.receive(WAIT).map_err(failure)?.is_none(),
        "a message from a child that sent none",
    )?;
    let exit = control
        .wait(Some(Duration::from_secs(10)))
        .map_err(io)?
        .ok_or("it did not end")?;
    ensure(
        exit.code == Some(7) && !exit.success(),
        &format!("{exit:?}"),
    )?;
    ensure(exit.to_string() == "exited with code 7", "Display")?;
    // Collected by kill, the status stays what it was.
    control.kill().map_err(io)?;
    ensure(
        control.try_wait().map_err(io)? == Some(exit),
        "the status changed after kill",
    )?;
    #[cfg(unix)]
    {
        let process = Process::start(me("idle")).map_err(io)?;
        let control = process.control();
        ensure(
            control
                .wait(Some(Duration::from_millis(50)))
                .map_err(io)?
                .is_none(),
            "a running child has ended",
        )?;
        ensure(
            control.try_wait().map_err(io)?.is_none(),
            "a running child has an exit",
        )?;
        control.kill().map_err(io)?;
        let exit = control
            .try_wait()
            .map_err(io)?
            .ok_or("no exit after kill")?;
        ensure(
            exit.signal == Some(9) && exit.code.is_none(),
            &format!("{exit:?}"),
        )?;
    }
    Ok(())
}

/// A program that is not there is an error, and nothing is left running.
fn a_missing_program_is_an_error() -> Outcome {
    let error = Process::start(Launch::new("coqui-transport-no-such-program"))
        .map(|_| ())
        .unwrap_err();
    ensure(
        error.kind() == std::io::ErrorKind::NotFound,
        &format!("{error:?}"),
    )?;
    let error = Connector::new(Launch::new("coqui-transport-no-such-program"))
        .connect(Duration::from_secs(10))
        .map(|_| ())
        .unwrap_err();
    ensure(
        error.kind() == std::io::ErrorKind::NotFound,
        &format!("{error:?}"),
    )
}

/// The environment, the working directory and the arguments reach the child as given.
fn the_launch_reaches_the_child() -> Outcome {
    let directory = std::env::temp_dir();
    let launch = me("report")
        .cwd(&directory)
        .env("COQUI_T_SET", "value with spaces")
        .remove_env("COQUI_T_REMOVED");
    let process = Process::start(launch).map_err(io)?;
    let control = process.control();
    let channel = channel_over(process)?;
    let got = channel.receive(WAIT).map_err(failure)?.ok_or("no report")?;
    let got = String::from_utf8_lossy(&got).into_owned();
    channel.close();
    control.end(Duration::from_secs(10)).map_err(io)?;
    ensure(
        got.starts_with("set=value with spaces;removed=-;cwd="),
        &got,
    )?;
    let reported = got
        .split(";cwd=")
        .nth(1)
        .and_then(|r| r.split(";vars=").next())
        .unwrap_or_default();
    ensure(
        fs::canonicalize(reported).ok() == fs::canonicalize(&directory).ok(),
        "the working directory is not the one given",
    )?;

    // An empty environment holds the variables set on the launch and no others the test can name.
    let process =
        Process::start(me("report").clear_env(true).env("COQUI_T_SET", "x")).map_err(io)?;
    let control = process.control();
    let channel = channel_over(process)?;
    let got = channel.receive(WAIT).map_err(failure)?.ok_or("no report")?;
    let got = String::from_utf8_lossy(&got).into_owned();
    channel.close();
    control.end(Duration::from_secs(10)).map_err(io)?;
    ensure(got.starts_with("set=x;"), &got)?;
    let vars: usize = got
        .rsplit("vars=")
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(usize::MAX);
    // On Windows the system may add a few of its own to a process.
    ensure(
        vars <= 8,
        &format!("an empty environment was not empty: {got}"),
    )
}

/// The connector starts a process per connect, and its timeout is real.
fn the_connector_starts_processes() -> Outcome {
    let connector = Connector::new(me("echo")).settings(ProcessSettings::new());
    for _ in 0..2 {
        let process = connector.connect(Duration::from_secs(20)).map_err(io)?;
        let control = process.control();
        let channel = channel_over(process)?;
        channel.send(b"ab").map_err(failure)?;
        ensure(
            channel.receive(WAIT).map_err(failure)?.as_deref() == Some(b"ba".as_slice()),
            "echo",
        )?;
        channel.close();
        control.end(Duration::from_secs(10)).map_err(io)?;
    }
    let error = connector.connect(Duration::ZERO).map(|_| ()).unwrap_err();
    ensure(
        error.kind() == std::io::ErrorKind::InvalidInput,
        "a zero timeout",
    )?;
    // A start that cannot happen in a nanosecond times out; one that does is ended with its handle.
    match connector.connect(Duration::from_nanos(1)) {
        Ok(_) => Ok(()),
        Err(error) => ensure(
            error.kind() == std::io::ErrorKind::TimedOut,
            &format!("{error:?}"),
        ),
    }
}

/// A started process nobody asks to end ends when its handles drop, and a `Debug` of the launch
/// never shows a variable's value.
fn handles_and_text() -> Outcome {
    let launch = me("idle")
        .env("COQUI_T_SECRET", "hunter2")
        .arg("a b")
        .arg("");
    let shown = format!("{launch:?}");
    ensure(
        shown.contains("COQUI_T_SECRET") && !shown.contains("hunter2"),
        &shown,
    )?;
    let line = launch.to_string();
    ensure(line.ends_with(" \"a b\" \"\""), &line)?;
    let process = Process::start(launch).map_err(io)?;
    let id = process.id();
    let control = process.control();
    ensure(control.id() == id, "ids differ")?;
    drop(process);
    ensure(
        control.try_wait().map_err(io)?.is_none(),
        "ended with a handle still held",
    )?;
    let clone = control.clone();
    drop(control);
    ensure(
        clone.try_wait().map_err(io)?.is_none(),
        "ended with a clone still held",
    )?;
    clone.kill().map_err(io)?;
    ensure(
        clone
            .wait(Some(Duration::from_secs(5)))
            .map_err(io)?
            .is_some(),
        "kill did not end it",
    )
}

fn main() -> ExitCode {
    if let Ok(role) = std::env::var(ROLE) {
        return child(&role);
    }
    let scenarios: &[Scenario] = &[
        ("messages_cross_unchanged", messages_cross_unchanged),
        (
            "standard_error_is_drained_apart_and_bounded",
            standard_error_is_drained_apart_and_bounded,
        ),
        ("standard_error_settings", standard_error_settings),
        ("kill_ends_the_tree", kill_ends_the_tree),
        (
            "dropping_the_last_handle_ends_the_tree",
            dropping_the_last_handle_ends_the_tree,
        ),
        (
            "closing_waits_for_a_child_winding_down",
            closing_waits_for_a_child_winding_down,
        ),
        (
            "closing_ends_a_child_that_ignores_it",
            closing_ends_a_child_that_ignores_it,
        ),
        ("the_exit_status_is_reported", the_exit_status_is_reported),
        (
            "a_missing_program_is_an_error",
            a_missing_program_is_an_error,
        ),
        ("the_launch_reaches_the_child", the_launch_reaches_the_child),
        (
            "the_connector_starts_processes",
            the_connector_starts_processes,
        ),
        ("handles_and_text", handles_and_text),
    ];
    let filters: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with('-'))
        .collect();
    let mut failed = 0;
    let mut ran = 0;
    for (name, scenario) in scenarios {
        if !filters.is_empty() && !filters.iter().any(|f| name.contains(f.as_str())) {
            continue;
        }
        ran += 1;
        let start = Instant::now();
        match scenario() {
            Ok(()) => println!("test {name} ... ok ({:.1?})", start.elapsed()),
            Err(why) => {
                failed += 1;
                println!("test {name} ... FAILED: {why}");
            }
        }
    }
    println!("\n{} passed; {failed} failed", ran - failed);
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
