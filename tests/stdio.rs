//! This process's standard streams as a channel: the test starts its own binary again as a child
//! whose standard input and output carry frames, and talks to it as to any other peer.
//!
//! It has no test harness, since the harness would write its report to the child's standard output
//! between the frames.

use std::process::{Command, ExitCode, Stdio};
use std::time::Duration;

use coqui_transport::frame::LengthPrefix;
use coqui_transport::{Channel, Duplex, StreamChannel, stdio};

const CHILD: &str = "COQUI_TRANSPORT_TEST_STDIO_CHILD";
const WAIT: Option<Duration> = Some(Duration::from_secs(20));

/// The child: answers every message with its bytes reversed, until its input ends.
fn child() -> ExitCode {
    let Ok(channel) = StreamChannel::start(stdio::current(), LengthPrefix::u32_be()) else {
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

fn parent() -> Result<(), String> {
    let program = std::env::current_exe().map_err(|error| error.to_string())?;
    let mut process = Command::new(program)
        .env(CHILD, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| error.to_string())?;
    let input = process.stdin.take().ok_or("no child stdin")?;
    let output = process.stdout.take().ok_or("no child stdout")?;
    let channel = StreamChannel::start(Duplex::new(output, input), LengthPrefix::u32_be())
        .map_err(|error| error.to_string())?;

    // Bytes a text mode would translate or stop at: newlines, carriage returns, Ctrl-Z, and bytes
    // that are not UTF-8; an empty message; one larger than a pipe's buffer.
    let large: Vec<u8> = (0..300 * 1024).map(|at| (at % 253) as u8).collect();
    let messages: Vec<Vec<u8>> = vec![
        b"hello".to_vec(),
        b"\n\r\n\x1a\x00\xff\xfe\r".to_vec(),
        Vec::new(),
        large,
    ];
    for message in &messages {
        channel
            .send(message)
            .map_err(|failure| failure.to_string())?;
    }
    for message in &messages {
        let mut expected = message.clone();
        expected.reverse();
        let got = channel
            .receive(WAIT)
            .map_err(|failure| failure.to_string())?;
        if got.as_ref() != Some(&expected) {
            return Err(format!(
                "a message of {} bytes came back changed",
                message.len()
            ));
        }
    }
    // Closing ends the child's input; it answers nothing more, ends, and its output ends in order.
    channel.close();
    let status = process.wait().map_err(|error| error.to_string())?;
    if !status.success() {
        return Err(format!("the child ended with {status}"));
    }
    match channel.receive(WAIT) {
        Ok(None) => Ok(()),
        other => Err(format!("after the child ended: {other:?}")),
    }
}

fn main() -> ExitCode {
    if std::env::var_os(CHILD).is_some() {
        return child();
    }
    match parent() {
        Ok(()) => {
            println!("stdio: a child spoke frames over its standard streams: ok");
            ExitCode::SUCCESS
        }
        Err(problem) => {
            eprintln!("stdio: {problem}");
            ExitCode::FAILURE
        }
    }
}
