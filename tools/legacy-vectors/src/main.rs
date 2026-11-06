//! Writes `peer_frame.json` and `webtransport_frame.json`: every encode and decode outcome in them
//! is what the crate that defined the wire produces, run here at a pinned revision.
//!
//! `cargo run --manifest-path tools/legacy-vectors/Cargo.toml -- <vectors dir> [--check]`
//!
//! With `--check` it writes nothing and exits 1 when a file differs from what it would write.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use coqui_peer_pipe::{MessageTransport, Messages, ReadTimeout, WriteTimeout};
use coqui_web_channel::frame as web;
use serde_json::{Value, json};

const MIB: usize = 1024 * 1024;

const PEER_SOURCE: &str = "coqui-peer 1f00bfe6319f64e7ec038a7003746f4f031d538c (write_frame, read_frame) and \
     coqui-peer-pipe 8ae70cd42f36ac7a54fea890e2ee006e81cb8775 (Messages)";
const WEB_SOURCE: &str = "coqui-web-channel 0b5b05c9af3417f6b13edc0fc7d9c86c02a63eca (frame::encode, frame::Decoder)";

/// A stream that reads the given chunks in order, each at most what one read asks for, then ends;
/// and keeps what is written.
#[derive(Default)]
struct Script {
    chunks: VecDeque<Vec<u8>>,
    written: Vec<u8>,
}

impl Read for Script {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let Some(mut chunk) = self.chunks.pop_front() else {
            return Ok(0);
        };
        let count = chunk.len().min(buffer.len());
        buffer[..count].copy_from_slice(&chunk[..count]);
        if count < chunk.len() {
            self.chunks.push_front(chunk.split_off(count));
        }
        Ok(count)
    }
}

impl Write for Script {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.written.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ReadTimeout for Script {
    fn set_read_timeout(&self, _: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
}

impl WriteTimeout for Script {
    fn set_write_timeout(&self, _: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex"))
        .collect()
}

/// A body: text, or bytes given as hex.
enum Body {
    Text(&'static str),
    Hex(String),
}

impl Body {
    fn bytes(&self) -> Vec<u8> {
        match self {
            Self::Text(text) => text.as_bytes().to_vec(),
            Self::Hex(hex) => bytes(hex),
        }
    }

    fn field(&self) -> (&'static str, Value) {
        match self {
            Self::Text(text) => ("text", json!(text)),
            Self::Hex(hex) => ("body", json!(hex)),
        }
    }
}

fn counting(length: usize) -> Body {
    Body::Hex(hex(&(0..length).map(|at| (at % 251) as u8).collect::<Vec<_>>()))
}

struct EncodeCase {
    name: &'static str,
    max: usize,
    body: Body,
}

struct DecodeCase {
    name: &'static str,
    max: usize,
    chunks: Vec<&'static str>,
}

fn encode_cases(empty_is_message: bool) -> Vec<EncodeCase> {
    let case = |name, max, body| EncodeCase { name, max, body };
    let mut cases = vec![
        case("a short body", 10, Body::Text("hello")),
        case("exactly the limit", 5, Body::Text("hello")),
        case(
            "a JSON-RPC message",
            MIB,
            Body::Text("{\"jsonrpc\":\"2.0\",\"id\":1}"),
        ),
        case("multi-byte text counts bytes", 10, Body::Text("café")),
        case("an empty body", 10, Body::Text("")),
        case("one byte over the limit", 4, Body::Text("hello")),
        case("binary bytes", 10, Body::Hex("00ff0a0d1a".into())),
        case("a 300-byte body", MIB, counting(300)),
        case("a 300-byte body one over the limit", 299, counting(300)),
    ];
    if empty_is_message {
        cases.push(case("an empty body under a limit of 0", 0, Body::Text("")));
    }
    cases
}

fn shared_decode_cases(prefix: fn(u32) -> String) -> Vec<DecodeCase> {
    let leak = |text: String| -> &'static str { Box::leak(text.into_boxed_str()) };
    let hello = leak(format!("{}68656c6c6f", prefix(5)));
    let world = leak(format!("{}776f726c64", prefix(5)));
    let both = leak(format!("{hello}{world}"));
    let oversized = leak(prefix(11));
    let case = |name, max, chunks| DecodeCase { name, max, chunks };
    vec![
        case("one frame whole", 10, vec![hello]),
        case("split inside the prefix", 10, vec![&hello[..4], &hello[4..]]),
        case("split inside the body", 10, vec![&hello[..10], &hello[10..]]),
        case(
            "one byte at a time",
            10,
            (0..hello.len() / 2).map(|at| &hello[at * 2..at * 2 + 2]).collect(),
        ),
        case("two frames coalesced", 10, vec![both]),
        case("coalesced then split", 10, vec![&both[..22], &both[22..]]),
        case("an incomplete frame waits", 10, vec![&hello[..10]]),
        case("an incomplete prefix waits", 10, vec![&hello[..6]]),
        case("exactly the limit", 5, vec![hello]),
        case("an oversized header", 10, vec![oversized]),
        case("a huge header, the high bit set", 10, vec!["ffffffff"]),
        case(
            "a good frame before a bad header",
            10,
            vec![leak(format!("{hello}{oversized}"))],
        ),
        case(
            "a good frame before a bad header, later chunk",
            10,
            vec![hello, oversized],
        ),
        case(
            "a bad header stops what follows it",
            10,
            vec![leak(format!("{oversized}{hello}"))],
        ),
        case("an empty frame", 10, vec![leak(prefix(0))]),
        case(
            "invalid UTF-8 is a message like any other",
            10,
            vec![leak(format!("{}ff", prefix(1)))],
        ),
    ]
}

/// Decode outcomes as the JSON fields every decode case shares.
fn outcome(chunks: &[&str], messages: &[Vec<u8>], error: bool) -> [(&'static str, Value); 3] {
    let total: usize = chunks.iter().map(|chunk| chunk.len() / 2).sum();
    let consumed: usize = messages.iter().map(|message| 4 + message.len()).sum();
    [
        (
            "messages",
            json!(messages.iter().map(|m| hex(m)).collect::<Vec<_>>()),
        ),
        ("error", json!(error)),
        ("truncated", json!(!error && consumed < total)),
    ]
}

fn object(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn peer_vectors() -> Value {
    let mut encode = Vec::new();
    for case in encode_cases(true) {
        let body = case.body.bytes();
        let mut messages = Messages::new(Script::default()).with_max_message(case.max);
        let sent = messages.send(&body);
        let written = messages.into_inner().written;
        let mut fields = vec![("name", json!(case.name)), ("max", json!(case.max)), case.body.field()];
        match sent {
            Ok(()) => {
                // coqui-peer's own writer frames the same bytes, whatever its fixed bound.
                let mut by_peer = Vec::new();
                coqui_peer::write_frame(&mut by_peer, &body).expect("within coqui-peer's bound");
                assert_eq!(by_peer, written, "{}: coqui-peer and coqui-peer-pipe differ", case.name);
                fields.push(("hex", json!(hex(&written))));
            }
            Err(error) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{}", case.name);
                assert!(written.is_empty(), "{}: wrote before refusing", case.name);
                fields.push(("error", json!(true)));
            }
        }
        encode.push(object(fields));
    }

    let mut cases = shared_decode_cases(|length| hex(&length.to_be_bytes()));
    cases.extend([
        DecodeCase {
            name: "empty messages between others",
            max: 10,
            chunks: vec!["00000000000000016100000000"],
        },
        DecodeCase {
            name: "an empty message under a limit of 0",
            max: 0,
            chunks: vec!["00000000"],
        },
        DecodeCase {
            name: "a little-endian frame declares more than the limit here",
            max: MIB,
            chunks: vec!["0500000068656c6c6f"],
        },
    ]);
    let mut decode = Vec::new();
    for case in cases {
        let script = Script {
            chunks: case.chunks.iter().map(|chunk| bytes(chunk)).collect(),
            written: Vec::new(),
        };
        let mut messages = Messages::new(script).with_max_message(case.max);
        let mut got = Vec::new();
        let error = loop {
            match messages.recv() {
                Ok(message) => got.push(message),
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break false,
                Err(error) if error.kind() == io::ErrorKind::InvalidData => break true,
                Err(error) => panic!("{}: {error}", case.name),
            }
        };
        if case.max == MIB {
            // coqui-peer's reader, whose bound is the same 1 MiB, reads the same messages.
            let all: Vec<u8> = case.chunks.iter().flat_map(|chunk| bytes(chunk)).collect();
            let mut cursor = io::Cursor::new(all);
            let mut by_peer = Vec::new();
            while let Ok(message) = coqui_peer::read_frame(&mut cursor) {
                by_peer.push(message);
            }
            assert_eq!(by_peer, got, "{}: coqui-peer and coqui-peer-pipe differ", case.name);
        }
        let mut fields = vec![
            ("name", json!(case.name)),
            ("max", json!(case.max)),
            ("chunks", json!(case.chunks)),
        ];
        fields.extend(outcome(&case.chunks, &got, error));
        decode.push(object(fields));
    }
    document(
        "PeerFrame",
        "big-endian u32 body length, then the body; a body may be empty and is at most `max` bytes",
        PEER_SOURCE,
        encode,
        decode,
    )
}

fn webtransport_vectors() -> Value {
    let mut encode = Vec::new();
    for case in encode_cases(false) {
        let body = case.body.bytes();
        let mut fields = vec![("name", json!(case.name)), ("max", json!(case.max)), case.body.field()];
        match web::encode(&body, case.max) {
            Ok(frame) => fields.push(("hex", json!(hex(&frame)))),
            Err(web::FrameError::Size) => fields.push(("error", json!(true))),
            Err(other) => panic!("{}: {other:?}", case.name),
        }
        encode.push(object(fields));
    }

    let mut cases = shared_decode_cases(|length| hex(&length.to_le_bytes()));
    cases.push(DecodeCase {
        name: "a big-endian frame declares more than the limit here",
        max: MIB,
        chunks: vec!["0000000568656c6c6f"],
    });
    let mut decode = Vec::new();
    for case in cases {
        let mut decoder = web::Decoder::new(case.max);
        let mut got = Vec::new();
        let mut error = false;
        'chunks: for chunk in &case.chunks {
            decoder.push(&bytes(chunk));
            loop {
                match decoder.next_frame() {
                    Ok(Some(message)) => got.push(message),
                    Ok(None) => break,
                    Err(web::FrameError::Size) => {
                        error = true;
                        break 'chunks;
                    }
                    Err(other) => panic!("{}: {other:?}", case.name),
                }
            }
        }
        let mut fields = vec![
            ("name", json!(case.name)),
            ("max", json!(case.max)),
            ("chunks", json!(case.chunks)),
        ];
        fields.extend(outcome(&case.chunks, &got, error));
        decode.push(object(fields));
    }
    document(
        "WebTransportFrame",
        "little-endian u32 body length, then the body; a body is non-empty and at most `max` bytes",
        WEB_SOURCE,
        encode,
        decode,
    )
}

fn document(codec: &str, wire: &str, source: &str, encode: Vec<Value>, decode: Vec<Value>) -> Value {
    json!({
        "codec": codec,
        "wire": wire,
        "source": source,
        "encode": encode,
        "decode": decode,
    })
}

/// The document as JSON with one case per line, so a change shows as the cases it touches.
fn render(document: &Value) -> String {
    let mut out = String::from("{\n");
    for key in ["codec", "wire", "source"] {
        let _ = writeln!(out, "  {}: {},", json!(key), document[key]);
    }
    for (index, key) in ["encode", "decode"].into_iter().enumerate() {
        let _ = writeln!(out, "  {}: [", json!(key));
        let cases = document[key].as_array().expect("cases");
        for (at, case) in cases.iter().enumerate() {
            let comma = if at + 1 < cases.len() { "," } else { "" };
            let _ = writeln!(out, "    {case}{comma}");
        }
        let comma = if index == 0 { "," } else { "" };
        let _ = writeln!(out, "  ]{comma}");
    }
    out.push_str("}\n");
    out
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(directory) = args.next() else {
        eprintln!("usage: legacy-vectors <vectors dir> [--check]");
        return ExitCode::from(2);
    };
    let check = args.next().as_deref() == Some("--check");
    let mut differ = false;
    for (name, document) in [
        ("peer_frame.json", peer_vectors()),
        ("webtransport_frame.json", webtransport_vectors()),
    ] {
        let path = Path::new(&directory).join(name);
        let text = render(&document);
        serde_json::from_str::<Value>(&text).expect("the rendering is JSON");
        if check {
            let current = std::fs::read(&path).unwrap_or_default();
            if current != text.as_bytes() {
                eprintln!("{} differs from what the old crates produce", path.display());
                differ = true;
            }
        } else {
            std::fs::write(&path, text.as_bytes()).expect("the vectors directory is writable");
            eprintln!("wrote {}", path.display());
        }
    }
    if differ { ExitCode::FAILURE } else { ExitCode::SUCCESS }
}
