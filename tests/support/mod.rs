//! Helpers the test binaries share: the vectors, and hex.
#![allow(dead_code, reason = "each test binary uses some of these")]

use serde_json::Value;

/// A wait long enough for any loopback exchange, short enough to fail a hung test.
pub const WAIT: Option<core::time::Duration> = Some(core::time::Duration::from_secs(10));

pub fn vectors(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("vectors")
        .join(name);
    serde_json::from_slice(&std::fs::read(path).expect("the vectors are in vectors/"))
        .expect("the vectors are JSON")
}

pub fn bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("the vectors hold hex"))
        .collect()
}

pub fn hex(bytes: &[u8]) -> String {
    use core::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

pub fn max(case: &Value) -> usize {
    usize::try_from(case["max"].as_u64().expect("a max")).expect("a small max")
}

pub fn name(case: &Value) -> &str {
    case["name"].as_str().expect("a name")
}

/// An encode case's body: its text, or its bytes as hex.
pub fn body(case: &Value) -> Vec<u8> {
    match (case.get("text"), case.get("body")) {
        (Some(text), _) => text.as_str().expect("text").as_bytes().to_vec(),
        (None, Some(body)) => bytes(body.as_str().expect("hex")),
        (None, None) => panic!("{}: no body", name(case)),
    }
}

pub fn chunks(case: &Value) -> Vec<Vec<u8>> {
    case["chunks"]
        .as_array()
        .expect("chunks")
        .iter()
        .map(|chunk| bytes(chunk.as_str().expect("hex")))
        .collect()
}

pub fn messages(case: &Value) -> Vec<Vec<u8>> {
    case["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|message| bytes(message.as_str().expect("hex")))
        .collect()
}

pub fn flag(case: &Value, key: &str) -> bool {
    case.get(key).and_then(Value::as_bool) == Some(true)
}

pub fn cases<'a>(vectors: &'a Value, key: &str) -> &'a [Value] {
    vectors[key].as_array().expect("cases")
}
