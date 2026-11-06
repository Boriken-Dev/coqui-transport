//! The legacy wires, held to the vectors the crates that defined them produce, through the codecs
//! and through a `StreamChannel` on a real stream.

mod support;

use std::io::{Read, Write};

use coqui_transport::frame::{Codec, Decode, FrameError, PeerFrame, WebTransportFrame};
use coqui_transport::{Channel, Failure, StreamChannel, memory};
use serde_json::Value;
use support::{WAIT, body, bytes, cases, chunks, flag, hex, max, messages, name, vectors};

fn encode_vectors_hold<C: Codec>(codec: &C, vectors: &Value) {
    for case in cases(vectors, "encode") {
        let got = codec.encode(&body(case), max(case));
        if flag(case, "error") {
            assert_eq!(got, Err(FrameError::Size), "{}", name(case));
        } else {
            assert_eq!(
                hex(&got.expect("encodes")),
                case["hex"].as_str().unwrap(),
                "{}",
                name(case)
            );
        }
    }
}

fn decode_vectors_hold<C: Codec>(codec: &C, vectors: &Value) {
    for case in cases(vectors, "decode") {
        let mut decoder = codec.decoder(max(case));
        let mut got = Vec::new();
        let mut failed = false;
        'chunks: for chunk in chunks(case) {
            decoder.push(&chunk);
            loop {
                match decoder.next_frame() {
                    Ok(Some(message)) => got.push(message),
                    Ok(None) => break,
                    Err(FrameError::Size) => {
                        failed = true;
                        break 'chunks;
                    }
                    Err(other) => panic!("{}: {other:?}", name(case)),
                }
            }
        }
        assert_eq!(got, messages(case), "{}", name(case));
        assert_eq!(failed, flag(case, "error"), "{}", name(case));
        assert_eq!(
            !failed && decoder.buffered() > 0,
            flag(case, "truncated"),
            "{}",
            name(case)
        );
    }
}

#[test]
fn every_peer_frame_vector_holds() {
    let vectors = vectors("peer_frame.json");
    assert_eq!(vectors["codec"], "PeerFrame");
    encode_vectors_hold(&PeerFrame, &vectors);
    decode_vectors_hold(&PeerFrame, &vectors);
}

#[test]
fn every_webtransport_frame_vector_holds() {
    let vectors = vectors("webtransport_frame.json");
    assert_eq!(vectors["codec"], "WebTransportFrame");
    encode_vectors_hold(&WebTransportFrame, &vectors);
    decode_vectors_hold(&WebTransportFrame, &vectors);
}

/// What a channel sends reaches the stream as the vector's bytes, and what it refuses reaches it not
/// at all.
fn channel_sends_the_vector_bytes<C: Codec + Clone>(codec: &C, vectors: &Value) {
    for case in cases(vectors, "encode") {
        let (mine, theirs) = memory::pipe(1 << 16);
        let channel = StreamChannel::builder(codec.clone())
            .max_message(max(case))
            .start(mine)
            .unwrap();
        let sent = channel.send(&body(case));
        channel.close();
        let mut wire = Vec::new();
        let (mut reader, _writer) = theirs.into_halves();
        reader.read_to_end(&mut wire).unwrap();
        if flag(case, "error") {
            assert_eq!(sent, Err(Failure::Protocol), "{}", name(case));
            assert!(wire.is_empty(), "{}", name(case));
        } else {
            assert_eq!(sent, Ok(()), "{}", name(case));
            assert_eq!(hex(&wire), case["hex"].as_str().unwrap(), "{}", name(case));
        }
    }
}

/// What a channel receives from the vector's bytes, written in its chunks, is the vector's
/// messages, then the end the vector says: a failure, a torn frame, or an orderly end.
fn channel_receives_the_vector_messages<C: Codec + Clone>(codec: &C, vectors: &Value) {
    for case in cases(vectors, "decode") {
        let (mine, theirs) = memory::pipe(1 << 16);
        let channel = StreamChannel::builder(codec.clone())
            .max_message(max(case))
            .start(mine)
            .unwrap();
        let (_reader, mut writer) = theirs.into_halves();
        for chunk in chunks(case) {
            writer.write_all(&chunk).unwrap();
        }
        drop(writer);
        let mut got = Vec::new();
        let end = loop {
            match channel.receive(WAIT) {
                Ok(Some(message)) => got.push(message),
                other => break other,
            }
        };
        assert_eq!(got, messages(case), "{}", name(case));
        let expected = if flag(case, "error") {
            Err(Failure::Protocol)
        } else if flag(case, "truncated") {
            Err(Failure::PathLost)
        } else {
            Ok(None)
        };
        assert_eq!(end, expected, "{}", name(case));
    }
}

#[test]
fn a_channel_on_the_peer_frame_puts_and_takes_the_vector_bytes() {
    let vectors = vectors("peer_frame.json");
    channel_sends_the_vector_bytes(&PeerFrame, &vectors);
    channel_receives_the_vector_messages(&PeerFrame, &vectors);
}

#[test]
fn a_channel_on_the_webtransport_frame_puts_and_takes_the_vector_bytes() {
    let vectors = vectors("webtransport_frame.json");
    channel_sends_the_vector_bytes(&WebTransportFrame, &vectors);
    channel_receives_the_vector_messages(&WebTransportFrame, &vectors);
}

#[test]
fn the_two_vector_files_cover_the_same_cases_where_the_wires_agree() {
    let peer = vectors("peer_frame.json");
    let web = vectors("webtransport_frame.json");
    for (key, field) in [("encode", "hex"), ("decode", "messages")] {
        for case in cases(&peer, key) {
            let Some(twin) = cases(&web, key).iter().find(|c| name(c) == name(case)) else {
                continue;
            };
            // The same body under the same limit: equal lengths, opposite byte order.
            if key == "encode" && !flag(case, "error") && !flag(twin, "error") {
                let one = bytes(case[field].as_str().unwrap());
                let other = bytes(twin[field].as_str().unwrap());
                assert_eq!(one[4..], other[4..], "{}", name(case));
                let mut reversed = other[..4].to_vec();
                reversed.reverse();
                assert_eq!(one[..4], reversed[..], "{}", name(case));
            }
        }
    }
}
