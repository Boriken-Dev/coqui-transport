//! The versioned frame's bytes, against vectors made by a second implementation (tools/frame_vectors.py),
//! and its decoder against arbitrary input.

use coqui_transport::frame::versioned::{
    DecodeError, Frame, HEADER, Kind, OVERHEAD, TRAILER, VERSION, crc32c, tagged,
};
use proptest::prelude::*;
use serde_json::Value;

fn vectors() -> Value {
    serde_json::from_str(include_str!("vectors/versioned_frame.json")).expect("the vector file")
}

fn bytes(text: &Value) -> Vec<u8> {
    let text = text.as_str().expect("a hex string");
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

fn kind(name: &Value) -> Kind {
    match name.as_str().expect("a kind name") {
        "message" => Kind::Message,
        "datagram" => Kind::Datagram,
        "close" => Kind::Close,
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn the_layout_constants_are_what_the_specification_says() {
    assert_eq!(VERSION, 1);
    assert_eq!((HEADER, TRAILER, OVERHEAD), (8, 4, 12));
    assert_eq!(vectors()["version"], 1);
    assert_eq!(
        (
            Kind::Message.byte(),
            Kind::Datagram.byte(),
            Kind::Close.byte()
        ),
        (0, 1, 2)
    );
    assert_eq!(Kind::from_byte(3), None);
}

#[test]
fn the_checksum_has_the_published_check_value() {
    let check = &vectors()["crc32c_check"];
    let input = check["input"].as_str().expect("an input");
    assert_eq!(
        format!("{:08x}", crc32c(input.as_bytes())),
        check["crc"].as_str().expect("a crc")
    );
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(crc32c(b""), 0);
}

#[test]
fn every_vector_frame_encodes_to_its_bytes_and_decodes_back() {
    let vectors = vectors();
    let frames = vectors["frames"].as_array().expect("frames");
    assert!(frames.len() >= 6);
    for case in frames {
        let name = case["name"].as_str().expect("a name");
        let expected = bytes(&case["frame"]);
        let frame = Frame::new(
            kind(&case["kind"]),
            u16::try_from(case["channel"].as_u64().expect("a channel")).expect("fits"),
            u32::try_from(case["sequence"].as_u64().expect("a sequence")).expect("fits"),
            bytes(&case["payload"]),
        );
        assert_eq!(frame.encode(), expected, "encoding {name}");
        assert_eq!(Frame::decode(&expected), Ok(frame), "decoding {name}");
    }
}

#[test]
fn every_invalid_vector_is_refused_for_its_reason() {
    let vectors = vectors();
    for case in vectors["invalid"].as_array().expect("invalid") {
        let name = case["name"].as_str().expect("a name");
        let expected = match case["error"].as_str().expect("an error") {
            "short" => DecodeError::Short,
            "checksum" => DecodeError::Checksum,
            "payload" => DecodeError::Payload,
            other => {
                let (label, number) = other.split_once(':').expect("a label and a number");
                let number = number.parse().expect("a byte");
                match label {
                    "version" => DecodeError::Version(number),
                    "kind" => DecodeError::Kind(number),
                    _ => panic!("unknown error {other}"),
                }
            }
        };
        assert_eq!(
            Frame::decode(&bytes(&case["frame"])),
            Err(expected),
            "decoding {name}"
        );
    }
}

#[test]
fn the_tagged_vectors_are_one_byte_before_the_payload() {
    let vectors = vectors();
    for case in vectors["tagged"].as_array().expect("tagged") {
        let tag = match case["kind"].as_str().expect("a kind") {
            "message" => tagged::MESSAGE,
            "datagram" => tagged::DATAGRAM,
            other => panic!("unknown kind {other}"),
        };
        let wire = bytes(&case["wire"]);
        assert_eq!(wire[0], tag);
        assert_eq!(wire[1..], bytes(&case["payload"])[..]);
    }
}

proptest! {
    #[test]
    fn a_frame_decodes_to_what_was_encoded(
        channel in any::<u16>(),
        sequence in any::<u32>(),
        payload in proptest::collection::vec(any::<u8>(), 0..600),
        which in 0_u8..2,
    ) {
        let kind = if which == 0 { Kind::Message } else { Kind::Datagram };
        let frame = Frame::new(kind, channel, sequence, payload);
        let wire = frame.encode();
        prop_assert_eq!(wire.len(), OVERHEAD + frame.payload.len());
        prop_assert_eq!(Frame::decode(&wire), Ok(frame));
    }

    #[test]
    fn any_single_flipped_bit_is_found(
        sequence in any::<u32>(),
        payload in proptest::collection::vec(any::<u8>(), 0..200),
        at in any::<usize>(),
        bit in 0_u8..8,
    ) {
        let mut wire = Frame::new(Kind::Message, 4, sequence, payload).encode();
        let at = at % wire.len();
        wire[at] ^= 1 << bit;
        // The version byte is judged before the checksum; every other bit is the checksum's to find.
        let expected = if at == 0 { DecodeError::Version(wire[0]) } else { DecodeError::Checksum };
        let found = Frame::decode(&wire);
        // A flipped kind bit still fails its checksum first.
        prop_assert_eq!(found, Err(expected));
    }

    #[test]
    fn arbitrary_bytes_never_panic(wire in proptest::collection::vec(any::<u8>(), 0..64)) {
        let _ = Frame::decode(&wire);
    }
}
