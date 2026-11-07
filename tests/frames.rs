//! The length-prefix codec on arbitrary input, in every configuration: it returns frames or an error,
//! never panics, never exceeds its bounds, and round-trips any message sequence however it is split.

mod support;

use coqui_transport::frame::{Codec, Decode, FrameError, LengthPrefix};
use proptest::prelude::*;
use support::{Preset, presets};

/// Every message `decoder` yields from `chunks`, and whether it failed.
fn decode_all<D: Decode>(decoder: &mut D, chunks: &[Vec<u8>]) -> (Vec<Vec<u8>>, bool) {
    let mut messages = Vec::new();
    for chunk in chunks {
        decoder.push(chunk);
        loop {
            match decoder.next_frame() {
                Ok(Some(message)) => messages.push(message),
                Ok(None) => break,
                Err(FrameError::Size) => return (messages, true),
                Err(other) => panic!("{other:?}"),
            }
        }
    }
    (messages, false)
}

/// Cuts `bytes` at the given points, in order.
fn split(bytes: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
    let mut points: Vec<usize> = cuts.iter().map(|cut| cut % (bytes.len() + 1)).collect();
    points.sort_unstable();
    let mut chunks = Vec::new();
    let mut from = 0;
    for point in points {
        chunks.push(bytes[from..point].to_vec());
        from = point;
    }
    chunks.push(bytes[from..].to_vec());
    chunks
}

/// What `preset` can carry: no message of 256 bytes or more on a `u8` prefix.
fn fits(preset: &Preset, message: &[u8]) -> bool {
    (preset.empty_allowed || !message.is_empty()) && (preset.width > 1 || message.len() < 256)
}

fn round_trip(preset: &Preset, messages: &[Vec<u8>], cuts: &[usize]) {
    let messages: Vec<Vec<u8>> = messages
        .iter()
        .filter(|message| fits(preset, message))
        .cloned()
        .collect();
    let mut wire = Vec::new();
    for message in &messages {
        wire.extend(preset.codec.encode(message, 300).unwrap());
    }
    let mut decoder = preset.codec.decoder(300);
    let (got, failed) = decode_all(&mut decoder, &split(&wire, cuts));
    assert!(!failed, "{}", preset.name);
    assert_eq!(got, messages, "{}", preset.name);
    assert_eq!(decoder.buffered(), 0, "{}", preset.name);
}

fn never_panics(preset: &Preset, chunks: &[Vec<u8>], maximum: usize) {
    let mut decoder = preset.codec.decoder(maximum);
    let pushed: usize = chunks.iter().map(Vec::len).sum();
    let (messages, failed) = decode_all(&mut decoder, chunks);
    let consumed: usize = messages
        .iter()
        .map(|message| preset.width + message.len())
        .sum();
    for message in &messages {
        assert!(message.len() <= maximum, "{}", preset.name);
        assert!(
            preset.empty_allowed || !message.is_empty(),
            "{}",
            preset.name
        );
    }
    if failed {
        assert_eq!(decoder.buffered(), 0, "{}", preset.name);
        assert_eq!(
            decoder.next_frame(),
            Err(FrameError::Size),
            "{}",
            preset.name
        );
    } else {
        // What is held is exactly what no returned message accounts for.
        assert_eq!(decoder.buffered(), pushed - consumed, "{}", preset.name);
    }
}

proptest! {
    #[test]
    fn arbitrary_bytes_return_frames_or_an_error_and_never_panic(
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16),
        maximum in 0_usize..128,
    ) {
        for preset in presets() {
            never_panics(&preset, &chunks, maximum);
        }
    }

    #[test]
    fn any_message_sequence_round_trips_through_any_split(
        messages in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 0..8),
        cuts in prop::collection::vec(any::<usize>(), 0..12),
    ) {
        for preset in presets() {
            round_trip(&preset, &messages, &cuts);
        }
    }

    #[test]
    fn a_header_over_the_bound_fails_before_its_body_arrives(
        maximum in 0_usize..200,
        excess in 1_usize..50,
    ) {
        for preset in presets() {
            let mut decoder = preset.codec.decoder(maximum);
            decoder.push(&preset.header(maximum + excess));
            prop_assert_eq!(decoder.next_frame(), Err(FrameError::Size), "{}", preset.name);
        }
    }

    #[test]
    fn an_encoder_refuses_exactly_what_its_decoder_would(
        length in 0_usize..300,
        maximum in 0_usize..300,
    ) {
        let message = vec![7_u8; length];
        for preset in presets() {
            let accepted = preset.codec.encode(&message, maximum).is_ok();
            let mut decoder = preset.codec.decoder(maximum);
            if length < 256 || preset.width > 1 {
                decoder.push(&preset.header(length));
                // A header the decoder passes (it then waits for the body) is one the encoder accepts.
                prop_assert_eq!(decoder.next_frame().is_ok(), accepted, "{}", preset.name);
            } else {
                prop_assert!(!accepted, "{}", preset.name);
            }
        }
    }
}

#[test]
fn a_decoder_holds_no_more_than_what_was_pushed_and_not_returned() {
    let codec = LengthPrefix::u32_be();
    let mut decoder = codec.decoder(1 << 20);
    let frame = codec.encode(&[1; 1000], 1 << 20).unwrap();
    for _ in 0..100 {
        decoder.push(&frame);
        assert!(decoder.next_frame().unwrap().is_some());
        assert_eq!(decoder.buffered(), 0);
    }
    decoder.push(&frame[..500]);
    assert_eq!(decoder.next_frame(), Ok(None));
    assert_eq!(decoder.buffered(), 500);
}

#[test]
fn a_lowered_maximum_applies_to_the_next_header_checked() {
    let codec = LengthPrefix::u32_be();
    let mut decoder = codec.decoder(10);
    decoder.push(&codec.encode(b"12345", 10).unwrap());
    decoder.set_maximum(4);
    assert_eq!(decoder.next_frame(), Err(FrameError::Size));
}
