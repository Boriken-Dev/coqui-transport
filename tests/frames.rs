//! The codecs on arbitrary input: they return frames or an error, never panic, never exceed their
//! bounds, and round-trip any message sequence however it is split.

use coqui_transport::frame::{Codec, Decode, FrameError, PeerFrame, WebTransportFrame};
use proptest::prelude::*;

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

fn round_trip<C: Codec>(codec: &C, messages: &[Vec<u8>], cuts: &[usize], maximum: usize) {
    let mut wire = Vec::new();
    for message in messages {
        wire.extend(codec.encode(message, maximum).unwrap());
    }
    let mut decoder = codec.decoder(maximum);
    let (got, failed) = decode_all(&mut decoder, &split(&wire, cuts));
    assert!(!failed);
    assert_eq!(got, messages);
    assert_eq!(decoder.buffered(), 0);
}

fn never_panics<C: Codec>(codec: &C, chunks: &[Vec<u8>], maximum: usize, empty_allowed: bool) {
    let mut decoder = codec.decoder(maximum);
    let pushed: usize = chunks.iter().map(Vec::len).sum();
    let (messages, failed) = decode_all(&mut decoder, chunks);
    let consumed: usize = messages.iter().map(|message| 4 + message.len()).sum();
    for message in &messages {
        assert!(message.len() <= maximum);
        assert!(empty_allowed || !message.is_empty());
    }
    if failed {
        assert_eq!(decoder.buffered(), 0);
        assert_eq!(decoder.next_frame(), Err(FrameError::Size));
    } else {
        // What is held is exactly what no returned message accounts for.
        assert_eq!(decoder.buffered(), pushed - consumed);
    }
}

proptest! {
    #[test]
    fn arbitrary_bytes_return_frames_or_an_error_and_never_panic(
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16),
        maximum in 0_usize..128,
    ) {
        never_panics(&PeerFrame, &chunks, maximum, true);
        never_panics(&WebTransportFrame, &chunks, maximum, false);
    }

    #[test]
    fn any_message_sequence_round_trips_through_any_split_on_the_peer_wire(
        messages in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 0..8),
        cuts in prop::collection::vec(any::<usize>(), 0..12),
    ) {
        round_trip(&PeerFrame, &messages, &cuts, 300);
    }

    #[test]
    fn any_message_sequence_round_trips_through_any_split_on_the_webtransport_wire(
        messages in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..300), 0..8),
        cuts in prop::collection::vec(any::<usize>(), 0..12),
    ) {
        round_trip(&WebTransportFrame, &messages, &cuts, 300);
    }

    #[test]
    fn a_header_over_the_bound_fails_before_its_body_arrives(
        maximum in 0_usize..1024,
        excess in 1_u32..1024,
    ) {
        let declared = u32::try_from(maximum).unwrap() + excess;
        let mut peer = PeerFrame.decoder(maximum);
        peer.push(&declared.to_be_bytes());
        prop_assert_eq!(peer.next_frame(), Err(FrameError::Size));
        let mut web = WebTransportFrame.decoder(maximum);
        web.push(&declared.to_le_bytes());
        prop_assert_eq!(web.next_frame(), Err(FrameError::Size));
    }

    #[test]
    fn an_encoder_refuses_exactly_what_its_decoder_would(
        length in 0_usize..64,
        maximum in 0_usize..64,
    ) {
        let message = vec![7_u8; length];
        for (encoded, empty_allowed) in [
            (PeerFrame.encode(&message, maximum), true),
            (WebTransportFrame.encode(&message, maximum), false),
        ] {
            let fits = length <= maximum && (empty_allowed || length > 0);
            prop_assert_eq!(encoded.is_ok(), fits);
        }
    }
}

#[test]
fn a_decoder_holds_no_more_than_what_was_pushed_and_not_returned() {
    let mut decoder = PeerFrame.decoder(1 << 20);
    let frame = PeerFrame.encode(&[1; 1000], 1 << 20).unwrap();
    for _ in 0..100 {
        decoder.push(&frame);
        assert!(decoder.next_frame().unwrap().is_some());
        assert_eq!(decoder.buffered(), 0);
    }
    decoder.push(&frame[..500]);
    assert_eq!(decoder.next_frame(), Ok(None));
    assert_eq!(decoder.buffered(), 500);
}
