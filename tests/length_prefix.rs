//! The length-prefix codec in every configuration (width, byte order, empty rule), through the codec
//! and through a `StreamChannel` on a real stream.

mod support;

use std::io::{Read, Write};

use coqui_transport::frame::{Codec, Decode, FrameError, LengthPrefix};
use coqui_transport::{Channel, Failure, StreamChannel, memory};
use support::{WAIT, decode_cases, encode_cases, presets};

#[test]
fn the_named_constructors_are_what_they_say() {
    let ab = |codec: LengthPrefix| codec.encode(b"ab", 10).unwrap();
    assert_eq!(ab(LengthPrefix::u32_be()), [0, 0, 0, 2, b'a', b'b']);
    assert_eq!(ab(LengthPrefix::u32_le()), [2, 0, 0, 0, b'a', b'b']);
    assert_eq!(ab(LengthPrefix::u16_be()), [0, 2, b'a', b'b']);
    assert_eq!(ab(LengthPrefix::u16_le()), [2, 0, b'a', b'b']);
    assert_eq!(ab(LengthPrefix::u8()), [2, b'a', b'b']);
}

#[test]
fn empty_messages_are_allowed_unless_the_codec_refuses_them() {
    for codec in [
        LengthPrefix::u8(),
        LengthPrefix::u16_be(),
        LengthPrefix::u16_le(),
        LengthPrefix::u32_be(),
        LengthPrefix::u32_le(),
    ] {
        assert!(codec.empty_allowed());
        assert!(!codec.refuse_empty().empty_allowed());
        assert!(codec.refuse_empty().allow_empty().empty_allowed());
    }
}

#[test]
fn the_accessors_describe_the_codec() {
    let codec = LengthPrefix::u16_le().refuse_empty();
    assert_eq!(codec.width(), 2);
    assert!(!codec.is_big_endian());
    assert!(!codec.empty_allowed());
    assert_eq!(LengthPrefix::u32_be().width(), 4);
    assert!(LengthPrefix::u32_be().is_big_endian());
    assert_eq!(LengthPrefix::u8().width(), 1);
}

#[test]
fn every_encode_case_holds_in_every_configuration() {
    for preset in presets() {
        for case in encode_cases(&preset) {
            let got = preset.codec.encode(&case.body, case.max);
            match case.wire {
                Some(wire) => assert_eq!(got, Ok(wire), "{}: {}", preset.name, case.name),
                None => assert_eq!(got, Err(FrameError::Size), "{}: {}", preset.name, case.name),
            }
        }
    }
}

#[test]
fn every_decode_case_holds_in_every_configuration() {
    for preset in presets() {
        for case in decode_cases(&preset) {
            let label = format!("{}: {}", preset.name, case.name);
            let mut decoder = preset.codec.decoder(case.max);
            let mut got = Vec::new();
            let mut failed = false;
            'chunks: for chunk in &case.chunks {
                decoder.push(chunk);
                loop {
                    match decoder.next_frame() {
                        Ok(Some(message)) => got.push(message),
                        Ok(None) => break,
                        Err(FrameError::Size) => {
                            failed = true;
                            break 'chunks;
                        }
                        Err(other) => panic!("{label}: {other:?}"),
                    }
                }
            }
            assert_eq!(got, case.messages, "{label}");
            assert_eq!(failed, case.error, "{label}");
            assert_eq!(!failed && decoder.buffered() > 0, case.truncated, "{label}");
        }
    }
}

#[test]
fn a_channel_puts_the_frame_on_the_stream_and_refuses_what_the_codec_refuses() {
    for preset in presets() {
        for case in encode_cases(&preset) {
            let label = format!("{}: {}", preset.name, case.name);
            let (mine, theirs) = memory::pipe(1 << 17);
            let channel = StreamChannel::builder(preset.codec)
                .max_message(case.max)
                .start(mine)
                .unwrap();
            let sent = channel.send(&case.body);
            channel.close();
            let mut wire = Vec::new();
            let (mut reader, _writer) = theirs.into_halves();
            reader.read_to_end(&mut wire).unwrap();
            match case.wire {
                Some(expected) => {
                    assert_eq!(sent, Ok(()), "{label}");
                    assert_eq!(wire, expected, "{label}");
                }
                None => {
                    assert_eq!(sent, Err(Failure::Protocol), "{label}");
                    assert!(wire.is_empty(), "{label}");
                }
            }
        }
    }
}

#[test]
fn a_channel_takes_the_messages_then_the_end_the_case_says() {
    for preset in presets() {
        for case in decode_cases(&preset) {
            let label = format!("{}: {}", preset.name, case.name);
            let (mine, theirs) = memory::pipe(1 << 17);
            let channel = StreamChannel::builder(preset.codec)
                .max_message(case.max)
                .start(mine)
                .unwrap();
            let (_reader, mut writer) = theirs.into_halves();
            for chunk in &case.chunks {
                writer.write_all(chunk).unwrap();
            }
            drop(writer);
            let mut got = Vec::new();
            let end = loop {
                match channel.receive(WAIT) {
                    Ok(Some(message)) => got.push(message),
                    other => break other,
                }
            };
            assert_eq!(got, case.messages, "{label}");
            let expected = if case.error {
                Err(Failure::Protocol)
            } else if case.truncated {
                Err(Failure::PathLost)
            } else {
                Ok(None)
            };
            assert_eq!(end, expected, "{label}");
        }
    }
}

#[test]
fn the_two_byte_orders_write_the_same_length_in_opposite_order() {
    let body = vec![1; 258];
    let big = LengthPrefix::u32_be().encode(&body, 1000).unwrap();
    let little = LengthPrefix::u32_le().encode(&body, 1000).unwrap();
    assert_eq!(big[..4], [0, 0, 1, 2]);
    assert_eq!(little[..4], [2, 1, 0, 0]);
    assert_eq!(big[4..], little[4..]);
}

#[test]
fn a_frame_in_the_other_byte_order_is_not_read_as_this_one() {
    let mut decoder = LengthPrefix::u32_be().decoder(1 << 20);
    decoder.push(&[5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o']);
    assert_eq!(decoder.next_frame(), Err(FrameError::Size));
}
