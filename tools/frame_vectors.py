"""Writes tests/vectors/versioned_frame.json: the bytes of fixed frames, made here in Python from the
layout in docs/frame.md and a CRC-32C written from its definition, so the Rust code is checked against a
second implementation.

    python tools/frame_vectors.py > tests/vectors/versioned_frame.json

CRC-32C: polynomial 0x1EDC6F41 reflected (0x82F63B78), initial value and final xor 0xFFFFFFFF; its check value
for b"123456789" is 0xE3069283.
"""

import json
import struct
import sys

VERSION = 1
KINDS = {"message": 0, "datagram": 1, "close": 2}


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def frame(kind: int, channel: int, sequence: int, payload: bytes, version: int = VERSION) -> bytes:
    body = struct.pack("<BBHI", version, kind, channel, sequence) + payload
    return body + struct.pack("<I", crc32c(body))


def valid(name: str, kind: str, channel: int, sequence: int, payload: bytes) -> dict:
    return {
        "name": name,
        "kind": kind,
        "channel": channel,
        "sequence": sequence,
        "payload": payload.hex(),
        "frame": frame(KINDS[kind], channel, sequence, payload).hex(),
    }


def invalid(name: str, error: str, data: bytes) -> dict:
    return {"name": name, "error": error, "frame": data.hex()}


def main() -> None:
    pattern = bytes(range(200))
    frames = [
        valid("an empty message on channel 0", "message", 0, 0, b""),
        valid("a short message", "message", 1, 7, b"hello"),
        valid("a datagram on the highest channel", "datagram", 65535, 4294967295, b"ping"),
        valid("a close", "close", 3, 9, b""),
        valid("a message holding every byte value below 200", "message", 2, 65536, pattern),
        valid("a message of sixteen zero bytes", "message", 256, 1, bytes(16)),
    ]
    good = frame(0, 5, 3, b"abc")
    flipped = bytearray(good)
    flipped[9] ^= 0x01
    wrong_trailer = bytearray(good)
    wrong_trailer[-1] ^= 0x80
    invalid_frames = [
        invalid("eleven bytes", "short", good[:11]),
        invalid("nothing", "short", b""),
        invalid("version 2", "version:2", frame(0, 5, 3, b"abc", version=2)),
        invalid("version 0", "version:0", frame(0, 5, 3, b"abc", version=0)),
        invalid("a payload bit flipped", "checksum", bytes(flipped)),
        invalid("a checksum bit flipped", "checksum", bytes(wrong_trailer)),
        invalid("kind 3 with a valid checksum", "kind:3", frame(3, 5, 3, b"abc")),
        invalid("kind 255 with a valid checksum", "kind:255", frame(255, 5, 3, b"")),
        invalid("a close that carries a payload", "payload", frame(2, 5, 3, b"x")),
    ]
    tagged = [
        {"name": "a message", "kind": "message", "payload": b"hi".hex(), "wire": (b"\x00" + b"hi").hex()},
        {"name": "an empty message", "kind": "message", "payload": "", "wire": "00"},
        {"name": "a datagram", "kind": "datagram", "payload": b"hi".hex(), "wire": (b"\x01" + b"hi").hex()},
    ]
    document = {
        "crc32c_check": {"input": "123456789", "crc": "e3069283", "computed": format(crc32c(b"123456789"), "08x")},
        "version": VERSION,
        "frames": frames,
        "invalid": invalid_frames,
        "tagged": tagged,
    }
    assert document["crc32c_check"]["crc"] == document["crc32c_check"]["computed"]
    del document["crc32c_check"]["computed"]
    sys.stdout.reconfigure(newline="\n")
    print(json.dumps(document, indent=2))


if __name__ == "__main__":
    main()
