#!/usr/bin/env python3
"""Write the golden on-disk fixtures from the formats' written specs, with
Python's zlib.crc32 (IEEE CRC-32, as crc32fast) — an implementation
independent of the Rust code. The Rust tests require their reader to read
these exactly and their writer to reproduce them byte for byte, so a
dependency bump or a code change that alters a frame, a checksum or the
snapshot format fails the gate (review U5, D-046). Regenerate only for a
deliberate, versioned format change:

    python3 -I crates/durable-journal/tests/fixtures/make_fixtures.py
"""
import os
import struct
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
ENGINE = os.path.join(HERE, "..", "..", "..", "durable-engine", "tests", "fixtures")

PAYLOADS = [b'{"run":1,"event":"golden-0"}', b'{"run":2,"event":"golden-1"}', b'{"run":3,"event":"golden-2"}']


def v1_frame(seq, payload):
    return struct.pack(">HIQI", 1, len(payload), seq, zlib.crc32(payload)) + payload


def v2_frame(seq, batch_len, payload):
    head = struct.pack(">HIQII", 2, len(payload), seq, batch_len, zlib.crc32(payload))
    return head + struct.pack(">I", zlib.crc32(head)) + payload


def v2_batch(first_seq, payloads):
    batch_len = sum(26 + len(p) for p in payloads)
    return b"".join(v2_frame(first_seq + i, batch_len, p) for i, p in enumerate(payloads))


def s(x):
    b = x.encode()
    return struct.pack(">I", len(b)) + b


def drix():
    out = b"DRIX" + struct.pack(">HQQ", 1, 3, 2)
    # run 1: queued, a 3-byte input
    out += struct.pack(">Q", 1) + s("golden-1") + struct.pack(">II", 1, 1) + s("a") + s("h")
    out += struct.pack(">I", 3) + b"\x00\x01\xff" + b"\x00" + struct.pack(">Q", 5) + b"\x00"
    # run 2: done (its input already dropped), finished at a fixed time
    out += struct.pack(">Q", 2) + s("golden-2") + struct.pack(">II", 2, 1) + s("b") + s("h2")
    out += struct.pack(">I", 0) + b"\x04" + b"\x01" + struct.pack(">Q", 1_700_000_000_000)
    return out


def snapshot(covered_seq, payload):
    return b"DRSNAP\x00\x02" + struct.pack(">QQI", covered_seq, len(payload), zlib.crc32(payload)) + payload


with open(os.path.join(HERE, "v1.log"), "wb") as f:
    f.write(b"".join(v1_frame(i + 1, p) for i, p in enumerate(PAYLOADS)))
with open(os.path.join(HERE, "v2.log"), "wb") as f:
    # one single append (a batch of one), then one batch of two
    f.write(v2_batch(1, PAYLOADS[:1]) + v2_batch(2, PAYLOADS[1:]))
with open(os.path.join(HERE, "snapshot.bin"), "wb") as f:
    f.write(snapshot(7, b"golden snapshot payload"))
with open(os.path.join(ENGINE, "index.drix"), "wb") as f:
    f.write(drix())
assert zlib.crc32(b"123456789") == 0xCBF43926
print("fixtures written")
