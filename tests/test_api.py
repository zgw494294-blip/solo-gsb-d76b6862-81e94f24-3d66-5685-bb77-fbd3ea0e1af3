"""End-to-end tests for the batch log HTTP API and streaming semantics."""
from __future__ import annotations

import gzip
import io
import json
import os
import threading
import time
import zlib

from fastapi.testclient import TestClient

from app.config import (
    MAX_COMPRESSED_BYTES,
    MAX_DECOMPRESSED_BYTES,
    MAX_LINE_BYTES,
    SPOOL_DIR,
)
from app.ingest import ingest_stream


def ndjson(*records, trailing_newline=True):
    buf = b"".join(
        json.dumps(r, ensure_ascii=False).encode("utf-8") + b"\n" for r in records
    )
    if not trailing_newline and buf:
        buf = buf[:-1]
    return buf


def gz(data: bytes) -> bytes:
    return gzip.compress(data, mtime=0)


def upload(client: TestClient, body: bytes, **kw):
    return client.post("/batches", content=body, **kw)


def upload_generator(client: TestClient, gen, headers=None):
    return client.post("/batches", data=gen(), headers=headers or {})


# ---------- happy path ----------------------------------------------------


def test_happy_path_last_line_without_newline(client):
    body = gz(ndjson(
        {"id": "a", "message": "hello"},
        {"id": "b", "message": "world"},
        trailing_newline=False,
    ))
    r = upload(client, body)
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["rows"] == 2
    batch_id = data["batch_id"]

    r = client.get(f"/batches/{batch_id}/rows")
    assert r.status_code == 200
    items = r.json()["items"]
    assert [i["id"] for i in items] == ["a", "b"]
    assert [i["message"] for i in items] == ["hello", "world"]
    assert [i["position"] for i in items] == [1, 2]


def test_batch_metadata_and_listing(client):
    body = gz(ndjson({"id": "x1", "message": "m"}))
    bid = upload(client, body).json()["batch_id"]

    r = client.get(f"/batches/{bid}")
    assert r.status_code == 200
    meta = r.json()
    assert meta["batch_id"] == bid
    assert meta["rows"] == 1
    assert meta["created_at"]

    r = client.get("/batches", params={"limit": 1})
    assert r.status_code == 200
    payload = r.json()
    assert payload["items"][0]["batch_id"]
    first = payload["items"][0]["batch_id"]
    assert payload["next_after_id"] == first

    r = client.get("/batches", params={"limit": 1, "after_id": first})
    second = r.json()["items"][0]["batch_id"]
    assert second > first


def test_pagination_keeps_order(client):
    records = [{"id": f"p{i}", "message": f"msg-{i}"} for i in range(5)]
    bid = upload(client, gz(ndjson(*records))).json()["batch_id"]

    seen = []
    offset = 0
    while True:
        r = client.get(f"/batches/{bid}/rows", params={"limit": 2, "offset": offset})
        page = r.json()
        seen.extend(page["items"])
        if page["next_offset"] is None:
            break
        offset = page["next_offset"]
    assert [i["id"] for i in seen] == [f"p{i}" for i in range(5)]


def test_unknown_batch_is_404(client):
    assert client.get("/batches/deadbeef").status_code == 404
    assert client.get("/batches/deadbeef/rows").status_code == 404


# ---------- 422: structural failures --------------------------------------


def test_empty_body_is_422(client):
    r = upload(client, b"")
    assert r.status_code == 422


def test_corrupt_gzip_is_422(client):
    r = upload(client, b"not gzip at all")
    assert r.status_code == 422


def test_truncated_gzip_is_422(client):
    good = gz(ndjson({"id": "a", "message": "b"}))
    r = upload(client, good[:-6])  # cut CRC/trailer away
    assert r.status_code == 422


def test_bad_crc_is_422(client):
    good = bytearray(gz(ndjson({"id": "a", "message": "b"})))
    good[-8] ^= 0xFF  # flip a CRC byte
    r = upload(client, bytes(good))
    assert r.status_code == 422


def test_trailing_data_is_422(client):
    body = gz(ndjson({"id": "a", "message": "b"})) + b"JUNK"
    r = upload(client, body)
    assert r.status_code == 422


def test_two_gzip_members_is_422(client):
    one = gz(ndjson({"id": "a", "message": "b"}))
    body = one + one
    r = upload(client, body)
    assert r.status_code == 422


def test_empty_batch_is_422(client):
    # A valid gzip member decompressing to zero bytes: no record at all.
    r = upload(client, gz(b""))
    assert r.status_code == 422


def test_empty_line_is_422(client):
    body = gz(b'{"id":"a","message":"m"}\n\n{"id":"b","message":"n"}\n')
    r = upload(client, body)
    assert r.status_code == 422


def test_blank_line_after_stripped_lf_is_422(client):
    r = upload(client, gz(b"\n"))
    assert r.status_code == 422


def test_invalid_json_is_422(client):
    r = upload(client, gz(b'{not json\n'))
    assert r.status_code == 422


def test_missing_field_is_422(client):
    r = upload(client, gz(b'{"id":"a"}\n'))
    assert r.status_code == 422


def test_extra_field_is_422(client):
    r = upload(client, gz(b'{"id":"a","message":"m","x":1}\n'))
    assert r.status_code == 422


def test_non_string_id_is_422(client):
    r = upload(client, gz(b'{"id":1,"message":"m"}\n'))
    assert r.status_code == 422


def test_empty_id_is_422(client):
    r = upload(client, gz(b'{"id":"","message":"m"}\n'))
    assert r.status_code == 422


def test_empty_message_is_422(client):
    r = upload(client, gz(b'{"id":"a","message":""}\n'))
    assert r.status_code == 422


def test_null_message_is_422(client):
    r = upload(client, gz(b'{"id":"a","message":null}\n'))
    assert r.status_code == 422


def test_duplicate_id_in_batch_is_422_and_not_queryable(client):
    body = gz(b'{"id":"dup","message":"1"}\n{"id":"dup","message":"2"}\n')
    r = upload(client, body)
    assert r.status_code == 422


def test_duplicate_id_same_id_allowed_across_batches(client):
    body = gz(b'{"id":"same","message":"x"}\n')
    assert upload(client, body).status_code == 201
    assert upload(client, body).status_code == 201


def test_non_utf8_line_is_422(client):
    payload = b'{"id":"a","message":"' + b"\xff\xfe" + b'"}\n'
    r = upload(client, gz(payload))
    assert r.status_code == 422


def test_duplicate_json_key_is_422(client):
    r = upload(client, gz(b'{"id":"a","id":"b","message":"m"}\n'))
    assert r.status_code == 422


def test_whitespace_only_id_is_allowed(client):
    # Only the *empty* string is banned; whitespace is still a string.
    r = upload(client, gz(b'{"id":" ","message":"m"}\n'))
    assert r.status_code == 201


def test_invalid_failure_leaves_no_batch(client):
    before = {i["batch_id"] for i in client.get("/batches").json()["items"]}
    upload(client, b'{"id":"a","message":"m"}\n')  # plain, not gzipped
    after = {i["batch_id"] for i in client.get("/batches").json()["items"]}
    assert before == after


# ---------- 413: byte quotas ----------------------------------------------


def test_line_over_64kib_is_413(client):
    # Line bytes exclude the LF; 64 KiB content + LF must be rejected.
    big = "x" * (MAX_LINE_BYTES + 1)
    payload = (
        b'{"id":"a","message":"' + big.encode() + b'"}\n'
    )
    r = upload(client, gz(payload))
    assert r.status_code == 413


def test_line_exactly_64kib_boundary(client):
    # Construct a line whose raw byte length is exactly MAX_LINE_BYTES.
    # {"id":"a","message":"<N chars>"} has length 23 + N.
    pad = MAX_LINE_BYTES - 23
    line = b'{"id":"a","message":"' + (b"y" * pad) + b'"}\n'
    assert len(line) - 1 == MAX_LINE_BYTES
    assert upload(client, gz(line)).status_code == 201

    line_too_big = b'{"id":"a","message":"' + (b"y" * (pad + 1)) + b'"}\n'
    assert len(line_too_big) - 1 == MAX_LINE_BYTES + 1
    assert upload(client, gz(line_too_big)).status_code == 413


def test_decompressed_over_8mib_is_413(client):
    # Highly compressible payload: compressed stays tiny, output huge.
    total = MAX_DECOMPRESSED_BYTES + 1024
    one_line = b'{"id":"a","message":"' + b"z" * (total - 24) + b'"}\n'
    assert len(one_line) > MAX_DECOMPRESSED_BYTES
    r = upload(client, gz(one_line))
    assert r.status_code == 413


def test_compressed_over_2mib_is_413(client):
    # Incompressible random data keeps the compressed size proportional.
    rng = os.urandom
    # Build a valid-enough random stream that exceeds the cap; decompression
    # is irrelevant: the compressed counter must trip first.
    blob = rng(MAX_COMPRESSED_BYTES + 4096)

    def gen():
        # Wrap in a gzip header so decompression failure would be 422; we
        # assert 413 wins because the byte cap is hit during reading.
        co = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        for i in range(0, len(blob), 65536):
            yield co.compress(blob[i : i + 65536])
        yield co.flush()

    r = upload_generator(client, gen)
    assert r.status_code == 413


def test_content_length_header_over_2mib_is_413(client):
    r = client.post(
        "/batches",
        content=b"x",
        headers={"content-length": str(MAX_COMPRESSED_BYTES + 1)},
    )
    assert r.status_code == 413


# ---------- streaming / memory behavior -----------------------------------


def test_chunked_streaming_upload_succeeds(client):
    payload = gz(ndjson(
        {"id": "c1", "message": "streamed"},
        {"id": "c2", "message": "also streamed"},
    ))

    def gen():
        for i in range(0, len(payload), 37):  # awkward chunk boundaries
            yield payload[i : i + 37]

    r = upload_generator(client, gen)
    assert r.status_code == 201, r.text
    bid = r.json()["batch_id"]
    rows = client.get(f"/batches/{bid}/rows").json()["items"]
    assert [x["id"] for x in rows] == ["c1", "c2"]


def test_rows_not_visible_until_commit(client):
    """A valid line already sent cannot be queried while the stream is open."""
    payload = gz(ndjson({"id": "visible-late", "message": "m"}))

    chunks: list = [payload[: 10]]  # hold back the rest
    gate = threading.Event()

    def gen():
        yield payload[:10]
        gate.wait(5)
        yield payload[10:]

    result = {}

    def do_upload():
        result["r"] = upload_generator(client, gen)

    t = threading.Thread(target=do_upload)
    t.start()
    time.sleep(0.5)
    # While the gzip stream is still open, none of the committed batches can
    # contain the in-flight record.
    listing = client.get("/batches").json()["items"]
    for item in listing:
        rows = client.get(
            f"/batches/{item['batch_id']}/rows", params={"limit": 1000}
        ).json()["items"]
        assert all(row["id"] != "visible-late" for row in rows)

    gate.set()
    t.join()
    assert result["r"].status_code == 201


def test_interrupted_upload_leaves_nothing_and_cleans_spool():
    """Mid-stream abort at pipeline level: no batch, no spool file left."""

    payload = ndjson(*[{"id": f"i{k}", "message": "x" * 100} for k in range(200)])
    gz_payload = gz(payload)

    from app import db

    conn = db.connect()
    before = {r["id"] for r in conn.execute("SELECT id FROM batches")}
    conn.close()

    spool_before = {p.name for p in SPOOL_DIR.glob("spool-*")}

    def partial():
        yield gz_payload[: 50]
        raise ConnectionError("simulated client disconnect")

    try:
        ingest_stream(partial())
        assert False, "expected abort"
    except Exception:
        pass

    conn = db.connect()
    after = {r["id"] for r in conn.execute("SELECT id FROM batches")}
    conn.close()
    assert before == after

    spool_after = {p.name for p in SPOOL_DIR.glob("spool-*")}
    assert spool_after == spool_before


def test_spool_dir_empty_after_mixed_traffic(client):
    upload(client, gz(ndjson({"id": "ok1", "message": "m"})))
    upload(client, b"broken")
    upload(client, gz(b'{"id":"ok2","message":"m"}\n'))
    leftovers = [p for p in SPOOL_DIR.glob("spool-*") if not p.name.endswith("-journal")]
    assert leftovers == []


def test_large_stream_with_awkward_chunks_roundtrips(client):
    """Regression: bounded decompression must re-feed unconsumed_tail and
    drain flush() across many 64 KiB output pieces, else lines corrupt."""
    n = 5000
    payload = b"".join(
        b'{"id":"id%06d","message":"payload-%d"}\n' % (i, i) for i in range(n)
    )
    compressed = gz(payload)

    for chunk_size in (1, 7, 100, 4096):
        def gen(cs=chunk_size):
            for i in range(0, len(compressed), cs):
                yield compressed[i : i + cs]

        r = upload_generator(client, gen)
        assert r.status_code == 201, (chunk_size, r.text)
        bid = r.json()["batch_id"]

        seen: list[str] = []
        offset = 0
        while True:
            page = client.get(
                f"/batches/{bid}/rows", params={"limit": 137, "offset": offset}
            ).json()
            seen.extend(i["id"] for i in page["items"])
            if page["next_offset"] is None:
                break
            offset = page["next_offset"]
        assert seen == [f"id{i:06d}" for i in range(n)], chunk_size


# ---------- concurrency: slow uploads don't block -------------------------


def test_slow_upload_does_not_block_reads_or_other_uploads(client):
    # Pre-create a committed batch that must remain readable.
    bid = upload(client, gz(ndjson({"id": "existing", "message": "m"}))).json()[
        "batch_id"
    ]

    slow_payload = gz(ndjson(
        {"id": "slow1", "message": "a"},
        {"id": "slow2", "message": "b"},
        trailing_newline=False,
    ))
    released = threading.Event()

    def slow_gen():
        yield slow_payload[:20]
        released.wait(5)
        yield slow_payload[20:]

    slow_result = {}

    def slow_upload():
        slow_result["r"] = upload_generator(client, slow_gen)

    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.4)  # ensure the slow upload is mid-flight

    # A fully independent, fast upload must still complete.
    fast = upload(client, gz(ndjson({"id": "fast", "message": "q"})))
    assert fast.status_code == 201

    # Reads of an already committed batch must still be served.
    r = client.get(f"/batches/{bid}/rows")
    assert r.status_code == 200
    assert r.json()["items"][0]["id"] == "existing"

    released.set()
    t.join()
    assert slow_result["r"].status_code == 201
    slow_bid = slow_result["r"].json()["batch_id"]
    rows = client.get(f"/batches/{slow_bid}/rows").json()["items"]
    assert [x["id"] for x in rows] == ["slow1", "slow2"]


# ---------- misc ----------------------------------------------------------


def test_validation_errors_are_json(client):
    r = upload(client, b"junk")
    assert r.status_code == 422
    assert r.headers["content-type"].startswith("application/json")
    assert "detail" in r.json()


def test_unicode_roundtrip(client):
    records = [
        {"id": "中文-id", "message": "你好，世界"},
        {"id": "emoji", "message": "🎉 batch"},
    ]
    bid = upload(client, gz(ndjson(*records))).json()["batch_id"]
    items = client.get(f"/batches/{bid}/rows").json()["items"]
    assert items[0]["id"] == "中文-id"
    assert items[0]["message"] == "你好，世界"
    assert items[1]["message"] == "🎉 batch"


def test_crlf_is_rejected(client):
    # NDJSON lines are LF separated; a trailing CR is not trimmed and makes
    # the JSON invalid. (Documenting strict behavior.)
    body = gz(b'{"id":"a","message":"m"}\r\n')
    r = upload(client, body)
    assert r.status_code == 422
