"""Streaming gzip -> NDJSON ingestion.

Memory contract
---------------
* The request body is fed chunk by chunk from the ASGI receiver; the whole
  compressed payload is never buffered.
* ``zlib.decompressobj`` is drained with an explicit ``max_length`` cap so a
  small "zip bomb" header cannot make us materialize the full 8 MiB (or more)
  output in one allocation.
* Validated rows are streamed straight into an on-disk staging SQLite file;
  neither the decoded lines nor an id set are accumulated in memory.

Visibility contract
-------------------
Rows are only copied into the main database in a single transaction after the
gzip member ended cleanly, its CRC was verified by zlib and the request body
ended exactly at the member boundary. Any failure discards the staging file
and leaves no queryable batch behind.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import uuid
import zlib
from collections.abc import Iterable

from . import db
from .config import (
    MAX_COMPRESSED_BYTES,
    MAX_DECOMPRESSED_BYTES,
    MAX_LINE_BYTES,
    SPOOL_DIR,
)
from .errors import InvalidRecord, PayloadTooLarge

# 16 + MAX_WBITS tells zlib to expect the gzip (RFC 1952) wrapper.
_GZIP_WBITS = 16 + zlib.MAX_WBITS
_DRAIN_SIZE = 64 * 1024


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise InvalidRecord(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _validate_line(raw_line: bytes) -> tuple[str, str]:
    # ``raw_line`` is the line content without the trailing LF; it is never
    # empty here because empty lines are filtered by the caller.
    try:
        text = raw_line.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise InvalidRecord(f"line is not valid UTF-8: {exc.reason}") from None

    try:
        obj = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise InvalidRecord(f"line is not valid JSON: {exc.msg}") from None

    if not isinstance(obj, dict) or set(obj.keys()) != {"id", "message"}:
        raise InvalidRecord("each line must be an object with exactly 'id' and 'message'")
    record_id = obj["id"]
    message = obj["message"]
    if not isinstance(record_id, str) or not record_id:
        raise InvalidRecord("'id' must be a non-empty string")
    if not isinstance(message, str) or not message:
        raise InvalidRecord("'message' must be a non-empty string")
    return record_id, message


def _open_stage() -> tuple[sqlite3.Connection, str]:
    """Create a fresh per-batch staging database."""
    fd, path = tempfile.mkstemp(prefix="spool-", suffix=".sqlite3", dir=str(SPOOL_DIR))
    os.close(fd)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=MEMORY")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute(
            "CREATE TABLE staged ("
            "position INTEGER PRIMARY KEY, "
            "id TEXT NOT NULL UNIQUE, "
            "message TEXT NOT NULL)"
        )
        conn.execute("BEGIN")
    except Exception:
        conn.close()
        _unlink(path)
        raise
    return conn, path


def _unlink(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _drain_decompressed(compressed: Iterable[bytes]):
    """Yield bounded pieces of plaintext while enforcing the byte caps.

    Raises PayloadTooLarge/InvalidRecord so the HTTP layer can map them.

    Bounded drain algorithm
    -----------------------
    ``decompress(data, max_length)`` caps one call's *output*; compressed
    bytes it could not consume are returned in ``unconsumed_tail`` and MUST
    be passed back as the data of the next call (not as ``b""``, and not
    mixed with later request chunks -- feeding ``b""`` while input remains
    buffered corrupts CPython's inflate state). Request chunks therefore go
    through a small local queue; only request bytes, never the whole body,
    are held here.
    """
    d = zlib.decompressobj(_GZIP_WBITS)
    compressed_total = 0
    decompressed_total = 0

    def account(piece: bytes) -> None:
        nonlocal decompressed_total
        if piece:
            decompressed_total += len(piece)
            if decompressed_total > MAX_DECOMPRESSED_BYTES:
                raise PayloadTooLarge(
                    f"decompressed stream exceeds {MAX_DECOMPRESSED_BYTES} bytes"
                )

    pending_comp = b""

    for chunk in compressed:
        compressed_total += len(chunk)
        if compressed_total > MAX_COMPRESSED_BYTES:
            raise PayloadTooLarge(
                f"compressed stream exceeds {MAX_COMPRESSED_BYTES} bytes"
            )
        if chunk:
            if d.eof:
                # Previous chunks already ended the single allowed member.
                raise InvalidRecord("trailing data after gzip member")
            pending_comp += chunk

        try:
            while pending_comp and not d.eof:
                piece = d.decompress(pending_comp, _DRAIN_SIZE)
                account(piece)
                if piece:
                    yield piece
                # Consume exactly the bytes zlib ate; the tail is re-fed.
                consumed = len(pending_comp) - len(d.unconsumed_tail)
                pending_comp = pending_comp[consumed:]
                # unused_data can surface right away (two members in one chunk)
                # or only after a subsequent call reaches the member end.
                if d.unused_data:
                    raise InvalidRecord("trailing data after gzip member")
        except zlib.error as exc:
            raise InvalidRecord(f"corrupt gzip stream: {exc}") from None

    # All request chunks consumed. Finish the member and drain the bounded
    # output buffer (large decompressed payloads need several flush calls).
    try:
        idle = 0
        while not d.eof:
            tail = d.flush(_DRAIN_SIZE)
            account(tail)
            if tail:
                idle = 0
                yield tail
            else:
                idle += 1
                if idle >= 2:
                    break
    except zlib.error as exc:
        raise InvalidRecord(f"corrupt gzip stream: {exc}") from None

    if not d.eof:
        raise InvalidRecord("truncated gzip stream: end of member not reached")
    if d.unused_data:
        raise InvalidRecord("trailing data after gzip member")


def ingest_stream(compressed: Iterable[bytes]) -> dict[str, object]:
    """Consume an iterable of compressed byte chunks and publish one batch.

    Returns ``{"id": <batch id>, "rows": <row count>}`` on success.
    """
    stage, spool_path = _open_stage()
    row_count = 0
    pending = bytearray()
    stage_closed = False

    def cleanup() -> None:
        nonlocal stage_closed
        if not stage_closed:
            stage.close()
            stage_closed = True
        _unlink(spool_path)

    try:
        for piece in _drain_decompressed(compressed):
            pending.extend(piece)
            while True:
                newline = pending.find(b"\n")
                if newline == -1:
                    break
                # Check the finished line *before* accepting it: pending may
                # already hold more than MAX_LINE_BYTES of a line completed
                # by this piece.
                if newline > MAX_LINE_BYTES:
                    raise PayloadTooLarge(
                        f"single line exceeds {MAX_LINE_BYTES} bytes"
                    )
                line = bytes(pending[:newline])
                del pending[: newline + 1]
                _process_line(line, stage)
                row_count += 1
            # Unterminated line so far: its accumulated size is bounded too.
            if len(pending) > MAX_LINE_BYTES:
                raise PayloadTooLarge(
                    f"single line exceeds {MAX_LINE_BYTES} bytes"
                )

        # Final line is allowed to omit its trailing newline; a mere trailing
        # newline leaves an empty buffer and must not be treated as a line.
        if pending:
            if len(pending) > MAX_LINE_BYTES:
                raise PayloadTooLarge(f"single line exceeds {MAX_LINE_BYTES} bytes")
            _process_line(bytes(pending), stage)
            row_count += 1

        if row_count == 0:
            raise InvalidRecord("batch contains no records")

        stage.commit()
        stage.close()
        stage_closed = True
    except:  # noqa: E722 - every failure invalidates the whole batch
        try:
            stage.rollback()
        except sqlite3.Error:
            pass
        cleanup()
        raise

    batch_id = uuid.uuid4().hex
    main_conn = db.connect()
    try:
        try:
            row_count = db.commit_spool(main_conn, batch_id, spool_path)
        except sqlite3.Error as exc:
            # Validation passed but persistence failed: this is a server
            # problem (5xx), not a 422. The transaction rolled back so the
            # batch id remains unknown.
            raise RuntimeError(f"failed to persist batch: {exc}") from exc
    finally:
        main_conn.close()
        _unlink(spool_path)

    return {"id": batch_id, "rows": row_count}


def _process_line(line: bytes, stage: sqlite3.Connection) -> None:
    if not line:
        raise InvalidRecord("empty line is not allowed")
    if b"\r" in line:
        # NDJSON here is strictly LF separated; CRLF would let JSON's
        # trailing-whitespace tolerance hide a malformed stream.
        raise InvalidRecord("carriage return in line; lines must be LF separated")
    record_id, message = _validate_line(line)
    try:
        stage.execute(
            "INSERT INTO staged (id, message) VALUES (?, ?)",
            (record_id, message),
        )
    except sqlite3.IntegrityError:
        raise InvalidRecord(f"duplicate id within batch: {record_id!r}") from None
