"""HTTP layer for the gzip/NDJSON batch service."""
from __future__ import annotations

import asyncio
import queue
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from . import db, ingest
from .config import MAX_COMPRESSED_BYTES, QUEUE_SLOTS, SPOOL_DIR, ensure_dirs
from .errors import IngestError, PayloadTooLarge, ProducerAborted


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_dirs()
    db.init_db()
    # Staging files from a process that died mid-batch can never be committed;
    # remove them so they do not accumulate on disk.
    for stale in Path(SPOOL_DIR).glob("spool-*.sqlite3*"):
        try:
            stale.unlink()
        except OSError:
            pass
    yield


app = FastAPI(
    title="Gzip NDJSON Batch Log",
    version="1.0.0",
    description="Upload a gzip stream of NDJSON records; query committed batches.",
    lifespan=lifespan,
)


@app.exception_handler(IngestError)
async def ingest_error_handler(_request: Request, exc: IngestError) -> JSONResponse:
    # We stop reading the body immediately on 413; closing the connection
    # avoids needing to drain the oversized remainder.
    headers = {"Connection": "close"} if exc.status_code == 413 else None
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.__class__.__name__, "detail": str(exc)},
        headers=headers,
    )


def _chunk_iter(q: "queue.Queue[bytes | None]", stop: threading.Event):
    """Blocking iterator consumed by the ingestion worker thread."""
    while not stop.is_set():
        try:
            item = q.get(timeout=0.05)
        except queue.Empty:
            continue
        if item is None:
            return
        yield item
    raise ProducerAborted()


def _offer(q: "queue.Queue[bytes | None]", item: bytes | None,
           stop: threading.Event) -> None:
    """Block until one slot frees or the worker/loop asks us to give up."""
    while not stop.is_set():
        try:
            q.put(item, timeout=0.05)
            return
        except queue.Full:
            continue
    raise ProducerAborted()


@app.post("/batches", status_code=201)
async def upload_batch(request: Request) -> JSONResponse:
    """Receive one gzip stream and, after full validation, commit one batch.

    Reception runs on the event loop while the blocking (CPU/disk) validation
    runs on a dedicated per-request thread. A bounded queue back-pressures a
    slow client; the event loop stays free for other uploads and reads.
    """
    declared_length = request.headers.get("content-length")
    if declared_length is not None:
        try:
            if int(declared_length) > MAX_COMPRESSED_BYTES:
                raise PayloadTooLarge(
                    f"content-length {declared_length} exceeds "
                    f"{MAX_COMPRESSED_BYTES} bytes"
                )
        except ValueError:
            pass

    chunks: queue.Queue[bytes | None] = queue.Queue(maxsize=QUEUE_SLOTS)
    stop = threading.Event()
    failure: list[BaseException] = []
    result: dict[str, object] = {}

    def worker() -> None:
        try:
            result.update(ingest.ingest_stream(_chunk_iter(chunks, stop)))
        except BaseException as exc:  # noqa: BLE001 - surfaced back on loop
            failure.append(exc)

    thread = threading.Thread(target=worker, name="ingest-worker", daemon=True)
    thread.start()

    async def receive() -> None:
        """Pump ASGI receive chunks to the worker, then signal clean EOF."""
        async for chunk in request.stream():
            if chunk:
                await asyncio.to_thread(_offer, chunks, chunk, stop)
        await asyncio.to_thread(_offer, chunks, None, stop)

    receive_task = asyncio.create_task(receive())
    join_task = asyncio.create_task(asyncio.to_thread(thread.join))

    # Wake when the request stream ends (normally or by a disconnect) or the
    # worker finishes, whichever comes first.
    await asyncio.wait(
        {receive_task, join_task}, return_when=asyncio.FIRST_COMPLETED
    )

    if receive_task.done() and receive_task.exception() is not None:
        # The stream raised mid-reception: a genuine client disconnect.
        stop.set()
        await asyncio.gather(receive_task, join_task, return_exceptions=True)
        exc = failure[0] if failure else None
        if isinstance(exc, IngestError):
            raise exc
        return JSONResponse(
            status_code=499, content={"detail": "client closed request"}
        )

    if not join_task.done():
        # Receiver finished cleanly after queueing the EOF sentinel, or is
        # still pumping: wait for validation and commit to finish.
        await join_task

    # The worker thread has ended.
    if not receive_task.done():
        # Worker failed while the receiver was still pumping; release it.
        stop.set()
        receive_task.cancel()
    await asyncio.gather(receive_task, join_task, return_exceptions=True)

    if failure:
        exc = failure[0]
        if isinstance(exc, IngestError):
            raise exc
        if isinstance(exc, ProducerAborted):
            return JSONResponse(
                status_code=499, content={"detail": "client closed request"}
            )
        raise exc

    return JSONResponse(
        status_code=201,
        content={"batch_id": result["id"], "rows": result["rows"]},
    )


# -- Read endpoints --------------------------------------------------------
# Defended as ``def`` (not ``async def``) so Starlette runs them in its
# threadpool and slow SQLite work never blocks the event loop.


@app.get("/batches")
def list_batches(
    limit: int = Query(100, ge=1, le=1000),
    after_id: str | None = Query(None, min_length=1, max_length=64),
):
    """List committed batch ids and row counts, keyset-paginated by batch id."""
    conn = db.connect()
    try:
        rows = db.list_batches(conn, limit, after_id)
        items = [
            {"batch_id": r["id"], "rows": r["row_count"], "created_at": r["created_at"]}
            for r in rows
        ]
        return {
            "items": items,
            "next_after_id": items[-1]["batch_id"] if len(items) == limit else None,
        }
    finally:
        conn.close()


def _require_batch(conn, batch_id: str):
    batch = db.get_batch(conn, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"unknown batch id: {batch_id}")
    return batch


@app.get("/batches/{batch_id}")
def get_batch(batch_id: str):
    conn = db.connect()
    try:
        batch = _require_batch(conn, batch_id)
        return {
            "batch_id": batch["id"],
            "rows": batch["row_count"],
            "created_at": batch["created_at"],
        }
    finally:
        conn.close()


@app.get("/batches/{batch_id}/rows")
def get_batch_rows(
    batch_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Read rows of one batch in original upload order."""
    conn = db.connect()
    try:
        _require_batch(conn, batch_id)
        rows = list(db.iter_batch_rows(conn, batch_id, limit, offset))
        items = [
            {"position": r["position"], "id": r["id"], "message": r["message"]}
            for r in rows
        ]
        return {
            "batch_id": batch_id,
            "offset": offset,
            "limit": limit,
            "items": items,
            "next_offset": offset + limit if len(items) == limit else None,
        }
    finally:
        conn.close()


@app.get("/health")
def health():
    return {"status": "ok"}
