"""Runtime configuration, all limits are expressed in bytes."""
from __future__ import annotations

import os
from pathlib import Path

# Hard limits from the specification (byte counted).
MAX_COMPRESSED_BYTES = 2 * 1024 * 1024   # 2 MiB gzip request body
MAX_DECOMPRESSED_BYTES = 8 * 1024 * 1024  # 8 MiB after decompression
MAX_LINE_BYTES = 64 * 1024               # 64 KiB per NDJSON line (the LF is not counted)

# Network tuning.
CHUNK_SIZE = 64 * 1024
QUEUE_SLOTS = 16
BUSY_TIMEOUT_SECONDS = 10.0

DATA_DIR = Path(os.environ.get("BATCHLOG_DATA_DIR", str(Path.cwd() / "data")))
DB_PATH = DATA_DIR / "batchlog.sqlite3"
SPOOL_DIR = DATA_DIR / "spool"


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
