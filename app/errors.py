"""Domain errors carried by the ingestion pipeline."""
from __future__ import annotations


class IngestError(Exception):
    """Base class for ingestion failures. ``status_code`` is the HTTP result."""

    status_code = 422
    detail = "unprocessable entity"

    def __init__(self, detail: str | None = None) -> None:
        super().__init__(detail or self.detail)
        if detail:
            self.detail = detail


class PayloadTooLarge(IngestError):
    """One of the byte quotas (compressed/decompressed/line) was exceeded."""

    status_code = 413
    detail = "payload too large"


class InvalidRecord(IngestError):
    """The payload could be decoded but does not satisfy the record contract."""

    status_code = 422
    detail = "invalid record"


class ProducerAborted(Exception):
    """Internal signal: the reader side stopped while the worker was blocked."""
