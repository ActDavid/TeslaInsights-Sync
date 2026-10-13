"""Builds ingest payloads, HMAC headers and content-addressed idempotency keys.

The backend verifies hex(HMAC-SHA256(timestamp + "." + nonce + "." + raw_body)),
so the exact bytes that are signed are the bytes that are sent.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import time

SCHEMA_VERSION = "1"
MAX_RECORDS = 1000
MAX_BODY_BYTES = 4_000_000  # backend accepts 5 MB; keep a margin
_KEY_PART = re.compile(r"^[A-Za-z0-9._-]{1,40}$")


def encode(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")


def idempotency_key(source: str, kind: str, body: bytes) -> str:
    if not _KEY_PART.match(source) or not _KEY_PART.match(kind):
        raise ValueError("source and kind must be short identifiers")
    return f"{source}:{kind}:{hashlib.sha256(body).hexdigest()[:40]}"


def build_batches(
    source: str,
    kind: str,
    records: list[dict],
    *,
    cursor: str | None = None,
    error: str | None = None,
    max_records: int = 500,
    max_bytes: int = MAX_BODY_BYTES,
) -> list[tuple[bytes, int]]:
    """Splits records into (body, record_count) pairs under both the record and byte limits.

    Content decides the body, so identical data always yields identical keys.
    """
    max_records = min(max_records, MAX_RECORDS)

    def body(chunk: list[dict]) -> bytes:
        payload = {"source": source, "schema_version": SCHEMA_VERSION, "kind": kind, "records": chunk}
        if cursor is not None:
            payload["cursor"] = cursor
        if error is not None:
            # The backend limits error to 500 bytes, not characters.
            payload["error"] = error.encode("utf-8")[:500].decode("utf-8", errors="ignore")
        return encode(payload)

    if not records:
        return [(body([]), 0)] if error is not None else []

    base = len(body([]))
    bodies: list[tuple[bytes, int]] = []
    chunk: list[dict] = []
    size = base
    for record in records:
        record_size = len(encode(record)) + 1
        if base + record_size > max_bytes:
            raise ValueError(f"A single {kind} record exceeds {max_bytes} bytes")
        if chunk and (len(chunk) >= max_records or size + record_size > max_bytes):
            bodies.append((body(chunk), len(chunk)))
            chunk, size = [], base
        chunk.append(record)
        size += record_size
    bodies.append((body(chunk), len(chunk)))
    return bodies


def sign(secret: str, timestamp: str, nonce: str, body: bytes) -> str:
    message = timestamp.encode("ascii") + b"." + nonce.encode("ascii") + b"." + body
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def signed_headers(secret: str, body: bytes, key: str, *, now: float | None = None, nonce: str | None = None) -> dict[str, str]:
    timestamp = str(int(now if now is not None else time.time()))
    nonce = nonce or secrets.token_hex(32)
    return {
        "Content-Type": "application/json",
        "X-Sync-Timestamp": timestamp,
        "X-Sync-Nonce": nonce,
        "X-Sync-Signature": sign(secret, timestamp, nonce, body),
        "Idempotency-Key": key,
    }
