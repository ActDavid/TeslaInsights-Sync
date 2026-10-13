"""Delivers queued batches to POST /api/v1/sync/ingest."""

from __future__ import annotations

import json
import logging
import time

from . import http, payload
from .outbox import Outbox

log = logging.getLogger(__name__)

# 400/409/413 will never succeed with the same body, so retrying would only block the queue.
_PERMANENT = {400, 409, 413, 422}


class Sender:
    def __init__(self, ingest_url: str, secret: str, outbox: Outbox, *, transport=http.request, pause: float = 0.3, sleep=time.sleep):
        self._url = ingest_url
        self._secret = secret
        self._outbox = outbox
        self._transport = transport
        self._pause = pause
        self._sleep = sleep

    def flush(self, max_batches: int = 500) -> dict[str, int]:
        result = {"sent": 0, "duplicate": 0, "retry": 0, "dead": 0}
        while result["sent"] + result["duplicate"] < max_batches:
            due = self._outbox.due(limit=min(50, max_batches))
            if not due:
                break
            for batch in due:
                outcome = self._send(batch)
                result[outcome] += 1
                if outcome == "retry":
                    # Network, auth or server trouble affects every batch; try again next cycle.
                    return result
                self._sleep(self._pause)
        return result

    def _send(self, batch) -> str:
        headers = payload.signed_headers(self._secret, batch.body, batch.key)
        try:
            status, raw = self._transport("POST", self._url, headers=headers, body=batch.body, timeout=60)
        except OSError as err:
            log.warning("Could not reach backend: %s", err)
            self._outbox.mark_retry(batch.key, f"network: {err}")
            return "retry"

        message = _error_message(raw)
        if 200 <= status < 300:
            self._outbox.mark_sent(batch.key)
            duplicate = _is_duplicate(raw)
            log.debug("Delivered %s (%d records, duplicate=%s)", batch.key, batch.record_count, duplicate)
            return "duplicate" if duplicate else "sent"
        if status in _PERMANENT:
            log.error("Backend rejected %s permanently (HTTP %d): %s", batch.key, status, message)
            self._outbox.mark_dead(batch.key, f"HTTP {status}: {message}")
            return "dead"
        if status == 401:
            log.error("Backend refused authentication (%s). Check hmac_secret and that the clock is in sync.", message)
        else:
            log.warning("Backend returned HTTP %d for %s: %s", status, batch.key, message)
        self._outbox.mark_retry(batch.key, f"HTTP {status}: {message}")
        return "retry"


def _decode(raw: bytes) -> dict:
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except ValueError:
        return {}


def _error_message(raw: bytes) -> str:
    error = _decode(raw).get("error")
    return str(error.get("message", "")) if isinstance(error, dict) else ""


def _is_duplicate(raw: bytes) -> bool:
    data = _decode(raw).get("data")
    return bool(isinstance(data, dict) and data.get("duplicate"))
