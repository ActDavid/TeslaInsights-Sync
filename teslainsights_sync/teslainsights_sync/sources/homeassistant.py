"""State history for selected Home Assistant entities (e.g. the Zaptec energy meter).

Reads through the Supervisor proxy, which needs `homeassistant_api: true` and the
SUPERVISOR_TOKEN that Supervisor injects. History is walked in windows of at most
one day; the cursor is the end of the last completed window.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable
from urllib.parse import quote

from .. import http
from ..outbox import Outbox

log = logging.getLogger(__name__)

SOURCE = "homeassistant"
API = "http://supervisor/core/api"
WINDOW = timedelta(days=1)
SETTLE = timedelta(seconds=60)


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


class HomeAssistantSource:
    name = SOURCE

    def __init__(
        self,
        entities: tuple[str, ...],
        token: str,
        *,
        backfill_days: int = 10,
        get_json: Callable[[str], object] | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._entities = entities
        self._token = token
        self._backfill = timedelta(days=backfill_days)
        self._get = get_json or self._supervisor_get
        self._now = now

    def _supervisor_get(self, path: str):
        return http.request_json("GET", f"{API}{path}", headers={"Authorization": f"Bearer {self._token}"}, timeout=60)

    def collect(self, outbox: Outbox) -> bool:
        now = self._now()
        self._entity_meta(outbox)

        cursor = outbox.get_cursor(f"{SOURCE}.history_until")
        start = _parse(cursor) if cursor else now - self._backfill - 2 * SETTLE
        end = min(start + WINDOW, now - SETTLE)
        if end <= start:
            return False

        history = self._get(
            f"/history/period/{quote(start.isoformat(), safe='')}"
            f"?filter_entity_id={','.join(self._entities)}"
            f"&end_time={quote(end.isoformat(), safe='')}"
            "&minimal_response&no_attributes"
        ) or []

        records = []
        for series in history:
            if not series:
                continue
            entity_id = series[0].get("entity_id")
            for item in series:
                changed = item.get("last_changed") or item.get("last_updated")
                # HA repeats the state at `start` as the first item; it was sent in the previous window.
                if not changed or _parse(changed) <= start or item.get("state") in ("unavailable", "unknown"):
                    continue
                records.append({
                    "type": "ha_state",
                    "source_id": f"{entity_id}:{changed}",
                    "data": {"entity_id": entity_id, "state": item.get("state"), "last_changed": changed},
                })

        outbox.enqueue(
            SOURCE,
            "history",
            records,
            cursor_updates={f"{SOURCE}.history_until": end.isoformat()},
            payload_cursor=end.isoformat(),
        )
        if records:
            log.info("Queued %d Home Assistant state changes up to %s", len(records), end.isoformat())
        return end < now - SETTLE - timedelta(minutes=5)

    def _entity_meta(self, outbox: Outbox) -> None:
        records = []
        for entity_id in self._entities:
            state = self._get(f"/states/{quote(entity_id, safe='')}") or {}
            attrs = state.get("attributes") or {}
            records.append({
                "type": "ha_entity",
                "source_id": entity_id,
                "data": {
                    "entity_id": entity_id,
                    "friendly_name": attrs.get("friendly_name"),
                    "unit_of_measurement": attrs.get("unit_of_measurement"),
                    "device_class": attrs.get("device_class"),
                    "state_class": attrs.get("state_class"),
                },
            })
        outbox.enqueue(SOURCE, "entities", records)
