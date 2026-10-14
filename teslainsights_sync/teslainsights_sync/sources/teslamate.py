"""Incremental, read-only export from TeslaMate's Postgres database.

Rows are exported as `to_jsonb(row)` so the app tolerates TeslaMate schema
additions; the backend maps fields. TeslaMate stores timestamps as UTC without
a zone, which the payload states via `tz: "UTC"` on every record.

Cursors are row ids. A page is exported up to the first row that is still open
(a drive or charge in progress), so nothing is skipped. Rows that have been open
longer than `stale_open_hours` are exported as-is so a broken session cannot
block the queue forever.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Callable

from ..config import TeslaMateConfig
from ..outbox import Outbox

log = logging.getLogger(__name__)

SOURCE = "teslamate"
PAGE_SIZE = 25
RECORD_BUDGET = 20_000  # per collect(), keeps the outbox bounded during backfill

_NOISY_POSITION_COLUMNS = (
    "ARRAY['fan_status','driver_temp_setting','passenger_temp_setting','is_rear_defroster_on',"
    "'is_front_defroster_on','battery_heater','battery_heater_on','battery_heater_no_power']"
)
_STALE = "(now() AT TIME ZONE 'UTC') - CAST(:stale AS integer) * interval '1 hour'"
# Short address parts so the backend can show "Storgatan 1, Karlstad" instead of the full display name.
_ADDRESS_FIELDS = ("name", "road", "house_number", "neighbourhood", "city", "town", "village", "municipality")


def _address(alias: str) -> str:
    pairs = ", ".join(f"'{f}', to_jsonb({alias})->>'{f}'" for f in _ADDRESS_FIELDS)
    return f"CASE WHEN {alias}.id IS NULL THEN NULL ELSE jsonb_build_object({pairs}) END"


# Bump when exported fields change: cursors restart and history is re-sent once (the backend upserts).
EXPORT_VERSION = "v2"


@dataclass(frozen=True)
class _Table:
    kind: str
    record_type: str
    sql: str
    child_type: str | None = None
    child_sql: str | None = None
    child_interval: str | None = None  # attribute name on the source holding the sample interval


TABLES = (
    _Table(
        kind="drives",
        record_type="drive",
        sql=f"""
            SELECT d.id, (d.end_date IS NOT NULL OR d.start_date < {_STALE}) AS closed,
                   to_jsonb(d) || jsonb_build_object(
                     'start_address', sa.display_name, 'end_address', ea.display_name,
                     'start_address_details', {_address('sa')}, 'end_address_details', {_address('ea')},
                     'start_geofence', sg.name, 'end_geofence', eg.name) AS row
              FROM drives d
              LEFT JOIN addresses sa ON sa.id = d.start_address_id
              LEFT JOIN addresses ea ON ea.id = d.end_address_id
              LEFT JOIN geofences sg ON sg.id = d.start_geofence_id
              LEFT JOIN geofences eg ON eg.id = d.end_geofence_id
             WHERE d.id > :cursor
             ORDER BY d.id
             LIMIT :limit""",
        child_type="drive_position",
        child_sql=f"""
            SELECT s.id, to_jsonb(s) - {_NOISY_POSITION_COLUMNS} AS row
              FROM (SELECT DISTINCT ON (floor(extract(epoch FROM p.date) / CAST(:interval AS integer))) p.*
                      FROM positions p
                     WHERE p.drive_id = :parent
                     ORDER BY floor(extract(epoch FROM p.date) / CAST(:interval AS integer)), p.date) s
             ORDER BY s.date""",
        child_interval="position_interval",
    ),
    _Table(
        kind="charging_processes",
        record_type="charging_process",
        sql=f"""
            SELECT c.id, (c.end_date IS NOT NULL OR c.start_date < {_STALE}) AS closed,
                   to_jsonb(c) || jsonb_build_object(
                     'address', a.display_name, 'address_details', {_address('a')}, 'geofence', g.name) AS row
              FROM charging_processes c
              LEFT JOIN addresses a ON a.id = c.address_id
              LEFT JOIN geofences g ON g.id = c.geofence_id
             WHERE c.id > :cursor
             ORDER BY c.id
             LIMIT :limit""",
        child_type="charge",
        child_sql="""
            SELECT s.id, to_jsonb(s) AS row
              FROM (SELECT DISTINCT ON (floor(extract(epoch FROM c.date) / CAST(:interval AS integer))) c.*
                      FROM charges c
                     WHERE c.charging_process_id = :parent
                     ORDER BY floor(extract(epoch FROM c.date) / CAST(:interval AS integer)), c.date) s
             ORDER BY s.date""",
        child_interval="charge_interval",
    ),
    _Table(
        kind="states",
        record_type="state",
        # The current state stays open; it is covered by the status snapshot instead.
        sql="""
            SELECT s.id, (s.end_date IS NOT NULL) AS closed, to_jsonb(s) AS row
              FROM states s
             WHERE s.id > :cursor
             ORDER BY s.id
             LIMIT :limit""",
    ),
)


def _connect(cfg: TeslaMateConfig):
    import pg8000.native  # imported lazily so tests run without the driver

    return pg8000.native.Connection(
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        application_name="teslainsights-sync",
        timeout=120,
    )


def _json(value):
    if isinstance(value, (bytes, str)):
        import json

        return json.loads(value)
    return value


class TeslaMateSource:
    name = SOURCE

    def __init__(
        self,
        cfg: TeslaMateConfig,
        *,
        position_interval: int = 10,
        charge_interval: int = 60,
        stale_open_hours: int = 48,
        connect: Callable[[TeslaMateConfig], object] = _connect,
    ):
        self._cfg = cfg
        self.position_interval = position_interval
        self.charge_interval = charge_interval
        self._stale = stale_open_hours
        self._connect = connect

    def collect(self, outbox: Outbox) -> bool:
        conn = self._connect(self._cfg)
        try:
            car_ids = self._cars(conn, outbox)
            budget = RECORD_BUDGET
            more = False
            for table in TABLES:
                while budget > 0:
                    exported, has_more = self._page(conn, outbox, table)
                    budget -= exported
                    if not has_more:
                        break
                else:
                    more = True
            self._status(conn, outbox, car_ids)
            return more
        finally:
            conn.close()

    def _cars(self, conn, outbox: Outbox) -> list[int]:
        rows = conn.run(
            "SELECT c.id, c.vin, to_jsonb(c) - ARRAY['vin','eid','vid'] AS row FROM cars c ORDER BY c.id"
        )
        records = []
        for car_id, vin, row in rows:
            data = _json(row)
            data["vin_hash"] = hashlib.sha256(vin.strip().upper().encode()).hexdigest() if vin else None
            records.append({"type": "car", "source_id": str(car_id), "tz": "UTC", "data": data})
        outbox.enqueue(SOURCE, "cars", records)
        return [car_id for car_id, _, _ in rows]

    def _page(self, conn, outbox: Outbox, table: _Table) -> tuple[int, bool]:
        cursor_name = f"{SOURCE}.{EXPORT_VERSION}.{table.kind}"
        cursor = int(outbox.get_cursor(cursor_name, "0"))
        params = {"cursor": cursor, "limit": PAGE_SIZE}
        if ":stale" in table.sql:
            params["stale"] = self._stale
        rows = conn.run(table.sql, **params)

        records: list[dict] = []
        last = cursor
        blocked = False
        for row_id, closed, row in rows:
            if not closed:
                blocked = True
                break
            data = _json(row)
            records.append({"type": table.record_type, "source_id": str(row_id), "tz": "UTC", "data": data})
            if table.child_sql:
                interval = getattr(self, table.child_interval)
                for child_id, child in conn.run(table.child_sql, parent=row_id, interval=interval):
                    records.append({
                        "type": table.child_type,
                        "source_id": str(child_id),
                        "parent_id": str(row_id),
                        "tz": "UTC",
                        "data": _json(child),
                    })
            last = row_id

        if last > cursor:
            outbox.enqueue(
                SOURCE,
                table.kind,
                records,
                cursor_updates={cursor_name: str(last)},
                payload_cursor=f"{table.kind}:{last}",
            )
            log.info("Queued %d %s records up to id %d", len(records), table.kind, last)
        has_more = not blocked and len(rows) == PAGE_SIZE
        return len(records), has_more

    def _status(self, conn, outbox: Outbox, car_ids: list[int]) -> None:
        records = []
        for car_id in car_ids:
            position = conn.run(
                f"SELECT p.id, to_jsonb(p) - {_NOISY_POSITION_COLUMNS} FROM positions p"
                " WHERE p.car_id = :car ORDER BY p.date DESC LIMIT 1",
                car=car_id,
            )
            state = conn.run(
                "SELECT to_jsonb(s) FROM states s WHERE s.car_id = :car ORDER BY s.start_date DESC LIMIT 1",
                car=car_id,
            )
            if not position and not state:
                continue
            drive_open = conn.run("SELECT 1 FROM drives WHERE car_id = :car AND end_date IS NULL"
                                  f" AND start_date > {_STALE} LIMIT 1", car=car_id, stale=self._stale)
            charge_open = conn.run("SELECT 1 FROM charging_processes WHERE car_id = :car AND end_date IS NULL"
                                   f" AND start_date > {_STALE} LIMIT 1", car=car_id, stale=self._stale)
            position_id = position[0][0] if position else 0
            records.append({
                "type": "vehicle_status",
                "source_id": f"{car_id}:{position_id}",
                "car_id": str(car_id),
                "tz": "UTC",
                "data": {
                    "position": _json(position[0][1]) if position else None,
                    "state": _json(state[0][0]) if state else None,
                    "drive_open": bool(drive_open),
                    "charge_open": bool(charge_open),
                },
            })
        # Content-addressed keys mean an unchanged snapshot is never re-sent.
        outbox.enqueue(SOURCE, "status", records)
