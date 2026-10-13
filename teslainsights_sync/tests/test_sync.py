import hashlib
import hmac
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from teslainsights_sync import config, payload  # noqa: E402
from teslainsights_sync.outbox import Outbox  # noqa: E402
from teslainsights_sync.sender import Sender  # noqa: E402
from teslainsights_sync.sources.homeassistant import HomeAssistantSource  # noqa: E402
from teslainsights_sync.sources.teslamate import TeslaMateSource  # noqa: E402
from teslainsights_sync.sources.tibber import TibberSource  # noqa: E402

SECRET = "s" * 40


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def make_outbox(clock=None):
    tmp = tempfile.mkdtemp()
    return Outbox(Path(tmp) / "sync.db", clock=clock or Clock())


def records(n, prefix="r"):
    return [{"type": "drive", "source_id": f"{prefix}{i}", "data": {"i": i}} for i in range(n)]


class PayloadTests(unittest.TestCase):
    def test_signature_matches_backend_formula(self):
        body = b'{"source":"ha","schema_version":"1","records":[]}'
        headers = payload.signed_headers(SECRET, body, "teslamate:x:abc", now=1791883200, nonce="n" * 64)
        expected = hmac.new(SECRET.encode(), b"1791883200." + b"n" * 64 + b"." + body, hashlib.sha256).hexdigest()
        self.assertEqual(headers["X-Sync-Signature"], expected)
        self.assertEqual(headers["X-Sync-Timestamp"], "1791883200")
        self.assertEqual(headers["Idempotency-Key"], "teslamate:x:abc")

    def test_nonce_is_unique_per_request(self):
        a = payload.signed_headers(SECRET, b"{}", "k" * 8)["X-Sync-Nonce"]
        b = payload.signed_headers(SECRET, b"{}", "k" * 8)["X-Sync-Nonce"]
        self.assertNotEqual(a, b)
        self.assertEqual(len(a), 64)

    def test_batches_respect_record_and_byte_limits(self):
        by_count = payload.build_batches("teslamate", "drives", records(1200), max_records=1000)
        self.assertEqual([c for _, c in by_count], [1000, 200])
        by_size = payload.build_batches("teslamate", "drives", records(50), max_bytes=1000)
        self.assertTrue(all(len(b) <= 1000 for b, _ in by_size))
        self.assertEqual(sum(c for _, c in by_size), 50)

    def test_same_content_gives_same_key_and_key_matches_backend_rule(self):
        (body1, _), = payload.build_batches("tibber", "prices", records(3))
        (body2, _), = payload.build_batches("tibber", "prices", records(3))
        key = payload.idempotency_key("tibber", "prices", body1)
        self.assertEqual(key, payload.idempotency_key("tibber", "prices", body2))
        self.assertRegex(key, r"^[A-Za-z0-9._:-]{8,128}$")
        data = json.loads(body1)
        self.assertEqual((data["source"], data["schema_version"]), ("tibber", "1"))
        self.assertRegex(data["source"], r"^[a-z0-9._-]{1,64}$")

    def test_error_report_has_no_records(self):
        (body, count), = payload.build_batches("tibber", "error", [], error="boom")
        self.assertEqual(count, 0)
        self.assertEqual(json.loads(body)["error"], "boom")
        self.assertEqual(payload.build_batches("tibber", "x", []), [])


class ConfigTests(unittest.TestCase):
    base = {"api_url": "https://api.leott.se/tesla", "hmac_secret": SECRET}

    def test_requires_https_and_secret(self):
        with self.assertRaises(config.ConfigError):
            config.parse({**self.base, "api_url": "http://api.leott.se/tesla"})
        with self.assertRaises(config.ConfigError):
            config.parse({**self.base, "hmac_secret": "short"})

    def test_teslamate_optional_and_ingest_url(self):
        cfg = config.parse({**self.base, "teslamate": {"host": ""}})
        self.assertIsNone(cfg.teslamate)
        self.assertEqual(cfg.ingest_url, "https://api.leott.se/tesla/api/v1/sync/ingest")


class OutboxTests(unittest.TestCase):
    def test_enqueue_dedupes_and_moves_cursor_atomically(self):
        box = make_outbox()
        self.assertEqual(box.enqueue("teslamate", "drives", records(3), cursor_updates={"c": "3"}), 1)
        self.assertEqual(box.enqueue("teslamate", "drives", records(3)), 0)
        self.assertEqual(box.get_cursor("c"), "3")
        self.assertEqual(box.pending_count(), 1)

    def test_failed_enqueue_does_not_move_cursor(self):
        box = make_outbox()
        huge = [{"type": "x", "data": "y" * 5_000_000}]
        with self.assertRaises(ValueError):
            box.enqueue("teslamate", "drives", huge, cursor_updates={"c": "9"})
        self.assertIsNone(box.get_cursor("c"))

    def test_retry_backoff_hides_batch_until_due(self):
        clock = Clock()
        box = make_outbox(clock)
        box.enqueue("teslamate", "drives", records(1))
        key = box.due()[0].key
        box.mark_retry(key, "down")
        self.assertEqual(box.due(), [])
        clock.t += 16
        self.assertEqual(box.due()[0].attempts, 1)


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, headers=None, body=None, timeout=None):
        self.calls.append((url, headers, body))
        status, data = self.responses.pop(0)
        if isinstance(status, Exception):
            raise status
        return status, json.dumps(data).encode()


class SenderTests(unittest.TestCase):
    def setUp(self):
        self.box = make_outbox()
        self.box.enqueue("teslamate", "a", records(1, "a"))
        self.box.enqueue("teslamate", "b", records(1, "b"))

    def sender(self, transport):
        return Sender("https://x/api/v1/sync/ingest", SECRET, self.box, transport=transport, sleep=lambda _: None)

    def test_success_and_duplicate_mark_sent(self):
        t = FakeTransport([(202, {"data": {"duplicate": False}}), (200, {"data": {"duplicate": True}})])
        self.assertEqual(self.sender(t).flush(), {"sent": 1, "duplicate": 1, "retry": 0, "dead": 0})
        self.assertEqual(self.box.pending_count(), 0)
        url, headers, body = t.calls[0]
        self.assertTrue(hmac.compare_digest(
            headers["X-Sync-Signature"],
            payload.sign(SECRET, headers["X-Sync-Timestamp"], headers["X-Sync-Nonce"], body),
        ))

    def test_server_error_stops_flush_and_keeps_batch(self):
        t = FakeTransport([(500, {"error": {"message": "x"}})])
        self.assertEqual(self.sender(t).flush()["retry"], 1)
        self.assertEqual(len(t.calls), 1)
        self.assertEqual(self.box.pending_count(), 2)

    def test_network_error_is_retried(self):
        t = FakeTransport([(OSError("timeout"), None)])
        self.assertEqual(self.sender(t).flush()["retry"], 1)
        self.assertEqual(self.box.pending_count(), 2)

    def test_conflict_is_dead_and_does_not_block(self):
        t = FakeTransport([(409, {"error": {"message": "conflict"}}), (202, {"data": {}})])
        self.assertEqual(self.sender(t).flush(), {"sent": 1, "duplicate": 0, "retry": 0, "dead": 1})
        self.assertEqual(self.box.stats(), {"dead": 1, "sent": 1})


class FakeConn:
    """Answers TeslaMate queries from in-memory rows."""

    def __init__(self, drives, positions=None):
        self.drives = drives  # list of (id, closed)
        self.positions = positions or {}
        self.closed = False

    def run(self, sql, **params):
        if "FROM cars" in sql:
            return [[1, "5YJ3E1EA7KF000000", {"id": 1, "name": "Model Y"}]]
        if "FROM drives" in sql:
            rows = [d for d in self.drives if d[0] > params["cursor"]][: params["limit"]]
            return [[i, closed, {"id": i}] for i, closed in rows]
        if "FROM positions p" in sql and "drive_id" in sql:
            return [[pid, {"id": pid}] for pid in self.positions.get(params["parent"], [])]
        if "FROM positions p" in sql:
            return [[99, {"id": 99}]]
        if "FROM states" in sql and "LIMIT 1" in sql:
            return [[{"state": "online"}]]
        return []

    def close(self):
        self.closed = True


class TeslaMateTests(unittest.TestCase):
    def source(self, conn):
        from teslainsights_sync.config import TeslaMateConfig
        return TeslaMateSource(TeslaMateConfig("h", 5432, "teslamate", "u", "p"), connect=lambda _: conn)

    def bodies(self, box, kind):
        return [json.loads(b.body) for b in box.due(500) if b.kind == kind]

    def test_exports_closed_drives_with_positions_and_stops_at_open(self):
        conn = FakeConn([(1, True), (2, True), (3, False), (4, True)], positions={1: [10, 11], 2: [12]})
        box = make_outbox()
        self.source(conn).collect(box)
        self.assertTrue(conn.closed)
        self.assertEqual(box.get_cursor("teslamate.drives"), "2")
        recs = [r for b in self.bodies(box, "drives") for r in b["records"]]
        self.assertEqual([(r["type"], r["source_id"]) for r in recs],
                         [("drive", "1"), ("drive_position", "10"), ("drive_position", "11"), ("drive", "2"), ("drive_position", "12")])

    def test_car_vin_is_hashed_and_not_sent(self):
        box = make_outbox()
        self.source(FakeConn([])).collect(box)
        car = self.bodies(box, "cars")[0]["records"][0]["data"]
        self.assertNotIn("vin", car)
        self.assertEqual(car["vin_hash"], hashlib.sha256(b"5YJ3E1EA7KF000000").hexdigest())
        self.assertNotIn(b"5YJ3E1EA7KF000000", b"".join(b.body for b in box.due(500)))

    def test_unchanged_status_is_not_requeued(self):
        box = make_outbox()
        src = self.source(FakeConn([]))
        src.collect(box)
        before = box.pending_count()
        src._connect = lambda _: FakeConn([])
        src.collect(box)
        self.assertEqual(box.pending_count(), before)

    def test_reports_more_when_page_full(self):
        conn = FakeConn([(i, True) for i in range(1, 60)])
        box = make_outbox()
        self.assertFalse(self.source(conn).collect(box))  # 59 rows fit in the record budget
        self.assertEqual(box.get_cursor("teslamate.drives"), "59")


class TibberTests(unittest.TestCase):
    def test_collects_prices_and_settled_consumption_once_per_hour(self):
        clock = Clock()
        resp = {"data": {"viewer": {"homes": [{
            "id": "home1",
            "currentSubscription": {"priceInfo": {"today": [{"startsAt": "2026-10-13T00:00:00+02:00", "total": 1.2}], "tomorrow": []}},
            "consumption": {"nodes": [{"from": "2026-10-12T00:00:00+02:00", "consumption": 1.5}, {"from": "x", "consumption": None}]},
        }]}}}
        t = FakeTransport([(200, resp)])
        box = make_outbox(clock)
        src = TibberSource("token", transport=t, clock=clock)
        src.collect(box)
        self.assertEqual(json.loads(t.calls[0][2])["variables"]["last"], 744)
        kinds = {b.kind: json.loads(b.body)["records"] for b in box.due()}
        self.assertEqual(len(kinds["prices"]), 1)
        self.assertEqual(len(kinds["consumption"]), 1)
        src.collect(box)  # within the hour: no request
        self.assertEqual(len(t.calls), 1)


class HomeAssistantTests(unittest.TestCase):
    def test_walks_history_and_skips_start_state(self):
        now = datetime(2026, 10, 13, 12, tzinfo=timezone.utc)
        start = now - timedelta(days=10) - timedelta(seconds=120)
        calls = []

        def get_json(path):
            calls.append(path)
            if path.startswith("/states/"):
                return {"attributes": {"unit_of_measurement": "kWh"}}
            return [[
                {"entity_id": "sensor.zaptec_energy", "state": "100", "last_changed": start.isoformat()},
                {"state": "101", "last_changed": (start + timedelta(hours=1)).isoformat()},
                {"state": "unavailable", "last_changed": (start + timedelta(hours=2)).isoformat()},
            ]]

        box = make_outbox()
        src = HomeAssistantSource(("sensor.zaptec_energy",), "t", get_json=get_json, now=lambda: now)
        self.assertTrue(src.collect(box))
        hist = [json.loads(b.body) for b in box.due() if b.kind == "history"][0]
        self.assertEqual([r["data"]["state"] for r in hist["records"]], ["101"])
        self.assertEqual(box.get_cursor("homeassistant.history_until"), (start + timedelta(days=1)).isoformat())
        self.assertIn("%2B00%3A00", calls[1])


if __name__ == "__main__":
    unittest.main()
