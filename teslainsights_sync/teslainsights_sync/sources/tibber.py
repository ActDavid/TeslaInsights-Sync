"""Prices and hourly consumption from Tibber's GraphQL API.

The first run backfills about a month of hourly consumption (including the unit
price per hour); later runs fetch the last 48 hours and today's/tomorrow's prices.
"""

from __future__ import annotations

import json
import logging
import time

from .. import http
from ..outbox import Outbox

log = logging.getLogger(__name__)

SOURCE = "tibber"
ENDPOINT = "https://api.tibber.com/v1-beta/gql"
FETCH_INTERVAL_SECONDS = 3600
BACKFILL_HOURS = 744
RECENT_HOURS = 48

QUERY = """
query TeslaInsights($last: Int!) {
  viewer {
    homes {
      id
      currentSubscription {
        priceInfo {
          today { startsAt total energy tax currency level }
          tomorrow { startsAt total energy tax currency level }
        }
      }
      consumption(resolution: HOURLY, last: $last) {
        nodes { from to unitPrice unitPriceVAT consumption consumptionUnit cost currency }
      }
    }
  }
}
"""


class TibberSource:
    name = SOURCE

    def __init__(self, token: str, *, transport=http.request, clock=time.time):
        self._token = token
        self._transport = transport
        self._clock = clock

    def collect(self, outbox: Outbox) -> bool:
        now = self._clock()
        last_fetch = float(outbox.get_cursor(f"{SOURCE}.last_fetch", "0"))
        if now - last_fetch < FETCH_INTERVAL_SECONDS:
            return False
        backfilled = outbox.get_cursor(f"{SOURCE}.backfilled") == "1"
        homes = self._query(RECENT_HOURS if backfilled else BACKFILL_HOURS)

        prices, consumption = [], []
        for home in homes:
            home_id = home["id"]
            price_info = ((home.get("currentSubscription") or {}).get("priceInfo")) or {}
            for day in ("today", "tomorrow"):
                for price in price_info.get(day) or []:
                    prices.append({"type": "price", "source_id": f"{home_id}:{price['startsAt']}", "home_id": home_id, "data": price})
            for node in ((home.get("consumption") or {}).get("nodes")) or []:
                if node.get("consumption") is None:
                    continue  # hour not settled yet
                consumption.append({"type": "consumption", "source_id": f"{home_id}:{node['from']}", "home_id": home_id, "data": node})

        outbox.enqueue(SOURCE, "prices", prices)
        outbox.enqueue(
            SOURCE,
            "consumption",
            consumption,
            cursor_updates={f"{SOURCE}.last_fetch": str(now), f"{SOURCE}.backfilled": "1"},
        )
        log.info("Queued %d Tibber prices and %d consumption hours", len(prices), len(consumption))
        return False

    def _query(self, last: int) -> list[dict]:
        body = json.dumps({"query": QUERY, "variables": {"last": last}}).encode()
        status, raw = self._transport(
            "POST",
            ENDPOINT,
            headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"},
            body=body,
            timeout=60,
        )
        if status != 200:
            raise RuntimeError(f"Tibber API returned HTTP {status}")
        data = json.loads(raw)
        if data.get("errors"):
            raise RuntimeError("Tibber API error: " + "; ".join(str(e.get("message")) for e in data["errors"]))
        return ((data.get("data") or {}).get("viewer") or {}).get("homes") or []
