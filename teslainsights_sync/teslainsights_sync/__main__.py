"""Entry point: collect from every configured source, then deliver the outbox."""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time

from . import __version__, config
from .outbox import Outbox
from .sender import Sender
from .sources.homeassistant import HomeAssistantSource
from .sources.teslamate import TeslaMateSource
from .sources.tibber import TibberSource

log = logging.getLogger("teslainsights_sync")

MAX_PENDING_BATCHES = 2000  # stop collecting while the backend is unreachable
ERROR_REPORT_INTERVAL = 3600


def build_sources(cfg: config.Config) -> list:
    sources = []
    if cfg.teslamate:
        sources.append(TeslaMateSource(
            cfg.teslamate,
            position_interval=cfg.position_interval_seconds,
            charge_interval=cfg.charge_sample_interval_seconds,
            stale_open_hours=cfg.stale_open_hours,
        ))
    if cfg.tibber_token:
        sources.append(TibberSource(cfg.tibber_token))
    if cfg.ha_entities:
        token = os.environ.get("SUPERVISOR_TOKEN", "")
        if token:
            sources.append(HomeAssistantSource(cfg.ha_entities, token, backfill_days=cfg.ha_backfill_days))
        else:
            log.error("ha_entities is set but SUPERVISOR_TOKEN is missing; Home Assistant source disabled")
    return sources


def run_cycle(sources: list, outbox: Outbox, sender: Sender, reported: dict[str, tuple[str, float]]) -> bool:
    more = False
    if outbox.pending_count() < MAX_PENDING_BATCHES:
        for source in sources:
            try:
                more |= bool(source.collect(outbox))
            except Exception as err:  # one failing source must not stop the others
                message = f"{type(err).__name__}: {err}"[:500]
                log.error("%s failed: %s", source.name, message)
                last = reported.get(source.name)
                if not last or last[0] != message or time.time() - last[1] > ERROR_REPORT_INTERVAL:
                    outbox.enqueue(source.name, "error", [], error=message, payload_cursor=str(int(time.time())))
                    reported[source.name] = (message, time.time())
            else:
                reported.pop(source.name, None)
    else:
        log.warning("Outbox has %d undelivered batches; pausing collection", outbox.pending_count())

    result = sender.flush()
    if any(result.values()):
        log.info("Delivery: %s", ", ".join(f"{k}={v}" for k, v in result.items() if v))
    outbox.prune()
    return more and result["retry"] == 0


def main() -> int:
    try:
        cfg = config.load()
    except (OSError, ValueError) as err:
        logging.basicConfig(level=logging.INFO)
        log.error("Invalid configuration: %s", err)
        return 1

    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    sources = build_sources(cfg)
    if not sources:
        log.error("No sources configured. Set teslamate.host, tibber_token or ha_entities.")
        return 1

    outbox = Outbox(cfg.data_dir / "sync.db")
    sender = Sender(cfg.ingest_url, cfg.hmac_secret, outbox)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    log.info("TeslaInsights Sync %s -> %s (sources: %s)", __version__, cfg.ingest_url, ", ".join(s.name for s in sources))
    reported: dict[str, tuple[str, float]] = {}
    while not stop.is_set():
        more = run_cycle(sources, outbox, sender, reported)
        stop.wait(2 if more else cfg.sync_interval_seconds)

    outbox.close()
    log.info("Stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
