"""Reads the app options that Supervisor writes to /data/options.json."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

OPTIONS_PATH = Path("/data/options.json")
DATA_DIR = Path("/data")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class TeslaMateConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class Config:
    api_url: str
    hmac_secret: str
    teslamate: TeslaMateConfig | None
    sync_interval_seconds: int = 300
    position_interval_seconds: int = 10
    charge_sample_interval_seconds: int = 60
    stale_open_hours: int = 48
    tibber_token: str = ""
    ha_entities: tuple[str, ...] = field(default_factory=tuple)
    ha_backfill_days: int = 10
    log_level: str = "info"
    data_dir: Path = DATA_DIR

    @property
    def ingest_url(self) -> str:
        return self.api_url.rstrip("/") + "/api/v1/sync/ingest"


def parse(options: dict, data_dir: Path = DATA_DIR) -> Config:
    api_url = str(options.get("api_url", "")).strip()
    parsed = urlparse(api_url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ConfigError("api_url must be an https:// URL")

    secret = str(options.get("hmac_secret", ""))
    if len(secret) < 32:
        raise ConfigError("hmac_secret must be set (at least 32 characters, same value as SYNC_HMAC_SECRET)")

    tm = options.get("teslamate") or {}
    teslamate = None
    if str(tm.get("host", "")).strip():
        teslamate = TeslaMateConfig(
            host=str(tm["host"]).strip(),
            port=int(tm.get("port", 5432)),
            database=str(tm.get("database", "teslamate")),
            user=str(tm.get("user", "")),
            password=str(tm.get("password", "")),
        )

    entities = tuple(e.strip() for e in options.get("ha_entities", []) or [] if str(e).strip())
    for entity in entities:
        if "." not in entity or "," in entity:
            raise ConfigError(f"Invalid entity id: {entity!r}")

    return Config(
        api_url=api_url,
        hmac_secret=secret,
        teslamate=teslamate,
        sync_interval_seconds=int(options.get("sync_interval_seconds", 300)),
        position_interval_seconds=int(options.get("position_interval_seconds", 10)),
        charge_sample_interval_seconds=int(options.get("charge_sample_interval_seconds", 60)),
        stale_open_hours=int(options.get("stale_open_hours", 48)),
        tibber_token=str(options.get("tibber_token", "") or ""),
        ha_entities=entities,
        ha_backfill_days=int(options.get("ha_backfill_days", 10)),
        log_level=str(options.get("log_level", "info")),
        data_dir=data_dir,
    )


def load(path: Path = OPTIONS_PATH) -> Config:
    with path.open(encoding="utf-8") as fh:
        return parse(json.load(fh), data_dir=path.parent)
