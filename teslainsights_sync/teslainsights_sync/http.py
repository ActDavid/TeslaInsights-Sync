"""Minimal stdlib HTTP helper. TLS certificates are always verified."""

from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request

from . import __version__

USER_AGENT = f"teslainsights-sync/{__version__}"
_TLS = ssl.create_default_context()


def request(method: str, url: str, *, headers: dict[str, str] | None = None, body: bytes | None = None, timeout: float = 30) -> tuple[int, bytes]:
    """Returns (status, body). HTTP errors are returned, network errors raise OSError."""
    req = urllib.request.Request(url, data=body, method=method, headers={"User-Agent": USER_AGENT, **(headers or {})})
    context = _TLS if url.startswith("https://") else None
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()


def request_json(method: str, url: str, **kwargs) -> object:
    status, raw = request(method, url, **kwargs)
    if status >= 400:
        raise RuntimeError(f"{method} {url.split('?')[0]} returned HTTP {status}")
    return json.loads(raw) if raw else None
