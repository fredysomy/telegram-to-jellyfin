"""Torrent source adapter.

Flow
----
::

    get_episode_details(...)          # YOUR PLACEHOLDER: returns (infohash, trackers)
            |
            v
    resolve_episode(...)              # gets the details, then calls the builder
            |
            v
    build_magnet(info_hash, trackers) # infohash + trackers -> magnet:?xt=urn:btih:...

``resolve_episode()`` returns the magnet link for one episode, or ``None`` when
there is no source (the worker then waits for a torrent dropped in
``/media/Incoming/torrents``).

The only part you fill in is ``get_episode_details()``.
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request


import requests

_CACHE: dict[str, tuple[float, dict]] = {}

STREAM_SOURCE_URL = os.environ.get("STREAM_SOURCE_URL", "").rstrip("/")
STREAM_SOURCE_REFERER = os.environ.get("STREAM_SOURCE_REFERER", "")
STREAM_SOURCE_USER_AGENT = os.environ.get("STREAM_SOURCE_USER_AGENT", "")


DEFAULT_TRACKERS: tuple[str, ...] = (
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
)


def normalise_infohash(value: str) -> str:
    """Return a lowercase 40-character hex info hash.

    Accepts ``urn:btih:`` prefixes, raw hex, or base32.
    """
    token = value.strip()
    if token.lower().startswith("urn:btih:"):
        token = token[9:]
    token = token.strip()
    if len(token) == 40:
        try:
            bytes.fromhex(token)
        except ValueError as error:
            raise ValueError(f"not a hex info hash: {value!r}") from error
        return token.lower()
    if len(token) == 32:
        try:
            return base64.b32decode(token.upper()).hex()
        except Exception as error:  # noqa: BLE001
            raise ValueError(f"not a base32 info hash: {value!r}") from error
    raise ValueError(f"unsupported info hash length {len(token)}: {value!r}")


def fetch_json(
    url: str,
    headers: dict[str, str] | None = None,
    timeout: int = 15,
    retries: int = 2,
    cache_ttl: float = 300.0,
) -> dict:
    """GET a JSON document with retries and a short in-process cache.

    Source-agnostic plumbing for ``get_episode_details()``: handles timeouts,
    transient network errors, and repeated lookups for the same URL (the
    dispatcher resolves episodes in bursts of five).
    """
    now = time.monotonic()
    cached = _CACHE.get(url)
    if cached and now - cached[0] < cache_ttl:
        return cached[1]

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": os.environ.get("STREAM_SOURCE_USER_AGENT", "home-server-telegram-ingest/1.0"),
            **(headers or {}),
        },
    )
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.load(response)
            _CACHE[url] = (now, payload)
            return payload
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
            if attempt < retries:
                time.sleep(min(5, 0.5 * (2 ** attempt)))
    raise RuntimeError(f"fetch_json failed for {url}: {last_error}")


def build_magnet(info_hash: str, trackers: list[str] | None = None, display_name: str = "") -> str:
    """Build a magnet URI from an info hash plus announce URLs.

    >>> build_magnet("0" * 40, ["udp://t.example:1337/announce"], "Show S01E01")
    'magnet:?xt=urn:btih:0000...&dn=Show+S01E01&tr=udp%3A%2F%2Ft.example%3A1337%2Fannounce'
    """
    digest = normalise_infohash(info_hash)
    # xt/urn need their colons preserved, so encode the query manually.
    parts = [f"xt=urn:btih:{digest}"]
    if display_name:
        parts.append(f"dn={urllib.parse.quote(display_name, safe='')}")
    for tracker in trackers or ():
        tracker = tracker.strip()
        if tracker:
            parts.append(f"tr={urllib.parse.quote(tracker, safe='')}")
    return "magnet:?" + "&".join(parts)


def get_episode_details(
    imdb_id: str,
    series_name: str,
    season: int,
    episode: int,
    quality: str = "",
) -> tuple[str, list[str]] | None:
    """Fetch episode streams and return the selected torrent's infoHash and trackers.

    Returns:
        (info_hash, trackers) when a valid stream is found.
        None when the episode or a valid stream is not available yet.

    Raises:
        RuntimeError for actual source/API failures.
    """
    if not STREAM_SOURCE_URL:
        # No source configured: park the episode instead of failing it.
        return None

    video_id = f"{imdb_id}:{season}:{episode}"
    url = f"{STREAM_SOURCE_URL}/series/{urllib.parse.quote(video_id, safe='')}.json"

    headers = {
        "Referer": STREAM_SOURCE_REFERER,
        "User-Agent": STREAM_SOURCE_USER_AGENT,
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

    except requests.RequestException as exc:
        raise RuntimeError(f"Stream source request failed: {exc}") from exc
    except ValueError as exc:
        raise RuntimeError("Stream source returned invalid JSON") from exc

    streams = data.get("streams", [])

    if not isinstance(streams, list):
        raise RuntimeError("Invalid stream response: 'streams' is not a list")

    candidates = []

    for stream in streams:
        if not isinstance(stream, dict):
            continue

        # Ignore informational/browser entries.
        info_hash = stream.get("infoHash")
        if not info_hash:
            continue

        # Sources may be absent or malformed. Don't discard an otherwise
        # valid stream just because its tracker list is missing.
        sources = stream.get("sources") or []
        if not isinstance(sources, list):
            sources = []

        trackers = [
            source.removeprefix("tracker:")
            for source in sources
            if isinstance(source, str)
            and source.startswith("tracker:")
            and source.removeprefix("tracker:").startswith("udp://")
        ]

        # Validate the hash using the project's existing normalizer.
        try:
            normalized_hash = normalise_infohash(info_hash)
        except (TypeError, ValueError):
            continue

        if not normalized_hash:
            continue

        name = str(stream.get("name") or "")
        title = str(stream.get("title") or "")

        # Match the requested quality when possible.
        if quality and quality.lower() not in name.lower():
            continue

        # Extract the 👤 count rather than the unrelated N/100 rating.
        match = re.search(r"👤\s*(\d+)", title)
        seeders = int(match.group(1)) if match else 0

        candidates.append({
            "info_hash": normalized_hash,
            "trackers": trackers,
            "seeders": seeders,
        })

    if not candidates and quality:
        # Retry selection without the quality restriction. This avoids
        # reporting unavailable if the source labels quality differently.
        return get_episode_details(
            imdb_id, series_name, season, episode, quality=""
        )

    if not candidates:
        # Episode may not have been released or indexed yet.
        return None

    # Prefer the stream with the highest 👤 count.
    best_stream = max(candidates, key=lambda item: item["seeders"])

    return best_stream["info_hash"], best_stream["trackers"]


def resolve_episode(
    imdb_id: str,
    series_name: str,
    season: int,
    episode: int,
    quality: str = "",
) -> str | None:
    """Resolve one episode to a magnet link.

    Gets the episode details (infohash + trackers) and passes them to
    ``build_magnet()``. Returns the magnet URI, or ``None`` when
    ``get_episode_details()`` has nothing for this episode.
    """
    details = get_episode_details(imdb_id, series_name, season, episode, quality)
    if not details:
        return None

    info_hash, trackers = details
    if not info_hash:
        return None

    display_name = f"{series_name} S{season:02d}E{episode:02d}"
    return build_magnet(info_hash, list(trackers or ()), display_name)


def available_qualities(
    imdb_id: str, series_name: str, season: int, episode: int
) -> list[str]:
    """Return quality labels to offer before downloading.

    Return an empty list to skip the quality step entirely.  Returning labels
    such as ``["1080p", "720p"]`` makes the bot render a quality keyboard and
    pass the chosen label to ``get_episode_details()`` as ``quality``.
    """
    return []
