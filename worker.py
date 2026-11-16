#!/usr/bin/env python3
"""Private Telegram channel to Jellyfin media ingestion worker.

Two ingestion paths share one SQLite queue:

1. Media posted to the paired private channel is downloaded straight from the
   local Bot API and moved into the Movies library (original behaviour).
2. ``/SeriesSearch <name>`` renders inline keyboards (series -> season ->
   episode -> optional quality).  Selected episodes become ``episode_jobs``,
   are resolved to a torrent by ``torrent_source``, handed to qBittorrent, and
   organised into ``TV Shows/<Series> (<Year>)/Season NN/``.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import http.cookiejar
import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import torrent_source


LOG = logging.getLogger("telegram-ingest")
STOP = threading.Event()
ALLOWED_EXTENSIONS = {
    ".avi", ".m2ts", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg",
    ".mpg", ".mts", ".ts", ".webm", ".wmv",
}
VIDEO_SUFFIXES = tuple(sorted(ALLOWED_EXTENSIONS))
CALLBACK_PREFIX = "ss"
SEARCH_COMMANDS = {"/seriessearch", "/series", "/ss"}
DONE_STATES = {
    "uploading", "stalledUP", "pausedUP", "queuedUP", "forcedUP", "checkingUP",
}
ERROR_STATES = {"error", "missingFiles"}
SOURCE_RETRY_SECONDS = 120
MONITOR_INTERVAL = 5
# Progress numbers change constantly; editing the panel more than once a
# minute burns Telegram's edit budget for no benefit. Status changes still
# render immediately.
PANEL_REFRESH_SECONDS = 60
# How long a "some episodes have no stream" choice stays clickable.
PREFLIGHT_TTL_SECONDS = 15 * 60
BATCH_STATUS_ORDER = ("completed", "downloading", "queued", "awaiting_source", "failed")


def load_secrets(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


class BotAPI:
    def __init__(self, root: str, token: str) -> None:
        self.root = root.rstrip("/")
        self.token = token

    def call(self, method: str, data: dict[str, Any] | None = None, timeout: int = 90) -> Any:
        payload = dict(data or {})
        for key in ("reply_markup", "link_preview_options"):
            if isinstance(payload.get(key), (dict, list)):
                payload[key] = json.dumps(payload[key])
        for key, value in list(payload.items()):
            if isinstance(value, bool):
                payload[key] = "true" if value else "false"
        encoded = urllib.parse.urlencode(payload).encode()
        request = urllib.request.Request(
            f"{self.root}/bot{self.token}/{method}", data=encoded, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read(2048).decode("utf-8", "replace")
            raise RuntimeError(f"Telegram {method} HTTP {error.code}: {detail}") from error
        if not body.get("ok"):
            raise RuntimeError(f"Telegram {method} failed: {body.get('description', 'unknown error')}")
        return body.get("result")

    def send(self, chat_id: int, text: str, reply_markup: dict | None = None) -> dict:
        data: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text[:4096],
            "disable_web_page_preview": True,
        }
        if reply_markup:
            data["reply_markup"] = reply_markup
        return self.call("sendMessage", data) or {}

    def edit(self, chat_id: int, message_id: int, text: str, reply_markup: dict | None = None) -> None:
        data: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text[:4096],
            "disable_web_page_preview": True,
        }
        if reply_markup:
            data["reply_markup"] = reply_markup
        try:
            self.call("editMessageText", data)
        except RuntimeError as error:
            # "message is not modified" is expected when nothing changed.
            if "not modified" not in str(error):
                raise

    def answer(self, callback_id: str, text: str = "") -> None:
        data: dict[str, Any] = {"callback_query_id": callback_id}
        if text:
            data["text"] = text[:200]
        try:
            self.call("answerCallbackQuery", data, timeout=15)
        except Exception:  # noqa: BLE001
            LOG.debug("answerCallbackQuery failed", exc_info=True)

    def delete(self, chat_id: int, message_id: int) -> None:
        self.call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})


class Database:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.connection:
            self.connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    update_id INTEGER NOT NULL UNIQUE,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    media_group_id TEXT,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL DEFAULT 0,
                    mime_type TEXT,
                    caption TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt INTEGER NOT NULL DEFAULT 0,
                    destination TEXT,
                    sha256 TEXT,
                    error TEXT,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS jobs_ready
                    ON jobs(status, next_attempt, id);
                CREATE TABLE IF NOT EXISTS completed_files (
                    file_unique_id TEXT PRIMARY KEY,
                    destination TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    completed_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS episode_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    imdb_id TEXT NOT NULL,
                    series_name TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    episode INTEGER NOT NULL,
                    quality TEXT NOT NULL DEFAULT '',
                    info_hash TEXT,
                    magnet TEXT,
                    torrent_hash TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    destination TEXT,
                    batch_id TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    UNIQUE(imdb_id, season, episode)
                );
                CREATE INDEX IF NOT EXISTS episode_jobs_ready
                    ON episode_jobs(status, next_attempt, id);
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                UPDATE jobs SET status='queued' WHERE status='downloading';
                """
            )

    # -- key/value -------------------------------------------------------
    def get_meta(self, key: str, default: str = "") -> str:
        with self.lock:
            row = self.connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self.lock, self.connection:
            self.connection.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # -- channel media jobs ----------------------------------------------
    def enqueue(self, values: tuple[Any, ...]) -> bool:
        with self.lock, self.connection:
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO jobs(
                    update_id,chat_id,message_id,media_group_id,file_id,file_unique_id,
                    file_name,file_size,mime_type,caption,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                values,
            )
        return cursor.rowcount == 1

    def completed(self, file_unique_id: str) -> sqlite3.Row | None:
        with self.lock:
            return self.connection.execute(
                "SELECT * FROM completed_files WHERE file_unique_id=?", (file_unique_id,)
            ).fetchone()

    def next_job(self) -> sqlite3.Row | None:
        now = int(time.time())
        with self.lock, self.connection:
            row = self.connection.execute(
                "SELECT * FROM jobs WHERE status='queued' AND next_attempt<=? ORDER BY id LIMIT 1",
                (now,),
            ).fetchone()
            if row:
                self.connection.execute(
                    "UPDATE jobs SET status='downloading', attempts=attempts+1, error=NULL WHERE id=?",
                    (row["id"],),
                )
                row = self.connection.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
        return row

    def finish(self, job_id: int, file_unique_id: str, destination: Path, size: int, digest: str) -> None:
        now = int(time.time())
        with self.lock, self.connection:
            self.connection.execute(
                "UPDATE jobs SET status='completed',destination=?,sha256=?,error=NULL WHERE id=?",
                (str(destination), digest, job_id),
            )
            self.connection.execute(
                "INSERT OR REPLACE INTO completed_files(file_unique_id,destination,size,completed_at) "
                "VALUES(?,?,?,?)",
                (file_unique_id, str(destination), size, now),
            )

    def retry_or_fail(self, job_id: int, attempts: int, error: str, max_attempts: int) -> bool:
        failed = attempts >= max_attempts
        status = "failed" if failed else "queued"
        delay = min(1800, 30 * (2 ** max(0, attempts - 1)))
        with self.lock, self.connection:
            self.connection.execute(
                "UPDATE jobs SET status=?,next_attempt=?,error=? WHERE id=?",
                (status, int(time.time()) + delay, error[:2000], job_id),
            )
        return failed

    # -- episode jobs ----------------------------------------------------
    EPISODE_COLUMNS = {
        "status", "attempts", "next_attempt", "error", "destination",
        "info_hash", "magnet", "torrent_hash", "quality",
    }

    def queue_episodes(self, rows: list[tuple[Any, ...]], hold_seconds: int = 0) -> int:
        """Insert episode jobs.

        ``hold_seconds`` keeps the dispatcher away while the caller attaches the
        already-resolved torrents, so a pre-flight batch is never re-resolved.
        """
        inserted = 0
        hold_until = int(time.time()) + max(0, int(hold_seconds))
        with self.lock, self.connection:
            for row in rows:
                cursor = self.connection.execute(
                    """
                    INSERT INTO episode_jobs(
                        imdb_id,series_name,season,episode,quality,batch_id,
                        chat_id,created_at,updated_at,next_attempt
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(imdb_id,season,episode) DO UPDATE SET
                        batch_id=excluded.batch_id,
                        quality=excluded.quality,
                        status='queued',
                        attempts=0,
                        next_attempt=excluded.next_attempt,
                        error=NULL,
                        updated_at=excluded.updated_at
                    WHERE episode_jobs.status IN ('failed','completed')
                    """,
                    (*row, hold_until),
                )
                inserted += cursor.rowcount
        return inserted

    def update_episode(self, job_id: int, **fields: Any) -> None:
        columns = [name for name in fields if name in self.EPISODE_COLUMNS]
        if not columns:
            return
        assignments = ", ".join(f"{name}=?" for name in columns)
        values = [fields[name] for name in columns]
        with self.lock, self.connection:
            self.connection.execute(
                f"UPDATE episode_jobs SET {assignments}, updated_at=? WHERE id=?",
                [*values, int(time.time()), job_id],
            )

    def episodes_ready(self, limit: int) -> list[sqlite3.Row]:
        now = int(time.time())
        with self.lock:
            return self.connection.execute(
                """
                SELECT * FROM episode_jobs
                WHERE status IN ('queued','awaiting_source') AND next_attempt<=?
                ORDER BY id LIMIT ?
                """,
                (now, limit),
            ).fetchall()

    def episodes_active(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.connection.execute(
                "SELECT * FROM episode_jobs WHERE status IN ('submitted','downloading','organizing') ORDER BY id"
            ).fetchall()

    def episodes_with_hash(self, torrent_hash: str) -> list[sqlite3.Row]:
        with self.lock:
            return self.connection.execute(
                "SELECT id, season, episode, status FROM episode_jobs WHERE torrent_hash=? ORDER BY id",
                (torrent_hash,),
            ).fetchall()

    def episode_job(self, job_id: int) -> sqlite3.Row | None:
        with self.lock:
            return self.connection.execute(
                "SELECT * FROM episode_jobs WHERE id=?", (job_id,)
            ).fetchone()

    def episode_jobs_by_hash(self, torrent_hash: str) -> list[sqlite3.Row]:
        with self.lock:
            return self.connection.execute(
                "SELECT * FROM episode_jobs WHERE torrent_hash=? ORDER BY id",
                (torrent_hash,),
            ).fetchall()

    def active_torrents_for_series(self, imdb_id: str, season: int) -> list[sqlite3.Row]:
        """Jobs of one season that already have a torrent in flight."""
        with self.lock:
            return self.connection.execute(
                """
                SELECT * FROM episode_jobs
                WHERE imdb_id=? AND season=? AND torrent_hash IS NOT NULL
                  AND status IN ('submitted','downloading','organizing')
                ORDER BY id
                """,
                (imdb_id, season),
            ).fetchall()

    def batch_jobs(self, batch_id: str) -> list[sqlite3.Row]:
        with self.lock:
            return self.connection.execute(
                "SELECT * FROM episode_jobs WHERE batch_id=? ORDER BY season, episode", (batch_id,)
            ).fetchall()

    def active_batch_ids(self) -> list[str]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT DISTINCT batch_id FROM episode_jobs
                WHERE status IN ('queued','awaiting_source','submitted','downloading','organizing','failed','completed')
                  AND created_at > ?
                """,
                (int(time.time()) - 7 * 24 * 3600,),
            ).fetchall()
        return [str(row[0]) for row in rows]

    def set_batch(self, batch_id: str, chat_id: int, message_id: int, title: str) -> None:
        with self.lock, self.connection:
            self.connection.execute(
                "INSERT INTO batches(batch_id,chat_id,message_id,title,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET message_id=excluded.message_id, updated_at=excluded.updated_at",
                (batch_id, chat_id, message_id, title, int(time.time())),
            )

    def get_batch(self, batch_id: str) -> sqlite3.Row | None:
        with self.lock:
            return self.connection.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()


def sanitize_name(file_name: str, caption: str) -> str:
    original = Path(file_name).name
    extension = Path(original).suffix.lower()
    base = caption.strip().splitlines()[0] if caption.strip() and not caption.lstrip().startswith("/") else Path(original).stem
    base = re.sub(r"[\x00-\x1f<>:\"/\\|?*]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip(" .")[:180]
    if not base:
        base = f"telegram-{uuid.uuid4().hex[:12]}"
    return f"{base}{extension}"


def safe_component(value: str, limit: int = 120) -> str:
    cleaned = re.sub(r"[\x00-\x1f<>:\"/\\|?*]+", " ", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    return (cleaned or "Untitled")[:limit]


def choose_destination(directory: Path, file_name: str, expected_size: int) -> tuple[Path, bool]:
    candidate = directory / file_name
    if not candidate.exists():
        return candidate, False
    if candidate.is_file() and expected_size and candidate.stat().st_size == expected_size:
        return candidate, True
    stem, suffix = candidate.stem, candidate.suffix
    for number in range(2, 1000):
        alternate = directory / f"{stem} ({number}){suffix}"
        if not alternate.exists():
            return alternate, False
    raise RuntimeError("Unable to choose a non-conflicting destination filename")


def media_from_update(update: dict[str, Any]) -> tuple[dict[str, Any], str] | None:
    post = update.get("channel_post")
    if not post:
        return None
    for key in ("document", "video", "animation", "audio"):
        if isinstance(post.get(key), dict):
            return post[key], key
    return None


def verify_media(path: Path) -> None:
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=format_name,duration", "-of", "json", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip()[-500:]
        raise RuntimeError(f"ffprobe rejected the completed media file: {detail}")


def copy_and_hash(source: Path, destination: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    with source.open("rb") as input_file, destination.open("xb") as output_file:
        while True:
            chunk = input_file.read(4 * 1024 * 1024)
            if not chunk:
                break
            output_file.write(chunk)
            digest.update(chunk)
            total += len(chunk)
        output_file.flush()
        os.fsync(output_file.fileno())
    return total, digest.hexdigest()


def human_size(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def human_speed(value: float) -> str:
    if value <= 0:
        return "0 B/s"
    return f"{human_size(int(value))}/s"


def human_eta(seconds: int) -> str:
    # qBittorrent reports 8640000 for "unknown / stalled".
    if seconds <= 0 or seconds >= 8640000:
        return "—"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {sec:02d}s"
    return f"{sec}s"


def episode_ranges(episodes: list[int]) -> str:
    """'E01–E04, E09' for a list of episode numbers."""
    spans: list[list[int]] = []
    for number in sorted(set(int(value) for value in episodes)):
        if spans and number == spans[-1][1] + 1:
            spans[-1][1] = number
        else:
            spans.append([number, number])
    return ", ".join(
        f"E{start:02d}" if start == end else f"E{start:02d}–E{end:02d}"
        for start, end in spans
    )


def plural(count: int, word: str, suffix: str = "s") -> str:
    return f"{count} {word}{'' if count == 1 else suffix}"


# ---------------------------------------------------------------------------
# BitTorrent helpers
# ---------------------------------------------------------------------------

def _bencode_skip(data: bytes, index: int) -> int:
    """Return the index just past the bencoded value starting at ``index``."""
    marker = data[index:index + 1]
    if marker == b"i":
        return data.index(b"e", index) + 1
    if marker in (b"l", b"d"):
        pointer = index + 1
        while data[pointer:pointer + 1] != b"e":
            pointer = _bencode_skip(data, pointer)
        return pointer + 1
    if marker.isdigit():
        colon = data.index(b":", index)
        length = int(data[index:colon])
        return colon + 1 + length
    raise ValueError("invalid bencode")


def torrent_infohash(path: Path) -> str | None:
    """Return the v1 info hash of a ``.torrent`` file, or ``None``."""
    data = path.read_bytes()
    if data[:1] != b"d":
        return None
    index = 1
    while data[index:index + 1] not in (b"e", b""):
        key_end = _bencode_skip(data, index)
        key = data[index:key_end]
        value_start = key_end
        value_end = _bencode_skip(data, value_start)
        if key.split(b":", 1)[-1] == b"info":
            return hashlib.sha1(data[value_start:value_end]).hexdigest()
        index = value_end
    return None


def magnet_infohash(magnet: str) -> str | None:
    match = re.search(r"urn:btih:([0-9A-Za-z]+)", magnet)
    if not match:
        return None
    token = match.group(1)
    try:
        return torrent_source.normalise_infohash(token)
    except ValueError:
        return None


def episode_patterns(season: int, episode: int) -> list[re.Pattern[str]]:
    return [
        re.compile(rf"S{season:02d}[\s._-]*E{episode:02d}", re.I),
        re.compile(rf"\b{season}x{episode:02d}\b", re.I),
        re.compile(rf"\bE{episode:02d}\b", re.I),
        re.compile(rf"\bEpisode[\s._-]*{episode}\b", re.I),
    ]


def file_matches_episode(name: str, season: int, episode: int) -> bool:
    return any(pattern.search(str(name)) for pattern in episode_patterns(season, episode))


def torrent_has_episode(files: list[dict], season: int, episode: int) -> bool:
    """True when the torrent contains a real video file for this episode."""
    return any(
        str(entry.get("name", "")).lower().endswith(VIDEO_SUFFIXES)
        and float(entry.get("size") or 0) > 0
        and file_matches_episode(str(entry.get("name", "")), season, episode)
        for entry in files
    )


def pick_video_file(files: list[dict], season: int, episode: int) -> dict | None:
    """Choose the file for one episode inside a possibly multi-episode pack."""
    videos = [
        entry for entry in files
        if str(entry.get("name", "")).lower().endswith(VIDEO_SUFFIXES)
        and float(entry.get("size") or 0) > 0
    ]
    if not videos:
        return None
    patterns = episode_patterns(season, episode)
    for pattern in patterns:
        matches = [entry for entry in videos if pattern.search(str(entry.get("name", "")))]
        if len(matches) == 1:
            return matches[0]
        if matches:
            return max(matches, key=lambda entry: float(entry.get("size") or 0))
    if len(videos) == 1:
        return videos[0]
    return max(videos, key=lambda entry: float(entry.get("size") or 0))


def derive_episode_title(file_name: str, series_name: str) -> str:
    stem = Path(file_name).stem
    stem = re.sub(rf"^{re.escape(series_name)}[\s._-]*", "", stem, flags=re.I)
    stem = re.sub(r"S\d{1,2}[\s._-]*E\d{1,3}", " ", stem, flags=re.I)
    stem = re.sub(r"\b(2160p|1080p|720p|480p|WEB-?DL|WEBRip|BluRay|HDRip|x264|x265|HEVC|AVC|AAC|AC3|DDP?5\.1|ESub|ESubs)\b", " ", stem, flags=re.I)
    stem = re.sub(r"-[A-Za-z0-9]{2,14}$", " ", stem)  # release group suffix
    stem = re.sub(r"[\[\](){}._]+", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip(" -")
    if len(stem) < 3:
        return ""
    # A single ALL-CAPS token is almost always a release group, not a title.
    if len(stem.split()) == 1 and stem.isupper():
        return ""
    return stem


# ---------------------------------------------------------------------------
# Cinemeta catalogue client
# ---------------------------------------------------------------------------

class Cinemeta:
    BASE = "https://v3-cinemeta.strem.io"

    def __init__(self, timeout: int = 25) -> None:
        self.timeout = timeout

    def _get(self, path: str) -> dict:
        request = urllib.request.Request(
            f"{self.BASE}{path}",
            headers={"User-Agent": "home-server-telegram-ingest/1.0"},
        )
        try:
            # urllib follows the 307 to cinemeta-live.strem.io automatically.
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"Cinemeta HTTP {error.code} for {path}") from error
        except urllib.error.URLError as error:
            raise RuntimeError(f"Cinemeta unreachable: {error.reason}") from error

    def search(self, query: str, limit: int = 6) -> list[dict]:
        quoted = urllib.parse.quote(query.strip(), safe="")
        payload = self._get(f"/catalog/series/top/search={quoted}.json")
        results = []
        for meta in payload.get("metas") or []:
            if str(meta.get("type")) != "series":
                continue
            if not str(meta.get("id", "")).startswith("tt"):
                continue
            results.append(meta)
            if len(results) >= limit:
                break
        return results

    def meta(self, imdb_id: str) -> dict:
        payload = self._get(f"/meta/series/{urllib.parse.quote(imdb_id)}.json")
        meta = payload.get("meta") or {}
        if not meta:
            raise RuntimeError(f"No metadata for {imdb_id}")
        return meta

    @staticmethod
    def title(meta: dict) -> str:
        name = str(meta.get("name") or "Unknown")
        year = str(meta.get("releaseInfo") or "").strip()[:4]
        return f"{name} ({year})" if year.isdigit() else name

    @staticmethod
    def seasons(meta: dict) -> list[tuple[int, int]]:
        counts: Counter[int] = Counter()
        for video in meta.get("videos") or []:
            try:
                season = int(video.get("season"))
                int(video.get("episode"))
            except (TypeError, ValueError):
                continue
            if season >= 1:
                counts[season] += 1
        return sorted(counts.items())

    @staticmethod
    def episodes(meta: dict, season: int) -> list[int]:
        numbers = set()
        for video in meta.get("videos") or []:
            try:
                if int(video.get("season")) != season:
                    continue
                numbers.add(int(video.get("episode")))
            except (TypeError, ValueError):
                continue
        return sorted(number for number in numbers if number >= 1)


# ---------------------------------------------------------------------------
# Inline keyboards
# ---------------------------------------------------------------------------

def series_keyboard(results: list[dict]) -> dict:
    rows = [
        [{
            "text": Cinemeta.title(result)[:60],
            "callback_data": f"{CALLBACK_PREFIX}:s:{result['id']}",
        }]
        for result in results
    ]
    return {"inline_keyboard": rows}


def season_keyboard(imdb_id: str, seasons: list[tuple[int, int]]) -> dict:
    buttons = [
        {"text": f"Season {season} ({count} eps)", "callback_data": f"{CALLBACK_PREFIX}:n:{imdb_id}:{season}"}
        for season, count in seasons
    ]
    rows = [buttons[index:index + 3] for index in range(0, len(buttons), 3)]
    return {"inline_keyboard": rows}


def episode_keyboard(imdb_id: str, season: int, episodes: list[int], page: int = 0, per_page: int = 18) -> dict:
    total = len(episodes)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    chunk = episodes[page * per_page:(page + 1) * per_page]
    rows: list[list[dict]] = []
    if total > 1:
        rows.append([{
            "text": f"⬇️ All {total} episodes",
            "callback_data": f"{CALLBACK_PREFIX}:e:{imdb_id}:{season}:a",
        }])
    grid = [
        {"text": f"E{number:02d}", "callback_data": f"{CALLBACK_PREFIX}:e:{imdb_id}:{season}:{number}"}
        for number in chunk
    ]
    rows += [grid[index:index + 6] for index in range(0, len(grid), 6)]
    if pages > 1:
        navigation = []
        if page > 0:
            navigation.append({"text": "« Prev", "callback_data": f"{CALLBACK_PREFIX}:e:{imdb_id}:{season}:p{page - 1}"})
        navigation.append({"text": f"{page + 1}/{pages}", "callback_data": f"{CALLBACK_PREFIX}:e:{imdb_id}:{season}:p{page}"})
        if page < pages - 1:
            navigation.append({"text": "Next »", "callback_data": f"{CALLBACK_PREFIX}:e:{imdb_id}:{season}:p{page + 1}"})
        rows.append(navigation)
    rows.append([{"text": "« Seasons", "callback_data": f"{CALLBACK_PREFIX}:back:{imdb_id}"}])
    return {"inline_keyboard": rows}


def quality_keyboard(imdb_id: str, season: int, selection: str, qualities: list[str]) -> dict:
    rows = [
        [{
            "text": quality,
            "callback_data": f"{CALLBACK_PREFIX}:q:{imdb_id}:{season}:{selection}:{quality[:12]}",
        }]
        for quality in qualities[:10]
    ]
    return {"inline_keyboard": rows}


# ---------------------------------------------------------------------------
# qBittorrent Web API client
# ---------------------------------------------------------------------------

class QBittorrent:
    def __init__(self, url: str, username: str, password: str, save_path: str, category: str = "tv") -> None:
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.save_path = save_path
        self.category = category
        self.lock = threading.Lock()
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))
        self.authenticated = False

    def _request(
        self,
        path: str,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
        retry: bool = True,
    ) -> bytes:
        request = urllib.request.Request(f"{self.url}{path}", data=data, headers=headers or {})
        try:
            with self.opener.open(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 403 and retry:
                self.login(force=True)
                return self._request(path, data, headers, timeout, retry=False)
            detail = error.read(300).decode("utf-8", "replace")
            raise RuntimeError(f"qBittorrent HTTP {error.code} on {path}: {detail}") from error
        except urllib.error.URLError as error:
            raise RuntimeError(f"qBittorrent unreachable: {error.reason}") from error

    def login(self, force: bool = False) -> None:
        with self.lock:
            if self.authenticated and not force:
                return
            body = urllib.parse.urlencode({"username": self.username, "password": self.password}).encode()
            payload = self._request(
                "/api/v2/auth/login",
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                retry=False,
            )
            # qBittorrent 5.x answers 204 with an empty body on success and
            # "Ok." on older versions; a failure raises HTTP 401 earlier.
            if payload.strip() not in (b"", b"Ok.", b"Ok"):
                raise RuntimeError(
                    f"qBittorrent login rejected the configured credentials: "
                    f"{payload[:120].decode('utf-8', 'replace')}"
                )
            self.authenticated = True

    def _form(self, fields: dict[str, str]) -> tuple[bytes, dict[str, str]]:
        return (
            urllib.parse.urlencode(fields).encode(),
            {"Content-Type": "application/x-www-form-urlencoded"},
        )

    def _multipart(self, fields: dict[str, str], filename: str, content: bytes) -> tuple[bytes, dict[str, str]]:
        boundary = uuid.uuid4().hex
        body = bytearray()
        for key, value in fields.items():
            body += f"--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n{value}\r\n".encode()
        body += (
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"torrents\"; "
            f"filename=\"{filename}\"\r\nContent-Type: application/x-bittorrent\r\n\r\n"
        ).encode()
        body += content + b"\r\n"
        body += f"--{boundary}--\r\n".encode()
        return bytes(body), {"Content-Type": f"multipart/form-data; boundary={boundary}"}

    def add(self, magnet: str | None = None, torrent_path: Path | None = None) -> str | None:
        self.login()
        fields = {"savepath": self.save_path, "category": self.category, "paused": "false"}
        if magnet:
            fields["urls"] = magnet
            data, headers = self._form(fields)
            digest = magnet_infohash(magnet)
            try:
                self._request("/api/v2/torrents/add", data=data, headers=headers)
            except RuntimeError as error:
                # qBittorrent answers 409 when the info hash is already queued.
                # A source that hands the same torrent to several episodes is
                # not an error; reuse it instead of retrying forever.
                if "HTTP 409" not in str(error) or not self.info(digest):
                    raise
                LOG.info("qBittorrent already had torrent %s; reusing it", digest[:10])
            return digest
        if torrent_path:
            data, headers = self._multipart(fields, torrent_path.name, torrent_path.read_bytes())
            digest = torrent_infohash(torrent_path)
            try:
                self._request("/api/v2/torrents/add", data=data, headers=headers)
            except RuntimeError as error:
                if "HTTP 409" not in str(error) or not self.info(digest):
                    raise
                LOG.info("qBittorrent already had torrent %s; reusing it", digest[:10])
            return digest
        raise ValueError("add() needs a magnet or a torrent path")

    def info(self, torrent_hash: str) -> dict | None:
        self.login()
        payload = self._request(f"/api/v2/torrents/info?hashes={urllib.parse.quote(torrent_hash)}")
        entries = json.loads(payload or b"[]")
        return entries[0] if entries else None

    def files(self, torrent_hash: str) -> list[dict]:
        self.login()
        payload = self._request(f"/api/v2/torrents/files?hash={urllib.parse.quote(torrent_hash)}")
        return json.loads(payload or b"[]")

    def delete(self, torrent_hash: str, delete_files: bool = True) -> None:
        self.login()
        body = urllib.parse.urlencode({
            "hashes": torrent_hash,
            "deleteFiles": "true" if delete_files else "false",
        }).encode()
        self._request(
            "/api/v2/torrents/delete",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class Worker:
    def __init__(self) -> None:
        secrets = load_secrets(Path(os.environ.get("TELEGRAM_SECRETS_FILE", "/run/secrets/telegram")))
        token = secrets.get("TELEGRAM_BOT_TOKEN", "")
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is empty")
        self.api = BotAPI(os.environ.get("TELEGRAM_BOT_API_URL", "http://telegram-bot-api:8081"), token)
        self.database = Database(Path(os.environ.get("QUEUE_DATABASE", "/state/queue.sqlite3")))
        self.partial_dir = Path(os.environ.get("PARTIAL_DIRECTORY", "/media/Incoming/.partial"))
        self.movies_dir = Path(os.environ.get("MOVIES_DIRECTORY", "/media/Movies"))
        self.tv_dir = Path(os.environ.get("TV_DIRECTORY", "/media/TV Shows"))
        self.torrent_drop_dir = Path(os.environ.get("TORRENT_DROP_DIRECTORY", "/media/Incoming/torrents"))
        self.downloads_dir = Path(os.environ.get("DOWNLOAD_DIRECTORY", "/media/Incoming/torrent-downloads"))
        self.qb_save_path = os.environ.get("QBITTORRENT_SAVE_PATH", "/downloads")
        self.bot_data_dir = Path(os.environ.get("BOT_DATA_DIRECTORY", "/var/lib/telegram-bot-api"))
        self.reserve_bytes = int(os.environ.get("FREE_SPACE_RESERVE_BYTES", str(10 * 1024**3)))
        self.max_attempts = int(os.environ.get("MAX_ATTEMPTS", "5"))
        self.metadata_timeout = max(300, int(os.environ.get("METADATA_TIMEOUT_SECONDS", "900")))
        self.delete_after_organise = os.environ.get("DELETE_TORRENT_AFTER_ORGANISE", "true").lower() != "false"
        self.get_file_retry_window = max(
            60, int(os.environ.get("GET_FILE_RETRY_WINDOW_SECONDS", str(20 * 60)))
        )
        self.catalog = Cinemeta()
        self.meta_cache: dict[str, tuple[float, dict]] = {}
        self.panel_cache: dict[str, str] = {}
        self.panel_signature: dict[str, str] = {}
        self.panel_last_edit: dict[str, float] = {}
        # Source checks the user has not answered yet, keyed by a short id.
        self.preflight: dict[str, dict[str, Any]] = {}
        self.preflight_lock = threading.Lock()
        self.owner_id = secrets.get("TELEGRAM_ALLOWED_USER_ID") or secrets.get("TELEGRAM_OWNER_ID") or ""
        # Optional comma-separated chat ids (groups, supergroups, channels) where
        # commands are accepted in addition to the owner's private chat.
        self.command_chats = {
            part.strip()
            for part in str(secrets.get("TELEGRAM_COMMAND_CHAT_IDS", "") or "").split(",")
            if part.strip()
        }

        qb_url = os.environ.get("QBITTORRENT_URL", "")
        self.qb = None
        if qb_url:
            self.qb = QBittorrent(
                qb_url,
                os.environ.get("QBITTORRENT_USERNAME", "admin"),
                os.environ.get("QBITTORRENT_PASSWORD", "REDACTED"),
                self.qb_save_path,
                os.environ.get("QBITTORRENT_CATEGORY", "tv"),
            )

        configured_chat = secrets.get("TELEGRAM_CHANNEL_ID", "")
        if configured_chat and not self.database.get_meta("allowed_chat_id"):
            self.database.set_meta("allowed_chat_id", configured_chat)
        for directory in (self.partial_dir, self.movies_dir, self.tv_dir, self.torrent_drop_dir, self.downloads_dir):
            directory.mkdir(parents=True, exist_ok=True)

    # -- startup ---------------------------------------------------------
    def wait_for_api(self) -> None:
        while not STOP.is_set():
            try:
                identity = self.api.call("getMe", timeout=10)
                LOG.info("Connected to Telegram as @%s", identity.get("username", "unknown"))
                return
            except Exception as error:  # noqa: BLE001
                LOG.warning("Waiting for local Bot API: %s", error)
                STOP.wait(5)

    def wait_for_qbittorrent(self) -> None:
        if not self.qb:
            LOG.warning("QBITTORRENT_URL is empty; series downloads will only queue")
            return
        while not STOP.is_set():
            try:
                self.qb.login(force=True)
                LOG.info("Connected to qBittorrent at %s", self.qb.url)
                return
            except Exception as error:  # noqa: BLE001
                LOG.warning("Waiting for qBittorrent: %s", error)
                STOP.wait(5)

    # -- update routing --------------------------------------------------
    def polling_loop(self) -> None:
        offset = int(self.database.get_meta("update_offset", "0"))
        while not STOP.is_set():
            try:
                updates = self.api.call(
                    "getUpdates",
                    {
                        "offset": offset,
                        "timeout": 30,
                        "limit": 100,
                        "allowed_updates": json.dumps(["channel_post", "message", "callback_query"]),
                    },
                    timeout=40,
                )
                for update in updates:
                    update_id = int(update["update_id"])
                    try:
                        self.handle_update(update)
                    except Exception:  # noqa: BLE001
                        LOG.exception("Update %s failed", update_id)
                    offset = max(offset, update_id + 1)
                    self.database.set_meta("update_offset", str(offset))
            except Exception as error:  # noqa: BLE001
                LOG.error("Polling error: %s", error)
                STOP.wait(5)

    def handle_update(self, update: dict[str, Any]) -> None:
        if "channel_post" in update:
            self.handle_channel_post(update)
        elif "callback_query" in update:
            self.handle_callback(update)
        elif "message" in update:
            self.handle_message(update)

    def authorize_user(self, user_id: int) -> bool:
        if not user_id:
            return False
        # TELEGRAM_OWNER_ID wins over a stored pairing, so a stale pairing can
        # always be overridden from the secrets file.
        allowed = str(self.owner_id or self.database.get_meta("allowed_user_id") or "")
        if not allowed:
            # Fail closed: an unconfigured bot must not hand control to the
            # first stranger who messages it.
            LOG.warning(
                "Refused unpaired Telegram user %s; set TELEGRAM_OWNER_ID=%s to allow it",
                user_id,
                user_id,
            )
            return False
        if str(user_id) != allowed:
            LOG.warning("Refused Telegram user %s (owner is %s)", user_id, allowed)
            return False
        if not self.database.get_meta("allowed_user_id"):
            self.database.set_meta("allowed_user_id", allowed)
        return True

    # -- channel ingestion (existing behaviour) --------------------------
    def handle_channel_post(self, update: dict[str, Any]) -> None:
        post = update.get("channel_post") or {}
        chat = post.get("chat") or {}
        if chat.get("type") != "channel":
            return
        chat_id = int(chat["id"])
        text = str(post.get("text") or "").strip()

        # A channel post carries no sender, so the channel itself must be on the
        # command allow-list; there is nothing else to authenticate against.
        if text.startswith("/"):
            if str(chat_id) in self.command_chats:
                self.handle_command(chat_id, text)
            else:
                LOG.warning(
                    "Ignored command in unapproved channel %s; add it to TELEGRAM_COMMAND_CHAT_IDS",
                    chat_id,
                )
                self.api.send(
                    chat_id,
                    "⛔ Commands are not enabled in this channel yet.\n"
                    f"Add TELEGRAM_COMMAND_CHAT_IDS={chat_id}\n"
                    "to telegram-secrets.env, then restart the container.",
                )
            return

        allowed = self.database.get_meta("allowed_chat_id")
        if not allowed:
            self.database.set_meta("allowed_chat_id", str(chat_id))
            allowed = str(chat_id)
            self.api.send(chat_id, "🔐 This private channel is now paired with the Jellyfin downloader.")
            LOG.info("Paired with channel ID %s", chat_id)
        if str(chat_id) != allowed:
            LOG.warning("Ignored a post from unapproved channel %s", chat_id)
            return

        selected = media_from_update(update)
        if not selected:
            return
        media, media_kind = selected
        file_id = media.get("file_id")
        unique_id = media.get("file_unique_id")
        if not file_id or not unique_id:
            return
        file_name = media.get("file_name") or f"telegram-{unique_id}.mp4"
        extension = Path(file_name).suffix.lower()
        mime_type = str(media.get("mime_type") or "")
        if extension not in ALLOWED_EXTENSIONS and not mime_type.startswith("video/"):
            self.api.send(chat_id, f"⚠️ Ignored unsupported attachment: {Path(file_name).name}")
            return

        completed = self.database.completed(unique_id)
        if completed:
            self.api.send(chat_id, f"↩️ Already downloaded: {Path(completed['destination']).name}")
            self.api.delete(chat_id, int(post["message_id"]))
            return

        inserted = self.database.enqueue(
            (
                int(update["update_id"]), chat_id, int(post["message_id"]),
                str(post.get("media_group_id") or ""), file_id, unique_id, file_name,
                int(media.get("file_size") or 0), mime_type or media_kind,
                str(post.get("caption") or ""), int(time.time()),
            )
        )
        if inserted:
            self.api.send(chat_id, f"📥 Queued: {Path(file_name).name}")

    # -- command handling ------------------------------------------------
    def handle_message(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_type = str(chat.get("type") or "")
        chat_id = int(chat.get("id") or 0)
        user_id = int((message.get("from") or {}).get("id") or 0)
        text = str(message.get("text") or "").strip()

        if chat_type == "private":
            if not self.authorize_user(user_id):
                self.refuse_private(chat_id, user_id)
                return
            self.handle_command(chat_id, text)
            return

        if chat_type in ("group", "supergroup"):
            if str(chat_id) not in self.command_chats:
                LOG.warning(
                    "Ignored %s message from unapproved group %s; add it to TELEGRAM_COMMAND_CHAT_IDS",
                    chat_type,
                    chat_id,
                )
                # Only the owner learns how to enable the chat; everyone else
                # gets silence so the bot cannot be used to probe the group.
                if text.startswith("/") and self.authorize_user(user_id):
                    self.api.send(
                        chat_id,
                        "⛔ Commands are not enabled in this chat yet.\n"
                        f"Add TELEGRAM_COMMAND_CHAT_IDS={chat_id}\n"
                        "to telegram-secrets.env, then restart the container.",
                    )
                return
            if not self.authorize_user(user_id):
                self.api.send(chat_id, "⛔ Not authorised.")
                return
            self.handle_command(chat_id, text)
            return

    def refuse_private(self, chat_id: int, user_id: int) -> None:
        if self.owner_id or self.database.get_meta("allowed_user_id"):
            self.api.send(chat_id, "⛔ This bot is paired with a different Telegram account.")
            return
        self.api.send(
            chat_id,
            "⛔ No owner is configured for this bot yet.\n"
            f"Your Telegram ID is {user_id}. Put\n"
            f"TELEGRAM_OWNER_ID={user_id}\n"
            "in telegram-secrets.env (as user fredy), then restart the container.",
        )

    def handle_command(self, chat_id: int, text: str) -> None:
        if not text.startswith("/"):
            return
        head, _, rest = text.partition(" ")
        # Telegram appends "@<bot>" when a command is sent from a group.
        command = head.split("@", 1)[0].lower()
        if command not in SEARCH_COMMANDS:
            return
        query = rest.strip()
        if not query:
            self.api.send(chat_id, "Usage: /SeriesSearch <series name>")
            return
        try:
            results = self.catalog.search(query)
        except Exception as error:  # noqa: BLE001
            LOG.exception("Search failed")
            self.api.send(chat_id, f"⚠️ Search failed: {error}")
            return
        if not results:
            self.api.send(chat_id, f"🔎 No series matched “{query}”.")
            return
        self.api.send(chat_id, f"🔎 Results for “{query}”:", series_keyboard(results))

    def series_meta(self, imdb_id: str) -> dict:
        cached = self.meta_cache.get(imdb_id)
        if cached and time.time() - cached[0] < 3600:
            return cached[1]
        meta = self.catalog.meta(imdb_id)
        self.meta_cache[imdb_id] = (time.time(), meta)
        return meta

    def handle_callback(self, update: dict[str, Any]) -> None:
        query = update.get("callback_query") or {}
        user_id = int((query.get("from") or {}).get("id") or 0)
        if not self.authorize_user(user_id):
            self.api.answer(str(query.get("id") or ""), "Not authorised.")
            return
        message = query.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = int(chat.get("id") or 0)
        message_id = int(message.get("message_id") or 0)
        if str(chat.get("type") or "") != "private" and str(chat_id) not in self.command_chats:
            self.api.answer(str(query.get("id") or ""), "Not enabled in this chat.")
            return
        self.api.answer(str(query.get("id") or ""))
        parts = str(query.get("data") or "").split(":")
        if parts[0] != CALLBACK_PREFIX or len(parts) < 2 or not chat_id:
            return
        try:
            kind = parts[1]
            if kind in ("s", "back") and len(parts) == 3:
                self.show_seasons(chat_id, message_id, parts[2])
            elif kind == "n" and len(parts) == 4:
                self.show_episodes(chat_id, message_id, parts[2], int(parts[3]), 0)
            elif kind == "e" and len(parts) == 5:
                self.choose_episode(chat_id, message_id, parts[2], int(parts[3]), parts[4])
            elif kind == "q" and len(parts) == 6:
                self.start_batch(chat_id, message_id, parts[2], int(parts[3]), parts[4], parts[5])
            elif kind == "pf" and len(parts) == 4 and parts[2] in ("y", "n"):
                self.resolve_preflight(chat_id, message_id, parts[2], parts[3])
        except Exception as error:  # noqa: BLE001
            LOG.exception("Callback %s failed", query.get("data"))
            self.api.send(chat_id, f"⚠️ {error}")

    def show_seasons(self, chat_id: int, message_id: int, imdb_id: str) -> None:
        meta = self.series_meta(imdb_id)
        seasons = self.catalog.seasons(meta)
        if not seasons:
            self.api.edit(chat_id, message_id, "⚠️ No episodes are listed for this title yet.")
            return
        self.api.edit(
            chat_id, message_id,
            f"📺 {Cinemeta.title(meta)}\n\nPick a season:",
            season_keyboard(imdb_id, seasons),
        )

    def show_episodes(self, chat_id: int, message_id: int, imdb_id: str, season: int, page: int) -> None:
        meta = self.series_meta(imdb_id)
        episodes = self.catalog.episodes(meta, season)
        if not episodes:
            self.api.edit(chat_id, message_id, f"⚠️ No episodes listed for season {season}.")
            return
        self.api.edit(
            chat_id, message_id,
            f"📺 {Cinemeta.title(meta)} — Season {season:02d}\n"
            f"{len(episodes)} episodes. Pick one, or take them all:",
            episode_keyboard(imdb_id, season, episodes, page),
        )

    def choose_episode(self, chat_id: int, message_id: int, imdb_id: str, season: int, selection: str) -> None:
        if selection.startswith("p"):
            try:
                self.show_episodes(chat_id, message_id, imdb_id, season, int(selection[1:]))
            except ValueError:
                pass
            return
        meta = self.series_meta(imdb_id)
        episodes = self.catalog.episodes(meta, season)
        if selection == "a":
            selected = episodes
        elif selection.isdigit() and int(selection) in episodes:
            selected = [int(selection)]
        else:
            self.api.edit(chat_id, message_id, "⚠️ That episode is no longer listed.")
            return
        if not selected:
            self.api.edit(chat_id, message_id, "⚠️ No episodes selected.")
            return
        qualities: list[str] = []
        try:
            qualities = list(torrent_source.available_qualities(
                imdb_id, str(meta.get("name") or ""), season, selected[0]
            ))
        except Exception:  # noqa: BLE001
            LOG.exception("available_qualities failed")
        if qualities:
            self.api.edit(
                chat_id, message_id,
                f"🎚 {Cinemeta.title(meta)} — Season {season:02d}, "
                f"{'all episodes' if selection == 'a' else f'E{int(selection):02d}'}\n"
                "Pick a quality:",
                quality_keyboard(imdb_id, season, selection, qualities),
            )
            return
        self.start_batch(chat_id, message_id, imdb_id, season, selection, "")

    def start_batch(self, chat_id: int, message_id: int, imdb_id: str, season: int, selection: str, quality: str) -> None:
        meta = self.series_meta(imdb_id)
        title = Cinemeta.title(meta)
        series_name = str(meta.get("name") or "Unknown")
        episodes = self.catalog.episodes(meta, season)
        if selection == "a":
            chosen = episodes
        elif selection.isdigit() and int(selection) in episodes:
            chosen = [int(selection)]
        else:
            self.api.edit(chat_id, message_id, "⚠️ Nothing queued: invalid episode selection.")
            return
        if not chosen:
            self.api.edit(chat_id, message_id, "⚠️ Nothing queued: no episodes listed.")
            return
        # Looking 20+ episodes up takes a while; keep the update loop free.
        threading.Thread(
            target=self.preflight_batch,
            args=(chat_id, message_id, imdb_id, season, chosen, quality, title, series_name),
            name="preflight", daemon=True,
        ).start()

    # -- source pre-flight -----------------------------------------------
    def preflight_batch(
        self, chat_id: int, message_id: int, imdb_id: str, season: int,
        chosen: list[int], quality: str, title: str, series_name: str,
    ) -> None:
        """Check every chosen episode for a stream before queueing anything."""
        total = len(chosen)
        try:
            self.api.edit(
                chat_id, message_id,
                f"🔍 {title} — S{season:02d}\nChecking {total} episode(s) for streams…",
            )
        except Exception:  # noqa: BLE001
            LOG.debug("Could not start pre-flight message", exc_info=True)

        found: dict[int, str] = {}
        missing: list[int] = []
        for index, episode in enumerate(chosen, start=1):
            magnet = None
            # A pack that is already downloading may cover this episode; then
            # there is nothing to look up.
            existing = self.reuse_episode_torrent(
                {"id": 0, "imdb_id": imdb_id, "season": season, "episode": episode}
            )
            if existing is not None and existing["magnet"]:
                magnet = str(existing["magnet"])
            else:
                try:
                    magnet = torrent_source.resolve_episode(
                        imdb_id, series_name, season, episode, quality or "",
                    )
                except NotImplementedError:
                    magnet = None
                except Exception as error:  # noqa: BLE001
                    LOG.warning("Pre-flight lookup failed for S%02dE%02d: %s", season, episode, error)
                    magnet = None
            if magnet:
                found[episode] = magnet
            else:
                missing.append(episode)
            if total > 6 and (index % 5 == 0 or index == total):
                try:
                    self.api.edit(
                        chat_id, message_id,
                        f"🔍 {title} — S{season:02d}\nChecking sources… {index}/{total}",
                    )
                except Exception:  # noqa: BLE001
                    pass

        groups: dict[str, list[int]] = {}
        magnets: dict[str, str] = {}
        for episode, magnet in found.items():
            digest = magnet_infohash(magnet) or magnet
            groups.setdefault(digest, []).append(episode)
            magnets.setdefault(digest, magnet)

        head = f"🔍 {title} — S{season:02d}\n{len(found)}/{total} episodes have a stream"
        if missing:
            head += f"\n📥 Nothing for {episode_ranges(missing)}"
        if groups:
            head += "\n\n" + self.describe_torrents(groups)

        if not found:
            self.api.edit(
                chat_id, message_id,
                head + "\n\nNothing queued: the source has no streams for these episodes yet.",
            )
            return

        state = {
            "chat_id": chat_id, "message_id": message_id, "imdb_id": imdb_id,
            "season": season, "quality": quality, "title": title,
            "series_name": series_name, "chosen": list(chosen),
            "found": found, "missing": missing, "groups": groups,
            "magnets": magnets, "created": time.time(),
        }
        if not missing:
            self.queue_preflight(state)
            return

        key = uuid.uuid4().hex[:8]
        with self.preflight_lock:
            cutoff = time.time() - PREFLIGHT_TTL_SECONDS
            for stale in [name for name, item in self.preflight.items() if item["created"] < cutoff]:
                self.preflight.pop(stale, None)
            self.preflight[key] = state
        self.api.edit(
            chat_id, message_id,
            head + "\n\nQueue the episodes that have a stream? The rest stay queued and keep retrying.",
            {"inline_keyboard": [[
                {"text": f"Queue {len(found)} available", "callback_data": f"{CALLBACK_PREFIX}:pf:y:{key}"},
                {"text": "Cancel", "callback_data": f"{CALLBACK_PREFIX}:pf:n:{key}"},
            ]]},
        )

    def describe_torrents(self, groups: dict[str, list[int]]) -> str:
        count = len(groups)
        total = sum(len(episodes) for episodes in groups.values())
        if count == 1:
            lines = [f"📦 1 torrent holds {plural(total, 'episode')} — it downloads once:"]
        else:
            lines = [f"📦 {count} torrents hold all {plural(total, 'episode')} — one download each:"]
        for episodes in sorted(groups.values(), key=min):
            files = len(episodes)
            detail = "single file" if files == 1 else f"{files} files in one download"
            lines.append(f"   • {episode_ranges(episodes)} — {detail}")
        return "\n".join(lines)

    def resolve_preflight(self, chat_id: int, message_id: int, choice: str, key: str) -> None:
        with self.preflight_lock:
            state = self.preflight.pop(key, None)
        if not state:
            self.api.edit(chat_id, message_id, "⌛ That selection expired. Run /SeriesSearch again.")
            return
        if choice != "y":
            self.api.edit(chat_id, message_id, f"✖️ Cancelled — nothing queued for {state['title']}.")
            return
        self.queue_preflight(state)

    def queue_preflight(self, state: dict[str, Any]) -> None:
        chat_id = int(state["chat_id"])
        message_id = int(state["message_id"])
        imdb_id = str(state["imdb_id"])
        season = int(state["season"])
        quality = str(state["quality"] or "")
        title = str(state["title"])
        series_name = str(state["series_name"])
        batch_title = f"{title} — S{season:02d}"

        batch_id = uuid.uuid4().hex[:10]
        now = int(time.time())
        self.database.queue_episodes(
            [
                (imdb_id, series_name, season, episode, quality, batch_id, chat_id, now, now)
                for episode in sorted(state["chosen"])
            ],
            hold_seconds=300,
        )
        jobs = {int(job["episode"]): job for job in self.database.batch_jobs(batch_id)}
        if not jobs:
            self.api.edit(
                chat_id, message_id,
                f"ℹ️ {batch_title}: those episodes are already in the queue.",
            )
            return

        attached = 0
        for torrent_hash, episodes in state["groups"].items():
            magnet = state["magnets"][torrent_hash]
            try:
                actual = self.qb.add(magnet=magnet) if self.qb else None
            except Exception as error:  # noqa: BLE001
                LOG.warning("Pre-flight add failed for %s: %s", torrent_hash[:10], error)
                actual = None
            for episode in episodes:
                job = jobs.get(int(episode))
                if not job:
                    continue
                if actual:
                    self.database.update_episode(
                        int(job["id"]), status="submitted", magnet=magnet,
                        info_hash=torrent_hash, torrent_hash=actual,
                        attempts=0, next_attempt=0, error=None,
                    )
                    attached += 1
                else:
                    self.database.update_episode(
                        int(job["id"]), status="queued", next_attempt=0,
                        attempts=0, error=None,
                    )
        for episode in state["missing"]:
            job = jobs.get(int(episode))
            if not job:
                continue
            self.database.update_episode(
                int(job["id"]), status="awaiting_source", attempts=1,
                next_attempt=now + SOURCE_RETRY_SECONDS * 2, error=None,
            )

        self.database.set_batch(batch_id, chat_id, 0, batch_title)
        text = self.render_panel(batch_id, fallback_title=batch_title)
        panel = self.api.send(chat_id, text)
        self.database.set_batch(batch_id, chat_id, int(panel.get("message_id") or 0), batch_title)
        self.panel_cache[batch_id] = text

        summary = f"✅ Queued {plural(attached, 'episode')} from {plural(len(state['groups']), 'torrent')}: {batch_title}"
        if state["missing"]:
            summary += (
                f"\n📥 {plural(len(state['missing']), 'episode')} have no stream yet"
                f" ({episode_ranges(state['missing'])}) and will keep retrying."
            )
        self.api.edit(chat_id, message_id, summary, {"inline_keyboard": []})
        LOG.info(
            "Pre-flight queued %s S%02d: %s attached, %s waiting for a source",
            series_name, season, attached, len(state["missing"]),
        )

    # -- status panel ----------------------------------------------------
    def render_panel(self, batch_id: str, fallback_title: str = "") -> str:
        batch = self.database.get_batch(batch_id)
        title = str(batch["title"]) if batch else fallback_title
        rows = self.database.batch_jobs(batch_id)
        if not rows:
            return f"🎬 {title}\n\nNothing queued."
        counts = Counter(str(row["status"]) for row in rows)
        legend = [
            ("completed", "✅"),
            ("downloading", "⬇️"),
            ("submitted", "⏳"),
            ("organizing", "📦"),
            ("queued", "⏸"),
            ("awaiting_source", "📥"),
            ("failed", "❌"),
        ]
        summary = " · ".join(
            f"{icon} {counts[name]}" for name, icon in legend if counts.get(name)
        )
        lines = [f"🎬 {title}", f"{counts['completed']}/{len(rows)} completed", summary]
        pending = [row for row in rows if row["status"] != "completed"]
        entries = self.pending_entries(pending)
        if entries:
            lines.append("")
            lines.extend(entries[:12])
            if len(entries) > 12:
                lines.append(f"…and {len(entries) - 12} more")
        details = self.torrent_summaries(rows)
        if details:
            lines.append("")
            lines.extend(details)
        return "\n".join(lines)[:4000]

    def pending_entries(self, pending: list[sqlite3.Row]) -> list[str]:
        """Episode lines for jobs that have not finished yet.

        Episodes that are downloading are dropped here: the torrent summary
        below carries their live stats for the whole range at once.
        """
        entries: list[str] = []
        for row in pending:
            label = f"S{int(row['season']):02d}E{int(row['episode']):02d}"
            status = str(row["status"])
            if status == "downloading":
                if row["torrent_hash"]:
                    continue
                entries.append(f"⬇️ {label}")
            elif status == "awaiting_source":
                entries.append(f"📥 {label} — waiting for a torrent source")
            elif status == "failed":
                entries.append(f"❌ {label} — {str(row['error'] or '')[:60]}")
            else:
                entries.append(f"{'⏳' if status in ('submitted', 'organizing') else '⏸'} {label}")
        return entries

    def torrent_summaries(self, rows: list[sqlite3.Row]) -> list[str]:
        """Live download detail for the torrents behind active episode jobs."""
        groups: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            status = str(row["status"])
            if status in ("submitted", "downloading", "organizing") and row["torrent_hash"]:
                groups.setdefault(str(row["torrent_hash"]), []).append(row)
        lines: list[str] = []
        for torrent_hash, jobs in groups.items():
            episodes = sorted(int(job["episode"]) for job in jobs)
            season = int(jobs[0]["season"])
            label = f"S{season:02d}E{episodes[0]:02d}"
            if len(episodes) > 1:
                label = f"S{season:02d}E{episodes[0]:02d}–E{episodes[-1]:02d}"

            info = None
            if self.qb:
                try:
                    info = self.qb.info(torrent_hash)
                except Exception as error:  # noqa: BLE001
                    LOG.debug("qBittorrent info failed for %s: %s", torrent_hash[:10], error)
            if not info:
                lines.append(f"⬇️ {label} — queued in qBittorrent")
                continue

            state = str(info.get("state") or "")
            percent = float(info.get("progress") or 0) * 100
            if state == "metaDL" and percent <= 0:
                swarm = int(info.get("num_complete") or 0)
                lines.append(f"📡 {label} — fetching metadata ({swarm} peers in swarm)")
                continue

            total = int(info.get("size") or 0)
            done = int(info.get("downloaded") or 0)
            seeds = int(info.get("num_seeds") or 0)
            swarm = int(info.get("num_complete") or 0)
            seed_text = f"{seeds} seeds" if seeds or not swarm else f"0 seeds ({swarm} in swarm)"
            pack = ""
            if len(episodes) > 1:
                try:
                    videos = [
                        entry for entry in self.qb.files(torrent_hash)
                        if str(entry.get("name", "")).lower().endswith(VIDEO_SUFFIXES)
                    ]
                    if videos:
                        pack = f" · {len(videos)} files, one download"
                except Exception:  # noqa: BLE001
                    LOG.debug("Could not count files in %s", torrent_hash[:10], exc_info=True)
            lines.append(
                f"⬇️ {label} — {percent:.1f}% · {human_size(done)}/{human_size(total)}"
                f" · {seed_text} · {human_speed(float(info.get('dlspeed') or 0))}"
                f" · ETA {human_eta(int(info.get('eta') or 0))}{pack}"
            )
        return lines

    def refresh_panels(self) -> None:
        now = time.monotonic()
        for batch_id in self.database.active_batch_ids():
            batch = self.database.get_batch(batch_id)
            if not batch or not batch["message_id"]:
                continue
            text = self.render_panel(batch_id)
            if self.panel_cache.get(batch_id) == text:
                continue
            rows = self.database.batch_jobs(batch_id)
            signature = ",".join(
                f"{row['id']}:{row['status']}:{row['error'] or ''}" for row in rows
            )
            # Status transitions show up at once; percentages are throttled to
            # PANEL_REFRESH_SECONDS so the panel does not fight Telegram's
            # rate limiter.
            if signature == self.panel_signature.get(batch_id):
                if now - self.panel_last_edit.get(batch_id, 0.0) < PANEL_REFRESH_SECONDS:
                    continue
            try:
                self.api.edit(int(batch["chat_id"]), int(batch["message_id"]), text)
                self.panel_cache[batch_id] = text
                self.panel_signature[batch_id] = signature
                self.panel_last_edit[batch_id] = now
            except Exception:  # noqa: BLE001
                LOG.debug("Panel edit failed for %s", batch_id, exc_info=True)

    # -- torrent submission ----------------------------------------------
    def find_dropped_torrent(self, series_name: str, season: int, episode: int) -> Path | None:
        patterns = [
            re.compile(rf"S{season:02d}[\s._-]*E{episode:02d}", re.I),
            re.compile(rf"\b{season}x{episode:02d}\b", re.I),
        ]
        candidates: list[Path] = []
        for path in sorted(self.torrent_drop_dir.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in (".torrent", ".magnet", ".txt"):
                continue
            name = path.name
            if any(pattern.search(name) for pattern in patterns):
                return path
            if series_name.lower().replace(" ", "") in name.lower().replace(" ", "").replace(".", ""):
                candidates.append(path)
        return candidates[0] if len(candidates) == 1 else None

    def reuse_episode_torrent(self, job: sqlite3.Row) -> sqlite3.Row | None:
        """Find an in-flight torrent of this season that holds this episode."""
        if not self.qb:
            return None
        for other in self.database.active_torrents_for_series(
            str(job["imdb_id"]), int(job["season"])
        ):
            torrent_hash = str(other["torrent_hash"] or "")
            if not torrent_hash or int(other["id"]) == int(job["id"]):
                continue
            try:
                files = self.qb.files(torrent_hash)
            except Exception as error:  # noqa: BLE001
                LOG.debug("Could not list torrent %s: %s", torrent_hash[:10], error)
                continue
            if torrent_has_episode(files, int(job["season"]), int(job["episode"])):
                return other
        return None

    def submit_episode(self, job: sqlite3.Row) -> None:
        if not self.qb:
            self.database.update_episode(
                int(job["id"]), status="awaiting_source",
                next_attempt=int(time.time()) + SOURCE_RETRY_SECONDS,
                error="qBittorrent is not configured",
            )
            return

        # A season pack already downloading for this season may contain this
        # episode's file. Attach to it instead of asking the source again and
        # adding a second copy of the same torrent.
        shared = self.reuse_episode_torrent(job)
        if shared is not None:
            torrent_hash = str(shared["torrent_hash"])
            self.database.update_episode(
                int(job["id"]), status="submitted", magnet=shared["magnet"],
                info_hash=torrent_hash, torrent_hash=torrent_hash,
                attempts=0, next_attempt=0, error=None,
            )
            LOG.info(
                "%s S%02dE%02d is already inside torrent %s; not downloading it twice",
                job["series_name"], job["season"], job["episode"], torrent_hash[:10],
            )
            return

        magnet = None
        resolve_error = ""
        try:
            magnet = torrent_source.resolve_episode(
                str(job["imdb_id"]), str(job["series_name"]),
                int(job["season"]), int(job["episode"]), str(job["quality"] or ""),
            )
        except NotImplementedError:
            magnet = None
        except Exception as error:  # noqa: BLE001
            resolve_error = str(error)[:300]
            LOG.warning("resolve_episode failed for job %s: %s", job["id"], resolve_error)

        if magnet:
            torrent_hash = self.qb.add(magnet=magnet)
            self.database.update_episode(
                int(job["id"]), status="submitted", magnet=magnet,
                info_hash=magnet_infohash(magnet), torrent_hash=torrent_hash,
                attempts=0, next_attempt=0, error=None,
            )
            LOG.info(
                "Queued %s S%02dE%02d from resolved magnet",
                job["series_name"], job["season"], job["episode"],
            )
            return

        dropped = self.find_dropped_torrent(str(job["series_name"]), int(job["season"]), int(job["episode"]))
        if not dropped:
            # Distinguish "the source has nothing" from "the source errored";
            # the second one must show up instead of waiting silently.
            # A missing episode is retried with a growing delay, so a batch
            # the index simply does not carry does not poll every two minutes
            # for the rest of the week.
            if resolve_error:
                attempts = int(job["attempts"]) + 1
                delay = min(1800, 60 * (2 ** min(attempts - 1, 5)))
                status = "queued"
            else:
                attempts = int(job["attempts"]) + 1
                delay = min(21600, SOURCE_RETRY_SECONDS * (2 ** min(attempts - 1, 8)))
                status = "awaiting_source"
            self.database.update_episode(
                int(job["id"]),
                status=status, attempts=attempts,
                next_attempt=int(time.time()) + delay,
                error=resolve_error or None,
            )
            return

        if dropped.suffix.lower() == ".torrent":
            torrent_hash = self.qb.add(torrent_path=dropped)
            magnet = None
        else:
            text = dropped.read_text(encoding="utf-8", errors="replace")
            match = re.search(r"magnet:\?[^\s\"']+", text)
            if not match:
                self.database.update_episode(
                    int(job["id"]), status="awaiting_source",
                    next_attempt=int(time.time()) + SOURCE_RETRY_SECONDS,
                    error=f"No magnet found in {dropped.name}",
                )
                return
            magnet = match.group(0)
            torrent_hash = self.qb.add(magnet=magnet)

        self.database.update_episode(
            int(job["id"]), status="submitted", magnet=magnet,
            info_hash=torrent_hash, torrent_hash=torrent_hash,
            attempts=0, next_attempt=0, error=None,
        )
        LOG.info("Queued %s S%02dE%02d from %s", job["series_name"], job["season"], job["episode"], dropped.name)

    def dispatch_loop(self) -> None:
        while not STOP.is_set():
            try:
                jobs = self.database.episodes_ready(5)
                if not jobs:
                    STOP.wait(3)
                    continue
                for job in jobs:
                    try:
                        self.submit_episode(job)
                    except Exception as error:  # noqa: BLE001
                        LOG.exception("Submitting episode job %s failed", job["id"])
                        attempts = int(job["attempts"]) + 1
                        self.database.update_episode(
                            int(job["id"]),
                            status="failed" if attempts >= self.max_attempts else "queued",
                            attempts=attempts,
                            next_attempt=int(time.time()) + min(1800, 30 * attempts),
                            error=str(error)[:500],
                        )
            except Exception:  # noqa: BLE001
                LOG.exception("Dispatch loop error")
                STOP.wait(5)

    # -- torrent monitoring ----------------------------------------------
    def map_qb_path(self, value: str) -> Path:
        raw = str(value or "")
        if raw.startswith(self.qb_save_path):
            return self.downloads_dir / raw[len(self.qb_save_path):].lstrip("/")
        return Path(raw)

    def downloaded_path(self, chosen: dict) -> Path:
        """Absolute path of one file inside a torrent.

        qBittorrent reports ``files[].name`` relative to the save path, so for a
        multi-file torrent it already contains the torrent's own folder.
        """
        source = (self.downloads_dir / str(chosen.get("name") or "")).resolve()
        downloads_root = self.downloads_dir.resolve()
        # Torrent file names are attacker-controlled; never let one point
        # outside the download directory.
        if not source.is_relative_to(downloads_root):
            raise RuntimeError(f"torrent file escapes the download directory: {source}")
        return source

    def organise(self, job: sqlite3.Row, info: dict) -> None:
        torrent_hash = str(job["torrent_hash"])
        files = self.qb.files(torrent_hash)
        chosen = pick_video_file(files, int(job["season"]), int(job["episode"]))
        if not chosen:
            raise RuntimeError("torrent contains no video file")

        # A source that returns one torrent for several episodes is fine for a
        # season pack, but a single-file torrent must not be filed as every
        # episode in the batch.
        shared = self.database.episodes_with_hash(torrent_hash)
        if len(shared) > 1 and not file_matches_episode(
            str(chosen.get("name", "")), int(job["season"]), int(job["episode"])
        ):
            raise RuntimeError(
                f"torrent {torrent_hash[:10]} is shared by {len(shared)} episodes but has no "
                f"file for S{int(job['season']):02d}E{int(job['episode']):02d}"
            )

        source = self.downloaded_path(chosen)
        if not source.is_file():
            raise RuntimeError(f"downloaded file is missing at {source}")

        meta = self.series_meta(str(job["imdb_id"]))
        title = str(meta.get("name") or job["series_name"])
        year = str(meta.get("releaseInfo") or "").strip()[:4]
        season = int(job["season"])
        episode = int(job["episode"])
        episode_title = derive_episode_title(source.name, title)
        suffix = source.suffix.lower() if source.suffix.lower() in ALLOWED_EXTENSIONS else ".mkv"
        folder = self.tv_dir / safe_component(f"{title} ({year})" if year.isdigit() else title)
        stem = f"{safe_component(title)} - S{season:02d}E{episode:02d}"
        if episode_title:
            stem += f" - {safe_component(episode_title, 80)}"
        destination = folder / f"Season {season:02d}" / f"{stem}{suffix}"
        destination.parent.mkdir(parents=True, exist_ok=True)

        self.database.update_episode(int(job["id"]), status="organizing")
        partial = destination.with_name(f".{destination.name}.part")
        partial.unlink(missing_ok=True)
        try:
            os.link(source, partial)
        except OSError:
            shutil.copyfile(source, partial)
        try:
            verify_media(partial)
            os.replace(partial, destination)
        finally:
            partial.unlink(missing_ok=True)

        self.database.update_episode(
            int(job["id"]), status="completed", destination=str(destination),
            error=None, next_attempt=0,
        )
        LOG.info("Organised %s S%02dE%02d -> %s", title, season, episode, destination)

    def organise_torrent(self, torrent_hash: str, info: dict) -> None:
        """File every episode a finished torrent covers, then clean up once."""
        for job in self.database.episode_jobs_by_hash(torrent_hash):
            if str(job["status"]) in ("completed", "failed"):
                continue
            try:
                self.organise(job, info)
            except Exception as error:  # noqa: BLE001
                attempts = int(job["attempts"]) + 1
                self.database.update_episode(
                    int(job["id"]),
                    status="failed" if attempts >= self.max_attempts else "downloading",
                    attempts=attempts, error=str(error)[:500],
                    next_attempt=int(time.time()) + 60,
                )
                LOG.warning(
                    "Could not file %s S%02dE%02d: %s",
                    job["series_name"], job["season"], job["episode"], error,
                )
        self.release_torrent(torrent_hash)

    def release_torrent(self, torrent_hash: str) -> None:
        """Delete a torrent's files once no episode still needs them.

        A season pack feeds several episode jobs. Deleting the torrent as soon
        as the first episode is filed would destroy the source for the rest, so
        the files stay until every job on this hash has finished.
        """
        if not self.delete_after_organise or not self.qb:
            return
        needs_files = [
            row for row in self.database.episodes_with_hash(torrent_hash)
            if str(row["status"]) not in ("completed", "failed")
        ]
        if needs_files:
            episodes = ", ".join(f"E{int(row['episode']):02d}" for row in needs_files[:6])
            LOG.info(
                "Keeping torrent %s: still needed for %s%s",
                torrent_hash[:10], episodes, "…" if len(needs_files) > 6 else "",
            )
            return
        try:
            self.qb.delete(torrent_hash, delete_files=True)
            LOG.info("Removed finished torrent %s", torrent_hash[:10])
        except Exception as error:  # noqa: BLE001
            LOG.warning("Could not remove torrent %s: %s", torrent_hash, error)

    def check_episode(self, job: sqlite3.Row) -> None:
        # The monitor iterates a snapshot; another job on the same torrent may
        # have completed or failed this one while we were filing the pack.
        current = self.database.episode_job(int(job["id"]))
        if not current or str(current["status"]) not in (
            "submitted", "downloading", "organizing"
        ):
            return
        job = current
        torrent_hash = str(job["torrent_hash"] or "")
        if not torrent_hash:
            self.database.update_episode(
                int(job["id"]), status="queued", next_attempt=0,
                error="torrent has no hash yet",
            )
            return
        info = self.qb.info(torrent_hash)
        if not info:
            attempts = int(job["attempts"]) + 1
            if attempts >= 10:
                self.database.update_episode(
                    int(job["id"]), status="failed", attempts=attempts,
                    error="qBittorrent no longer knows this torrent",
                )
            else:
                self.database.update_episode(
                    int(job["id"]), status="queued", attempts=attempts,
                    next_attempt=int(time.time()) + 30,
                )
            return

        state = str(info.get("state") or "")
        if state in ERROR_STATES:
            self.database.update_episode(
                int(job["id"]), status="failed", attempts=int(job["attempts"]) + 1,
                error=f"qBittorrent state: {state}",
            )
            return
        if state == "metaDL":
            # A magnet normally fetches its metadata within seconds. If it has
            # not after the timeout, the info hash has no reachable peers and
            # waiting longer will not help.
            active = int(info.get("time_active") or 0)
            waited = int(time.time()) - int(job["updated_at"] or 0)
            if active >= self.metadata_timeout and waited >= self.metadata_timeout:
                attempts = int(job["attempts"]) + 1
                self.database.update_episode(
                    int(job["id"]),
                    status="failed" if attempts >= self.max_attempts else "queued",
                    attempts=attempts,
                    next_attempt=int(time.time()) + min(1800, 60 * attempts),
                    error=f"no metadata after {active // 60} min: the infohash has no reachable peers",
                )
                LOG.warning(
                    "Torrent %s gave no metadata after %s s; episode %s attempts=%s",
                    torrent_hash[:10], active, job["id"], attempts,
                )
            return
        progress = float(info.get("progress") or 0)
        if progress >= 0.999 and state in DONE_STATES:
            self.organise_torrent(torrent_hash, info)
            return
        if job["status"] != "downloading":
            self.database.update_episode(int(job["id"]), status="downloading", attempts=0, error=None)

    def monitor_loop(self) -> None:
        while not STOP.is_set():
            if self.qb:
                for job in self.database.episodes_active():
                    try:
                        self.check_episode(job)
                    except Exception as error:  # noqa: BLE001
                        LOG.warning("Monitor failed for job %s: %s", job["id"], error)
            try:
                self.refresh_panels()
            except Exception:  # noqa: BLE001
                LOG.debug("Panel refresh failed", exc_info=True)
            STOP.wait(MONITOR_INTERVAL)

    # -- existing movie download path ------------------------------------
    def download_loop(self) -> None:
        while not STOP.is_set():
            job = self.database.next_job()
            if not job:
                STOP.wait(2)
                continue
            try:
                self.process_job(job)
            except Exception as error:  # noqa: BLE001
                LOG.exception("Job %s failed", job["id"])
                terminal = self.database.retry_or_fail(
                    int(job["id"]), int(job["attempts"]), str(error), self.max_attempts
                )
                if terminal:
                    try:
                        self.api.send(
                            int(job["chat_id"]),
                            f"❌ Download failed after {self.max_attempts} attempts: "
                            f"{Path(job['file_name']).name}\n{str(error)[:500]}",
                        )
                    except Exception:  # noqa: BLE001
                        LOG.exception("Could not send terminal failure notification")

    def process_job(self, job: sqlite3.Row) -> None:
        expected_size = int(job["file_size"])
        free = shutil.disk_usage(self.movies_dir).free
        if expected_size and free < expected_size + self.reserve_bytes:
            raise RuntimeError(
                f"Insufficient disk space: {human_size(free)} free; "
                f"keeping {human_size(self.reserve_bytes)} reserved"
            )

        deadline = time.monotonic() + self.get_file_retry_window
        while True:
            remaining = max(1, int(deadline - time.monotonic()))
            try:
                file_info = self.api.call(
                    "getFile", {"file_id": job["file_id"]}, timeout=remaining
                )
                break
            except (ConnectionResetError, TimeoutError, http.client.RemoteDisconnected) as error:
                if deadline - time.monotonic() <= 0:
                    raise RuntimeError(
                        f"Telegram getFile did not complete within "
                        f"{self.get_file_retry_window // 60} minutes: {error}"
                    ) from error
                LOG.warning("Telegram getFile connection dropped; retrying: %s", error)
                STOP.wait(min(5, deadline - time.monotonic()))
        file_path = str(file_info.get("file_path") or "")
        if not file_path:
            raise RuntimeError("Telegram getFile did not return a file path")
        source = Path(file_path)
        if not source.is_absolute():
            source = self.bot_data_dir / source
        if not source.is_file():
            raise RuntimeError(f"Downloaded Telegram file is unavailable at {source}")

        final_name = sanitize_name(str(job["file_name"]), str(job["caption"] or ""))
        destination, already_present = choose_destination(self.movies_dir, final_name, expected_size)
        if already_present:
            size = destination.stat().st_size
            digest = "existing-file-size-match"
        else:
            partial = self.partial_dir / f"{uuid.uuid4().hex}.part"
            try:
                size, digest = copy_and_hash(source, partial)
                if expected_size and size != expected_size:
                    raise RuntimeError(f"Size mismatch: expected {expected_size}, received {size}")
                verify_media(partial)
                os.replace(partial, destination)
            finally:
                partial.unlink(missing_ok=True)

        self.database.finish(
            int(job["id"]), str(job["file_unique_id"]), destination, size, digest
        )
        self.api.send(
            int(job["chat_id"]),
            f"✅ Downloaded: {destination.name}\nSize: {human_size(size)}\nJellyfin folder: Movies",
        )
        try:
            self.api.delete(int(job["chat_id"]), int(job["message_id"]))
        except Exception as error:  # noqa: BLE001
            LOG.warning("File succeeded, but source message deletion failed: %s", error)

    def run(self) -> None:
        self.wait_for_api()
        if STOP.is_set():
            return
        self.wait_for_qbittorrent()
        threads = [
            threading.Thread(target=self.download_loop, name="downloader", daemon=True),
            threading.Thread(target=self.dispatch_loop, name="dispatcher", daemon=True),
            threading.Thread(target=self.monitor_loop, name="monitor", daemon=True),
        ]
        for thread in threads:
            thread.start()
        self.polling_loop()
        for thread in threads:
            thread.join(timeout=15)


def handle_signal(signum: int, _frame: Any) -> None:
    LOG.info("Received signal %s; stopping", signum)
    STOP.set()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    Worker().run()


if __name__ == "__main__":
    main()
