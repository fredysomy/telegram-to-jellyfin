#!/usr/bin/env python3
"""Private Telegram channel to Jellyfin media ingestion worker."""

from __future__ import annotations

import hashlib
import http.client
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
from pathlib import Path
from typing import Any


LOG = logging.getLogger("telegram-ingest")
STOP = threading.Event()
ALLOWED_EXTENSIONS = {
    ".avi", ".m2ts", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg",
    ".mpg", ".mts", ".ts", ".webm", ".wmv",
}


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
        encoded = urllib.parse.urlencode(data or {}).encode()
        request = urllib.request.Request(
            f"{self.root}/bot{self.token}/{method}", data=encoded, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read(2048).decode("utf-8", "replace")
            raise RuntimeError(f"Telegram {method} HTTP {error.code}: {detail}") from error
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram {method} failed: {payload.get('description', 'unknown error')}")
        return payload.get("result")

    def send(self, chat_id: int, text: str) -> dict[str, Any]:
        return self.call("sendMessage", {"chat_id": chat_id, "text": text[:4096]})

    def edit(self, chat_id: int, message_id: int, text: str) -> None:
        self.call(
            "editMessageText",
            {"chat_id": chat_id, "message_id": message_id, "text": text[:4096]},
        )

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
                UPDATE jobs SET status='queued' WHERE status='downloading';
                """
            )

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


def sanitize_name(file_name: str, caption: str) -> str:
    original = Path(file_name).name
    extension = Path(original).suffix.lower()
    base = caption.strip().splitlines()[0] if caption.strip() and not caption.lstrip().startswith("/") else Path(original).stem
    base = re.sub(r"[\x00-\x1f<>:\"/\\|?*]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip(" .")[:180]
    if not base:
        base = f"telegram-{uuid.uuid4().hex[:12]}"
    if extension and base.lower().endswith(extension):
        return base
    return f"{base}{extension}"


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
        self.bot_data_dir = Path(os.environ.get("BOT_DATA_DIRECTORY", "/var/lib/telegram-bot-api"))
        self.reserve_bytes = int(os.environ.get("FREE_SPACE_RESERVE_BYTES", str(10 * 1024**3)))
        self.max_attempts = int(os.environ.get("MAX_ATTEMPTS", "5"))
        self.get_file_retry_window = max(
            60, int(os.environ.get("GET_FILE_RETRY_WINDOW_SECONDS", str(20 * 60)))
        )
        self.progress_interval = max(15, int(os.environ.get("PROGRESS_INTERVAL_SECONDS", "20")))
        configured_chat = secrets.get("TELEGRAM_CHANNEL_ID", "")
        if configured_chat and not self.database.get_meta("allowed_chat_id"):
            self.database.set_meta("allowed_chat_id", configured_chat)
        self.partial_dir.mkdir(parents=True, exist_ok=True)
        self.movies_dir.mkdir(parents=True, exist_ok=True)

    def wait_for_api(self) -> None:
        while not STOP.is_set():
            try:
                identity = self.api.call("getMe", timeout=10)
                LOG.info("Connected to Telegram as @%s", identity.get("username", "unknown"))
                return
            except Exception as error:  # noqa: BLE001
                LOG.warning("Waiting for local Bot API: %s", error)
                STOP.wait(5)

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
                        "allowed_updates": json.dumps(["channel_post"]),
                    },
                    timeout=40,
                )
                for update in updates:
                    update_id = int(update["update_id"])
                    self.handle_update(update)
                    offset = max(offset, update_id + 1)
                    self.database.set_meta("update_offset", str(offset))
            except Exception as error:  # noqa: BLE001
                LOG.error("Polling error: %s", error)
                STOP.wait(5)

    def handle_update(self, update: dict[str, Any]) -> None:
        post = update.get("channel_post") or {}
        chat = post.get("chat") or {}
        if chat.get("type") != "channel":
            return
        chat_id = int(chat["id"])
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

    def staged_download_bytes(self) -> int:
        total = 0
        try:
            for candidate in self.bot_data_dir.glob("*/temp/*"):
                if candidate.is_file():
                    total += candidate.stat().st_size
        except OSError:
            pass
        return total

    def get_file_with_retry(self, file_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.get_file_retry_window
        while True:
            remaining = max(1, int(deadline - time.monotonic()))
            try:
                return self.api.call("getFile", {"file_id": file_id}, timeout=remaining)
            except (ConnectionResetError, TimeoutError, http.client.RemoteDisconnected) as error:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        f"Telegram getFile did not complete within "
                        f"{self.get_file_retry_window // 60} minutes: {error}"
                    ) from error
                LOG.warning(
                    "Telegram getFile connection dropped; retrying for up to %.1f more minutes: %s",
                    remaining / 60,
                    error,
                )
                STOP.wait(min(5, remaining))

    def progress_loop(
        self,
        chat_id: int,
        progress_message_id: int,
        file_name: str,
        expected_size: int,
        started: float,
        stopped: threading.Event,
    ) -> None:
        while not stopped.wait(self.progress_interval):
            elapsed_minutes = max(1, int((time.monotonic() - started) // 60))
            downloaded = self.staged_download_bytes()
            if not downloaded:
                try:
                    downloaded = sum(
                        candidate.stat().st_size
                        for candidate in self.partial_dir.glob("*.part")
                        if candidate.is_file()
                    )
                except OSError:
                    downloaded = 0
            progress = f"\nProgress: {human_size(downloaded)}"
            if expected_size:
                percent = min(100.0, downloaded * 100.0 / expected_size)
                progress += f" / {human_size(expected_size)} ({percent:.0f}%)"
            text = f"⏬ Downloading: {Path(file_name).name}\nElapsed: {elapsed_minutes} min{progress}"
            try:
                self.api.edit(chat_id, progress_message_id, text)
            except Exception as error:  # noqa: BLE001
                LOG.warning("Could not update download progress: %s", error)

    def process_job(self, job: sqlite3.Row) -> None:
        chat_id = int(job["chat_id"])
        progress_message_id: int | None = None
        progress_stop = threading.Event()
        progress_thread: threading.Thread | None = None
        started = time.monotonic()
        try:
            progress_message = self.api.send(
                chat_id,
                f"⏬ Downloading: {Path(job['file_name']).name}\nElapsed: 0 min\nProgress: starting",
            )
            progress_message_id = int(progress_message["message_id"])
            progress_thread = threading.Thread(
                target=self.progress_loop,
                args=(
                    chat_id,
                    progress_message_id,
                    str(job["file_name"]),
                    int(job["file_size"]),
                    started,
                    progress_stop,
                ),
                name=f"progress-{job['id']}",
                daemon=True,
            )
            progress_thread.start()
        except Exception as error:  # noqa: BLE001
            LOG.warning("Could not send download progress message: %s", error)
        expected_size = int(job["file_size"])
        try:
            free = shutil.disk_usage(self.movies_dir).free
            if expected_size and free < expected_size + self.reserve_bytes:
                raise RuntimeError(
                    f"Insufficient disk space: {human_size(free)} free; "
                    f"keeping {human_size(self.reserve_bytes)} reserved"
                )

            file_info = self.get_file_with_retry(str(job["file_id"]))
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
            success_text = (
                f"✅ Downloaded: {destination.name}\nSize: {human_size(size)}\nJellyfin folder: Movies"
            )
            if progress_message_id is not None:
                try:
                    self.api.edit(chat_id, progress_message_id, success_text)
                except Exception as error:  # noqa: BLE001
                    LOG.warning("Could not finalize download progress message: %s", error)
            else:
                self.api.send(chat_id, success_text)
            try:
                self.api.delete(chat_id, int(job["message_id"]))
            except Exception as error:  # noqa: BLE001
                LOG.warning("File succeeded, but source message deletion failed: %s", error)
        except Exception as error:
            if progress_message_id is not None:
                try:
                    elapsed_minutes = max(1, int((time.monotonic() - started) // 60))
                    self.api.edit(
                        chat_id,
                        progress_message_id,
                        f"⚠️ Download attempt failed after {elapsed_minutes} min; retrying.\n"
                        f"{Path(job['file_name']).name}",
                    )
                except Exception:  # noqa: BLE001
                    LOG.exception("Could not update failed download progress")
            raise
        finally:
            progress_stop.set()
            if progress_thread is not None:
                progress_thread.join(timeout=2)

    def run(self) -> None:
        self.wait_for_api()
        if STOP.is_set():
            return
        download_thread = threading.Thread(target=self.download_loop, name="downloader", daemon=True)
        download_thread.start()
        self.polling_loop()
        download_thread.join(timeout=15)


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
