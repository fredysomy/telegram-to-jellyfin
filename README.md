# Telegram → Jellyfin

A private, LAN-only stack that turns Telegram channel posts and series requests
into files in a Jellyfin media library. The Telegram Bot API server and the
ingestion worker run as internal Compose services with no published host ports.

## What runs here

| Service | Purpose |
|---------|---------|
| `telegram-bot-api` | Local Bot API server; downloads Telegram files into persistent storage |
| `telegram-ingest` | SQLite-backed queue, verification, and final file placement |
| `qbittorrent` | Downloads torrents for the series flow (LAN-only Web UI) |
| `jellyfin` | Media server; reads the library mount |
| `stremio` | Optional player / web UI on the LAN |

## Set it up on your own

1. **Get Telegram API credentials.** Sign in at <https://my.telegram.org> and
   create an application. Note its API ID and API hash.
2. **Create the bot.** Message [@BotFather](https://t.me/BotFather), run
   `/newbot`, and copy the token. Also run `/setjoingroups` and disable group
   joins so nobody can add the bot to third-party groups.
3. **Create the secrets file** (this is the only file holding credentials):
   ```sh
   cp telegram-secrets.env.example telegram-secrets.env
   chmod 600 telegram-secrets.env
   ```
   Fill in `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_BOT_TOKEN`, and
   `TELEGRAM_OWNER_ID`. `telegram-secrets.env` is gitignored and never committed.
4. **Create the environment file:**
   ```sh
   cp .env.example .env
   ```
   Adjust the media, Bot API data, and state directories. Set `STREAM_SOURCE_URL`
   only if you have a Stremio-compatible stream endpoint, and set the
   qBittorrent credentials.
5. **Prepare the directories** named in `.env`. The containers run as UID/GID
   `1000:1000`, so those paths must be writable by that account.
6. **Build and start:**
   ```sh
   docker compose build
   docker compose up -d
   docker compose ps
   docker compose logs -f telegram-ingest
   ```
7. **Pair the owner.** Send the bot any private message. It replies with your
   numeric id; put that in `TELEGRAM_OWNER_ID` and recreate the services:
   ```sh
   docker compose up -d --force-recreate telegram-bot-api telegram-ingest
   ```
   With no owner configured the worker fails closed and refuses every command.

## How it works

Both Telegram images build from this directory (`context: .`). The worker keeps
its SQLite queue under the configured state directory, stages partial downloads,
verifies the byte count and media container with `ffprobe`, and then atomically
moves finished files into the Movies or TV Shows library.

### Channel posts

The bot watches the paired private channel. Media attachments are queued in
SQLite, downloaded through the local Bot API, verified, and moved into the Jellyfin
Movies library. Channel ingestion stays pinned to `allowed_chat_id` in the queue
database. `TELEGRAM_CHANNEL_ID` seeds that value only when the database is empty
and needs the full `-100…` id.

### Series requests

In a private chat with the bot:

```
/SeriesSearch kappi squad
  -> inline buttons: one per matching series
  -> "Pick a season": one button per season
  -> "Pick episodes": "All N", individual episodes (paged), or "« Seasons"
  -> optional quality keyboard, only if torrent_source.available_qualities()
     returns labels
  -> batch status message that edits itself as episodes progress
```

`torrent_source.py` is the only file that needs editing to attach a source.
`get_episode_details()` is the placeholder: it returns `(infohash, trackers)` for
one episode. `resolve_episode()` passes those details to `build_magnet()` and
returns the magnet URI, which the worker hands to qBittorrent. As shipped
`get_episode_details()` returns `None`, so the worker instead matches a torrent
dropped into `Incoming/torrents` whose name contains `S01E01` (a `.torrent` file,
or a `.txt`/`.magnet` file holding a magnet URI).

Finished episodes are hard-linked (or copied across filesystems) to
`TV Shows/<Series> (<Year>)/Season NN/<Series> - S01E01 - <name>.mkv`, verified
with `ffprobe`, and the torrent is then removed from qBittorrent.

qBittorrent's Web UI is LAN-only. Credentials come from `QBITTORRENT_USERNAME` /
`QBITTORRENT_PASSWORD` in `.env`.

Note: `Session\DiskIOType=Posix` must be set in the qBittorrent configuration.
Without it libtorrent's io_uring backend wedges every torrent in `checkingDL`
at 0% inside this container.

## Access control

- `TELEGRAM_OWNER_ID` is required and overrides any previously stored pairing,
  so a stale `allowed_user_id` row in the queue database cannot keep access.
- Commands work in the owner's private chat by default.
  `TELEGRAM_COMMAND_CHAT_IDS` (comma-separated) adds groups or channels: in a
  group only `TELEGRAM_OWNER_ID` may run commands, while a channel post carries
  no sender, so anyone who can post in that channel can drive the bot.
- The local Bot API keeps its file store under a directory **named after the bot
  token**. If that path is ever exposed, rotate the token with BotFather
  (`/revoke`), update the secrets file, and force-recreate both services.

## Operations

Run commands from this directory:

```sh
docker compose ps
docker compose logs --tail=100 telegram-ingest
docker compose down
```

Never run `docker compose down --volumes` unless deleting all persistent Telegram
data is intentional. Transient connection drops while Telegram prepares a large
file are retried for 20 minutes by default
(`GET_FILE_RETRY_WINDOW_SECONDS=1200`); the outer job retry limit is five
attempts.

## Data safety

Keep original media until a completed transfer has been verified and an
independent backup has been restored successfully.
