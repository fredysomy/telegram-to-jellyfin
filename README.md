# Home Server Setup

This directory is the source of truth for self-hosted services on this machine.

## Files

- `home-server.yaml`: host, service, port, and storage inventory
- `compose.yaml`: Ente Docker services
- `ente-museum.yaml`: private Ente server configuration and credentials
- `nginx.conf`: prepared LAN reverse-proxy configuration; not enabled yet
- `telegram-ingestion-plan.md`: design for the private media-ingestion bot
- `telegram/`: pinned local Bot API build and downloader worker source

Runtime application data is kept separately in `/home/fredy/Home Server Data`.
Media libraries and downloader staging are in `/home/fredy/Media Library`.

## Ente lifecycle

Run commands from this directory:

```sh
docker compose up -d
docker compose ps
docker compose logs --tail=100 museum
docker compose down
```

Never run `docker compose down --volumes` unless permanent deletion of the Ente
database and stored files is intended.

## Jellyfin

Jellyfin is exposed on the LAN at `http://192.168.1.7:8096`. Its `/media` mount
is read-only. Downloaders write to `Media Library/Incoming/.partial`, verify the
completed file, and atomically move it into `Media Library/Movies` or `TV Shows`.

## Telegram ingestion

The Telegram Bot API and ingestion worker are internal-only Compose services. They
have no published host ports. The worker keeps its SQLite queue under
`/home/fredy/Home Server Data/Telegram/state`, stages partial downloads, verifies
the byte count and media container with `ffprobe`, then moves completed files into
the Jellyfin Movies library.

Both Telegram images build from this directory (`context: .`), so the `telegram/`
subdirectory referenced by older notes does not exist.

### Access control

`telegram-secrets.env` (owned by `fredy`, mode `0600`) is the whole trust boundary:
`TELEGRAM_BOT_TOKEN` plus `TELEGRAM_OWNER_ID`.

- **`TELEGRAM_OWNER_ID` is required.** The worker fails closed: with no owner
  configured it refuses every command and logs `Refused unpaired Telegram user
  <id>`. Send the bot any private message, copy the id it replies with, put
  `TELEGRAM_OWNER_ID=<id>` in the secrets file, then restart `telegram-ingest`.
- `TELEGRAM_OWNER_ID` overrides a previously stored pairing, so a stale
  `allowed_user_id` row in the queue database cannot keep access.
- Commands work in the owner's private chat by default. `TELEGRAM_COMMAND_CHAT_IDS`
  (comma-separated) adds groups or channels: in a group only `TELEGRAM_OWNER_ID`
  may run them, while a channel post carries no sender, so anyone who can post in
  that channel can drive the bot. Channel ingestion stays pinned to
  `allowed_chat_id` in the queue database (`TELEGRAM_CHANNEL_ID` seeds it only
  when the database is empty, and needs the full `-100…` id).
- The local Bot API keeps its file store under a directory **named after the bot
  token**. If that path is ever exposed, rotate the token with BotFather
  (`/revoke`), update the secrets file, then
  `docker compose up -d --force-recreate telegram-bot-api telegram-ingest`.
- In BotFather, disable group joins (`/setjoingroups`) so the bot cannot be added
  to third-party groups.

### Series flow

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
one episode. `resolve_episode()` takes those details, passes them to
`build_magnet()`, and returns the magnet URI; the worker hands that to
qBittorrent. As shipped `get_episode_details()` returns `None`, so the worker
instead matches a torrent dropped into
`/home/fredy/Media Library/Incoming/torrents` whose name contains `S01E01`
(a `.torrent` file, or a `.txt`/`.magnet` file holding a magnet URI).

Finished episodes are hard-linked (or copied across filesystems) to
`TV Shows/<Series> (<Year>)/Season NN/<Series> - S01E01 - <name>.mkv`, verified
with `ffprobe`, and the torrent is then removed from qBittorrent.

qBittorrent runs on `http://192.168.1.7:8095` (LAN only) with downloads in
`/home/fredy/Media Library/Incoming/torrent-downloads`. Credentials come from
`QBITTORRENT_USERNAME` / `QBITTORRENT_PASSWORD` in `.env`.

Note: `Session\DiskIOType=Posix` is set in
`/home/fredy/Home Server Data/qBittorrent/qBittorrent/qBittorrent.conf`. Without
it libtorrent's io_uring backend wedges every torrent in `checkingDL` at 0%
inside this container.


## Data safety

An Ente import creates an encrypted server-side copy; it is not an in-place move.
Keep the original media until the full item count is reconciled and an independent
backup/export has been restored successfully.
