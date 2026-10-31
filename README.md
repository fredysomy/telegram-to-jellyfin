# Telegram Downloader

Standalone Telegram-to-media-library downloader using the Telegram local Bot
API server and a SQLite-backed ingestion worker.

This repository intentionally contains only the Telegram services. It does not
contain the shared Ente, PostgreSQL, MinIO, web, or Jellyfin services.

## Services

`telegram-bot-api` downloads Telegram files into persistent Bot API storage.
`telegram-ingest` polls the local API, queues channel media in SQLite, verifies
the completed file, and moves it atomically into `Movies`.

The services have no published host ports; they communicate over the private
Compose network.

## Setup

```sh
cp .env.example .env
cp telegram-secrets.env.example telegram-secrets.env
# Edit .env and telegram-secrets.env
docker compose build
docker compose up -d
docker compose ps
docker compose logs -f telegram-ingest
```

The containers run as UID/GID `1000:1000`. The configured media, Bot API data,
and state directories must be writable by that account.

To preserve an existing installation, point `TELEGRAM_BOT_API_DATA_DIR` and
`TELEGRAM_STATE_DIR` at the existing directories before starting the stack.
The SQLite queue and channel pairing metadata are stored in `queue.sqlite3`.

## Retry behavior

Transient connection drops while Telegram is preparing a large file are retried
for 20 minutes by default (`GET_FILE_RETRY_WINDOW_SECONDS=1200`). The normal
outer job retry limit remains five attempts (`MAX_ATTEMPTS=5`).

## Security

Never commit `.env`, `telegram-secrets.env`, Telegram API credentials, bot
tokens, or persistent runtime data. The included example files contain no
credentials.

Do not use `docker compose down --volumes` unless deleting all persistent
Telegram data is intentional.
