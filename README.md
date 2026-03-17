# openclaw-openclaw-ingestor

Standalone OpenClaw chat + attachment ingestor that writes exclusively to the Memory Database API. Zero direct Postgres dependency.

## What It Does

- **Watches** OpenClaw JSONL session files for new messages in real-time
- **Extracts** text content and image attachments from message blocks
- **Uploads** everything to the Memory Database API (messages, attachments, links)
- **Handles** duplicates gracefully (409 = already exists = skip)

## Session File Locations

- `/data/.openclaw/agents/main/sessions/*.jsonl` — Chat sessions
- `/data/.openclaw/cron/runs/*.jsonl` — Cron job runs

## Setup

```bash
npm install
cp .env.example .env
# Edit .env with your API credentials
```

## Usage

### Real-time Watcher (main process)

```bash
npm start
# or
npm run dev  # with hot-reload via tsx
```

Watches both session directories, debounces changes (2s), tracks file offsets, polls every 60s as safety net.

### One-shot Sync

```bash
npm run sync              # Incremental (only new data since last run)
npm run sync -- --full    # Full re-sync (reprocess everything)
```

### Build

```bash
npm run build
```

## Docker

```bash
docker build -t openclaw-ingestor .
docker run -d \
  -e MEMORY_DATABASE_API_URL=https://memory-database.etdofresh.com \
  -e MEMORY_DATABASE_API_WRITE_TOKEN=your_token \
  -v /data/.openclaw:/data/.openclaw:ro \
  openclaw-ingestor
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MEMORY_DATABASE_API_URL` | No | `https://memory-database.etdofresh.com` | Memory DB API base URL |
| `MEMORY_DATABASE_API_WRITE_TOKEN` | Yes | — | Bearer token for API writes |

## Architecture

```
src/
├── index.ts        — Entry point, starts watcher
├── watcher.ts      — Real-time file watcher with debounce + polling
├── sync.ts         — One-shot full sync script
├── ingest.ts       — Process a single JSONL entry
├── attachments.ts  — Extract and upload image attachments
└── api.ts          — Memory DB API client (retries, 429 handling)
```

### Key Design Decisions

- **API-only writes**: No Postgres driver, no database connection. If the API is down, log and skip.
- **Offset tracking**: Both watcher and sync track byte offsets per file to avoid reprocessing.
- **Attachment handling**: Base64 images in content blocks are decoded, uploaded, and linked to messages.
- **Retry logic**: 3 attempts with exponential backoff, 429-aware (respects Retry-After header).
- **Content truncation**: Message content capped at 100k characters.

## License

MIT
