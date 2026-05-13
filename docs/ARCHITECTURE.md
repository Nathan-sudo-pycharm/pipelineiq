# Architecture — PipelineIQ

## System overview

PipelineIQ is built around a simple idea: separate the act of receiving data from the act of processing it. The ingest layer is fast and dumb — it just validates and queues. The worker layer is where the actual work happens, asynchronously and reliably.

```
Webhook POST
     ↓
FastAPI Ingest Layer
  - Pydantic validates the event shape
  - Invalid events → dead_letter table (never silently dropped)
  - Valid events → Redis Stream (XADD)
     ↓
Redis Streams (durable event log)
  - Append-only log, survives restarts
  - Consumer groups — multiple workers, no duplicates
  - If worker crashes before ACK, message is redelivered
     ↓
Stream Consumer Worker (asyncio)
  - XREADGROUP polls for new messages every 2 seconds
  - Filter  → should this event be processed?
  - Transform → normalise fields, convert units
  - Aggregate → rolling window stats written to aggregates table
  - Persist → raw event written to raw_events table
  - XACK → tells Redis this message is done
     ↓
PostgreSQL
  raw_events      — permanent record, never deleted
  aggregates      — pre-computed per-minute window stats
  pipeline_config — user-defined rules stored as JSONB
  dead_letter     — rejected events with raw payload + error
     ↓
FastAPI Query Layer
  GET /pipelines/:id/events    — paginated raw events
  GET /pipelines/:id/analytics — pre-computed aggregates
```

---

## Components

### FastAPI (ingest + query)

Two responsibilities handled by separate routers:

`routers/ingest.py` — accepts incoming events, validates them against a Pydantic model, publishes to Redis Stream. Returns immediately after XADD — no processing happens in the request cycle.

`routers/query.py` — serves paginated raw events and pre-computed aggregates from Postgres. Reads are fast because aggregates are pre-computed — no GROUP BY on large tables at query time.

`routers/pipelines.py` — CRUD for pipeline configs. Users create pipelines with filter and transform rules stored as JSONB. No redeploy needed to change pipeline behaviour.

`main.py` — registers all routers and a global `RequestValidationError` handler. Any endpoint that receives a malformed request writes to dead_letter before returning 422.

### Redis Streams

Used as the event broker between ingest and workers. Key properties:

- **Append-only** — every XADD gets a unique ID (timestamp + sequence). Events are always time-ordered.
- **Consumer groups** — multiple worker instances share a group. Each message is delivered to exactly one worker. No duplicates.
- **ACK semantics** — worker sends XACK only after successfully writing to Postgres. If it crashes mid-processing, Redis redelivers the message to another worker.
- **Replay** — historical events can be re-read from any point in the stream by resetting the consumer group offset.

### Stream consumer worker

Runs as a separate process (its own Docker container). Main loop:

1. `XREADGROUP` — ask Redis for up to 10 unprocessed messages, block for 2 seconds if stream is empty
2. For each message: filter → transform → aggregate → persist → XACK
3. On error: log, sleep 1 second, retry

Filter and transform are currently pass-through placeholders. In a real deployment they would load rules from `pipeline_config` and apply them per pipeline.

### PostgreSQL

Four tables:

**`pipeline_config`** — stores user-defined pipelines. `rules` column is JSONB — flexible schema, queryable, indexable. No redeploy needed to change processing behaviour.

**`raw_events`** — permanent ledger. Every event that passes validation lands here exactly as it arrived. Never deleted. Source of truth for replay.

**`aggregates`** — pre-computed rolling window stats. One row per pipeline + minute window + metric. Uses `ON CONFLICT DO UPDATE` (upsert) so the worker can update the same row as new events arrive in the same window without duplicate inserts.

**`dead_letter`** — safety net. `raw_payload` is stored as plain TEXT (not JSONB) because the payload may itself be malformed JSON. Plain text accepts anything.

### Docker Compose

Four services:

- `postgres` — mounts `schema.sql` into `docker-entrypoint-initdb.d/` so the schema is applied automatically on first startup
- `redis` — runs with `--appendonly yes` for persistence across restarts
- `app` — FastAPI, built from Dockerfile, depends on postgres and redis
- `worker` — same Docker image as app, overrides CMD to run `python -m workers.consumer`

One command starts everything: `docker compose up --build`

---

## Key design decisions

**Redis Streams over Kafka** — provides the same durable log semantics, consumer groups, and replay support without the operational overhead. Kafka scales further but requires ZooKeeper/KRaft and significant infra to run locally. Redis Streams is the right call for this scope.

**Pipeline config in the database** — filter and transform rules are stored as JSONB per pipeline, not hardcoded. This makes the system a configurable platform rather than a fixed script. Change behaviour without touching code.

**Pre-aggregated stats** — rolling window stats are computed by workers at write time. The query layer reads pre-computed results. This keeps dashboard queries fast regardless of how many raw events exist.

**Dead-letter log** — invalid events are never silently dropped. Full context is preserved: raw payload, source, error reason, timestamp. This enables debugging upstream data quality issues and replaying fixed events later.

**ACK after persist** — the worker only ACKs a message after successfully writing to Postgres. If the Postgres write fails, the message stays unacknowledged and Redis redelivers it. This gives at-least-once processing guarantees.

**Parameterized queries** — all database writes use `$1, $2` placeholders, never f-strings. This prevents SQL injection — user-supplied data is always treated as data, never as SQL.

---

## Database schema

```sql
pipeline_config (id, name, description, rules JSONB, created_at, updated_at)
raw_events      (id, pipeline_id, source, payload JSONB, received_at)
aggregates      (id, pipeline_id, window_start, window_end, metric, count, avg, min, max)
dead_letter     (id, pipeline_id, source, raw_payload TEXT, error, received_at)
```

Key indexes:

- `raw_events(pipeline_id, received_at DESC)` — fast paginated queries per pipeline
- `aggregates(pipeline_id, window_start, metric)` UNIQUE — enables upsert without duplicates
- `dead_letter(pipeline_id)` — fast lookup of rejected events per pipeline

---

## Local development

```bash
# Clone and start everything
git clone <your-repo-url>
cd pipelineiq
docker compose up --build

# API docs
http://localhost:8080/docs

# Check logs
docker logs pipelineiq_app
docker logs pipelineiq_worker

# Query Postgres directly
docker exec -it pipelineiq_postgres psql -U pipelineiq -d pipelineiq

# Run Redis smoke test (from app folder)
$env:REDIS_HOST="localhost"; python scripts/test_redis.py
```
