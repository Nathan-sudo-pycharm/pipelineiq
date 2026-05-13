# PipelineIQ

Real-time data ingestion and processing pipeline with streaming analytics.

## Run it

```bash
git clone https://github.com/Nathan-sudo-pycharm/pipelineiq.git
cd pipelineiq
docker compose up --build
```

API docs at `http://localhost:8080/docs`

## Endpoints

| Method | Endpoint                 | Description         |
| ------ | ------------------------ | ------------------- |
| POST   | /ingest/webhook          | Ingest a JSON event |
| POST   | /pipelines               | Create a pipeline   |
| GET    | /pipelines               | List pipelines      |
| GET    | /pipelines/:id/events    | Raw events          |
| GET    | /pipelines/:id/analytics | Aggregated stats    |
| GET    | /health                  | Health check        |

## Why this exists

Most data collection setups are fragile — invalid events get silently written to the database, processing logic is hardcoded, and analytics queries get slower as data grows. This project is built around fixing those problems from the ground up.

## Goal

To demonstrate production-grade pipeline thinking: reliable ingestion, configurable processing, fast reads, and full observability over bad data — without relying on AI tooling to make it interesting.

## Future improvements

- WebSocket live feed for real-time event monitoring
- CSV bulk upload endpoint
- Frontend dashboard (Next.js) with live charts
- Kafka as an optional drop-in for Redis Streams at scale
- Authentication on the API
- Replay endpoint to reprocess dead-letter events

## Stack

FastAPI · Redis Streams · PostgreSQL · Docker Compose

---

For architecture and design details see [Architecture](docs/ARCHITECTURE.md)
