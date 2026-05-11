# PipelineIQ

A real-time data ingestion and processing pipeline with streaming analytics.

---

## What is this?

PipelineIQ is a data pipeline platform that pulls in events from multiple sources, runs them through a configurable processing chain, and makes the results available via a REST API and live WebSocket feed.

Data comes in through webhooks, CSV uploads, or a WebSocket stream. Each event gets validated, filtered, transformed according to rules you define, and written to a database. No AI, no magic — just solid data engineering.

---

## The problem

When you're collecting data from more than one source, a few things tend to go wrong.

Events arrive in different shapes. Some are malformed. Invalid records get written to the database anyway and quietly corrupt your analytics. The logic that filters or cleans data is buried in code, so changing a rule means a redeployment. Aggregations run at query time against a growing raw events table, and dashboards get slower as data accumulates. And if there's ever a bug in your processing logic, the historical data that ran through it is already gone.

PipelineIQ is built around fixing those specific things. Events are validated on arrival and routed to a dead-letter log if they fail. Filter and transform rules live in the database, not in code. Aggregates are computed by workers at write time so queries stay fast. And because the event log is durable, you can replay historical data through updated pipeline logic whenever you need to.

---

## Tech stack

| Layer | Technology |
|---|---|
| API & Ingest | FastAPI (Python) |
| Stream Broker | Redis Streams |
| Workers | asyncio consumers |
| Database | PostgreSQL |
| Frontend | Next.js |
| Infrastructure | Docker Compose |

---

## Status

Active development.
