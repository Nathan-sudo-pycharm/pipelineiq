# Build Notes — PipelineIQ

Mistakes made, things learned, and concepts explained during the 5-day build.

---

## Docker & Environment

**Mistake: `.env` file not being read**
Docker kept warning `POSTGRES_USER variable is not set`. The file existed but Docker couldn't find it. Turned out the file was named correctly — the warnings were just noise because the containers started fine anyway. Lesson: check if the containers actually started before panicking about warnings.

**Mistake: `app` service in docker-compose.yml before Dockerfile existed**
Added the `app` service to `docker-compose.yml` on Day 1 but had no Dockerfile yet. Docker tried to build it and failed. Fix: remove the `app` service until the code exists. Don't define infrastructure for things that don't exist yet.

**Mistake: `volumes` block indented inside `services`**
The `volumes:` block at the bottom of `docker-compose.yml` was indented under `services:` instead of sitting at the top level. Docker read it as another service called `volumes`. Fix: `volumes:` must have zero indentation, same level as `services:`.

**Mistake: Ports mapped backwards in docker-compose.yml**
Had `"8000:8080"` instead of `"8080:8000"`. The format is `host_port:container_port`. So `8080:8000` means "my machine's port 8080 maps to the container's port 8000". Getting it backwards means nothing works and it's not obvious why.

**Mistake: REDIS_HOST and POSTGRES_HOST set to service names when running scripts locally**
`.env` had `REDIS_HOST=redis` and `POSTGRES_HOST=postgres` — the internal Docker network names. These only work from inside a container. When running Python scripts directly on your machine, you need `localhost`. Fix for local scripts: `$env:REDIS_HOST="localhost"; python script.py`. When everything runs inside Docker, switch back to the service names.

---

## Python & FastAPI

**Mistake: Calling `app.include_router()` before `app = FastAPI()` was created**
Python runs top to bottom. If you try to call a method on `app` before `app` exists, it crashes. Always create the FastAPI instance first, then call methods on it.

**Mistake: `routers/pipelines.py` named `routers/pipeline.py` (missing the s)**
Python couldn't find the module. `ModuleNotFoundError: No module named 'routers.pipelines'`. One character difference, entire import fails. Always double check file names match exactly what you're importing.

**Mistake: Running uvicorn from the wrong folder**
Running `uvicorn main:app` from the root `pipelineiq/` folder instead of the `app/` folder. Uvicorn looks for `main.py` in the current directory. If you're in the wrong folder, it can't find it. Always `cd app` first.

**Mistake: Port 8000 blocked on Windows**
`WinError 10013` when starting uvicorn on port 8000. Something else on the machine was using it. Fix: run on a different port with `--port 8080`.

---

## Redis

**Q: Why write to Redis instead of directly to Postgres?**
Because it decouples ingestion from processing. The ingest endpoint returns immediately after writing to the stream — it doesn't wait for the worker to finish. If the worker crashes, events are still sitting in the stream waiting. Nothing is lost.

**Q: What is a consumer group?**
A way for multiple worker instances to read from the same stream without each one seeing the same message. Redis keeps track of which messages have been delivered to which worker. When a worker finishes, it sends XACK. If it crashes without ACKing, Redis redelivers the message.

**Q: What does the message ID `1778584726435-0` mean?**
It's a Unix timestamp in milliseconds plus a sequence number. This means every event in a Redis Stream is automatically time-ordered. You never need to sort manually.

---

## PostgreSQL

**Mistake: Sending `"pipeline-001"` as a UUID**
The `pipeline_id` column was defined as `UUID` type in Postgres. `"pipeline-001"` is a plain string, not a valid UUID. Postgres rejected it. Fix for development: alter the column type to `TEXT` so we can test without real pipeline records. In production, `pipeline_id` should always reference a real row in `pipeline_config`.

**Mistake: Trying to ALTER a column type while a foreign key constraint existed**
Tried to change `pipeline_id` from UUID to TEXT but Postgres blocked it because of the foreign key referencing `pipeline_config.id`. You can't change a column type if a foreign key depends on it being a specific type. Fix: drop the foreign key constraint first, then alter the type.

**Q: Why use `$1, $2` placeholders instead of f-strings in SQL?**
SQL injection. If you build SQL strings with f-strings and a user sends malicious input like `'); DROP TABLE events; --`, it becomes part of the SQL and executes. With parameterized queries, values are always treated as data, never as SQL commands. Non-negotiable in any backend code.

**Q: Why is `dead_letter.raw_payload` TEXT and not JSONB?**
Because the events that land in dead_letter may be malformed JSON — that's often why they were rejected in the first place. JSONB would refuse to store invalid JSON. TEXT accepts anything, which is exactly what a safety net table needs.

**Q: Why does `aggregates` need a unique index?**
The worker uses `ON CONFLICT DO UPDATE` (upsert) to update aggregate stats as new events arrive in the same minute window. For this to work, Postgres needs to know what "conflict" means — a unique index on `(pipeline_id, window_start, metric)` tells it "these three columns together must be unique". Without it, the upsert fails.

**Q: What is `RETURNING` in an INSERT statement?**
Normally after an INSERT, Postgres just says "done". `RETURNING` tells it to also send back the newly created row — including auto-generated fields like `id` and `created_at`. This saves a second round-trip query to fetch the row you just inserted.

**Q: What is the difference between `fetch`, `fetchrow`, and `fetchval` in asyncpg?**

- `fetch` — returns a list of rows (use when expecting multiple results)
- `fetchrow` — returns a single row or None (use when expecting exactly one result)
- `fetchval` — returns a single value (use for COUNT(\*) or similar single-value queries)

---

## Architecture concepts

**Q: Why put the global exception handler in `main.py` and not `ingest.py`?**
Because `main.py` is the top level of the entire app. A handler registered there catches errors from every endpoint automatically. If it were in `ingest.py`, it would only cover webhook errors — not CSV, WebSocket, or any future endpoint.

**Q: What is pagination and why do we need it?**
Returning all events at once from a large table would be slow, memory-intensive, and could crash the client. Pagination returns a small chunk at a time using `limit` (how many to return) and `offset` (how many to skip). Like pages in a book — you read one page at a time, not the whole thing at once.

**Q: Why does the worker re-raise `HTTPException` separately from `Exception`?**
If we only caught the generic `Exception`, our deliberate `404 Not Found` responses would get swallowed and returned as `500 Internal Server Error` instead. By catching `HTTPException` first and re-raising it, FastAPI handles it correctly with the right status code.

**Q: Why does the Dockerfile copy `requirements.txt` before copying the app code?**
Docker layer caching. Each instruction in a Dockerfile creates a cached layer. If you copy requirements first and install them, Docker only re-runs `pip install` when `requirements.txt` changes — not every time you change a Python file. This makes rebuilds much faster during development.

**Q: What does `0.0.0.0` mean in `uvicorn main:app --host 0.0.0.0`?**
It means "listen on all network interfaces". Without it, uvicorn only accepts connections from inside the container itself. `0.0.0.0` is what makes the app reachable from outside the container.

---

## General lessons

- Always read the full error message top to bottom. The actual cause is usually at the bottom, not the middle.
- Check `docker logs <container_name>` before assuming a container is broken.
- `docker compose ps` is your first debugging tool — check status before anything else.
- When something works locally but not in Docker, the first thing to check is hostnames. `localhost` becomes the service name inside Docker networks.
- Commit after every working milestone. If something breaks, you can always go back.
