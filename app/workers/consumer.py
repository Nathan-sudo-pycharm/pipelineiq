# workers/consumer.py

import asyncio
import json
import asyncpg
from datetime import datetime, timezone
from services.redis_client import get_redis_client
from services.db import get_db_connection
from config import settings

# The stream name must match exactly what the ingest endpoint writes to
STREAM_NAME = "pipelineiq:events"

# Consumer group name — all worker instances share this group
# so each event is only processed by one worker, never duplicated
GROUP_NAME = "pipeline-workers"

# This worker's unique name within the group
# If you run multiple workers, give each a different name
CONSUMER_NAME = "worker-1"

# How long to wait for new messages before checking again (milliseconds)
# 2000ms = 2 seconds — worker checks Redis every 2 seconds if stream is quiet
BLOCK_MS = 2000


# ── FILTER STEP ──────────────────────────────────────────────────────────────
# Decides whether this event should be processed or dropped
# Returns True if the event should continue, False if it should be dropped
# Right now we accept everything — in Day 4 we'll load rules from pipeline_config
def should_process(payload: dict) -> bool:
    # Placeholder filter — always returns True for now
    # Later this will check against user-defined rules in pipeline_config
    return True


# ── TRANSFORM STEP ────────────────────────────────────────────────────────────
# Cleans and shapes the payload before persisting
# Returns the transformed payload as a dict
def transform(payload: dict) -> dict:
    # Placeholder transform — returns payload as-is for now
    # Later this will apply user-defined transformations e.g unit conversion
    # normalising field names, adding computed fields etc
    return payload


# ── AGGREGATE STEP ────────────────────────────────────────────────────────────
# Computes rolling window stats and writes them to the aggregates table
# We group by the current minute — so every event in the same minute
# contributes to the same aggregate row
async def aggregate(conn: asyncpg.Connection, pipeline_id: str, payload: dict):
    # We only aggregate numeric values — skip if no numeric fields found
    numeric_fields = {k: v for k, v in payload.items() if isinstance(v, (int, float))}

    if not numeric_fields:
        return

    # Get the start of the current minute as the window start
    # e.g if it's 12:25:38, window_start = 12:25:00
    now = datetime.now(timezone.utc)
    window_start = now.replace(second=0, microsecond=0)
    window_end = now.replace(second=59, microsecond=999999)

    # Write one aggregate row per numeric field
    for metric, value in numeric_fields.items():
        # ON CONFLICT handles the case where a row for this
        # pipeline + window + metric already exists
        # Instead of inserting a duplicate, we update the existing row
        # incrementing count and recalculating avg, min, max
        await conn.execute(
            """
            INSERT INTO aggregates (pipeline_id, window_start, window_end, metric, count, avg, min, max)
            VALUES ($1, $2, $3, $4, 1, $5, $5, $5)
            ON CONFLICT (pipeline_id, window_start, metric)
            DO UPDATE SET
                count = aggregates.count + 1,
                avg   = (aggregates.avg * aggregates.count + $5) / (aggregates.count + 1),
                min   = LEAST(aggregates.min, $5),
                max   = GREATEST(aggregates.max, $5)
            """,
            pipeline_id,
            window_start,
            window_end,
            metric,
            float(value)
        )


# ── PERSIST STEP ──────────────────────────────────────────────────────────────
# Writes the raw event to the raw_events table
# This is the permanent record — we never delete from this table
async def persist(conn: asyncpg.Connection, pipeline_id: str, source: str, payload: dict):
    await conn.execute(
        """
        INSERT INTO raw_events (pipeline_id, source, payload)
        VALUES ($1, $2, $3)
        """,
        pipeline_id,
        source,
        json.dumps(payload)
    )


# ── PROCESS ONE MESSAGE ───────────────────────────────────────────────────────
# Runs a single message through the full pipeline:
# filter → transform → aggregate → persist → ACK
async def process_message(redis, conn: asyncpg.Connection, stream: str, message_id: str, fields: dict):
    # Parse the payload back from JSON string to dict
    # Remember in ingest.py we did json.dumps() before writing to Redis
    # Now we reverse that with json.loads()
    try:
        payload = json.loads(fields.get("payload", "{}"))
    except json.JSONDecodeError:
        # Payload is corrupted — ACK it so it doesn't block the stream
        # and move on
        print(f"[worker] Corrupted payload in message {message_id}, skipping")
        await redis.xack(stream, GROUP_NAME, message_id)
        return

    pipeline_id = fields.get("pipeline_id", "unknown")
    source = fields.get("source", "unknown")

    # FILTER — should we process this event?
    if not should_process(payload):
        print(f"[worker] Filtered out message {message_id}")
        await redis.xack(stream, GROUP_NAME, message_id)
        return

    # TRANSFORM — clean and shape the payload
    payload = transform(payload)

    # AGGREGATE + PERSIST — write to Postgres
    await aggregate(conn, pipeline_id, payload)
    await persist(conn, pipeline_id, source, payload)

    # ACK — tell Redis this message has been fully processed
    # Only ACK after everything succeeded — if Postgres write fails,
    # we don't ACK and Redis will redeliver the message
    await redis.xack(stream, GROUP_NAME, message_id)
    print(f"[worker] Processed and ACK'd message {message_id}")


# ── MAIN WORKER LOOP ──────────────────────────────────────────────────────────
async def main():
    print("[worker] Starting up...")

    # Create connections to Redis and Postgres
    redis = get_redis_client()
    conn = await get_db_connection()

    # Create the consumer group if it doesn't exist yet
    # $ means "start from new messages only, ignore historical ones"
    # mkstream=True creates the stream if it doesn't exist yet
    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        print(f"[worker] Consumer group '{GROUP_NAME}' created")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            # Group already exists — that's fine, just continue
            print(f"[worker] Consumer group '{GROUP_NAME}' already exists")
        else:
            raise

    print(f"[worker] Listening on stream '{STREAM_NAME}'...")

    # Main loop — runs forever until the process is stopped
    while True:
        try:
            # Ask Redis for new messages assigned to this consumer
            # ">" means "give me messages not yet delivered to any consumer"
            # count=10 means process up to 10 messages per iteration
            # block=BLOCK_MS means wait up to 2 seconds if stream is empty
            messages = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=10,
                block=BLOCK_MS
            )

            if not messages:
                # No new messages — loop back and check again
                continue

            # Process each message
            for stream, entries in messages:
                for message_id, fields in entries:
                    await process_message(redis, conn, stream, message_id, fields)

        except Exception as e:
            print(f"[worker] Error: {e}")
            # Wait a second before retrying to avoid hammering Redis on errors
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())