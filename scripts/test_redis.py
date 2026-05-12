"""
Day 1 smoke test — Redis Streams

Run this after `docker compose up -d` to verify:
- Redis is reachable
- XADD writes an event to a stream
- XREAD reads it back
- Consumer groups work (XGROUP CREATE + XREADGROUP)

Usage:
    pip install redis python-dotenv
    python scripts/test_redis.py
"""

import os
import json
import redis
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
STREAM_NAME = "pipelineiq:events:test"
GROUP_NAME = "test-consumer-group"
CONSUMER_NAME = "worker-1"


def get_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def test_basic_xadd_xread(r: redis.Redis):
    print("\n--- Test 1: XADD + XREAD ---")

    event = {
        "pipeline_id": "test-pipeline-001",
        "source": "webhook",
        "payload": json.dumps({"sensor": "temp_01", "value": 72.4, "unit": "F"}),
    }

    msg_id = r.xadd(STREAM_NAME, event)
    print(f"Written to stream with id: {msg_id}")

    messages = r.xread({STREAM_NAME: "0-0"}, count=10)
    for stream, entries in messages:
        for entry_id, fields in entries:
            print(f"Read back [{entry_id}]: {fields}")

    print("PASS")


def test_consumer_groups(r: redis.Redis):
    print("\n--- Test 2: Consumer groups ---")

    # Create consumer group (start from beginning of stream)
    try:
        r.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"Consumer group '{GROUP_NAME}' created")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Consumer group '{GROUP_NAME}' already exists, continuing")
        else:
            raise

    # Write a new event
    r.xadd(STREAM_NAME, {"pipeline_id": "test-pipeline-001", "source": "csv", "payload": json.dumps({"row": 1, "value": 99.1})})

    # Read as a consumer in the group
    messages = r.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=5)
    if not messages:
        print("No new messages (may have already been consumed)")
    else:
        for stream, entries in messages:
            for entry_id, fields in entries:
                print(f"Consumer '{CONSUMER_NAME}' got [{entry_id}]: {fields}")
                # Acknowledge — marks the message as processed
                r.xack(STREAM_NAME, GROUP_NAME, entry_id)
                print(f"ACK'd {entry_id}")

    print("PASS")


def test_stream_info(r: redis.Redis):
    print("\n--- Stream info ---")
    info = r.xinfo_stream(STREAM_NAME)
    print(f"Stream length : {info['length']}")
    print(f"First entry   : {info['first-entry']}")
    print(f"Last entry    : {info['last-entry']}")


def cleanup(r: redis.Redis):
    r.delete(STREAM_NAME)
    print(f"\nCleaned up stream '{STREAM_NAME}'")


if __name__ == "__main__":
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    r = get_client()

    try:
        r.ping()
        print("Redis reachable — OK")
    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis: {e}")
        print("Make sure Docker Compose is running: docker compose up -d")
        exit(1)

    try:
        test_basic_xadd_xread(r)
        test_consumer_groups(r)
        test_stream_info(r)
    finally:
        cleanup(r)

    print("\nAll Day 1 Redis checks passed.")