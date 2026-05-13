# routers/query.py

import json
from fastapi import APIRouter, HTTPException, Query
from services.db import get_db_connection
from datetime import datetime

router = APIRouter()


# ── GET PIPELINE EVENTS ───────────────────────────────────────────────────────
# Returns paginated raw events for a given pipeline
# limit  — how many events to return (default 50, max 100)
# offset — how many events to skip before starting (default 0)
@router.get("/pipelines/{pipeline_id}/events")
async def get_events(
    pipeline_id: str,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0)
):
    conn = await get_db_connection()

    try:
        # Fetch one page of raw events for this pipeline
        # ORDER BY received_at DESC means newest events come first
        rows = await conn.fetch(
            """
            SELECT id, pipeline_id, source, payload, received_at
            FROM raw_events
            WHERE pipeline_id = $1
            ORDER BY received_at DESC
            LIMIT $2 OFFSET $3
            """,
            pipeline_id,
            limit,
            offset
        )

        # Get the total count so the caller knows how many pages exist
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM raw_events WHERE pipeline_id = $1",
            pipeline_id
        )

        # asyncpg returns Record objects, not plain dicts
        # We convert each row to a dict so FastAPI can serialize it to JSON
        # We also parse the payload back from JSON string to dict
        events = []
        for row in rows:
            events.append({
                "id": str(row["id"]),
                "pipeline_id": row["pipeline_id"],
                "source": row["source"],
                "payload": json.loads(row["payload"]),
                "received_at": row["received_at"].isoformat()
            })

        return {
            "pipeline_id": pipeline_id,
            "total": total,
            "limit": limit,
            "offset": offset,
            "events": events
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await conn.close()


# ── GET PIPELINE ANALYTICS ────────────────────────────────────────────────────
# Returns pre-computed aggregates for a given pipeline over a time range
# from_time and to_time are optional ISO format timestamps
# e.g. 2026-05-13T00:00:00 to 2026-05-13T23:59:59
@router.get("/pipelines/{pipeline_id}/analytics")
async def get_analytics(
    pipeline_id: str,
    from_time: datetime = Query(default=None),
    to_time: datetime = Query(default=None)
):
    conn = await get_db_connection()

    try:
        # Build the query dynamically depending on whether
        # time range filters were provided
        # If no time range given, return all aggregates for this pipeline
        if from_time and to_time:
            rows = await conn.fetch(
                """
                SELECT pipeline_id, window_start, window_end,
                       metric, count, avg, min, max
                FROM aggregates
                WHERE pipeline_id = $1
                  AND window_start >= $2
                  AND window_end   <= $3
                ORDER BY window_start DESC
                """,
                pipeline_id,
                from_time,
                to_time
            )
        else:
            rows = await conn.fetch(
                """
                SELECT pipeline_id, window_start, window_end,
                       metric, count, avg, min, max
                FROM aggregates
                WHERE pipeline_id = $1
                ORDER BY window_start DESC
                """,
                pipeline_id
            )

        # Convert rows to dicts
        # avg/min/max are Decimal types from Postgres NUMERIC
        # we convert to float so JSON serialization works
        analytics = []
        for row in rows:
            analytics.append({
                "pipeline_id": row["pipeline_id"],
                "window_start": row["window_start"].isoformat(),
                "window_end": row["window_end"].isoformat(),
                "metric": row["metric"],
                "count": row["count"],
                "avg": float(row["avg"]) if row["avg"] else None,
                "min": float(row["min"]) if row["min"] else None,
                "max": float(row["max"]) if row["max"] else None,
            })

        return {
            "pipeline_id": pipeline_id,
            "total": len(analytics),
            "analytics": analytics
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await conn.close()