
import json
from services.db import get_db_connection

# This function writes a rejected event to the dead_letter table
# It's called whenever an event fails validation anywhere in the app
# Parameters:
#   source     — where the event came from e.g. 'webhook', 'csv'
#   raw_payload — the raw request body as a string, exactly as it arrived
#   error      — the reason it was rejected
#   pipeline_id — optional, may not be known if it was missing from the payload
async def write_dead_letter(
    source: str,
    raw_payload: str,
    error: str,
    pipeline_id: str = None
):
    # Get a database connection
    conn = await get_db_connection()

    try:
        # Write the rejected event to the dead_letter table
        # We store raw_payload as plain text — not JSONB — because
        # the payload may be malformed JSON, and JSONB would reject it
        # Plain text accepts anything, which is exactly what we need
        # for a safety net table
        await conn.execute(
            """
            INSERT INTO dead_letter (pipeline_id, source, raw_payload, error)
            VALUES ($1, $2, $3, $4)
            """,
            pipeline_id,
            source,
            raw_payload,
            error
        )
    finally:
        # Always close the connection whether insert succeeded or failed
        await conn.close()