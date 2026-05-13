
import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from services.redis_client import get_redis_client

# APIRouter lets us define endpoints in separate files
# and plug them into the main app cleanly
router = APIRouter()

# This is the Redis Stream name all webhook events go into
# Using a namespaced name (pipelineiq:events) is a Redis convention
# to keep streams organised when multiple apps share one Redis instance
STREAM_NAME = "pipelineiq:events"


# This defines the shape of the incoming request body
# FastAPI reads the incoming JSON and maps it to this model automatically
# If pipeline_id or payload is missing, FastAPI rejects the request
# before our code even runs and returns a clear error to the caller
class WebhookEvent(BaseModel):
    pipeline_id: str
    source: str = "webhook"  # defaults to "webhook" if not provided
    payload: dict            # must be a JSON object, not a string or list


    # This is an extra custom validation on top of the type check
    # Pydantic confirms pipeline_id is a string, but this goes further
    # and makes sure it isn't just empty whitespace like "   "
    @field_validator("pipeline_id")
    @classmethod
    def pipeline_id_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError("pipeline_id cannot be empty")
        return v


@router.post("/ingest/webhook")
async def ingest_webhook(event: WebhookEvent):
    # Create a Redis connection for this request
    redis = get_redis_client()

    try:
        # Build the message we'll write to the Redis Stream
        # Redis stores everything as strings, so we convert
        # the payload dict to a JSON string with json.dumps()
        # A nested dictionary cannot be stored directly in Redis
        message = {
            "pipeline_id": event.pipeline_id,
            "source": event.source,
            "payload": json.dumps(event.payload)
        }

        # XADD writes the message to the stream and returns a unique ID
        # The ID is a timestamp + sequence number e.g. 1778584726435-0
        # "await" is needed because we're using the async Redis client
        # Without await, Python wouldn't actually wait for Redis to respond
        msg_id = await redis.xadd(STREAM_NAME, message)

        # Return immediately — our job as the ingest endpoint is done
        # The worker will pick this up from the stream and process it
        return {
            "status": "accepted",
            "message_id": msg_id,
            "pipeline_id": event.pipeline_id
        }

    except Exception as e:
        # If anything goes wrong talking to Redis, return a 500
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # This always runs whether the request succeeded or failed
        # It closes the Redis connection cleanly
        # Skipping this causes connections to pile up over time
        await redis.aclose()