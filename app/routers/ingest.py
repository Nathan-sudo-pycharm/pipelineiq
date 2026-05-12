import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from services.redis_client import get_redis_client

router = APIRouter()

STREAM_NAME = "pipelineiq:events"

class WebhookEvent(BaseModel):
    pipeline_id: str
    source: str = "webhook"
    payload: dict

    @field_validator("pipeline_id")
    @classmethod
    def pipeline_id_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError("pipeline_id cannot be empty")
        return v


@router.post("/ingest/webhook")
async def ingest_webhook(event: WebhookEvent):
    redis = get_redis_client()

    try:
        message = {
            "pipeline_id": event.pipeline_id,
            "source": event.source,
            "payload": json.dumps(event.payload)
        }

        msg_id = await redis.xadd(STREAM_NAME, message)

        return {
            "status": "accepted",
            "message_id": msg_id,
            "pipeline_id": event.pipeline_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await redis.aclose()