# routers/pipelines.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from services.db import get_db_connection
import json

router = APIRouter()


# This defines the shape of the request body when creating a pipeline
# name is required, description and rules are optional
# rules is a dict — it will store filter and transform config as JSON
class PipelineCreate(BaseModel):
    name: str
    description: Optional[str] = None
    # rules is where the user defines their filter and transform logic
    # e.g {"filter": {"field": "source", "value": "sensor"}, "transform": {}}
    # We default to empty dict — no rules means accept and pass through everything
    rules: Optional[dict] = {}


# ── CREATE PIPELINE ───────────────────────────────────────────────────────────
# Creates a new pipeline config and stores it in the database
# Returns the created pipeline with its auto-generated UUID
@router.post("/pipelines", status_code=201)
async def create_pipeline(pipeline: PipelineCreate):
    conn = await get_db_connection()

    try:
        # Insert the new pipeline into pipeline_config
        # json.dumps converts the rules dict to a JSON string for storage
        # fetchrow returns the newly created row including the auto-generated id
        row = await conn.fetchrow(
            """
            INSERT INTO pipeline_config (name, description, rules)
            VALUES ($1, $2, $3)
            RETURNING id, name, description, rules, created_at
            """,
            pipeline.name,
            pipeline.description,
            json.dumps(pipeline.rules)
        )

        # Return the created pipeline as a clean dict
        return {
            "id": str(row["id"]),
            "name": row["name"],
            "description": row["description"],
            "rules": json.loads(row["rules"]),
            "created_at": row["created_at"].isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await conn.close()


# ── GET PIPELINE ──────────────────────────────────────────────────────────────
# Returns the config for a single pipeline by its ID
@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    conn = await get_db_connection()

    try:
        row = await conn.fetchrow(
            """
            SELECT id, name, description, rules, created_at
            FROM pipeline_config
            WHERE id = $1
            """,
            pipeline_id
        )

        # If no row found, return a 404 — pipeline doesn't exist
        if not row:
            raise HTTPException(status_code=404, detail="Pipeline not found")

        return {
            "id": str(row["id"]),
            "name": row["name"],
            "description": row["description"],
            "rules": json.loads(row["rules"]),
            "created_at": row["created_at"].isoformat()
        }

    except HTTPException:
        # Re-raise HTTP exceptions so FastAPI handles them correctly
        # Without this, the generic Exception handler below would catch
        # our 404 and turn it into a 500
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await conn.close()


# ── LIST PIPELINES ────────────────────────────────────────────────────────────
# Returns all pipelines — useful for the frontend dropdown
@router.get("/pipelines")
async def list_pipelines():
    conn = await get_db_connection()

    try:
        rows = await conn.fetch(
            """
            SELECT id, name, description, created_at
            FROM pipeline_config
            ORDER BY created_at DESC
            """
        )

        return {
            "total": len(rows),
            "pipelines": [
                {
                    "id": str(row["id"]),
                    "name": row["name"],
                    "description": row["description"],
                    "created_at": row["created_at"].isoformat()
                }
                for row in rows
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await conn.close()