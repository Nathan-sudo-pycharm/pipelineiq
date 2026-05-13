# main.py

import json
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from routers.ingest import router
from routers.query import router as query_router
from services.dead_letter import write_dead_letter
from routers.pipelines import router as pipelines_router

# Create the app first — everything else depends on this
app = FastAPI(title="PipelineIQ")

# Now we can include routers
app.include_router(router)
app.include_router(query_router)
app.include_router(pipelines_router)

# Global validation error handler
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    raw_body = await request.body()
    raw_body_str = raw_body.decode("utf-8")

    try:
        body_json = json.loads(raw_body_str)
        pipeline_id = body_json.get("pipeline_id", None)
        source = body_json.get("source", "webhook")
    except Exception:
        pipeline_id = None
        source = "unknown"

    error_message = json.dumps(exc.errors())

    await write_dead_letter(
        source=source,
        raw_payload=raw_body_str,
        error=error_message,
        pipeline_id=pipeline_id
    )

    return JSONResponse(
        status_code=422,
        content={
            "status": "rejected",
            "reason": exc.errors()
        }
    )


# Health check
@app.get("/health")
async def health():
    return {"status": "ok"}