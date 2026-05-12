import json
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from routers.ingest import router
from services.dead_letter import write_dead_letter

# This creates the FastAPI application instance
# title shows up in the auto-generated API docs at /docs
app = FastAPI(title="PipelineIQ")

# include_router plugs in all endpoints defined in ingest.py
# so POST /ingest/webhook is now live on this app
app.include_router(router)


# This is a global exception handler
# It catches ALL validation errors from every endpoint in the app
# automatically — we don't need to add this logic to each router
# RequestValidationError is raised by FastAPI whenever an incoming
# request doesn't match the expected Pydantic model shape
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):

    # Read the raw request body exactly as it arrived
    # We store this in dead_letter so we can inspect it later
    raw_body = await request.body()
    raw_body_str = raw_body.decode("utf-8")

    # Try to extract pipeline_id and source from the raw body
    # They may or may not be present since the request was malformed
    # That's why we wrap it in a try/except — if the body isn't even
    # valid JSON, json.loads() would crash without it
    try:
        body_json = json.loads(raw_body_str)
        pipeline_id = body_json.get("pipeline_id", None)
        source = body_json.get("source", "webhook")
    except Exception:
        # Body wasn't even valid JSON — that's fine
        # We still want to record it in dead_letter
        pipeline_id = None
        source = "unknown"

    # Turn the validation errors into a readable string
    # exc.errors() returns a list of dicts describing each field that failed
    error_message = json.dumps(exc.errors())

    # Write the rejected event to the dead_letter table
    # This is a fire-and-forget write — we don't block the response on it
    await write_dead_letter(
        source=source,
        raw_payload=raw_body_str,
        error=error_message,
        pipeline_id=pipeline_id
    )

    # Return a clean 422 response to the caller explaining what went wrong
    # 422 means Unprocessable Entity — the request was received but invalid
    return JSONResponse(
        status_code=422,
        content={
            "status": "rejected",
            "reason": exc.errors()
        }
    )


# A simple health check endpoint
# Useful for Docker and monitoring tools to confirm the app is alive
@app.get("/health")
async def health():
    return {"status": "ok"}