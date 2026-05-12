# main.py

from fastapi import FastAPI
from routers.ingest import router

# This creates the FastAPI application instance
# title shows up in the auto-generated API docs at /docs
app = FastAPI(title="PipelineIQ")

# include_router plugs in all endpoints defined in ingest.py
# so POST /ingest/webhook is now live on this app
app.include_router(router)

# A simple health check endpoint
# Useful for Docker and monitoring tools to confirm the app is alive
@app.get("/health")
async def health():
    return {"status": "ok"}