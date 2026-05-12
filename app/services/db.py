# services/db.py

import asyncpg
from config import settings

# This function creates and returns a single database connection
# We use asyncpg which is an async Postgres driver — same reason
# we used async Redis — FastAPI is async so everything talking
# to external services should be async too, otherwise one slow
# database call blocks the entire app
async def get_db_connection():
    connection = await asyncpg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        database=settings.postgres_db
    )
    return connection