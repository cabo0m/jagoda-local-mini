from fastapi import FastAPI
from app.db import init_db
from app.routes.backfill import router as backfill_router
from app.routes.memory import router as memory_router

app = FastAPI(title="Jagoda Memory API")

@app.on_event("startup")
def startup_event():
    init_db()

app.include_router(memory_router)
app.include_router(backfill_router)
