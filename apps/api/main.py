from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import health, stats, batches, events, signals, hotspots

app = FastAPI(title="GDELT Dashboard API", docs_url="/api/docs", openapi_url="/api/openapi.json")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api")
app.include_router(stats.router, prefix="/api")
app.include_router(batches.router, prefix="/api")
app.include_router(events.router, prefix="/api")
app.include_router(signals.router, prefix="/api")
app.include_router(hotspots.router, prefix="/api")
