import os
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "heatmap")
DB_USER = os.getenv("POSTGRES_USER", "heatmap")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "heatmap")
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = FastAPI(title="ET Heatmap API")
engine: AsyncEngine = create_async_engine(DATABASE_URL, future=True, echo=False)

class HeatItem(BaseModel):
    rank: int
    entity: str
    heat: float
    reasons: List[str] = []

@app.get("/health")
async def health():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/top", response_model=List[HeatItem])
async def top(limit: int = 10):
    q = text("""
        WITH latest AS (
          SELECT s.entity_id, MAX(s.ts) AS ts
          FROM scores s
          WHERE s.ts >= NOW() - INTERVAL '3 days'
          GROUP BY s.entity_id
        )
        SELECT e.name, s.heat, s.velocity_z, s.accel, s.xplat, s.tentpole
        FROM scores s
        JOIN latest l ON l.entity_id = s.entity_id AND l.ts = s.ts
        JOIN entities e ON e.id = s.entity_id
        ORDER BY s.heat DESC
        LIMIT :limit
    """)
    async with engine.connect() as conn:
        res = await conn.execute(q, {"limit": limit})
        rows = res.fetchall()
    items = []
    for i, (name, heat, v, a, x, tp) in enumerate(rows, start=1):
        reasons = []
        if v and v > 0.8: reasons.append("High velocity vs 30-day baseline")
        if a and a > 0: reasons.append("Acceleration positive")
        if x and x >= 1.0: reasons.append("Cross-platform confirmation")
        if tp and tp > 0: reasons.append("Tentpole boost active")
        items.append(HeatItem(rank=i, entity=name, heat=float(heat), reasons=reasons))
    return items
