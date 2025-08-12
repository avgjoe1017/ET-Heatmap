import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from sqlalchemy import create_engine
from sqlalchemy import text

DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "heatmap")
DB_USER = os.getenv("POSTGRES_USER", "heatmap")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "heatmap")
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
SYNC_DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine: AsyncEngine = create_async_engine(DATABASE_URL, future=True, echo=False)
sync_engine = create_engine(SYNC_DATABASE_URL, future=True)

@asynccontextmanager
async def conn_ctx() -> AsyncGenerator[AsyncConnection, None]:
    async with engine.begin() as conn:
        yield conn

async def upsert_entity(conn: AsyncConnection, name: str, etype: str, aliases, wiki_id: str | None, category: str | None = None):
    q = text("""
        INSERT INTO entities (type, category, name, aliases, wiki_id)
        VALUES (:type, :category, :name, :aliases, :wiki_id)
        ON CONFLICT (name) DO UPDATE SET aliases = EXCLUDED.aliases, category = COALESCE(EXCLUDED.category, entities.category)
        RETURNING id
    """)
    res = await conn.execute(q, {"type": etype, "category": category, "name": name, "aliases": aliases, "wiki_id": wiki_id})
    return res.scalar_one()

async def insert_signal(conn: AsyncConnection, entity_id: int, source: str, ts, metric: str, value: float):
    await conn.execute(text("""
        INSERT INTO signals (entity_id, source, ts, metric, value)
        VALUES (:eid, :src, :ts, :metric, :val)
        ON CONFLICT DO NOTHING
    """), {"eid": entity_id, "src": source, "ts": ts, "metric": metric, "val": value})

async def insert_score(conn: AsyncConnection, entity_id: int, ts, comps: dict):
    await conn.execute(text("""
        INSERT INTO scores (entity_id, ts, velocity_z, accel, xplat, novelty, et_fit, tentpole, decay, risk, heat)
        VALUES (:eid, :ts, :velocity_z, :accel, :xplat, :novelty, :et_fit, :tentpole, :decay, :risk, :heat)
    """), {"eid": entity_id, "ts": ts, **comps})
