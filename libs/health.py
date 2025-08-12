from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from .db import conn_ctx


async def record_source_ok(source: str) -> None:
    now = datetime.now(timezone.utc)
    try:
        async with conn_ctx() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO source_health (source, last_ok, consecutive_errors, circuit_open_until)
                    VALUES (:s, :ts, 0, NULL)
                    ON CONFLICT (source)
                    DO UPDATE SET last_ok=EXCLUDED.last_ok, consecutive_errors=0, circuit_open_until=NULL
                    """
                ),
                {"s": source, "ts": now},
            )
    except Exception:
        return


async def record_source_error(source: str, open_minutes: int = 10) -> None:
    now = datetime.now(timezone.utc)
    try:
        async with conn_ctx() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO source_health (source, last_error, consecutive_errors, circuit_open_until)
                    VALUES (:s, :ts, 1, :open_until)
                    ON CONFLICT (source)
                    DO UPDATE SET last_error=EXCLUDED.last_error,
                                  consecutive_errors=source_health.consecutive_errors + 1,
                                  circuit_open_until=GREATEST(EXCLUDED.circuit_open_until, source_health.circuit_open_until)
                    """
                ),
                {"s": source, "ts": now, "open_until": now + timedelta(minutes=open_minutes)},
            )
    except Exception:
        return


async def is_circuit_open(source: str) -> bool:
    try:
        async with conn_ctx() as conn:
            row = (await conn.execute(text("SELECT circuit_open_until FROM source_health WHERE source=:s"), {"s": source})).first()
            if not row or not row[0]:
                return False
            return datetime.now(timezone.utc) < row[0]
    except Exception:
        return False
