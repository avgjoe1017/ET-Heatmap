from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text

from .db import conn_ctx


async def audit_event(
    source: str,
    event: str,
    level: str = "info",
    status: Optional[int] = None,
    extra: Optional[dict[str, Any]] = None,
) -> None:
    """Insert an audit log row. Safe no-op on failure.

    Columns: ts, source, event, level, status, extra(jsonb)
    """
    try:
        async with conn_ctx() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO audit_logs (ts, source, event, level, status, extra)
                    VALUES (:ts, :source, :event, :level, :status, :extra)
                    """
                ),
                {
                    "ts": datetime.now(timezone.utc),
                    "source": source,
                    "event": event,
                    "level": level,
                    "status": int(status) if status is not None else None,
                    "extra": json.dumps(extra or {}),
                },
            )
    except Exception:
        return
