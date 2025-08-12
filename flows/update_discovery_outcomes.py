from __future__ import annotations
from prefect import flow, task, get_run_logger
from sqlalchemy import text

from libs.db import conn_ctx
from libs.entity_learning import EntityLearningSystem


@task
async def update_outcomes(threshold: float = 0.6, hours: int = 72) -> int:
    logger = get_run_logger()
    learner = EntityLearningSystem()
    # Pull recently discovered entities if table exists
    names: list[str] = []
    async with conn_ctx() as conn:
        try:
            q = text(
                """
                SELECT DISTINCT entity
                FROM discovery_outcomes
                WHERE ts >= NOW() - make_interval(days => 14)
                """
            )
            rows = (await conn.execute(q)).scalars().all()
            names = [str(r) for r in rows]
        except Exception:
            # Fallback: no table, nothing to do
            return 0
    updated = 0
    for name in names:
        try:
            hit = await learner.mark_trending_if_threshold(name, threshold=threshold, window_hours=hours)
            if hit:
                updated += 1
        except Exception:
            continue
    logger.info(f"discovery outcomes updated to 'trending' for {updated} entities")
    return updated


@flow(name="update-discovery-outcomes")
def run_update_outcomes(threshold: float = 0.6, hours: int = 72):
    return update_outcomes.submit(threshold=threshold, hours=hours)


if __name__ == "__main__":
    import asyncio
    print(asyncio.run(update_outcomes()))
