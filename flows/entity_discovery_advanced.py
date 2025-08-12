from __future__ import annotations
from typing import List
from datetime import datetime, timezone
from prefect import flow, task, get_run_logger

from libs.entity_discovery import AdvancedEntityDiscovery
from libs.entity_learning import EntityLearningSystem


@task
async def run_discovery_once(top_n: int = 20) -> int:
    logger = get_run_logger()
    discovery = AdvancedEntityDiscovery()
    entities = await discovery.discover_entities_once(top_n=top_n)
    if not entities:
        logger.info("No new entities discovered")
        return 0
    inserted = await discovery.persist_discoveries(entities)
    # Record initial outcomes as 'steady' baseline; future jobs can update
    learner = EntityLearningSystem()
    for e in entities[:inserted]:
        await learner.record_discovery_outcome(e.name, outcome="steady", confidence=e.confidence, velocity=e.velocity)
    logger.info(f"Discovered and inserted {inserted} entities")
    return inserted


@flow(name="entity-discovery-advanced")
def run_discovery_flow(top_n: int = 20):
    return run_discovery_once.submit(top_n=top_n)


if __name__ == "__main__":
    import asyncio
    print(asyncio.run(run_discovery_once()))
