from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.exceptions import ObjectNotFound
import asyncio
from flows.daily_pipeline import daily_pipeline

# 9:30 AM PT ~ 16:30 UTC on DST; weekdays only
CRON = "30 16 * * 1-5"
async def ensure_pool():
    async with get_client() as client:
        try:
            await client.read_work_pool("default-pool")
        except ObjectNotFound:
            await client.create_work_pool(work_pool=WorkPoolCreate(name="default-pool", type="process"))

if __name__ == "__main__":
    asyncio.run(ensure_pool())
    dep = Deployment.build_from_flow(  # type: ignore
        flow=daily_pipeline,
        name="weekday-930PT",
        schedules=[CronSchedule(cron=CRON, timezone="UTC")],
        work_pool_name="default-pool",
        parameters={}
    )
    dep.apply()
    print("Deployment created: daily-pipeline @ weekday-930PT")
