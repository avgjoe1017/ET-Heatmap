from __future__ import annotations
import os
from libs.features import flag
from prefect import task

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()

@task
def maybe_scraperapi_ping() -> bool:
    if not flag("FEATURE_SCRAPERAPI") or not SCRAPERAPI_KEY:
        return False
    # no-op placeholder; wire real calls later
    return True

@task
def maybe_apify_ping() -> bool:
    if not flag("FEATURE_APIFY") or not APIFY_TOKEN:
        return False
    # no-op placeholder; wire real actors later
    return True
