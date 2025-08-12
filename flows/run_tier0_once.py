"""
Single execution of comprehensive Tier 0 data ingestion.
This is the entry point for the "Run: Daily Pipeline (once)" task.
"""

import asyncio
from flows.tier0_orchestration import run_tier0_once


async def main():
    """Run comprehensive Tier 0 scraping once."""
    await run_tier0_once()


if __name__ == "__main__":
    asyncio.run(main())
