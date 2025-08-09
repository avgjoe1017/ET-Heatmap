import asyncio
from flows.wiki_trends_ingest import ingest_once

if __name__ == "__main__":
    inserted = asyncio.run(ingest_once())
    print(f"Inserted scores for {inserted} entities")
