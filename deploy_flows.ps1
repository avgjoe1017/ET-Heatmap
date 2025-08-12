# -------------------------------
# Deploy ET Heatmap Prefect Flows
# -------------------------------

# 1. INGESTION FLOWS (run at top of hour)
$ingest_flows = @(
    @{ entry = "flows/cc_tiktok.py:run_cc_tiktok"; name = "cc_tiktok" },
    @{ entry = "flows/apify_tiktok.py:run_apify_tiktok"; name = "apify_tiktok" },
    @{ entry = "flows/apify_tiktok_search.py:run_apify_tiktok_search"; name = "apify_tiktok_search" },
    @{ entry = "flows/reddit_ingest.py:run_reddit_ingest"; name = "reddit_ingest" },
    @{ entry = "flows/wiki_trends_ingest.py:run_ingest"; name = "wiki_trends_ingest" },
    @{ entry = "flows/gdelt_gkg.py:run_gdelt_gkg"; name = "gdelt_gkg" },
    @{ entry = "flows/trade_rss_ingest.py:run_trade_rss_ingest"; name = "trade_rss_ingest" },
    @{ entry = "flows/scrape_news.py:run_scrape_news"; name = "scrape_news" },
    @{ entry = "flows/entity_discovery_advanced.py:run_discovery_flow"; name = "entity_discovery" }
)

# 2. SCORING FLOWS (run 5 minutes after ingestion)
$scoring_flows = @(
    @{ entry = "flows/mvp_scoring.py:run_mvp_scoring"; name = "mvp_scoring" },
    @{ entry = "flows/scoring_job.py:run_scoring_hourly"; name = "scoring_hourly" }
)

# Ensure the work pool exists before deploying (run inside container shell for reliability)
docker compose exec -T worker sh -lc "prefect work-pool create -t process default-pool || true" | Out-Null

# Build & deploy ingestion flows (hourly at :00)
foreach ($flow in $ingest_flows) {
    $yamlFile = "$($flow.name)-deployment.yaml"
    Write-Host ">>> Deploying ingestion flow: $($flow.name) hourly at :00"
    $cmd = "prefect deployment build '" + $flow.entry + "' -n $($flow.name) -q default --pool default-pool --cron '0 * * * *' -o $yamlFile && prefect deployment apply $yamlFile"
    docker compose exec -T worker sh -lc $cmd
}

# Build & deploy scoring flows (hourly at :05)
foreach ($flow in $scoring_flows) {
    $yamlFile = "$($flow.name)-deployment.yaml"
    Write-Host ">>> Deploying scoring flow: $($flow.name) hourly at :05"
    $cmd = "prefect deployment build '" + $flow.entry + "' -n $($flow.name) -q default --pool default-pool --cron '5 * * * *' -o $yamlFile && prefect deployment apply $yamlFile"
    docker compose exec -T worker sh -lc $cmd
}

Write-Host ">>> All deployments registered. Checking status..."
docker compose exec worker prefect deployment ls
