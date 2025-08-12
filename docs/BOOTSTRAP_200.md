# ET Heatmap — $200/Month Bootstrap Guide

This guide shows how to deploy ET Heatmap on a single Hetzner VPS within a strict $200/month budget. It’s aligned to this repo’s stack (Docker Compose with TimescaleDB/Postgres, Redis, FastAPI API, Prefect worker/scheduler, Metabase).

## Budget

```yaml
MONTHLY COSTS (Total: $200):
- Hetzner VPS (CX31): $13/month (4 vCPU, 8GB RAM, 160GB)
- PostgreSQL on same VPS: $0 (included)
- Redis on same VPS: $0 (included)
- ScraperAPI Hobby Plan: $49/month (100K API credits)
- OpenAI API Budget: $30/month (GPT-4o-mini for insights)
- NewsAPI Developer Plan: $0/month (500 req/day free)
- Backup Storage: $5/month (Backblaze B2)
- Domain + Cloudflare: $1/month
- Buffer for overages: $102/month

TOTAL: $200/month ✅
```

## Phase 1: Provision & Secure the Server (Ubuntu 22.04)

1) Create a Hetzner Cloud instance (CX31). Enable backups. Choose region near users.
2) SSH in and update:

```bash
sudo apt update && sudo apt upgrade -y
```

3) Install Docker + Compose and basic tooling:

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker
sudo apt install -y git ufw
```

4) Firewall:

```bash
sudo ufw allow 22 && sudo ufw allow 80 && sudo ufw allow 443 && sudo ufw enable
```

## Phase 2: Pull Repo and Configure

```bash
git clone https://github.com/avgjoe1017/ET-Heatmap.git
cd ET-Heatmap
cp .env.example .env
```

Edit `.env` to set passwords/URLs and feature flags. Then open `configs/sources.yml` to toggle sources and features (like `advanced_scoring`).

Notes:
- Postgres/TimescaleDB, Redis, API, Prefect components, and Metabase are run by `docker-compose.yml`.
- The database schema is initialized automatically from `sql/init.sql` on first boot.
 - Storage policies: `sql/init.sql` enables TimescaleDB compression and retention (signals: compress after 7d, keep 30d; scores: compress after 14d, keep 60d) and creates a daily continuous aggregate for heat.

## Phase 3: Bring Up the Stack

```bash
docker compose pull
docker compose build
docker compose up -d
```

Verify:
- API: http://YOUR_SERVER_IP:8080/health
- Prefect: http://YOUR_SERVER_IP:4200
- Metabase: http://YOUR_SERVER_IP:3000

## Phase 4: Configure Features (Budget-aware)

Environment flags (in `.env`):
- API_REDIS_CACHE=1 to enable API Redis caching.
- FEATURE_SCRAPERAPI, FEATURE_APIFY (existing).
- Add keys for OPENAI_API_KEY, NEWS_API_KEY, SCRAPERAPI_KEY if you plan to use those services.

Source toggles (in `configs/sources.yml`):
- `entity_discovery.enabled`: enables discovery flow.
- `advanced_scoring.enabled`: gates the advanced scoring endpoint.

Rate limiting/budget tips:
- Keep discovery enabled selectively; it’s lightweight.
- Use OpenAI only for top entities (already guarded in code paths that use it; if added later, keep limits at or below $30/month).

## Phase 5: Scheduling

This repo uses Prefect for orchestration (scheduler + worker). You can:
- Use Prefect UI (http://YOUR_SERVER_IP:4200) to schedule flows hourly/daily.
- Or run flows manually inside the worker container, e.g.:

```bash
docker compose exec worker python -m flows.hello
docker compose exec worker python -m flows.entity_discovery_advanced
docker compose exec worker python -m flows.update_discovery_outcomes
```

Suggested cadence to stay within budget:
- Every 6h: lightweight collections (existing ingestion flows).
- Every 12h: news scraping for top entities (respect free tiers).
- Every 24h: discovery + outcomes update.
 - Every 24h: storage maintenance (`flows.storage_maintenance`) to purge beyond retention and ANALYZE.
 - Every hour: alerts (`flows.alerts`) to warn on stale data and budget nearing limits.

## Phase 6: API Endpoints

- Intelligent trends (JSON): /trends/intelligent
- CSV export of intelligent trends: /trends/intelligent?format=csv
- Trends dashboard (HTML): /trends/dashboard
- Live updates (SSE): /trends/sse
- WebSocket stream: /ws (supports ?category=foo)
- Entity analysis: /entity/{name}/analysis
- Advanced scoring (flagged): /score/advanced/{entity}
- Discoveries (recent): /discoveries/recent
- Metrics (Prometheus): /metrics
- Metrics (Lite JSON): /metrics-lite

### Monthly usage snapshot (targets)

```
Monthly usage:
- ScraperAPI: ~25,000/100,000 credits (25% usage - plenty of buffer)
- OpenAI: ~$30/$30 budget (100% allocated)
- NewsAPI: ~450/500 daily (90% usage - safe)
- Wikipedia/Reddit/Trends: Unlimited (FREE)
```

## Phase 7: SSL and Domain (Nginx + Cloudflare)

Option A: Use a managed proxy (Cloudflare) with proxying enabled to your server; terminate SSL at Cloudflare.

Option B: Install Nginx and Certbot on the VPS. Proxy to API port 8080:

```nginx
server {
    listen 80;
    server_name your-domain.com;
    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

Then obtain certificates via Certbot and switch to listen 443.

## Phase 8: Backups

- DB: Nightly logical backup via `pg_dump` inside a small cronjob container or host cron, upload to Backblaze B2.
- Metabase: Volume snapshot (or export).

Example host cron (simplified):

```bash
0 3 * * * docker compose exec -T db pg_dump -U $POSTGRES_USER $POSTGRES_DB | gzip > /root/etheatmap_$(date +\%Y\%m\%d).sql.gz && \ 
   b2 upload-file etheatmap-backups /root/etheatmap_$(date +\%Y\%m\%d).sql.gz etheatmap_$(date +\%Y\%m\%d).sql.gz && \ 
   rm /root/etheatmap_$(date +\%Y\%m\%d).sql.gz
```

## Phase 9: Monitoring

- Built-in: /metrics (Prometheus format) and /metrics-lite (JSON) on the API.
- The Prometheus endpoint includes total requests, per-method and per-path counters, and latency summary.
- System: Use basic host monitoring (e.g., Netdata or `htop`). Keep it lightweight.
- Alerts: Slack webhook can be configured via `.env` (SLACK_WEBHOOK_URL) and wired from flows/notifier tasks.

Run housekeeping and alerts manually if needed:

```bash
docker compose exec worker python -m flows.storage_maintenance
docker compose exec worker python -m flows.alerts
```

## Optional: Bare-Metal (Non-Docker) Path

If you prefer bare metal:
1) Install Python 3.11, Postgres 16, Redis 7, Nginx.
2) Create a service user `etheatmap` and a virtualenv.
3) Apply schema (adapted from `sql/init.sql`).
4) Run API via systemd (uvicorn) and schedule ingestion via systemd timers or APScheduler.

Docker is recommended for simplicity and parity with local dev.

---

That’s it. You now have a cost-aware, single-node deployment that can scale later to multiple nodes or Kubernetes without changing app code.

## Launch Checklist

Week 1

Day 1-2: Infrastructure
- [ ] Provision Hetzner server ($13/month)
- [ ] Setup PostgreSQL + Redis
- [ ] Configure nginx + SSL
- [ ] Setup backup to B2 ($5/month)

Day 3-4: Data Collection
- [ ] Implement Wikipedia collector (FREE)
- [ ] Implement Reddit collector (FREE)
- [ ] Implement Google Trends (FREE)
- [ ] Test ScraperAPI integration ($49/month)

Day 5-6: Scoring & API
- [ ] Build scoring algorithm
- [ ] Create FastAPI endpoints
- [ ] Setup Slack notifications
- [ ] Basic web dashboard

Day 7: Testing & Launch
- [ ] Load test with 500 entities
- [ ] Verify budget tracking works
- [ ] Deploy to production
- [ ] Monitor for 24 hours

## Week 2: Optimization (Guidance)

Caching (targets)
- Wikipedia: 1h
- Reddit: 30m
- Google Trends: 2h
- News: 1h
- TikTok (expensive): 12h

Database
- Add indexes on heavy queries (signals entity/source, scores date, entities category)
- Refresh summaries periodically (use Metabase or materialized views if added later)

Entity list
- Deactivate entities with no activity for 7 days and low scores
- Prioritize high-ROI categories: music, entertainment, sports, viral

## Data Management Within Budget

- Signals kept <= 30 days; compressed after 7 days (Timescale policy)
- Scores kept <= 60 days; compressed after 14 days (Timescale policy)
- Optional hard purge + ANALYZE via `flows.storage_maintenance` (daily or weekly)

## Simple Frontend

- Use /trends/dashboard for a quick HTML table
- Optionally connect a SPA to /trends/intelligent and /budget

## Monitoring & Alerts (Free)

- Use /metrics (Prometheus) and /metrics-lite
- Slack webhooks for alerts (set SLACK_WEBHOOK_URL)

## GTM: First 10 Customers (Bootstrap)

- Agencies, labels, newsrooms, e-commerce. Offer 7-day trial, referral incentive.

## Scaling Plan (Budget-aware)

- M1 launch ($200), M2 first customer, M6 scale infra modestly, M12 strong growth

## Critical Success Factors

1) Cache Everything; fall back to stale data when credits are low
2) Manual > Automated early on for expensive sources
3) Focus on high-signal free sources (Wikipedia, Reddit, Trends)
4) Smart entity selection (track what matters most)
