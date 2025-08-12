# ET Heatmap — Comprehensive Entertainment Intelligence

## Overview

ET Heatmap is a real-time entertainment trend monitoring system that tracks entities (celebrities, movies, shows, etc.) across multiple data sources to calculate dynamic "heat" scores. The system uses a **Tier 0 comprehensive scraping architecture** that provides always-on, high-ROI data collection from entertainment-focused sources.

## Key Features

- **Real-time Dashboard**: Enhanced HTML dashboard with sparklines, priority scoring, and narrative insights
- **Tier 0 Scraping**: Always-on monitoring of Reddit, YouTube, trade publications, Google News, box office data
- **MVP Scoring Algorithm**: `0.5 × Velocity_Z + 0.3 × Platform_Spread + 0.2 × Affect × Freshness_Decay`
- **TimescaleDB**: Optimized time-series storage with retention policies and continuous aggregates
- **Intelligent Orchestration**: Dynamic scheduling based on source cadences and API rate limits
- **Multi-format APIs**: JSON, CSV, SSE streaming, WebSocket real-time updates

## Prereqs
- Docker + Docker Compose
- Copy `.env.example` to `.env` and configure API keys for optimal coverage

## Quick Start

```powershell
# Windows PowerShell
Copy-Item .env.example .env
# Edit .env to add your API keys (see Configuration section)

docker compose build
docker compose up -d
```

Or use VS Code tasks:
- **Compose Up**: Builds and starts all services
- **Logs: API**: Follow API logs in real-time
- **Run: Daily Pipeline (once)**: Execute complete data ingestion once
- **Compose Up (Rebuild)**: Force rebuild and restart

## Configuration

### Required API Keys (for maximum coverage)

Add these to your `.env` file:

```bash
# Reddit API (free at reddit.com/prefs/apps)
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret

# YouTube Data API v3 (free tier: 10,000 units/day)
YOUTUBE_API_KEY=your_youtube_api_key

# Optional: Enhanced features
OPENAI_API_KEY=your_openai_key  # For AI-powered insights
NEWS_API_KEY=your_news_api_key  # For additional news sources
```

### Source Configuration

Edit `configs/sources.yml` to enable/disable sources and adjust weights:

```yaml
sources:
  # Tier 0 (always-on, highest ROI)
  trade_rss:    {enabled: true, weight: 1.2}   # Variety, THR, Deadline
  reddit:       {enabled: true, weight: 1.0}   # Entertainment subreddits
  youtube:      {enabled: true, weight: 1.1}   # Trending + search
  google_news:  {enabled: true, weight: 0.9}   # News aggregation
  imdb_box_office: {enabled: true, weight: 1.0} # Box office & releases
```

## API Endpoints

### Core Endpoints
- **API Health**: http://localhost:8080/health
- **Source Health**: http://localhost:8080/health/sources (detailed source status)
- **Enhanced Dashboard**: http://localhost:8080/trends/dashboard (priority-based with sparklines)
- **Intelligent Trends (JSON)**: http://localhost:8080/trends/intelligent
- **Intelligent Trends (CSV)**: http://localhost:8080/trends/intelligent?format=csv
- **Entity Analysis**: http://localhost:8080/entity/Taylor%20Swift/analysis

### Real-time Streaming
- **Live Trends (SSE)**: http://localhost:8080/trends/sse
- **Live Trends (WebSocket)**: ws://localhost:8080/ws

### Data Export
- **Export (JSON)**: http://localhost:8080/export/json
- **Export (CSV)**: http://localhost:8080/export/csv
- **Recommendations**: http://localhost:8080/recommendations

### Monitoring
- **Metrics (Prometheus)**: http://localhost:8080/metrics
- **Metrics (Lite JSON)**: http://localhost:8080/metrics-lite
- **Budget Usage**: http://localhost:8080/budget

### Admin Interfaces
- **Prefect UI**: http://localhost:4200 (workflow management)
- **Metabase**: http://localhost:3000 (business intelligence)

## Tier 0 Scraping Architecture

The system implements a comprehensive **Tier 0 scraping plan** that maximizes entertainment intelligence with minimal friction:

### Always-On Sources (5-15 min intervals)
- **Trade Publications**: Variety, Hollywood Reporter, Deadline, Entertainment Weekly
- **Reddit Enhanced**: 13 entertainment subreddits with entity tracking
- **Google News**: Entertainment and business categories + entity-specific searches
- **Google Trends**: Real-time trend monitoring

### High-Frequency Sources (30-60 min intervals)
- **YouTube**: Trending videos + entity-specific searches using Data API v3
- **Wikipedia**: Trending articles with entertainment focus

### Moderate Frequency Sources (2-6 hours)
- **IMDb & Box Office Mojo**: Weekend box office, upcoming releases, popularity charts
- **Advanced Scoring**: Continuous re-scoring with fresh data

### Orchestration Features
- **Dynamic Scheduling**: Sources run based on optimal cadences and last execution times
- **Rate Limiting**: Intelligent throttling to respect API limits
- **Health Monitoring**: Automatic failure detection and fallback strategies
- **Priority Tiering**: Critical sources run first, optional sources run when resources allow

## Data Flow

1. **Ingestion**: Tier 0 orchestration collects raw signals from all sources
2. **Processing**: Signals are normalized and stored in TimescaleDB
3. **Scoring**: MVP algorithm calculates heat scores with velocity, spread, and affect components
4. **Aggregation**: Continuous aggregates provide efficient historical views
5. **Delivery**: Enhanced dashboard and APIs provide actionable intelligence

## Running Specific Flows

### Complete Data Ingestion (Recommended)
```powershell
# Run comprehensive Tier 0 orchestration once
docker compose exec worker python -m flows.run_tier0_once

# Or run the traditional daily pipeline (includes Tier 0)
docker compose exec worker python -m flows.run_ingest_once
```

### Individual Source Testing
```powershell
# Test Reddit enhanced monitoring
docker compose exec worker python -m flows.reddit_ingest_enhanced

# Test YouTube data collection  
docker compose exec worker python -m flows.youtube_ingest

# Test trade publication RSS feeds
docker compose exec worker python -m flows.trade_news_ingest

# Test box office and IMDb data
docker compose exec worker python -m flows.imdb_box_office_ingest
```

### Run discovery (optional)

```powershell
# Enable in configs/sources.yml: entity_discovery.enabled: true
docker compose exec worker python -m flows.entity_discovery_advanced
```

### Housekeeping flows

```powershell
# Storage maintenance (purge + analyze). Adjust days as needed.
docker compose exec worker python -m flows.storage_maintenance

# Alerts (Slack): stale scores + budget nearing limits
docker compose exec worker python -m flows.alerts
```

### Enable advanced scoring (optional)

```powershell
# In configs/sources.yml set advanced_scoring.enabled: true
# Then call the endpoint
curl http://localhost:8080/score/advanced/Taylor%20Swift
```

## Notes
- This is a bootstrap skeleton. We'll connect ingestion and scoring next.
	- New endpoint `/trends/intelligent` returns structured trends with lightweight reasons, reusing the latest scores in the DB.
	- Category filter supported: `/trends/intelligent?category=entertainment`.
	- Parameters `timeframe`, `depth`, `personalized` are accepted for future compatibility.
	- `/entity/{name}/analysis` returns latest components and reasons with an optional timeline.
	- `/trends/sse` streams periodic updates as Server-Sent Events.
	- `/trends/dashboard` serves a minimal HTML view with top items and reasons.
	- Discovery flow adds new entities from Google trending + Wikipedia validation, dependency-light.
	- DB: TimescaleDB retention and compression policies are applied in `sql/init.sql` for `signals` and `scores`.
	- DB: A continuous aggregate `cagg_scores_daily` provides 1d buckets of avg/max heat (auto-refreshed hourly).
	- Metrics: `/metrics` now includes per-method and per-path counters alongside latency summary.
