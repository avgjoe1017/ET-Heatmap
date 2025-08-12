#!/usr/bin/env pwsh
# Enable Enhanced Data Sources for ET Heatmap
# Run this script to activate more scrapers and improve data richness

Write-Host "🔥 ET Heatmap Data Source Enhancement" -ForegroundColor Yellow
Write-Host "=====================================" -ForegroundColor Yellow

# Check if we're in the right directory
if (!(Test-Path "docker-compose.yml")) {
    Write-Error "Please run this script from the ET Heatmap root directory"
    exit 1
}

Write-Host "`n📊 Current Data Sources Status:" -ForegroundColor Cyan

# Show current status
docker compose exec -T db psql -U heatmap -d heatmap -c "
SELECT source, 
       COUNT(*) as signal_count, 
       MIN(ts) as earliest, 
       MAX(ts) as latest 
FROM signals 
GROUP BY source 
ORDER BY source;"

Write-Host "`n🚀 Enabling Additional Data Sources..." -ForegroundColor Green

# Create enhanced sources.yml
@'
sources:
  wiki:         {enabled: true,  weight: 1.0}
  trends:       {enabled: true,  weight: 1.0}
  entity_discovery: {enabled: true, weight: 1.0}  # Enable discovery
  scrape_news:  {enabled: true, weight: 0.6}       # Enable news
  apify_tiktok: {enabled: false, weight: 0.7}      # Requires API key
  tt_cc:        {enabled: false, weight: 1.0}      # Requires API key
  tt_search:    {enabled: true,  weight: 1.0}      # Already working
  gdelt_gkg:    {enabled: true,  weight: 1.0}      # Enable news intelligence
  advanced_scoring: {enabled: true, weight: 1.0}   # Enable advanced ML
'@ | Out-File -FilePath "configs/sources.yml" -Encoding UTF8

Write-Host "✅ Updated configs/sources.yml" -ForegroundColor Green

# Check if API keys are needed
Write-Host "`n🔑 API Key Requirements:" -ForegroundColor Cyan
Write-Host "- TikTok scrapers require APIFY_API_TOKEN in .env"
Write-Host "- News scraping works without keys but rate-limited"
Write-Host "- GDELT is free and unlimited"

# Restart services
Write-Host "`n🔄 Restarting services..." -ForegroundColor Yellow
docker compose restart worker scheduler

Write-Host "`n⏱️  Waiting for services to start..." -ForegroundColor Yellow
Start-Sleep 10

# Run discovery flow once
Write-Host "`n🔍 Running entity discovery..." -ForegroundColor Green
docker compose exec worker python -m flows.entity_discovery_advanced

# Run a test pipeline
Write-Host "`n📈 Running enhanced data pipeline..." -ForegroundColor Green
docker compose exec worker python -m flows.run_ingest_once

Write-Host "`n✨ Enhancement Complete!" -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green

Write-Host "`n📊 Check your enhanced dashboard at:" -ForegroundColor Cyan
Write-Host "https://etheatmap.joebalewski.com/trends/dashboard" -ForegroundColor Blue

Write-Host "`n📈 New features enabled:" -ForegroundColor White
Write-Host "- Entity discovery from Google Trends"
Write-Host "- News intelligence via GDELT"  
Write-Host "- Advanced multi-dimensional scoring"
Write-Host "- Enhanced narrative insights"
Write-Host "- Cross-platform correlation"

Write-Host "`n⚡ Next steps:" -ForegroundColor Yellow
Write-Host "1. Add APIFY_API_TOKEN to .env for TikTok data"
Write-Host "2. Monitor dashboard for new entities (24h)"
Write-Host "3. Set up Slack alerts: python -m flows.alerts"
Write-Host "4. Review budget usage: /budget endpoint"

Write-Host "`n🎯 Expected improvements:" -ForegroundColor Magenta
Write-Host "- 5-10x more daily signals"
Write-Host "- 10x more tracked entities"
Write-Host "- Sentiment-aware priority scoring"
Write-Host "- Event-context intelligence"

Write-Host "`nEnhancement script completed! 🚀" -ForegroundColor Green
