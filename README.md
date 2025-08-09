# ET Heatmap — Skeleton

## Prereqs
- Docker + Docker Compose
- Copy `.env.example` to `.env`

## Run
```powershell
# Windows PowerShell
Copy-Item .env.example .env

docker compose build
docker compose up -d
```

## Verify

- API health: http://localhost:8080/health
- Prefect UI: http://localhost:4200
- Metabase: http://localhost:3000

## Run a test flow (one-off)

```powershell
docker compose exec worker python -m flows.hello
```

## Notes
- This is a bootstrap skeleton. We'll connect ingestion and scoring next.
