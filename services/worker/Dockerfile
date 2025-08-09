FROM python:3.11-slim

WORKDIR /app
COPY services/worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flows ./flows
COPY libs ./libs
COPY configs ./configs

ENV PYTHONPATH="/app"
CMD ["sh", "-lc", "prefect work-pool create -t process default-pool || true; prefect worker start -p default-pool"]
