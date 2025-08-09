FROM python:3.11-slim

WORKDIR /app
COPY services/worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flows ./flows
COPY libs ./libs
COPY configs ./configs

ENV PREFECT_API_URL=${PREFECT_API_URL}
ENV PYTHONPATH="/app"
CMD ["prefect", "agent", "start", "--work-queue", "default"]
