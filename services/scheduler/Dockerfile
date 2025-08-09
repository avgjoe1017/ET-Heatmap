FROM python:3.11-slim
WORKDIR /app
# Pin pydantic to a version compatible with Prefect 2.19.9 to avoid runtime errors
RUN pip install --no-cache-dir prefect==2.19.9 pydantic==2.8.2
EXPOSE 4200
CMD ["prefect", "server", "start", "--host", "0.0.0.0", "--port", "4200"]
