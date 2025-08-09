FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir prefect==2.19.9
EXPOSE 4200
CMD ["prefect", "server", "start", "--host", "0.0.0.0", "--port", "4200"]
