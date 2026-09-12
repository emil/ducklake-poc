# Minimal image for ducklake_poc.api_server. Built from ./src so it can
# `pip install .` the package directly; uv isn't strictly required at
# runtime, just for a fast, reproducible build.
FROM python:3.12-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir .

ENV API_HOST=0.0.0.0
ENV API_PORT=5001
EXPOSE 5001

CMD ["python", "-m", "ducklake_poc.api_server"]
