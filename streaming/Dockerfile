FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common.py whale_alert.py volatility_monitor.py market_momentum.py trades_sink.py ./

ENV PYTHONUNBUFFERED=1

# Default command is overridden per-service in docker-compose.
CMD ["python", "-u", "whale_alert.py"]
