FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common.py hourly_report.py trading_patterns.py large_trades.py scheduler.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "scheduler.py"]
