FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common.py trades_consumer.py quotes_consumer.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "trades_consumer.py"]
