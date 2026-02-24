FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY schemas/ ./schemas/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "src/produce_sales_events.py"]