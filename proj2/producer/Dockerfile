FROM python:3.9-slim

WORKDIR /app

COPY producer.py employee.py . 

RUN pip install --no-cache-dir psycopg2-binary confluent-kafka pandas

CMD ["python", "producer.py"]
