FROM python:3.10-slim
WORKDIR /app
COPY ./scripts/producer.py /app/producer.py
RUN pip install confluent-kafka==2.3.0
CMD ["python3", "/app/producer.py"]
