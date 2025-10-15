from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime
import uuid

# Cấu hình Kafka
kafka_config = {
    "bootstrap.servers": "localhost:9093"
}

# Khởi tạo producer
producer = Producer(kafka_config)

# Hàm tạo dữ liệu mô phỏng
def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "amount": round(random.uniform(10.0, 10000.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
        "merchant": random.choice(["Amazon", "Walmart", "Target"]),
        "geo": random.choice(["US", "EU", "ASIA"]),
        "is_fraud": random.choice([0, 1])
    }

def generate_auth_log():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "timestamp": datetime.utcnow().isoformat(),
        "action": random.choice(["login", "logout", "failed_attempt"]),
        "device_id": str(uuid.uuid4()),
        "ip_address": f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}",
        "geo": random.choice(["US", "EU", "ASIA"])
    }

def generate_system_log():
    return {
        "log_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "service": random.choice(["api", "database", "auth"]),
        "severity": random.choice(["INFO", "WARNING", "ERROR"]),
        "message": random.choice(["Yêu cầu xử lý", "Hết thời gian kết nối", "Lỗi truy vấn"])
    }

# Hàm đẩy message lên Kafka
def produce_messages(topic, generator, num_messages=5):
    for _ in range(num_messages):
        message = generator()
        producer.produce(topic, value=json.dumps(message).encode("utf-8"))
        producer.flush()
        time.sleep(0.5)
        print(f"Đã gửi tới {topic}: {message}")

# Chạy producer
if __name__ == "__main__":
    while True:
        produce_messages("transactions", generate_transaction, 5)
        produce_messages("auth_logs", generate_auth_log, 3)
        produce_messages("system_logs", generate_system_log, 2)
        time.sleep(1)