from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(5):
    msg = {"id": i, "content": f"hello message {i}"}
    producer.send("test-topic", msg)
    print(f"sent: {msg}")
    time.sleep(1)

producer.flush()