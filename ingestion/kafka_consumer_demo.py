from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "raw.youtube",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("waiting for messages... (Ctrl+C to stop)")
for message in consumer:
    print(f"received: {message.value}")