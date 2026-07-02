import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

TOPICS = ["raw.youtube", "raw.rss", "raw.batch"]
BRONZE_DIR = "data/bronze"
os.makedirs(BRONZE_DIR, exist_ok=True)

def consume_and_save():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,  # 10秒没有新消息就自动停止
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    records = {
        "raw.youtube": [],
        "raw.rss": [],
        "raw.batch": []
    }

    print("consuming messages from Kafka...")
    for message in consumer:
        topic = message.topic
        records[topic].append(message.value)

    consumer.close()

    # 把每个topic的数据分别存成Parquet文件
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for topic, data in records.items():
        if not data:
            print(f"[{topic} no records found]")
            continue

        source_name = topic.replace("raw.", "")
        output_path = f"{BRONZE_DIR}/{source_name}/{run_date}.parquet"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df = pd.DataFrame(data)
        df.to_parquet(output_path, index=False)
        print(f"[{topic}] saved {len(data)} records to {output_path}")

if __name__ == "__main__":
    consume_and_save()