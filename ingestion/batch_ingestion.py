import os
import csv
import shutil
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

INCOMING_DIR = "data/incoming"
PROCESSED_DIR = "data/processed"

def run():
    run_time = datetime.now(timezone.utc).isoformat()
    files = [f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")]

    if not files:
        print("[batch] no new files found in data/incoming")
        return
    
    for filename in files:
        filepath = os.path.join(INCOMING_DIR, filename)
        count = 0
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                message = {
                    "source": "batch_file",
                    "source_file": filename,
                    "external_id": row.get("external_id"),
                    "title": row.get("title"),
                    "author": row.get("author"),
                    "published_at": row.get("published_at"),
                    "url": row.get("url"),
                    "content_type": row.get("content_type"),
                    "ingested_at": run_time,
                }
                producer.send("raw.batch", message)
                count += 1
        producer.flush()
        print(f"[batch] processed {filename}: {count} rows sent to 'raw.batch'")
        shutil.move(filepath, os.path.join(PROCESSED_DIR, filename))
        print(f"[batch] moved {filename} to data/processed/")

if __name__ == "__main__":
    run()