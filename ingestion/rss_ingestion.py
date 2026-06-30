import feedparser
from kafka import KafkaProducer
import json
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

RSS_SOURCES = [
    "https://www.marktechpost.com/feed/",
    "https://rss.arxiv.org/rss/cs.AI",
]

def run():
    run_time = datetime.now(timezone.utc).isoformat()
    total = 0
    for url in RSS_SOURCES:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            message = {
                "source": "rss",
                "feed_url": url,
                "external_id": entry.get("id", entry.get("link")),
                "title": entry.get("title"),
                "published_at": entry.get("published", ""),
                "link": entry.get("link"),
                "ingested_at": run_time,
            }
            producer.send("raw.rss", message)
            total += 1
    producer.flush()
    print(f"[rss] sent {total} records to Kafka topic 'raw.rss'")

if __name__ == "__main__":
    run()