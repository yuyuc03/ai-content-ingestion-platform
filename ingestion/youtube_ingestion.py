import os
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
import json
from datetime import datetime

load_dotenv()
API_key = os.getenv('YOUTUBE_API_KEY')

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_youtube_videos(query='AI generated', max_result=10):
    # Step 01: find out the related videos and their id
    search_url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max_result,
        "key": API_key,
    }

    resp = requests.get(search_url, params=params)
    resp.raise_for_status() #Check the run results
    items = resp.json().get("items", [])
    video_ids = [item["id"]["videoId"] for item in items]

    #Step 02: Get the statistic results for those videos
    videos_url = "https://www.googleapis.com/youtube/v3/videos"
    params2 = {
        "part": "snippet,statistics",
        "id": ",".join(video_ids),
        "key": API_key,
    }
    resp2 = requests.get(videos_url, params=params2)
    resp2.raise_for_status()
    return resp2.json().get("items", [])

def run():
    videos = fetch_youtube_videos()
    run_time = datetime.utcnow().isoformat()
    count = 0
    for v in videos:
        message = {
            "source": "youtube",
            "external_id": v["id"],
            "title": v["snippet"]["title"],
            "published_at": v["snippet"]["publishedAt"],
            "view_count": v["statistics"].get("viewCount"),
            "like_count": v["statistics"].get("likeCount"),
            "ingested_at": run_time,
        }
        producer.send("raw.youtube", message)
        count += 1

    producer.flush()

    print(f"[youtube] sent {count} records to Kafka topic 'raw.youtube'")

if __name__ == "__main__":
    run()