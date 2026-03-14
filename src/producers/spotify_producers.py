import os
import json
import time
import uuid
import random
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

# Load .env from script directory (works regardless of CWD)
_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_env_path)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
USER_COUNT = int(os.getenv("USER_COUNT", 20))
EVENT_INTERVAL_SECONDS = int(os.getenv("EVENT_INTERVAL_SECONDS", 1))

fake = Faker()



# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0, 10, 1),
)

#Stable song/Artist Definitions

song_artist_pairs = [
    {"artist": "The Weeknd", "song": "Blinding Lights"},
    {"artist": "Dua Lipa", "song": "Levitating"},
    {"artist": "Drake", "song": "God's Plan"},
    {"artist": "Taylor Swift", "song": "Love Story"},
    {"artist": "Ed Sheeran", "song": "Shape of You"},
    {"artist": "Kanye West", "song": "Stronger"}
]


for pair in song_artist_pairs:
    name_for_uuid = f"{pair['artist']}::{pair['song']}"
    pair["song_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid))

    devices = ["mobile", "desktop", "web"]
    countries = ["US", "UK", "CA", "AU", "IN", "DE"]
    event_types = ["play", "pause", "skip", "add_to_playlist"]


# Generate random users

user_ids = [str(uuid.uuid4()) for _ in range(USER_COUNT)]


def generate_event():
    pair = random.choice(song_artist_pairs)
    user_id = random.choice(user_ids)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "song_id": pair["song_id"],
        "artist_name": pair["artist"],
        "song_name": pair["song"],
        "device_type": random.choice(devices),
        "country": random.choice(countries),
        "event_type": random.choice(event_types),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }


if __name__ == "__main__":
    import sys
    test_mode = "--test" in sys.argv or "-n" in sys.argv
    n_events = 5
    if "-n" in sys.argv:
        try:
            idx = sys.argv.index("-n") + 1
            n_events = int(sys.argv[idx])
        except (IndexError, ValueError):
            pass

    print("Starting Spotify data simulator...")
    print(f"Using {len(song_artist_pairs)} songs and {len(user_ids)} users.")
    for p in song_artist_pairs:
        print(f"{p['song']} = {p['artist']} -> song_id={p['song_id']}")

    count = 0
    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, event)
        count += 1
        print(f"Produced event {count}: {event['event_type']} - {event['song_name']} by {event['artist_name']}")
        if test_mode and count >= n_events:
            print(f"Test complete: {count} events sent.")
            producer.flush(timeout=10)
            producer.close()
            break
        time.sleep(EVENT_INTERVAL_SECONDS)