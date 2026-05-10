"""
Clickstream Event Generator for Azure Event Hubs

Simulates real-time e-commerce user activity events:
- page_view, add_to_cart, purchase, search

Usage:
    pip install azure-eventhub faker
    python generate_clickstream.py

Events are sent to Azure Event Hubs for Spark Structured Streaming.
"""

import json
import time
import uuid
import random
from datetime import datetime

from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker

fake = Faker()

# ============================================================
# Configuration — Update these values
# ============================================================
EVENT_HUB_CONNECTION_STRING = "<your-eventhub-connection-string>"
EVENT_HUB_NAME = "clickstream-events"
EVENTS_PER_SECOND = 5
DURATION_SECONDS = 300  # Run for 5 minutes

# ============================================================
# Sample data pools
# ============================================================
PRODUCT_CATEGORIES = [
    "health_beauty", "computers_accessories", "auto", "bed_bath_table",
    "furniture_decor", "sports_leisure", "housewares", "telephony",
    "watches_gifts", "food_drink", "baby", "toys", "electronics",
    "pet_shop", "fashion_bags_accessories", "garden_tools"
]

EVENT_TYPES = ["page_view", "page_view", "page_view", "page_view",
               "search", "search", "add_to_cart", "add_to_cart", "purchase"]

DEVICE_TYPES = ["mobile", "mobile", "mobile", "desktop", "desktop", "tablet"]

REFERRERS = ["google", "direct", "facebook", "instagram", "email", "organic"]

# Simulate ~50 active users
ACTIVE_USERS = [str(uuid.uuid4()) for _ in range(50)]
ACTIVE_SESSIONS = {user: str(uuid.uuid4()) for user in ACTIVE_USERS}
PRODUCT_IDS = [str(uuid.uuid4())[:8] for _ in range(200)]


def generate_event() -> dict:
    """Generate a single clickstream event."""
    user_id = random.choice(ACTIVE_USERS)
    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": ACTIVE_SESSIONS[user_id],
        "event_type": event_type,
        "product_id": random.choice(PRODUCT_IDS) if event_type != "search" else None,
        "category": random.choice(PRODUCT_CATEGORIES) if event_type != "search" else None,
        "search_query": fake.word() if event_type == "search" else None,
        "page_url": f"/products/{random.choice(PRODUCT_IDS)}" if event_type == "page_view" else f"/{event_type}",
        "referrer": random.choice(REFERRERS),
        "device_type": random.choice(DEVICE_TYPES),
        "event_timestamp": datetime.utcnow().isoformat(),
    }
    return event


def main():
    """Send simulated clickstream events to Event Hubs."""
    print(f"Starting clickstream generator...")
    print(f"  Target: {EVENT_HUB_NAME}")
    print(f"  Rate: {EVENTS_PER_SECOND} events/sec")
    print(f"  Duration: {DURATION_SECONDS} seconds")
    print(f"  Total expected: ~{EVENTS_PER_SECOND * DURATION_SECONDS} events")
    print()

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME,
    )

    total_sent = 0
    start_time = time.time()

    try:
        while time.time() - start_time < DURATION_SECONDS:
            # Create batch
            event_data_batch = producer.create_batch()

            for _ in range(EVENTS_PER_SECOND):
                event = generate_event()
                event_json = json.dumps(event)
                try:
                    event_data_batch.add(EventData(event_json))
                except ValueError:
                    # Batch is full, send and create new
                    producer.send_batch(event_data_batch)
                    event_data_batch = producer.create_batch()
                    event_data_batch.add(EventData(event_json))

            producer.send_batch(event_data_batch)
            total_sent += EVENTS_PER_SECOND

            if total_sent % 50 == 0:
                elapsed = time.time() - start_time
                print(f"  Sent {total_sent:,} events ({elapsed:.0f}s elapsed)")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        producer.close()

    elapsed = time.time() - start_time
    print(f"\nDone! Sent {total_sent:,} events in {elapsed:.1f} seconds")
    print(f"Effective rate: {total_sent / elapsed:.1f} events/sec")


if __name__ == "__main__":
    main()
