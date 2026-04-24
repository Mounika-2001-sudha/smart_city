#!/usr/bin/env python3
"""
Kafka Topic Setup
Creates the three required topics if they don't exist.
Run before starting the simulators.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import sys

BOOTSTRAP = "localhost:9092"

TOPICS = [
    NewTopic(name="traffic_stream",     num_partitions=3, replication_factor=1),
    NewTopic(name="air_quality_stream", num_partitions=3, replication_factor=1),
    NewTopic(name="weather_stream",     num_partitions=2, replication_factor=1),
]


def setup_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="smartcity-setup")
    except Exception as e:
        print(f"❌ Cannot connect to Kafka at {BOOTSTRAP}: {e}")
        print("   Make sure Kafka is running: docker compose up -d zookeeper kafka")
        sys.exit(1)

    existing = admin.list_topics()
    to_create = [t for t in TOPICS if t.name not in existing]

    if not to_create:
        print("✅ All topics already exist:")
        for t in TOPICS:
            print(f"   • {t.name} ({t.num_partitions} partitions)")
        admin.close()
        return

    try:
        admin.create_topics(new_topics=to_create, validate_only=False)
        print("✅ Topics created:")
        for t in to_create:
            print(f"   • {t.name} ({t.num_partitions} partitions)")
    except TopicAlreadyExistsError:
        print("Topics already exist (race condition - no action needed)")
    finally:
        admin.close()


if __name__ == "__main__":
    setup_topics()
