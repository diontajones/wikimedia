import os
from kafka import KafkaConsumer
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NAME = 'latest_events'
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)
for message in consumer:
    print(message.value.decode("utf-8"))