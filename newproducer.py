import os
import time
import random
import json
import requests
from kafka import KafkaProducer

TOPIC_NAME = 'latest_events'
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
)

url = 'https://stream.wikimedia.org/v2/stream/recentchange'
response = requests.get(url, stream=True)


for line in response.iter_lines():
    # Skip empty lines
    if line:
        message = {"message": line.decode("utf-8")}
        producer.send(
            TOPIC_NAME,
            json.dumps(message).encode("utf-8"),
        )

producer.flush()
