"""
json_producer_ragini.py

A simple Kafka JSON producer that sends easy-to-understand messages.
"""

import json
import time
from kafka import KafkaProducer

# List of messages to send
messages = [
    {"message": "Data engineering is my passion.", "author": "Eve"},
    {"message": "Distributed systems are fascinating.", "author": "Alice"},
    {"message": "Kafka makes streaming easy.", "author": "Bob"},
    {"message": "Python and Kafka are a great combo!", "author": "Ragini"},
]

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Change if needed
KAFKA_TOPIC = 'ragini_json_topic'           # Change if needed

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Sending messages to topic '{KAFKA_TOPIC}'...")

try:
    for msg in messages:
        print(f"Sending: {msg}")
        producer.send(KAFKA_TOPIC, value=msg)
        time.sleep(1)  # Wait 1 second between messages
    print("All messages sent!")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()
    print("Producer closed.")
