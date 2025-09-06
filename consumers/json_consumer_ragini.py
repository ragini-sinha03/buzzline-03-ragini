"""
json_consumer_ragini.py

A simple Kafka JSON consumer that reads and prints easy-to-understand messages.
"""

from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Change if needed
KAFKA_TOPIC = 'ragini_json_topic'           # Must match the producer topic

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ragini_json_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

try:
    for message in consumer:
        print(f"Received: {message.value}")
except KeyboardInterrupt:
    print("Consumer interrupted by user.")
finally:
    consumer.close()
    print("Consumer closed.")
