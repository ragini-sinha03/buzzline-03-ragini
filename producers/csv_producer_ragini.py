"""
csv_producer_ragini.py

A simple Kafka CSV producer that sends rows from a CSV file as messages.
"""


import time
from kafka import KafkaProducer

# List of custom messages to send (CSV-style: message,author)
messages = [
    ["Streaming data unlocks new possibilities.", "Sam"],
    ["Real-time analytics drive smarter decisions.", "Priya"],
    ["Kafka bridges the gap between systems.", "Liam"],
    ["Python makes data engineering approachable.", "Ragini"],
]

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Change if needed
KAFKA_TOPIC = 'ragini_csv_topic'            # Change if needed

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode('utf-8')
)

print(f"Sending custom messages to topic '{KAFKA_TOPIC}'...")

try:
    for row in messages:
        message = ','.join(row)
        print(f"Sending: {message}")
        producer.send(KAFKA_TOPIC, value=message)
        time.sleep(1)  # Wait 1 second between messages
    print("All custom messages sent!")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()
    print("Producer closed.")
