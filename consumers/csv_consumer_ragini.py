"""
csv_consumer_ragini.py

A simple Kafka CSV consumer that reads and prints custom CSV-style messages.
"""

from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Change if needed
KAFKA_TOPIC = 'ragini_csv_topic'            # Must match the producer topic

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ragini_csv_group',
    value_deserializer=lambda m: m.decode('utf-8')
)

print(f"Listening for CSV-style messages on topic '{KAFKA_TOPIC}'...")

try:
    for message in consumer:
        print(f"Received: {message.value}")
except KeyboardInterrupt:
    print("Consumer interrupted by user.")
finally:
    consumer.close()
    print("Consumer closed.")
