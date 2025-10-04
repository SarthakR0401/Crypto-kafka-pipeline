from kafka import KafkaConsumer
import json
from pymongo import MongoClient
import time

# Kafka config
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "api-raw-data"

# MongoDB config
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "crypto_db"
COLLECTION_NAME = "crypto_prices"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print("Starting Kafka Consumer...")

# Connect to Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    auto_offset_reset='earliest',  # read from beginning if first time
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    enable_auto_commit=True
)

try:
    for message in consumer:
        data = message.value
        try:
            collection.insert_one(data)
            print(f"Inserted into MongoDB: {data}")
        except Exception as e:
            print("MongoDB insert error:", e)

        time.sleep(0.5)  # optional small delay

except KeyboardInterrupt:
    print("Consumer stopped manually")
finally:
    consumer.close()
    client.close()
