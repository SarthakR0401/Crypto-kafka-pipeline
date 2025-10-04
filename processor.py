import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "api-raw-data"
PROCESSED_TOPIC = "api-processed-data"
WATCH = {"bitcoin", "ethereum"}

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="processor-group"
)

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Processor started...")

for msg in consumer:
    try:
        data = msg.value.get("data", {})
        for coin in WATCH:
            if coin in data:
                coin_data = data[coin]
                doc = {
                    "coin": coin,
                    "price_usd": coin_data.get("usd"),
                    "processed_at": datetime.utcnow().isoformat() + "Z",
                    "status": "OK"
                }
                producer.send(PROCESSED_TOPIC, value=doc)
                print(f"Processed {coin}: {doc['price_usd']}")
    except Exception as e:
        print("Processing error:", e)
