import requests
import json
import time
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "api-raw-data"

# Optional: set your CoinGecko API key here
COINGECKO_API_KEY = ""  # Leave empty if not using

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Starting CoinGecko Producer...")

try:
    while True:
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {"ids": "bitcoin,ethereum", "vs_currencies": "usd"}
            
            # Headers to mimic a browser + optional API key
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/116.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*"
            }
            if COINGECKO_API_KEY:
                headers["X-CoinGecko-Api-Key"] = COINGECKO_API_KEY

            # Try fetching from CoinGecko
            response = requests.get(url, params=params, timeout=5, verify=False, headers=headers)
            response.raise_for_status()
            data = response.json()
            source = "coingecko"

        except requests.RequestException as e:
            print(f"CoinGecko API error: {e}")
            # Fallback to mock data if API fails
            data = {
                "bitcoin": {"usd": 50000},
                "ethereum": {"usd": 3500}
            }
            source = "mock"

        # Prepare message for Kafka
        message = {
            "source": source,
            "data": data,
            "timestamp": time.time()
        }

        try:
            producer.send(TOPIC, value=message)
            producer.flush()
            print(f"Sent: {message}")
        except Exception as e:
            print("Kafka producer error:", e)

        # Wait before next request (adjust to avoid API rate limits)
        time.sleep(10)

except KeyboardInterrupt:
    print("Producer stopped manually")
