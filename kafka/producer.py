import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv
import requests
import os

load_dotenv()

config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
}

producer = Producer(config)
TOPIC = "crypto-prices"
COINS = ["bitcoin", "ethereum", "solana", "cardano", "polkadot", "avalanche-2", "chainlink", "polygon"]
API_URL = "https://api.coingecko.com/api/v3/simple/price"


def fetch_prices():
    params = {"ids": ",".join(COINS), "vs_currencies": "usd"}
    response = requests.get(API_URL, params=params)
    return response.json()


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def main():
    while True:
        prices = fetch_prices()
        timestamp = datetime.now(timezone.utc).isoformat()

        for coin, data in prices.items():
            message = {"coin": coin, "price_usd": data["usd"], "timestamp": timestamp}
            producer.produce(
                TOPIC, value=json.dumps(message), callback=delivery_report
            )

        producer.flush()
        print(f"Sent {len(prices)} events at {timestamp}")
        time.sleep(30)


if __name__ == "__main__":
    main()