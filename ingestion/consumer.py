import json
import os
from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

# Kafka config
kafka_config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "group.id": "crypto-consumer-group",
    "auto.offset.reset": "earliest",
}

# BigQuery config
credentials = service_account.Credentials.from_service_account_file(
    os.getenv("GCP_CREDENTIALS_PATH")
)
bq_client = bigquery.Client(
    project=os.getenv("GCP_PROJECT_ID"),
    credentials=credentials,
)
table_id = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}"


def ensure_table_exists():
    """Crée la table si elle n'existe pas."""
    schema = [
        bigquery.SchemaField("coin", "STRING"),
        bigquery.SchemaField("price_usd", "FLOAT"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    try:
        bq_client.create_table(table)
        print(f"Table {table_id} created.")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"Table {table_id} already exists.")
        else:
            raise e


def insert_rows(messages):
    """Insère les messages dans BigQuery."""
    from datetime import datetime, timezone

    rows = []
    for msg in messages:
        rows.append({
            "coin": msg["coin"],
            "price_usd": msg["price_usd"],
            "timestamp": msg["timestamp"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })

    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print(f"BigQuery insert errors: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into BigQuery.")


def main():
    ensure_table_exists()

    consumer = Consumer(kafka_config)
    consumer.subscribe(["crypto-prices"])

    print("Consumer started. Waiting for messages...")

    batch = []
    batch_size = 10

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if batch:
                    insert_rows(batch)
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            batch.append(data)
            print(f"Received: {data['coin']} = ${data['price_usd']}")

            if len(batch) >= batch_size:
                insert_rows(batch)
                batch = []

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        if batch:
            insert_rows(batch)
        consumer.close()


if __name__ == "__main__":
    main()