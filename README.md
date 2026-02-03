# Real-time Crypto ELT

Real-time ELT pipeline: Kafka → BigQuery → dbt

## Architecture
```
CoinGecko API → Kafka Producer → Kafka Topic → Python Consumer → BigQuery (raw) → dbt (transform) → Analytics tables
```

## Stack

- **Kafka** (Confluent Cloud) : event streaming
- **BigQuery** : data warehouse  
- **dbt** : transformation layer
- **Python** : producer & consumer