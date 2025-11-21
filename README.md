# Kafka Avro Producer Consumer Pipeline with Schema Registry + DLQ

```bash
docker compose up --build

docker compose up -d

docker compose down -v

docker exec -it orders-kafka-consumer-1 python consumer.py

docker exec -it orders-kafka-producer-1 python producer.py

docker exec -it orders-kafka-kafka-1 kafka-topics --bootstrap-server kafka:9092 --create --topic orders --partitions 1 --replication-factor 1

docker exec -it orders-kafka-kafka-1 kafka-topics --bootstrap-server kafka:9092 --list
