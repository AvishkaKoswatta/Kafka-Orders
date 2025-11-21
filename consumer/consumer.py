import time
import os
import requests
from confluent_kafka import KafkaError, SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

KAFKA_BROKER = "kafka:9092"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL") or "http://schema-registry:8081"

# Wait for Schema Registry to be ready
for i in range(30):
    try:
        res = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        if res.status_code == 200:
            print("Schema Registry ready!")
            break
    except Exception:
        print("Waiting for Schema Registry...")
        time.sleep(2)
else:
    raise Exception("Schema Registry not reachable after 60 seconds")

# Initialize Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Load Avro schema
schema_path = "./orders.avsc"
with open(schema_path) as f:
    value_schema_str = f.read()

# Consumer config
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'order-consumer-group-2',
    'auto.offset.reset': 'earliest',
    'key.deserializer': lambda k, ctx: k.decode() if k else None,
    'value.deserializer': AvroDeserializer(schema_registry_client, value_schema_str)
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(["orders"])
print("Subscribed to topic: orders", flush=True)

# DLQ producer config using SerializingProducer (non-deprecated)
dlq_producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'key.serializer': str.encode,
    'value.serializer': AvroSerializer(schema_registry_client, value_schema_str)
}
dlq_producer = SerializingProducer(dlq_producer_conf)

# Aggregation state
total_price = 0
total_count = 0

def process_message(msg):
    global total_price, total_count
    try:
        order = msg.value() # AvroDeserializer converts Avro → Python dict
        if order["price"] < 0:
            raise ValueError("Price cannot be negative")
        total_price += order["price"]
        total_count += 1
        running_avg = total_price / total_count
        print(f"Processed {order['orderId']} | Running average: {running_avg:.2f}")
    except Exception as e:
        print(f"Error processing {order.get('orderId', 'unknown')}: {e}")
        return False
    return True

MAX_RETRIES = 3

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                continue

        retries = 0
        while retries < MAX_RETRIES:
            success = process_message(msg)
            if success:
                break
            retries += 1
            time.sleep(2)

        if not success:
            dlq_producer.produce(topic="orders-dlq", key=msg.key(), value=msg.value())
            dlq_producer.flush()
            print(f"Moved {msg.key()} to DLQ")

    except KeyboardInterrupt:
        break

consumer.close()
