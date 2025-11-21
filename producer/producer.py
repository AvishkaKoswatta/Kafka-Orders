import json
import random
import time
import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Wait for Schema Registry
for i in range(60):
    try:
        res = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        if res.status_code == 200:
            print("Schema Registry ready!")
            break
    except Exception:
        print("Waiting for Schema Registry...")
        time.sleep(2)
else:
    raise Exception("Schema Registry not reachable after 2 minutes")

schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

schema_path = "./orders.avsc"
value_schema_str = open(schema_path).read()

producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'key.serializer': StringSerializer("utf_8"),
    'value.serializer': AvroSerializer(schema_registry_client, value_schema_str)
}

producer = SerializingProducer(producer_conf)

products = ["Item1", "Item2", "Item3", "Item4"]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.key()} to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

while True:
    order = {
        "orderId": str(random.randint(1000, 9999)),
        "product": random.choice(products),
        "price": round(random.uniform(10.0, 100.0), 2)
    }
    producer.produce(topic="orders", key=order["orderId"], value=order, on_delivery=delivery_report)
    producer.flush()
    print(f"Produced: {order}")
    time.sleep(60)
