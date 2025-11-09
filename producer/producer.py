import json
import random
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Schema Registry configuration
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Load Avro schema
schema_path = "./orders.avsc"
value_schema_str = open(schema_path).read()

# Define the Avro serializer for the message value
value_avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)

# Define the String serializer for the message key
key_serializer = StringSerializer("utf_8")

producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'key.serializer': key_serializer,
    'value.serializer': value_avro_serializer,
}

producer = SerializingProducer(producer_conf)

products = ["Item1", "Item2", "Item3", "Item4"]

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.key()} to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

while True:
    order = {
        "orderId": str(random.randint(1000, 9999)),
        "product": random.choice(products),
        "price": round(random.uniform(10.0, 100.0), 2)
    }

    producer.produce(
        topic="orders",
        key=order["orderId"],     # this will now be handled correctly
        value=order,              # Avro serialized
        on_delivery=delivery_report
    )
    producer.flush()
    print(f"Produced: {order}")
    time.sleep(2)
