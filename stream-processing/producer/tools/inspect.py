import signal

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# -------- CONFIG --------
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"
TOPIC = "ggazers.github.repos"
GROUP_ID = "users-avro-consumer"

# -------- SCHEMA REGISTRY --------
schema_registry_client = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL
})

avro_deserializer = AvroDeserializer(
    schema_registry_client
)

# -------- CONSUMER --------
consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": avro_deserializer,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",   # or "latest"
    "enable.auto.commit": False,       # manual commit (recommended)
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([TOPIC])

# -------- SHUTDOWN HANDLER --------
running = True

def shutdown(sig, frame):
    global running
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

print("🚀 Avro consumer started...")

# -------- POLL LOOP --------
try:
    while running:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        key = msg.key()
        value = msg.value()   # Already deserialized dict

        print(f"📥 Key={key} | Value={value}")

        # Commit AFTER successful processing
        consumer.commit(message=msg)

finally:
    consumer.close()
    print("🛑 Consumer closed")
