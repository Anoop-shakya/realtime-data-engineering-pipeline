import json
import psycopg2
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="my_realtime_db",
    user="datauser",
    password="password"
)

cursor = conn.cursor()

cursor.execute("""
    SELECT id, first_name, last_name, email, gender, country, city, phone
    FROM users
""")

users = cursor.fetchall()

print(f"Fetched {len(users)} users from PostgreSQL")


# Schema Registry
schema_registry_client = SchemaRegistryClient({
    "url": "http://localhost:8081"
})


# Avro schema
avro_schema = """
{
  "type": "record",
  "name": "User",
  "namespace": "realtime.data",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "first_name", "type": "string"},
    {"name": "last_name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "gender", "type": "string"},
    {"name": "country", "type": "string"},
    {"name": "city", "type": "string"},
    {"name": "phone", "type": "string"}
  ]
}
"""


# Convert Python dictionary to Avro
def user_to_dict(user, ctx):
    return {
        "id": user["id"],
        "first_name": user["first_name"],
        "last_name": user["last_name"],
        "email": user["email"],
        "gender": user["gender"],
        "country": user["country"],
        "city": user["city"],
        "phone": user["phone"]
    }


avro_serializer = AvroSerializer(
    schema_registry_client,
    avro_schema,
    user_to_dict
)


# Kafka producer
producer = SerializingProducer({
    "bootstrap.servers": "localhost:9092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer
})


print("Connected to Kafka + Schema Registry")


# Send users
for user in users:

    user_data = {
        "id": user[0],
        "first_name": user[1],
        "last_name": user[2],
        "email": user[3],
        "gender": user[4],
        "country": user[5],
        "city": user[6],
        "phone": user[7]
    }

    producer.produce(
        topic="users",
        key=str(user_data["id"]),
        value=user_data
    )

    print(f"Sent user: {user_data['id']}")


producer.flush()

print("All users sent to Kafka using Avro + Schema Registry!")


cursor.close()
conn.close()