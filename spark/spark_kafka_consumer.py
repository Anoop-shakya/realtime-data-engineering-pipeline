from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

import io
import struct
from fastavro import schemaless_reader


# -----------------------------
# Spark session
# -----------------------------

spark = (
    SparkSession.builder
    .appName("KafkaAvroToCassandra")
    .master("local[1]")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -----------------------------
# User schema
# -----------------------------

user_schema = {
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


# -----------------------------
# Decode Confluent Avro
# -----------------------------

def decode_avro(value):
    if value is None:
        return None

    # Confluent Avro format:
    # Byte 0 = magic byte
    # Bytes 1-4 = Schema Registry ID
    # Remaining bytes = Avro data

    raw = bytes(value)

    if raw[0] != 0:
        raise ValueError("Invalid Confluent Avro message")

    avro_data = raw[5:]

    return schemaless_reader(
        io.BytesIO(avro_data),
        user_schema
    )


user_struct = StructType([
    StructField("id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("gender", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("phone", StringType())
])


decode_avro_udf = udf(decode_avro, user_struct)


# -----------------------------
# Read Kafka
# -----------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "users")
    .option("startingOffsets", "earliest")
    .load()
)


# -----------------------------
# Decode Avro values
# -----------------------------

users = (
    kafka_df
    .select(decode_avro_udf("value").alias("user"))
    .select("user.*")
)


# -----------------------------
# Write to Cassandra
# -----------------------------

query = (
    users.writeStream
    .format("org.apache.spark.sql.cassandra")
    .outputMode("append")
    .option("checkpointLocation", "spark/checkpoint_avro")
    .option("keyspace", "realtime_data")
    .option("table", "users")
    .start()
)

query.awaitTermination()