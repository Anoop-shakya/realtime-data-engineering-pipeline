# 🚀 Real-Time Data Engineering Pipeline

An end-to-end **real-time data engineering pipeline** built to collect, store, stream, process, and persist user data using modern data engineering technologies.

The project demonstrates a complete data flow from an external API to **PostgreSQL → Apache Kafka → Spark Structured Streaming → Apache Cassandra**, with **Apache Airflow** used for workflow orchestration.

![System Architecture](architecture.png)
---



## 📌 Project Overview

This project simulates a real-world data engineering pipeline where data is:

- 🌐 Collected from the **RandomUser API**
- 🐍 Ingested using **Python**
- 🗄️ Stored in **PostgreSQL**
- ⚙️ Orchestrated using **Apache Airflow**
- 📡 Published to **Apache Kafka**
- 📋 Serialized using **Apache Avro**
- 🧩 Managed using **Confluent Schema Registry**
- ⚡ Processed using **Spark Structured Streaming**
- 💾 Stored in **Apache Cassandra**
- 🐳 Run using **Docker**

---

# 🏗️ Architecture

```text
                    ┌─────────────────────┐
                    │   RandomUser API    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Python        │
                    │   Data Ingestion    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     PostgreSQL      │
                    │    Source Storage   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Kafka Producer    │
                    │       Python        │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    Apache Kafka     │
                    │     users topic     │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   Schema Registry   │
                    │    Avro Schema      │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Spark Structured    │
                    │     Streaming       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Cassandra       │
                    │   realtime_data     │
                    │       users         │
                    └─────────────────────┘

             Apache Airflow
          ──► Workflow Orchestration
```

---

# 🛠️ Technologies Used

| Technology | Purpose |
|---|---|
| 🐍 Python | Data ingestion and Kafka producer |
| 🌐 RandomUser API | Source of user data |
| 🗄️ PostgreSQL | Relational data storage |
| ⚙️ Apache Airflow | Workflow orchestration |
| 📡 Apache Kafka | Real-time data streaming |
| 📋 Apache Avro | Data serialization |
| 🧩 Confluent Schema Registry | Schema management |
| ⚡ Apache Spark | Stream processing |
| 💾 Apache Cassandra | NoSQL data storage |
| 🐳 Docker | Containerization |
| 🔧 Git & GitHub | Version control |

---

# 🔄 Data Flow

### 1️⃣ Data Ingestion

User data is fetched from the **RandomUser API** using Python.

The following information is collected:

- First Name
- Last Name
- Email
- Gender
- Country
- City
- Phone

---

### 2️⃣ PostgreSQL Storage

The collected data is inserted into a PostgreSQL database.

```text
RandomUser API
      ↓
    Python
      ↓
  PostgreSQL
```

PostgreSQL acts as the initial structured data store.

---

### 3️⃣ Workflow Orchestration

**Apache Airflow** is used to orchestrate the data ingestion process.

The Airflow DAG triggers the Python ingestion script and manages the workflow.

```text
Apache Airflow
      ↓
load_users.py
      ↓
PostgreSQL
```

---

### 4️⃣ Kafka Streaming

Data stored in PostgreSQL is read by a Python Kafka producer and published to the Kafka topic:

```text
users
```

Kafka provides the real-time streaming layer of the pipeline.

---

### 5️⃣ Avro & Schema Registry

The Kafka messages are serialized using **Apache Avro**.

The Avro schema is registered with **Confluent Schema Registry**.

This provides a structured format for the data flowing through Kafka.

```text
PostgreSQL
     ↓
Kafka Producer
     ↓
Avro Serialization
     ↓
Schema Registry
     ↓
Kafka
```

---

### 6️⃣ Spark Structured Streaming

**Apache Spark Structured Streaming** continuously consumes messages from the Kafka `users` topic.

Spark:

- Reads Kafka messages
- Decodes the Avro data
- Converts the data into structured records
- Sends the processed data to Cassandra

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Avro Decoding
  ↓
Structured Data
```

---

### 7️⃣ Cassandra Storage

The processed records are finally stored in Apache Cassandra.

```text
Spark
  ↓
Cassandra
  ↓
realtime_data.users
```

---

# 📁 Project Structure

```text
realtime-data-engineering/
│
├── dags/
│   └── user_pipeline.py
│
├── kafka/
│   └── user_schema.json
│
├── postgres/
│   └── init.sql
│
├── scripts/
│   ├── test_postgres.py
│   ├── test_api.py
│   ├── load_users.py
│   └── postgres_to_kafka.py
│
├── spark/
│   ├── spark_kafka_consumer.py
│   └── test_python_worker.py
│
├── .gitignore
├── docker-compose.yml
└── README.md
```

---

# ⚙️ Main Components

### PostgreSQL

Stores the initial user records in a relational table.

**Database:**

```text
my_realtime_db
```

**Table:**

```text
users
```

---

### Apache Kafka

Kafka provides the real-time messaging layer.

**Topic:**

```text
users
```

The topic is configured with multiple partitions to demonstrate Kafka-based distributed streaming.

---

### Schema Registry

Schema Registry stores and manages the Avro schema used by Kafka messages.

**Schema subject:**

```text
users-value
```

---

### Apache Spark

Spark Structured Streaming consumes Kafka messages and processes the incoming data.

The Spark job decodes the Avro messages before writing them to Cassandra.

---

### Apache Cassandra

Cassandra acts as the final NoSQL data store.

**Keyspace:**

```text
realtime_data
```

**Table:**

```text
users
```

---

# 🐳 Running the Project

## 1. Start Docker Services

From the project directory:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

---

## 2. Access Services

| Service | Address |
|---|---|
| Airflow | http://localhost:8080 |
| Kafka Control Center | http://localhost:9021 |
| Schema Registry | http://localhost:8081 |
| PostgreSQL | localhost:5432 |
| Kafka | localhost:9092 |
| Cassandra | localhost:9042 |

---

## 3. Run the Airflow Pipeline

Open:

```text
http://localhost:8080
```

Trigger the:

```text
user_data_pipeline
```

DAG.

The DAG executes the data ingestion process and loads users into PostgreSQL.

---

## 4. Publish PostgreSQL Data to Kafka

Run:

```bash
python scripts/postgres_to_kafka.py
```

The script reads records from PostgreSQL and publishes them to Kafka using Avro serialization.

---

## 5. Start Spark Streaming

Run the Spark consumer:

```bash
spark-submit spark/spark_kafka_consumer.py
```

Spark consumes records from Kafka and writes them to Cassandra.

---

# ✅ Pipeline Verification

The complete pipeline was successfully tested end-to-end.

```text
RandomUser API
      ↓
    Python
      ↓
  PostgreSQL
      ↓
     Kafka
      ↓
 Schema Registry
      ↓
    Spark
      ↓
  Cassandra
```

### Verified Results

- ✅ RandomUser API data successfully fetched
- ✅ Data successfully inserted into PostgreSQL
- ✅ Airflow DAG executed successfully
- ✅ Kafka `users` topic created
- ✅ Avro schema registered in Schema Registry
- ✅ PostgreSQL records successfully published to Kafka
- ✅ Spark successfully consumed Kafka messages
- ✅ Avro messages successfully decoded
- ✅ Records successfully written to Cassandra
- ✅ 20 user records verified in Cassandra

---

# 📚 Key Data Engineering Concepts Demonstrated

This project provides practical exposure to:

- Data ingestion
- ETL / ELT concepts
- Workflow orchestration
- Relational databases
- NoSQL databases
- Event streaming
- Kafka topics and partitions
- Data serialization
- Avro schemas
- Schema Registry
- Stream processing
- Spark Structured Streaming
- Containerization with Docker
- End-to-end data pipelines

---

# 🚀 Future Improvements

Possible improvements include:

- 🔹 Incremental data ingestion
- 🔹 Better error handling and retry mechanisms
- 🔹 Kafka dead-letter queues
- 🔹 Data quality validation
- 🔹 Monitoring and alerting
- 🔹 Schema evolution
- 🔹 Cloud deployment
- 🔹 AWS S3 integration
- 🔹 Apache Airflow scheduling
- 🔹 CI/CD pipeline
- 🔹 Secure credential management

---

# 🎯 Learning Objective

The main objective of this project is to understand how different components of a modern data engineering ecosystem work together to build an end-to-end streaming data pipeline.

It combines **batch ingestion, workflow orchestration, event streaming, schema management, stream processing, and NoSQL storage** in a single project.

---

# 👨‍💻 Author

**Anoop Shakya**

B.Tech Engineering Student  
Interested in **Data Engineering, Data Platforms, Distributed Systems, and Big Data Technologies**.

---

## ⭐ Project

If you find this project useful, consider giving the repository a ⭐ on GitHub.
