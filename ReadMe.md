
# Real-Time Named Entity Recognition System

This project implements a real-time named entity recognition system using **Apache Kafka**, **PySpark Structured Streaming**, and the **ELK (Elasticsearch, Logstash, Kibana) stack**. The system processes news headlines from NewsAPI, extracts named entities, and visualizes the results in Kibana.

---

## Setup Instructions

### Start Kafka Services
1. Start ZooKeeper:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```

2. Start the Kafka broker:
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```

### Create Kafka Topics
Create the required Kafka topics:

```bash
bin/kafka-topics.sh --create --topic topic1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic2 --bootstrap-server localhost:9092
```

---

## Running the ELK Stack

### Start the ELK Stack
Run the ELK stack using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- Elasticsearch on port `9200`
- Kibana on port `5601`
- Logstash on `5044`

## Running the Application

### Start the News Fetcher
Run the Python script to fetch news from NewsAPI and send it to `topic1`:

```bash
python fetch_news.py
```

### Start the Spark Streaming Application
Run the PySpark structured streaming application to process data from `topic1` and send results to `topic2`:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1     structured_streaming.py     localhost:9092     subscribe     topic1
```

---

## Visualizing Results in Kibana

1. Open Kibana in your browser at: [http://localhost:5601](http://localhost:5601).
2. Create an index pattern for `ner-entities-*`.
3. Go to the **Visualize** tab and create a bar chart to display the top 10 named entities.