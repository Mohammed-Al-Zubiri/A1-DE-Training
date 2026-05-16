# Data Engineering Pipeline: Real-Time Event Streaming (NiFi, Kafka, HDFS)

This repository contains an end-to-end Data Engineering pipeline that ingests simulated e-commerce data, transforms it via Apache NiFi, streams the events through a Kafka broker, and dynamically partitions the batched data into a Hadoop Distributed File System (HDFS).

## 📁 Repository Structure

* `simulate_stream.py`: Python script that generates randomized, semi-structured e-commerce transactions (CSV) to simulate real-time ingestion.
* `nifi_flow_export/`: Contains the complete Apache NiFi canvas export (`.json`).
* `kafka_config/`: Contains the Kafka CLI scripts for topic provisioning and the isolated Publish/Consume Kafka processor configurations.
* `screenshots/`: Visual evidence of the pipeline's successful execution, including Kafka metrics, NiFi data flows, and final HDFS partitioning.
* `Technical_Documentation.pdf`: Comprehensive architectural breakdown, design decisions, and data transformation/partitioning explained.
* `nifi_kafka_hadoop.zip`: All previous contents in a compressed file.
