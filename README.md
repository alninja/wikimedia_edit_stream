# wikimedia_edit_stream
# Wiki-Gov Pulse: Real-Time Public Policy & Edit Tracker

## 📌 Problem Statement
Wikipedia is often the battleground for public information surrounding global policies and events. 
The goal of this project is to create an end-to-end streaming data pipeline that monitors real-time Wikipedia edits, analyzes human vs. bot behaviors, and visualizes edit spikes over time.

## 🛠️ Tech Stack
* **Cloud:** Google Cloud Platform (GCS, BigQuery)
* **IaC:** Terraform
* **Orchestration:** Kestra
* **Stream Ingestion:** Kafka (Redpanda) & Python
* **Stream Processing:** PySpark Structured Streaming
* **DWH & Transformations:** BigQuery & dbt
* **Dashboard:** Looker Studio


Dependencies:pyspark 3.5.3, requests
