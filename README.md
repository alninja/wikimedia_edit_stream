# wikimedia_edit_stream
# Wiki- Pulse: Real-Time tracking of the Wikimedia Ecosystem edits. 

1. ##  Problem Statement
Wikipedia is often the battleground for public information surrounding global events. 
The goal of this project is to create an end-to-end streaming data pipeline that monitors real-time Wikipedia edits, analyzes human vs. bot behaviors, and visualizes edit spikes over time.

The Solution: This project implements a real-time monitoring pipeline that ingests the global Wikimedia EventStream. It categorizes edits by type (Bot vs. Human) and volume, enabling the detection of "information surges" on specific topics as they happen.

## 🛠️ Tech Stack
* **Cloud:** Google Cloud Platform (GCS, BigQuery)
* **IaC:** Terraform
* **Orchestration:** Kestra
* **Stream Ingestion:** Kafka (Redpanda) & Python
* **Stream Processing:** PySpark Structured Streaming
* **DWH & Transformations:** BigQuery & dbt
* **Dashboard:** Looker Studio


```graph TD
    subgraph "External Source"
        A[Wikimedia SSE Stream]
    end

    subgraph "Local Infrastructure (Docker)"
        B[Python Producer]
        C((Kafka / Redpanda))
        D[PySpark Structured Streaming]
        E[(Local Parquet Storage)]
    end

    subgraph "Orchestration (Kestra)"
        F{Kestra Workflow}
    end

    subgraph "Google Cloud Platform (GCP)"
        G[GCS Bucket - Data Lake]
        H[(BigQuery - Raw Table)]
        I{dbt Cloud}
        J[(BigQuery - Prod Table)]
    end

    subgraph "Visualization"
        K[Looker Studio Dashboard]
    end

    %% Flow Connections
    A -->|Live Events| B
    B -->|Produce| C
    C -->|Consume| D
    D -->|Write Parquet| E
    
    E -->|Trigger: Pull| F
    F -->|Upload| G
    G -->|Load| H
    H -->|Trigger Transform| I
    I -->|Test & Model| J
    J -->|Query| K

    %% Styling
    style F fill:#f96,stroke:#333,stroke-width:2px
    style I fill:#ff6666,stroke:#333,stroke-width:2px
    style K fill:#4285F4,stroke:#fff,stroke-width:2px
```

## Project Structure
```Plaintext
├── flows/
│   └── wiki_lake_to_cloud_pull.yaml  # Kestra Pipeline Definition
├── models/
│   ├── staging/
│   │   ├── schema.yml               # Data sources and tests
│   │   └── stg_wikimedia_edits.sql  # Cleaning and casting
│   └── core/
│       └── fact_wiki_edits.sql      # Partitioned & Clustered Gold table
├── scripts/
│   └── producer.py                  # SSE Stream Producer
└── dbt_project.yml                  # dbt configuration
```

🚀 Getting Started
### Prerequisites
    Google Cloud Project with BigQuery and GCS enabled.
    Kestra instance (Local or Cloud).
    dbt Cloud account linked to your GitHub repository.

### Configuration
    1. GCP Setup: Create a service account with Storage Admin and BigQuery Admin roles. Add the JSON key to Kestra's KV Store as GC_CRED.

    2. dbt Cloud Setup:
        Create a Service Account Token with Job Admin permissions.
        Add the token to Kestra KV Store as DBT_CLOUD_API_TOKEN.

    3. Kestra: Import the wiki_lake_to_cloud_pull.yaml flow and update the accountId and jobId to match your dbt Cloud environment.

2. ## Cloud & Infrastructure as Code (IaC)
This project is hosted on Google Cloud Platform (GCP). All infrastructure is provisioned using **Terraform** to ensure reproducibility and state management.
    - Google Cloud Storage (GCS): Acts as our Data Lake (Bronze Layer) for storing raw Parquet files.
    - BigQuery: Functions as our Data Warehouse (Silver/Gold Layers).

To Provision:

    ``` Bash
    cd terraform
    terraform init
    terraform apply
    ```
*(Refer to main.tf and variables.tf in the repository for resource specifications.)*

3. ## Data Ingestion: Streaming Architecture
The pipeline uses a high-throughput streaming architecture to handle global edit volumes:

* **Producer**: A Python script utilizing the sseclient to listen to the mediawiki.recentchange stream and publish JSON messages to a Kafka/Redpanda topic.
* **Consumer/Processor**: A PySpark Structured Streaming job that:
    1. Subscribes to the Kafka topic.
    2. Parses the nested JSON schema.
    3. Writes the raw data into a local directory as Parquet files, which are then orchestrated to GCS via Kestra.

4. ## Data Warehouse Optimization
Data is moved from a local folder to the GCS Data Lake and then to BigQuery using Kestra. In BigQuery, we optimize for query performance and cost-efficiency by:
* **Partitioned by**: event_date (derived from event_timestamp). This reduces costs by ensuring queries only scan data for specific days.
* **Clustered by**: wiki_language. This accelerates dashboard filters when analyzing specific language projects or filtering out automated traffic.

5. ## Transformations (dbt)
We utilize dbt Cloud to transform raw event data into analytics-ready tables.
* **Staging Layer (stg_wikimedia_edits)**: Cleans raw fields, renames columns for clarity, and casts types (e.g., converting Unix integers to Timestamps).

* **Core Layer (fact_wiki_edits)**: Implements business logic, such as a boolean flag for is_human (where bot is false) and extracting domain-specific language codes.

* **Data Quality**: Automated tests (duplicate ID such as event_id) are executed on every run to prevent data degradation.

6. ## Dashboard (Looker Studio)
The final "Gold" (fact_wiki_edits) table is connected to Looker Studio to provide a real-time "Pulse" of the Wikimedia ecosystem.

* **Tile 1 (Categorical)**: A Bar Chart comparing edit volumes across different language projects (e.g., enwiki vs eswiki).

* **Tile 2 (Temporal)**: A Time-Series Line Chart showing the frequency of edits in 5-minute intervals to identify sudden surges in activity.

* **Filter**: A toggle to view "Human-only" edits vs "Bot-only" edits.

7. ## Reproducibility: Setup Guide
Follow these steps to spin up the entire environment:

    1. Infrastructure

    ```Bash
        terraform apply
    ```
        
    2. Services (Kafka & Spark)
    Use the provided docker-compose.yml to start the local message bus and processing engine:

        ```Bash
        docker-compose up -d
        ```
    3. Start Ingestion
    Run the producer to begin fetching live data:

        ```Bash
        python scripts/producer.py
        ```
    4. Orchestration
       * Import the upload_to_gcp.yaml flow into your Kestra instance.
       * The flow will automatically pick up Parquet files, move them to GCP.

    5. Transformations
    In dbt Cloud Run:

        ```Bash
        dbt build
        ```



## 📊 Live Dashboard
You can view the real-time Wikimedia Monitor here: 
[\[Link to Looker Studio Report\]](https://lookerstudio.google.com/reporting/f0a85813-6dcb-46c1-bc95-58d55a5b03f9)

*Note: The dashboard is powered by BigQuery and reflects live edits processed through our Kestra/dbt pipeline.*


