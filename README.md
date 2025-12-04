# **Monda — Mini Snowflake ETL**

A compact, reproducible local ETL pipeline built with **Prefect**, **Snowflake**, and **MinIO**.
It simulates a realistic modern lakehouse ingestion flow:

> **MinIO (object storage)** → **Snowflake internal stage + pipe** → **RAW** → **STAGING** → **CURATED (views/subsets)**

All behavior, structure, and parameters are defined in YAML config files — allowing one or many datasets to be ingested, evolved, and materialized automatically.

---

## **Project Structure**

```
monda/
├── Makefile, docker-compose.yaml, .env      # Infra, orchestration, credentials
├── config/user_activity.yaml                # Global + pipeline-specific YAML config
├── sample_data/user_events/                 # Example CSVs for ingestion
└── src/
    ├── flows/                               # Prefect flow entrypoints
    │   ├── create_pipeline.py               # Full setup + ingestion
    │   ├── trigger_pipeline.py              # Re-trigger only
    │   └── serve_all.py                     # Serves both flows
    └── utils/                               # Shared logic
        ├── helpers.py, minio_client.py, pipeline_tasks.py
        └── snowflake/ (client.py, pipeline.py, operations.py, sql/)
```

---

## **1) Prerequisites**

* **Docker**, **Docker Compose**, and **Make** installed
* A valid **Snowflake account**
* Update your `.env` file with credentials:

```bash
SNOWFLAKE_ACCOUNT=<your_account>.snowflakecomputing.com
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_WAREHOUSE=<warehouse>
```

---

## **2) Project Behavior Overview**

The pipeline simulates a minimal yet realistic multi-layer **lakehouse architecture**:

| Layer       | Description                                           | Key Features                                               |
| ----------- | ----------------------------------------------------- | ---------------------------------------------------------- |
| **RAW**     | Ingests CSVs from MinIO into Snowflake internal stage | `INFER_SCHEMA`, `USING TEMPLATE`, dynamic file staging     |
| **STAGING** | Deduplicated, flattened relational tables             | Dynamic schema evolution, `MERGE` with sort & primary keys |
| **CURATED** | Filtered tables or secure views                       | Configurable filters, optional `SECURE VIEW` creation      |

---

### **Core Behavior**

* **Fully config-driven** via YAML — schema, paths, keys, and flatten logic are declarative.
* Uses **Snowflake-native primitives**:

  * **Schema inference** via `INFER_SCHEMA` and `USING TEMPLATE`
  * **Schema evolution** adds only missing columns dynamically
  * **Flatten columns** expand nested JSON into relational fields
  * **MERGE INTO** performs deduplication and upserts from RAW → STAGING
  * **Subsets/Views** auto-generate from STAGING → CURATED using config filters
* **Snowpipe ingestion** with explicit `REFRESH` and completion polling.
* **Housekeeping columns** appended automatically (ingestion timestamp, filename, etc.)
* Modular, Jinja-rendered SQL templates ensure reproducibility.

---

**Note:**
`create_pipeline` automatically provisions all required databases and schemas (`RAW`, `STAGING`, `CURATED`, `UTILS`) during setup.
The **ACCOUNTADMIN** role was used to simplify ACLs and privilege handling for local execution.

---

## **3) Quick Start**

### **Step 1 — Start the stack**

```bash
make up
```

Starts local **Prefect** and **MinIO** services:

* Prefect UI → [http://localhost:4200](http://localhost:4200)
* MinIO Console → [http://localhost:9001](http://localhost:9001)

Volumes are persistent — data survives restarts.

---

### **Step 2 — Create bucket and path**

1. Log into the MinIO console.
2. Create a bucket named **`raw`**.
3. Inside it, create this path:

   ```
   user_activity/user_events/
   ```

---

### **Step 3 — Upload and Run**

1. Open **MinIO Console** → navigate to:

   ```
   raw/user_activity/user_events/
   ```
2. Upload the CSV files from your local folder:

   ```
   sample_data/user_events/
   ```
3. Run flows from the Prefect UI:

   * **`create_pipeline`** → first ingestion (`events_1.csv`)
   * **`trigger_pipeline`** → incremental + schema evolution (`events_2.csv`, `events_3.csv`)

*(The `sample_data` folder is demo-only — can be deleted later.)*

Each run will:

* Extract and stage files into Snowflake
* Infer and evolve schema
* Merge deduplicated data into **STAGING**
* Build filtered or secure subsets into **CURATED**

All runs are **idempotent** — re-execution safely reprocesses data without duplication.

---

## **4) Limitations**

* Uses **internal stages** instead of S3 external stages (MinIO has no native eventing).
* **Snowpipe auto-ingest** is simulated through polling (`SYSTEM$PIPE_STATUS`).
* Designed for **local testing and demonstration**, not production-scale automation.

### In production:

* MinIO → AWS S3
* CSV → Parquet
* Prefect orchestration → Cloud-hosted or CI/CD triggered
* Code modularized into microservices for scalability and monitoring