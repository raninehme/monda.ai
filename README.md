Perfect — here’s that section rewritten cleanly and integrated into the README while keeping formatting consistent and concise.

---

### **Updated Section**

```markdown
**Note:**  
The project uses the **ACCOUNTADMIN** role in Snowflake to simplify ACL and privilege management during local execution.  
All required databases, schemas, and file formats are created automatically as part of the flow.
```

---

And here’s the **final version of the full README** with that adjustment applied in context:

---

```markdown
# **Monda — Mini Snowflake ETL**

A compact, reproducible local ETL pipeline built with **Prefect**, **Snowflake**, and **MinIO**.  
It simulates a modern lakehouse ingestion flow:

> **MinIO (object storage)** → **Snowflake stage + pipe** → **schema inference & evolution** → **STAGING merge**

All logic and parameters are defined through YAML configuration — allowing one or many pipelines to run from a single definition.

---

## **Project Structure**

```

monda/
├── Makefile, docker-compose.yaml, .env      # Infra, orchestration, credentials
├── config/pipeline.yaml                     # Global + pipeline-specific YAML config
├── sample_data/user_events/                 # Example CSVs for ingestion
└── src/
├── flows/                               # Prefect entrypoints
│   ├── create_pipeline.py               # Full setup + initial ingestion
│   ├── trigger_pipeline.py              # Incremental / refresh ingestion
│   └── serve_all.py                     # Serves both flows
└── utils/                               # Shared logic and utilities
├── helpers.py, minio_client.py, pipeline_tasks.py
└── snowflake/ (client.py, pipeline.py, operations.py, sql/)

````

---

## **1) Prerequisites**

* **Docker**, **Docker Compose**, and **Make**
* A valid **Snowflake account**
* Update your `.env` file:

  ```bash
  SNOWFLAKE_ACCOUNT=<your_account>.snowflakecomputing.com
  SNOWFLAKE_USER=<your_user>
  SNOWFLAKE_PASSWORD=<your_password>
  SNOWFLAKE_WAREHOUSE=<warehouse>
````

---

## **2) Behavior Overview**

* Pipelines are fully **config-driven** (global + dataset scope).

* The ETL flow uses **Snowflake-native primitives** for ingestion and evolution:

  * Files are idempotently staged and loaded.
  * **Schema inference** with `INFER_SCHEMA` + `USING TEMPLATE`.
  * **Dynamic schema evolution** adds missing columns only.
  * **Flatten columns** expand nested JSON into relational fields.
  * **Snowpipe** with explicit `REFRESH` and ingestion completion polling.

* SQL logic is modularized with **Jinja templates**, ensuring deterministic DDL/DML generation.

* Prefect orchestrates the lifecycle:

  * `create_pipeline` → full bootstrap + first load
  * `trigger_pipeline` → incremental refresh (new data or schema change)

---

**Note:**
The project uses the **ACCOUNTADMIN** role in Snowflake to simplify ACL and privilege management during local execution.
All required databases, schemas, and file formats are created automatically as part of the flow.

---

## **3) Quick Start**

### **Step 1 — Start the stack**

```bash
make up
```

Starts local **Prefect** and **MinIO** services:

* Prefect UI → [http://localhost:4200](http://localhost:4200)
* MinIO Console → [http://localhost:9001](http://localhost:9001)

Data persists across restarts.

---

### **Step 2 — Create bucket and path**

1. Open MinIO console.
2. Create bucket: **`raw`**
3. Inside it, create the path:

   ```
   user_activity/user_events/
   ```

---

### **Step 3 — Upload and Run**

1. Upload CSVs from:

   ```
   sample_data/user_events/
   ```
2. Run flows from the Prefect UI:

   * **`create_pipeline`** → first ingestion (`events_1.csv`)
   * **`trigger_pipeline`** → incremental / schema evolution (`events_2.csv`, `events_3.csv`)

*(The sample folder is demo-only — can be safely removed later.)*

Each run will:

* Pull files from MinIO
* Stage them in Snowflake
* Infer and evolve schema dynamically
* Wait for Snowpipe ingestion
* Merge deduplicated data into STAGING

All runs are **idempotent** — re-execution produces consistent results without duplication.

---

## **4) Limitations**

* Uses **internal Snowflake stage** (no native S3 eventing from MinIO).
* File upload is manual (`PUT` command).
* **Snowpipe auto-ingest** is simulated via polling.
* Designed for **local simulation**, not production orchestration.

### In production:

* MinIO → AWS S3
* CSV → Parquet
* Prefect orchestration → Cloud-hosted / CI trigger
* Code split into microservices for modular deployment

---

This README now precisely matches your implemented system — compact, technical, and complete.

```
```
