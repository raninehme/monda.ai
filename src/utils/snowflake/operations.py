import os
import glob
import time
import json
from src.utils.helpers import render_template


# =============================================================================
# BASE
# =============================================================================

class _BaseOps:
    """Shared Snowflake helpers for rendering, metadata, and config access."""

    def __init__(self, client, config, pipeline_cfg):
        self.client = client
        self.config = config
        self.pipeline_cfg = pipeline_cfg or {}

        global_cfg = config["global"]
        self.utils_db = global_cfg["utils_database"]
        self.utils_schema = global_cfg["utils_schema"]
        self.file_format = global_cfg["file_format"]
        self.system_columns = global_cfg.get("system_columns", [])
        self.raw_db = global_cfg["databases"]["raw"]
        self.staging_db = global_cfg["databases"]["staging"]
        self.curated_db = global_cfg["databases"].get("curated", "CURATED")

        self.schema = self.pipeline_cfg.get("schema")
        self.table = self.pipeline_cfg.get("namespace")
        self.stage = self.table
        self.path = self.pipeline_cfg.get("bucket_path", "").rstrip("/").lower()
        self.max_files = self.pipeline_cfg.get("max_file_count", 5)

    def _get_columns(self, database, schema, table):
        """Retrieve column metadata from INFORMATION_SCHEMA."""
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION;
        """
        print(f"[TRACE] _get_columns() → {database}.{schema}.{table}")
        print("[TRACE] SQL:\n", sql)

        try:
            rows = self.client.execute(sql)
            print(f"[TRACE] Returned {len(rows) if rows else 0} rows")
            if rows:
                print("[TRACE] Sample:", rows[:3])
            return {r[0].upper(): r[1].upper() for r in rows}
        except Exception as e:
            print(f"[TRACE-ERROR] _get_columns() failed: {e}")
            raise

    def _render(self, template_name: str, context: dict) -> str:
        """Render Jinja SQL template."""
        return render_template(template_name, context).strip()


# =============================================================================
# ENVIRONMENT / STAGE MANAGEMENT
# =============================================================================

class _EnvOps(_BaseOps):
    def setup_environment(self):
        """Create databases, schemas, and file formats if missing."""
        g = self.config["global"]

        self.client.create_database(g["utils_database"])
        self.client.create_schema(g["utils_database"], g["utils_schema"])
        self.client.create_file_format(g["utils_database"], g["utils_schema"], g["file_format"])

        for db in g.get("databases", {}).values():
            self.client.create_database(db)

    def stage_files(self, local_dir: str):
        """Upload local files to a Snowflake internal stage."""
        file_format_ref = f"{self.utils_db}.{self.utils_schema}.{self.file_format}"
        self.client.create_stage(self.raw_db, self.schema, self.stage, file_format_ref)

        stage_path = f"@{self.raw_db}.{self.schema}.{self.stage}/{self.path}/"
        for file_path in glob.glob(os.path.join(local_dir, "*.csv")):
            file_name = os.path.basename(file_path)
            self.client.execute(
                f"PUT file://{file_path} {stage_path}{file_name} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
            )


# =============================================================================
# RAW LAYER
# =============================================================================

class _RawOps(_BaseOps):
    def create_inferred_table(self):
        """Infer schema from staged files and create or evolve RAW table."""
        file_format_ref = f"{self.utils_db}.{self.utils_schema}.{self.file_format}"

        sql = self._render(
            "create_inferred_table.sql",
            {
                "database": self.raw_db,
                "schema": self.schema,
                "table": self.table,
                "stage": self.stage,
                "file_format_ref": file_format_ref,
                "path": self.path,
                "max_files": self.max_files,
                "column_overrides": self.pipeline_cfg.get("column_overrides", {}),
            },
        )
        self.client.execute(sql)

        # Add housekeeping columns if missing
        for column in self.system_columns:
            self.client.execute(
                f"ALTER TABLE {self.raw_db}.{self.schema}.{self.table} "
                f"ADD COLUMN IF NOT EXISTS {column['name']} {column['type']};"
            )


# =============================================================================
# STAGING LAYER
# =============================================================================

class _StagingOps(_BaseOps):
    def create(self):
        """Create STAGING table based on RAW structure with JSON flatten support."""
        raw_cols = self._get_columns(self.raw_db, self.schema, self.table)
        staging_cfg = self.pipeline_cfg.get("staging", {})
        exclude = staging_cfg.get("exclude_columns", [])
        flatten_columns = staging_cfg.get("flatten_columns", [])

        sql = self._render(
            "create_staging_table.sql",
            {
                "raw_db": self.raw_db,
                "staging_db": self.staging_db,
                "schema": self.schema,
                "table": self.table,
                "all_columns": [c for c in raw_cols.keys() if c not in exclude],
                "exclude_columns": exclude,
                "flatten_columns": flatten_columns,
            },
        )

        print("[DEBUG] Rendered CREATE STAGING SQL:\n", sql)
        self.client.execute(sql)

    def evolve(self):
        """Evolve STAGING schema by adding newly discovered columns."""
        raw_cols = self._get_columns(self.raw_db, self.schema, self.table)
        staging_cols = self._get_columns(self.staging_db, self.schema, self.table)
        staging_cfg = self.pipeline_cfg.get("staging", {})
        exclude = staging_cfg.get("exclude_columns", [])

        new_columns = [
            {"name": n, "type": t}
            for n, t in raw_cols.items()
            if n not in staging_cols and n not in exclude
        ]

        if not new_columns:
            print(f"[DEBUG] No new columns to evolve for {self.schema}.{self.table}")
            return

        sql = self._render(
            "evolve_table_schema.sql",
            {
                "staging_db": self.staging_db,
                "schema": self.schema,
                "table": self.table,
                "new_columns": new_columns,
            },
        )
        print(f"[INFO] Evolving STAGING.{self.schema}.{self.table} with columns: {[c['name'] for c in new_columns]}")
        self.client.execute(sql)

    def merge(self):
        """Merge deduplicated data from RAW → STAGING, flattening JSON if configured."""
        cfg = self.pipeline_cfg.get("staging", {})
        exclude = cfg.get("exclude_columns", [])
        pk = cfg.get("primary_keys", [])
        sk = cfg.get("sort_key", ["__INGESTED_TIMESTAMP"])
        flatten_columns = cfg.get("flatten_columns", [])
        raw_cols = self._get_columns(self.raw_db, self.schema, self.table)

        flatten_fields = []
        for fc in flatten_columns:
            for field in fc.get("fields", []):
                alias = field.split("AS")[-1].strip() if "AS" in field else None
                if alias:
                    flatten_fields.append(alias)

        sql = self._render(
            "merge_into.sql",
            {
                "raw_db": self.raw_db,
                "staging_db": self.staging_db,
                "schema": self.schema,
                "table": self.table,
                "all_columns": list(raw_cols.keys()),
                "exclude_columns": exclude,
                "primary_keys": pk,
                "sort_keys": sk,
                "flatten_columns": flatten_columns,
                "flatten_fields": flatten_fields,
            },
        )

        print(f"[DEBUG] Rendered MERGE SQL for {self.schema}.{self.table}:\n", sql)
        self.client.execute(sql)


# =============================================================================
# PIPE MANAGEMENT
# =============================================================================

class _PipeOps(_BaseOps):
    def build_copy_query(self):
        """Construct COPY INTO statement with metadata columns."""
        file_format_ref = f"{self.utils_db}.{self.utils_schema}.{self.file_format}"
        sys_cols = self.config["global"].get("system_columns", [])
        include_meta = (
            ", ".join(f"{c['name']} = {c['expression']}" for c in sys_cols)
            if sys_cols else ""
        )

        return self._render(
            "copy_into.sql",
            {
                "database": self.raw_db,
                "schema": self.schema,
                "table": self.table,
                "stage": self.stage,
                "file_format_ref": file_format_ref,
                "include_metadata": include_meta,
            },
        )

    def create(self):
        """Create or replace Snowpipe definition."""
        copy_sql = self.build_copy_query().rstrip(";")
        sql = self._render(
            "create_pipe.sql",
            {
                "database": self.raw_db,
                "schema": self.schema,
                "table": self.table,
                "copy_sql": copy_sql,
            },
        )
        self.client.execute(sql)

    def trigger(self, delay: int = 3, max_wait: int = 120, settle_wait: int = 60):
        """Trigger Snowpipe ingestion, wait for completion, and allow metadata to settle."""
        pipe_name = f"{self.raw_db}.{self.schema}.{self.table}"
        self.client.execute(f"ALTER PIPE {pipe_name} REFRESH;")
        print(f"[INFO] Triggered Snowpipe refresh for {pipe_name}")

        self._wait_for_pipe(pipe_name, delay, max_wait)
        print(f"[INFO] Waiting {settle_wait}s for ingestion metadata to settle...")
        time.sleep(settle_wait)
        print(f"[INFO] Proceeding after metadata settle delay.")

    def _wait_for_pipe(self, pipe_name: str, delay: int = 3, max_wait: int = 30):
        """Poll SYSTEM$PIPE_STATUS until Snowpipe completes."""
        start_time = time.time()
        while time.time() - start_time < max_wait:
            raw_status = self.client.execute(f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')")[0][0]
            status = json.loads(raw_status)
            pending = int(status.get("pendingFileCount") or 0)
            state = status.get("executionState")
            last_path = status.get("lastIngestedFilePath")
            last_ts = status.get("lastIngestedTimestamp")

            print(
                f"[DEBUG] Pipe {pipe_name}: state={state}, pending={pending}, lastFile={last_path}, lastIngested={last_ts}")
            if pending == 0 and last_path:
                print(f"[INFO] Pipe {pipe_name} finished ingestion ({last_path})")
                return

            time.sleep(delay)

        print(f"[WARN] Pipe {pipe_name} did not finish within {max_wait}s.")


# =============================================================================
# CURATED LAYER
# =============================================================================

class _CuratedOps(_BaseOps):
    """Generate curated subset tables or secure views from STAGING layer."""

    def create_subsets(self):
        """Create filtered subsets or secure views in CURATED layer."""
        subsets = self.pipeline_cfg.get("subsets", [])
        if not subsets:
            print(f"[INFO] No subsets configured for {self.schema}.{self.table}")
            return

        for subset in subsets:
            name = subset["name"]
            filters = subset.get("filters", [])
            secure = subset.get("secure", False)

            where_clause = " AND ".join(filters) if filters else "1=1"
            object_type = "SECURE VIEW" if secure else "TABLE"

            sql = f"""
                CREATE OR REPLACE {object_type} {self.curated_db}.{self.schema}.{name} AS
                SELECT *
                FROM {self.staging_db}.{self.schema}.{self.table}
                WHERE {where_clause};
            """

            print(f"[INFO] Creating subset {object_type}: {self.curated_db}.{self.schema}.{name}")
            print(f"[DEBUG] SQL:\n{sql}")
            self.client.execute(sql)
