import os
import snowflake.connector
from src.utils.helpers import render_template


class SnowflakeClient:
    """Lightweight Snowflake connector and SQL executor."""

    def __init__(self):
        """Initialize Snowflake connection from environment variables."""
        self.conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        )

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def execute(self, sql: str):
        """Execute a SQL command and return results if available."""
        sql = " ".join(sql.strip().split())
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall() if cur.description else None
        finally:
            cur.close()

    def close(self):
        """Close the connection safely."""
        try:
            self.conn.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Object management
    # ------------------------------------------------------------------

    def create_database(self, name: str):
        """Create a database if it does not exist."""
        self.execute(f"CREATE DATABASE IF NOT EXISTS {name};")

    def create_schema(self, db: str, schema: str):
        """Create a schema if it does not exist."""
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema};")

    def create_file_format(self, db: str, schema: str, name: str):
        """Create a file format using a Jinja SQL template."""
        sql = render_template(
            "create_file_format.sql",
            {"database": db, "schema": schema, "name": name},
        )
        self.execute(sql)

    def create_stage(self, db: str, schema: str, stage: str, file_format_ref: str):
        """Create a stage using a Jinja SQL template."""
        sql = render_template(
            "create_stage.sql",
            {
                "database": db,
                "schema": schema,
                "stage": stage,
                "file_format_ref": file_format_ref,
            },
        )
        self.execute(sql)
