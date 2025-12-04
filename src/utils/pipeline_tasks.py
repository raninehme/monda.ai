import os
import tempfile
from prefect import task, get_run_logger
from src.utils.minio_client import MinioClient
from src.utils.snowflake.pipeline import SnowflakePipeline


# ---------------------------------------------------------------------
# ENVIRONMENT SETUP
# ---------------------------------------------------------------------

@task
def setup_environment(cfg: dict):
    """Ensure all Snowflake databases, schemas, and file formats exist."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg)
    try:
        sf.setup_environment()
        logger.info("Snowflake environment setup completed successfully.")
    finally:
        sf.close()


# ---------------------------------------------------------------------
# EXTRACTION
# ---------------------------------------------------------------------

@task
def extract_from_minio(cfg: dict, pipeline_cfg: dict) -> str:
    """Download source files from MinIO to a temporary directory."""
    logger = get_run_logger()
    minio = MinioClient(cfg)

    prefix = pipeline_cfg["bucket_path"].lower()
    tmp_dir = tempfile.mkdtemp(prefix=f"minio_{pipeline_cfg['namespace'].lower()}_")

    minio.ensure_bucket()
    objects = minio.list_objects(prefix=prefix)
    logger.info(f"Downloading {len(objects)} object(s) from '{prefix}'...")

    for obj in objects:
        local_path = os.path.join(tmp_dir, os.path.basename(obj))
        minio.download(obj, local_path)

    logger.info(f"All files downloaded to {tmp_dir}")
    return tmp_dir


# ---------------------------------------------------------------------
# SNOWFLAKE PIPELINE ACTIONS (DECOUPLED)
# ---------------------------------------------------------------------

@task
def stage_files(cfg: dict, pipeline_cfg: dict, local_dir: str):
    """Upload local CSVs into Snowflake stage."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        sf.stage_files(local_dir)
        logger.info(f"Files staged for {sf.raw.raw_db}.{sf.stage.schema}.{sf.stage.table}.")
    finally:
        sf.close()


@task
def create_raw_table(cfg: dict, pipeline_cfg: dict):
    """Create or evolve RAW layer table via schema inference."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        sf.build_raw()
        logger.info(f"RAW table ensured: {sf.raw.raw_db}.{sf.stage.schema}.{sf.stage.table}.")
    finally:
        sf.close()


@task
def create_staging_table(cfg: dict, pipeline_cfg: dict):
    """Create or alter the STAGING table and perform merge."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        sf.build_staging()
        logger.info(f"STAGING table ensured for {sf.stage.schema}.{sf.stage.table}.")
    finally:
        sf.close()


@task
def create_pipe(cfg: dict, pipeline_cfg: dict):
    """Create or replace the Snowpipe definition."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        sf.create_pipe()
        logger.info(f"Snowpipe ensured for {sf.raw.raw_db}.{sf.stage.schema}.{sf.stage.table}.")
    finally:
        sf.close()

@task
def trigger_pipe(cfg: dict, pipeline_cfg: dict):
    """Trigger Snowpipe ingestion for the given dataset."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        sf.trigger_pipe()
        logger.info(
            f"Snowpipe triggered for {sf.raw.raw_db}.{sf.stage.schema}.{sf.stage.table}."
        )
    finally:
        sf.close()


# ---------------------------------------------------------------------
# COMPOSED TASKS
# ---------------------------------------------------------------------

@task
def copy_to_snowflake(cfg: dict, pipeline_cfg: dict, local_dir: str):
    """Full RAW ingestion sequence: stage → create RAW table → create pipe → trigger."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        sf.stage_files(local_dir)
        sf.build_raw()
        sf.create_pipe()
        sf.trigger_pipe()
        sf.build_staging()
        logger.info(
            f"RAW ingestion completed for {sf.raw.raw_db}.{sf.stage.schema}.{sf.stage.table}."
        )
    finally:
        sf.close()


@task
def merge_to_staging(cfg: dict, pipeline_cfg: dict):
    """Execute STAGING layer creation + merge (deduped incremental)."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        sf.build_staging()
        logger.info(
            f"STAGING merge completed for {sf.stage.schema}.{sf.stage.table}."
        )

        sf.build_curated()
        logger.info(f"CURATED subsets created for {sf.stage.schema}.{sf.stage.table}.")
    finally:
        sf.close()


@task
def prepare_schemas(cfg: dict, pipeline_cfg: dict):
    """Ensure schemas exist across all configured databases (RAW, STAGING, CURATED)."""
    logger = get_run_logger()
    sf = SnowflakePipeline(cfg, pipeline_cfg)
    try:
        databases = cfg["global"].get("databases", {})
        schema = pipeline_cfg.get("schema")

        for layer, db in databases.items():
            sf.client.create_schema(db, schema)
            logger.info(f"Schema ensured: {db}.{schema} (layer: {layer})")

        logger.info(f"All schemas ensured for dataset '{pipeline_cfg['namespace']}'.")
    finally:
        sf.close()
