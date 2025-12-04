from src.utils.snowflake.client import SnowflakeClient
from src.utils.snowflake.operations import _EnvOps, _RawOps, _StagingOps, _PipeOps, _CuratedOps


class SnowflakePipeline:
    """High-level Snowflake ETL orchestrator composed of modular operation classes."""

    def __init__(self, config, pipeline_cfg=None):
        self.client = SnowflakeClient()
        self.env = _EnvOps(self.client, config, pipeline_cfg)
        self.raw = _RawOps(self.client, config, pipeline_cfg)
        self.stage = _StagingOps(self.client, config, pipeline_cfg)
        self.pipe = _PipeOps(self.client, config, pipeline_cfg)
        self.curated = _CuratedOps(self.client, config, pipeline_cfg)
        self.config = config

    # ------------------------------------------------------------------
    # Environment and staging
    # ------------------------------------------------------------------

    def setup_environment(self):
        """Provision all required databases, schemas, and file formats."""
        self.env.setup_environment()

    def stage_files(self, local_dir: str):
        """Upload local files into the Snowflake stage."""
        self.env.stage_files(local_dir)

    # ------------------------------------------------------------------
    # RAW and STAGING layer orchestration
    # ------------------------------------------------------------------

    def build_raw(self):
        """Infer schema, create, and evolve the RAW layer."""
        self.raw.create_inferred_table()

    def build_staging(self):
        """Recreate and merge the STAGING layer with deduplication and evolution."""
        self.stage.create()
        self.stage.evolve()
        self.stage.merge()

    # ------------------------------------------------------------------
    # Snowpipe operations
    # ------------------------------------------------------------------

    def create_pipe(self):
        """Create or replace Snowpipe for automated ingestion."""
        self.pipe.create()

    def trigger_pipe(self):
        """Trigger Snowpipe ingestion and wait until ingestion completes."""
        self.pipe.trigger()

    # ------------------------------------------------------------------
    # CURATED layer
    # ------------------------------------------------------------------

    def build_curated(self):
        """Generate curated subsets or secure views from the STAGING layer."""
        self.curated.create_subsets()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self):
        """Close underlying Snowflake connection."""
        self.client.close()
