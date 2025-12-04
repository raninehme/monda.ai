from prefect import flow, get_run_logger, serve
from prefect.task_runners import ThreadPoolTaskRunner

from src.utils.helpers import discover_configs, load_configs
from src.utils.pipeline_tasks import (
    extract_from_minio,
    stage_files,
    create_pipe,
    trigger_pipe,
    merge_to_staging,
)


@flow(name="trigger_pipeline", task_runner=ThreadPoolTaskRunner(max_workers=3))
def trigger_pipelines(config_paths: list[str]):
    """
    Refresh Snowflake ingestion and rebuild STAGING + CURATED layers.

    Steps:
      1. Extract latest files from MinIO
      2. Restage files into Snowflake
      3. Recreate and trigger Snowpipe ingestion (RAW)
      4. Merge into STAGING layer (includes CURATED subsets if configured)
    """
    logger = get_run_logger()
    configs = load_configs(config_paths)

    for cfg in configs:
        for pipeline_cfg in cfg.get("pipelines", []):
            name = pipeline_cfg["namespace"]
            logger.info(f"Triggering pipeline refresh: {name}")

            local_dir = extract_from_minio(cfg, pipeline_cfg)
            stage_files(cfg, pipeline_cfg, local_dir)
            create_pipe(cfg, pipeline_cfg)
            trigger_pipe(cfg, pipeline_cfg)
            merge_to_staging(cfg, pipeline_cfg)

    logger.info("All pipelines refreshed, STAGING merged, and CURATED subsets created successfully.")


if __name__ == "__main__":
    config_files = discover_configs()
    print(f"Serving Prefect flow (trigger mode) with configuration(s): {config_files}")

    serve(
        trigger_pipelines.to_deployment(
            name="trigger_pipeline",
            tags=["monda", "demo", "refresh"],
            parameters={"config_paths": config_files},
        )
    )
