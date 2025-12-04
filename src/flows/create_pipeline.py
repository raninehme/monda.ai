from prefect import flow, get_run_logger, serve

from src.utils.helpers import discover_configs, load_configs
from src.utils.pipeline_tasks import (
    setup_environment,
    prepare_schemas,
    extract_from_minio,
    copy_to_snowflake,
    merge_to_staging,
)


@flow(name="create_pipeline")
def create_pipelines(config_paths: list[str]):
    """
    Full Snowflake ETL bootstrap flow.

    Steps:
      1. Setup environment (databases, utils, file formats)
      2. Prepare schemas (RAW + STAGING)
      3. Extract data from MinIO
      4. Copy into RAW layer (stage → infer → pipe → trigger)
      5. Merge into STAGING layer (create → evolve → merge → curated)
    """
    logger = get_run_logger()
    configs = load_configs(config_paths)

    for cfg in configs:
        setup_environment(cfg)

        for pipeline_cfg in cfg.get("pipelines", []):
            name = pipeline_cfg["namespace"]
            logger.info(f"Starting pipeline: {name}")

            prepare_schemas(cfg, pipeline_cfg)
            local_dir = extract_from_minio(cfg, pipeline_cfg)
            copy_to_snowflake(cfg, pipeline_cfg, local_dir)
            merge_to_staging(cfg, pipeline_cfg)

    logger.info("All pipelines created successfully.")


if __name__ == "__main__":
    config_files = discover_configs()
    print(f"Serving Prefect flow with configuration(s): {config_files}")

    serve(
        create_pipelines.to_deployment(
            name="create_pipeline",
            tags=["monda", "demo"],
            parameters={"config_paths": config_files},
        )
    )
