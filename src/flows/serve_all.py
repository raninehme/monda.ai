import logging
from prefect import serve
from src.flows.create_pipeline import create_pipelines
from src.flows.trigger_pipeline import trigger_pipelines
from src.utils.helpers import discover_configs

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config_files = discover_configs()

    create_deploy = create_pipelines.to_deployment(
        name="create_pipeline",
        tags=["monda", "demo"],
        parameters={"config_paths": config_files},
    )

    trigger_deploy = trigger_pipelines.to_deployment(
        name="trigger_pipeline",
        tags=["monda", "demo", "refresh"],
        parameters={"config_paths": config_files},
    )

    serve(create_deploy, trigger_deploy)
