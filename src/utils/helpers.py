import os
import glob
import yaml
from pathlib import Path
from typing import List
from jinja2 import Environment, FileSystemLoader

# ---------------------------------------------------------------------
# CONFIG HELPERS
# ---------------------------------------------------------------------

def discover_configs(config_dir: str = "config") -> List[str]:
    """Find all YAML pipeline configs in a directory."""
    configs = sorted(glob.glob(os.path.join(config_dir, "*.yaml")))
    if not configs:
        raise FileNotFoundError(f"No YAML configs found in '{config_dir}/'")
    return configs


def load_configs(config_paths: List[str]) -> List[dict]:
    """Load and parse all YAML configuration files."""
    configs = []
    for path in config_paths:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, "r") as f:
            configs.append(yaml.safe_load(f))
    return configs


# ---------------------------------------------------------------------
# JINJA RENDERING HELPER
# ---------------------------------------------------------------------

def render_template(template_name: str, context: dict, base_dir: str | None = None) -> str:
    """
    Render a Jinja2 SQL template with context variables.

    Args:
        template_name: Name of the SQL file (e.g., 'create_stage.sql')
        context: Dictionary of template variables
        base_dir: Optional base directory for templates (defaults to src/utils/snowflake/sql)
    """
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent / "snowflake" / "sql"

    env = Environment(loader=FileSystemLoader(str(base_dir)))

    template = env.get_template(template_name)
    return template.render(**context)
