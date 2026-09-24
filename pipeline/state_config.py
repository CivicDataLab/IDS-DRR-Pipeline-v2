"""Shared loader for per-state YAML configuration.

Every asset module used to carry its own copy of ``_load_state_config``;
this is the single canonical implementation. State YAMLs live in
``pipeline/config/states/<state_id>.yaml``.
"""

from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parent / "config" / "states"

# Repository root — relative paths inside state YAMLs (boundaries,
# reference data) are resolved against this.
REPO_ROOT = Path(__file__).resolve().parent.parent


def load_state_config(state: str) -> dict:
    """Load the YAML configuration for a state; empty dict if missing."""
    config_path = CONFIG_DIR / f"{state}.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def enabled_states() -> list[str]:
    """States whose YAML sets ``enabled: true``."""
    states = []
    for path in sorted(CONFIG_DIR.glob("*.yaml")):
        with open(path) as f:
            cfg = yaml.safe_load(f) or {}
        if cfg.get("enabled"):
            states.append(cfg.get("state_id", path.stem))
    return states


def resolve_path(relative: str) -> Path:
    """Resolve a repo-relative path from a state YAML to an absolute Path."""
    p = Path(relative)
    return p if p.is_absolute() else REPO_ROOT / p


def boundaries_path(state: str) -> Path | None:
    """Path to the canonical admin-boundary GeoJSON for a state, if configured."""
    cfg = load_state_config(state)
    geojson = cfg.get("boundaries", {}).get("geojson")
    return resolve_path(geojson) if geojson else None
