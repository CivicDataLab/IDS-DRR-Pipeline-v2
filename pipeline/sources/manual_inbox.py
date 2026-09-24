"""Manual drop-folder ("inbox") handling for sources without automated feeds.

Some sources (NRSC runoff, HPSDMA losses, tender exports) arrive as CSVs
prepared by humans. They are dropped into ``inbox/{state}/{source}/`` named
``{variable}_{YYYY_MM}.csv`` (or ``{variable}_{YYYY}.csv`` for annual data).
``promote`` validates a file, writes it to the staging variables contract,
and moves the original to ``inbox/.../processed/``. Invalid files stay in
place and raise, so the failure is visible in Dagster.
"""

import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipeline.sources import staging_layout

MONTHLY_RE = re.compile(r"^(?P<variable>.+)_(?P<timeperiod>\d{4}_\d{2})\.csv$")
ANNUAL_RE = re.compile(r"^(?P<variable>.+)_(?P<timeperiod>\d{4})\.csv$")

# Minimum fraction of rows whose object_id must join the state's admin
# boundaries for a file to be accepted.
MIN_JOIN_RATE = 0.9


@dataclass
class InboxSchema:
    """Validation rules for one manual source."""

    cadence: str = "monthly"  # "monthly" or "annual"
    # Regex the variable name (from the filename) must match; None = any.
    variable_pattern: str | None = None
    # Value columns required in the CSV. When None, the variable name
    # itself must be a column (the `<var>_<YYYY_MM>.csv` convention).
    required_columns: list[str] | None = None
    # Variables joined at district level (object_id like "02-030")
    district_level_variables: tuple[str, ...] = ()


INBOX_SCHEMAS: dict[str, InboxSchema] = {
    "nrsc": InboxSchema(
        variable_pattern=r"^runoff$",
        required_columns=["Mean_Daily_Runoff", "Sum_Runoff", "Peak_Runoff"],
    ),
    "tenders": InboxSchema(
        variable_pattern=r"^(total_tender_awarded_value|.+_tenders_awarded_value)$",
    ),
    "hpsdma": InboxSchema(
        district_level_variables=("relief_and_mitigation_sanction_value",),
    ),
    "worldpop": InboxSchema(
        cadence="annual",
        variable_pattern=(
            r"^(mean_sex_ratio|sum_aged_population|sum_young_population|sum_population)$"
        ),
    ),
}


def parse_inbox_filename(source: str, filename: str) -> tuple[str, str]:
    """Return ``(variable, timeperiod)`` from an inbox filename or raise."""
    schema = INBOX_SCHEMAS.get(source)
    if schema is None:
        raise ValueError(f"Unknown inbox source '{source}' (known: {sorted(INBOX_SCHEMAS)})")

    pattern = ANNUAL_RE if schema.cadence == "annual" else MONTHLY_RE
    match = pattern.match(filename)
    if not match:
        expected = "{variable}_YYYY.csv" if schema.cadence == "annual" else "{variable}_YYYY_MM.csv"
        raise ValueError(f"{source}: '{filename}' does not match the {expected} convention")

    variable = match.group("variable")
    if schema.variable_pattern and not re.match(schema.variable_pattern, variable):
        raise ValueError(
            f"{source}: variable '{variable}' not accepted (pattern: {schema.variable_pattern})"
        )
    return variable, match.group("timeperiod")


def scan_inbox(state: str, source: str) -> list[Path]:
    """Pending CSVs in a state/source inbox (processed/ excluded)."""
    inbox = staging_layout.inbox_dir(state, source)
    if not inbox.exists():
        return []
    return sorted(p for p in inbox.glob("*.csv") if p.is_file())


def scan_all_inboxes(state: str) -> dict[str, list[Path]]:
    """All pending files for a state, keyed by source."""
    return {source: files for source in INBOX_SCHEMAS if (files := scan_inbox(state, source))}


def _join_rate(ids: pd.Series, boundary_ids: set[str], district_level: bool) -> float:
    ids = ids.dropna().astype(str)
    if ids.empty:
        return 0.0
    if district_level:
        boundary_ids = {oid.rsplit("-", 1)[0] for oid in boundary_ids}
    return ids.isin(boundary_ids).mean()


def promote(
    state: str,
    source: str,
    path: Path,
    boundary_ids: set[str] | None = None,
) -> Path:
    """Validate one inbox CSV, write it to staging, move it to processed/.

    ``boundary_ids`` is the set of valid object_ids from the state's admin
    boundaries; when provided, files whose join rate falls below
    ``MIN_JOIN_RATE`` are rejected.

    Returns the staging path written. Raises ``ValueError`` on any
    validation failure, leaving the file where it is.
    """
    path = Path(path)
    schema = INBOX_SCHEMAS.get(source)
    if schema is None:
        raise ValueError(f"Unknown inbox source '{source}'")

    variable, timeperiod = parse_inbox_filename(source, path.name)

    df = pd.read_csv(path)
    if "object_id" not in df.columns:
        raise ValueError(f"{path.name}: missing required column 'object_id'")

    value_columns = schema.required_columns or [variable]
    missing = [c for c in value_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: missing value columns {missing}")

    district_level = variable in schema.district_level_variables
    if boundary_ids:
        rate = _join_rate(df["object_id"], boundary_ids, district_level)
        if rate < MIN_JOIN_RATE:
            raise ValueError(
                f"{path.name}: only {rate:.0%} of object_ids join the "
                f"{state} admin boundaries (minimum {MIN_JOIN_RATE:.0%})"
            )

    staged = staging_layout.write_variable_csv(
        df, state, source, variable, timeperiod, value_columns=value_columns
    )

    processed_dir = path.parent / "processed"
    processed_dir.mkdir(exist_ok=True)
    shutil.move(str(path), processed_dir / path.name)
    return staged


def inbox_cursor(state_files: dict[str, list[Path]]) -> str:
    """Stable JSON cursor of pending files (name + mtime) for sensors."""
    snapshot = {
        source: {p.name: p.stat().st_mtime for p in files} for source, files in state_files.items()
    }
    return json.dumps(snapshot, sort_keys=True)
