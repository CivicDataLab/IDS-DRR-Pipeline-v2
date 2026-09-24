#!/usr/bin/env python3
"""Seed reference data, historical staging CSVs, and golden test fixtures.

Nothing under ``data/reference/`` or ``staging/`` is committed to this
repository. This script materialises all of it from the public
flood-data-ecosystem and risk-score-model repositories, pinned to known
revisions:

    python scripts/seed_data.py                # everything
    python scripts/seed_data.py --only reference
    python scripts/seed_data.py --only seed    # historical staging CSVs
    python scripts/seed_data.py --only golden  # parity-test fixtures

The historical staging CSVs are derived from the ecosystem repo's
committed ``Sources/master/*.csv`` panels (one per variable, all months),
split into the per-month contract described in
``pipeline/sources/staging_layout.py``. Re-running is idempotent.

Use ``--eco-dir`` / ``--model-dir`` to point at existing clones instead of
cloning into ``.seed_cache/``.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline.sources import staging_layout  # noqa: E402

STATE = "himachal_pradesh"

ECO_URL = "https://github.com/CivicDataLab/flood-data-ecosystem-Himachal-Pradesh.git"
ECO_SHA = "404f0ee6bdbd00ccb8d41e9f0b9b633a48f589df"
MODEL_URL = "https://github.com/CivicDataLab/IDS-DRR-Himachal-Pradesh-Risk-Score-Model.git"
MODEL_SHA = "6fb04fa18c55a124fa7e73a8438adc1f383a3054"

CACHE_DIR = REPO_ROOT / ".seed_cache"

# Monthly panels in eco ``Sources/master/`` -> staging variables contract.
# Each entry: (master csv stem, staging source, staging variable, value columns)
MONTHLY_PANELS = [
    ("rainfall", "imd", "mean_rain", ["mean_rain"]),
    ("rainfall", "imd", "sum_rain", ["sum_rain"]),
    ("rainfall", "imd", "max_rain", ["max_rain"]),
    ("runoff", "nrsc", "runoff", ["Mean_Daily_Runoff", "Sum_Runoff", "Peak_Runoff"]),
    (
        "inundation",
        "bhuvan",
        "inundation_pct",
        [
            "inundation_pct",
            "inundation_intensity_mean",
            "inundation_intensity_mean_nonzero",
            "inundation_intensity_sum",
        ],
    ),
]
TENDER_VARIABLES = [
    "total_tender_awarded_value",
    "Repair and Restoration_tenders_awarded_value",
    "LWSS_tenders_awarded_value",
    "NDRF_tenders_awarded_value",
    "SDMF_tenders_awarded_value",
    "WSS_tenders_awarded_value",
    "Preparedness Measures_tenders_awarded_value",
    "Immediate Measures_tenders_awarded_value",
    "Others_tenders_awarded_value",
]
HPSDMA_VARIABLES = [
    "structure_lost",
    "health_centres_lost",
    "health_amount",
    "internalwatersupply",
    "electricwires",
    "Electricpoles",
    "Roadlength",
    "streetlights",
    "person_dead",
    "person_major_injury",
    "schools_damaged",
    "economic_loss",
    "total_livestock_loss",
    "relief_and_mitigation_sanction_value",
]
ANNUAL_VARIABLES = [
    "mean_sex_ratio",
    "sum_aged_population",
    "sum_young_population",
    "sum_population",
]
ONETIME_VARIABLES = [
    "Schools",
    "RailLengths",
    "RoadLengths",
    "slope_elevation",
    "antyodaya_variables",
    "drainage_density",
    "distance_from_river",
    "rural_vul",
]


def clone_pinned(url: str, sha: str, dest: Path) -> Path:
    if (dest / ".git").exists():
        head = subprocess.run(
            ["git", "-C", str(dest), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if head == sha:
            print(f"  reusing {dest} @ {sha[:9]}")
            return dest
        subprocess.run(["git", "-C", str(dest), "fetch", "origin", sha], check=True)
        subprocess.run(["git", "-C", str(dest), "checkout", sha], check=True)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  cloning {url} -> {dest}")
    subprocess.run(["git", "clone", "--filter=blob:none", url, str(dest)], check=True)
    subprocess.run(["git", "-C", str(dest), "checkout", sha], check=True)
    return dest


def copy(src: Path, dest: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"expected source file missing: {src}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    print(f"  {dest.relative_to(REPO_ROOT)}")


def _read_panel(master_dir: Path, stem: str) -> pd.DataFrame:
    df = pd.read_csv(master_dir / f"{stem}.csv", dtype={"timeperiod": str})
    if "object_id" not in df.columns or "timeperiod" not in df.columns:
        raise ValueError(f"{stem}.csv: expected object_id + timeperiod columns")
    return df


def _split_panel(
    df: pd.DataFrame, source: str, variable: str, value_columns: list[str]
) -> int:
    """Split one all-months panel into per-timeperiod contract CSVs."""
    df = df.dropna(subset=["object_id", "timeperiod"])
    df = df[df["timeperiod"].astype(str).str.len() > 0]
    written = 0
    for timeperiod, group in df.groupby("timeperiod"):
        timeperiod = str(timeperiod).split(".")[0]  # annual panels parse as floats
        group = group.drop_duplicates(subset=["object_id"])
        staging_layout.write_variable_csv(
            group, STATE, source, variable, timeperiod, value_columns=value_columns
        )
        written += 1
    print(f"  staging/variables/{STATE}/{source}/{variable}: {written} timeperiods")
    return written


def seed_reference(eco: Path, model: Path) -> None:
    print("Reference data:")
    ref = staging_layout.reference_dir(STATE)

    copy(eco / "Maps" / "hp_tehsil_final.geojson", ref / "boundaries" / "hp_tehsil_final.geojson")
    copy(eco / "Maps" / "hp_district_final.geojson", ref / "boundaries" / "hp_district_final.geojson")
    copy(eco / "Maps" / "HP_VILLAGES.csv", ref / "gazetteer" / "HP_VILLAGES.csv")
    copy(
        model / "RiskScoreModel" / "assets" / "district_objectid.csv",
        ref / "district_objectid.csv",
    )
    for name in ONETIME_VARIABLES:
        copy(eco / "Sources" / "master" / f"{name}.csv", ref / "variables" / f"{name}.csv")


def seed_staging(eco: Path) -> None:
    print("Historical staging variables:")
    master_dir = eco / "Sources" / "master"

    for stem, source, variable, cols in MONTHLY_PANELS:
        _split_panel(_read_panel(master_dir, stem), source, variable, cols)

    for variable in TENDER_VARIABLES:
        _split_panel(_read_panel(master_dir, variable), "tenders", variable, [variable])

    for variable in HPSDMA_VARIABLES:
        _split_panel(_read_panel(master_dir, variable), "hpsdma", variable, [variable])

    for variable in ANNUAL_VARIABLES:
        _split_panel(_read_panel(master_dir, variable), "worldpop", variable, [variable])


def seed_golden(eco: Path, model: Path) -> None:
    print("Golden fixtures:")
    golden = staging_layout.reference_dir(STATE) / "golden"
    copy(eco / "Sources" / "MASTER_VARIABLES.csv", golden / "MASTER_VARIABLES.csv")
    data = model / "RiskScoreModel" / "data"
    for name in [
        "risk_score.csv",
        "risk_score_final_district.csv",
        "factor_scores_l1_exposure.csv",
        "factor_scores_l1_flood-hazard.csv",
        "factor_scores_l1_government-response.csv",
        "factor_scores_l1_vulnerability.csv",
    ]:
        copy(data / name, golden / name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=["reference", "seed", "golden"], default=None)
    parser.add_argument("--eco-dir", type=Path, default=None, help="existing ecosystem clone")
    parser.add_argument("--model-dir", type=Path, default=None, help="existing model clone")
    args = parser.parse_args()

    print("Source repositories:")
    eco = args.eco_dir or clone_pinned(ECO_URL, ECO_SHA, CACHE_DIR / "ecosystem-hp")
    model = args.model_dir or clone_pinned(MODEL_URL, MODEL_SHA, CACHE_DIR / "risk-model-hp")

    if args.only in (None, "reference"):
        seed_reference(eco, model)
    if args.only in (None, "seed"):
        seed_staging(eco)
    if args.only in (None, "golden"):
        seed_golden(eco, model)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
