# Follow-up work

Phases 0 and 1 (foundations, automated sources, and the end-to-end risk
chain for Himachal Pradesh) are implemented. This file tracks what is
deliberately left out.

## Phase 2 — remaining manual sources and operational hardening

### HPSDMA loss and damage transformers

Twelve loss-domain variables (`structure_lost`, `person_dead`,
`electricwires`, ...) plus the SDRF relief sanction values currently reach
staging only through the seed script's historical backfill. Ongoing months
need `pipeline/sources/sdma_losses.py`, porting the transformers from
`flood-data-ecosystem-Himachal-Pradesh/Sources/HPSDMA/scripts/` and
`Sources/SDRF/scripts/`, behind an `hpsdma` drop-in asset using the same
inbox flow as NRSC runoff. The upstream SDRF transformer still carries
leftover Assam paths that need parameterising.

`INBOX_SCHEMAS` in `pipeline/sources/manual_inbox.py` already has an
`hpsdma` entry (including the district-level handling for
`relief_and_mitigation_sanction_value`); it needs an asset and a
`_INBOX_ASSET_BY_SOURCE` entry in `pipeline/definitions.py` to become live.

### Asset checks

`master_variables` should carry `@dg.asset_check`s for row count, the share
of imputed values per column, and month-over-month drift in the headline
variables, so a silently empty source fails loudly instead of quietly
imputing zeros.

### Retry policies

Network-backed assets (`imd_monthly_rain_data`, `bhuvan_flood_maps`,
`ffs_river_levels`, `sentinel_indices`) should carry
`dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL)`.
IMD in particular publishes the previous month's grids with a lag, so the
1st-of-month run can legitimately find nothing.

### Monthly runbook

`docs/monthly_runbook.md`: which files to drop into which inbox folder and
by when, how to read the coverage metadata on `master_variables`, and how
to backfill a month that was missed.

## Phase 3 — automation and the second state

### Tender scraping

`hptenders.gov.in` is behind a captcha, so tenders are a drop-in source
(`data_sources.procurement.tenders.mode: manual`). Automating it needs
Selenium plus captcha handling, added as a `pip install .[scrapers]` extra
so the core package stays light. The upstream classifier also has
Windows-only paths and an Assam-specific exclusion list to fix.

If direct HPSDMA database access is granted, the pyodbc route would replace
the loss-and-damage drop-in entirely.

### Onboard Assam

The real test of the abstraction: a new `pipeline/config/states/assam.yaml`
with `enabled: true`, an `assam` entry in `scripts/seed_data.py`, and an
entry in `STATE_SOURCES` — with no new Python. Assam is the most mature
fork of the ecosystem, so anything that forces a code change there is a
gap in the state parameterisation worth fixing before other states follow.

## Known divergences from the published model

Both are covered by assertions in `tests/test_risk_model.py`.

1. **DEA solver.** `pipeline/sources/risk_model/dea.py` solves the
   input-oriented CRS program with scipy's HiGHS instead of Gurobi, which
   drops the license requirement. Efficiency scores agree within 1e-3 on
   ~99.2% of tehsil-months and the resulting vulnerability bins agree on
   ~99.7%; the remainder are degenerate LPs where both solvers are equally
   optimal but settle on different vertices. Worth a stakeholder decision
   on which is canonical before the scores are published.

2. **Cumulative tender values.** In the published table, the raw
   `total-tender-awarded-value` column actually holds financial-year
   cumulative sums: `govtresponse.py` replaced those columns in place
   before writing its factor CSV, and the risk script then used whichever
   factor CSV `glob` happened to return first as its base frame. This port
   keeps monthly values in the raw column and cumulative values in the
   explicit `-fy-cumsum` columns. Downstream consumers reading the raw
   column expecting cumulative values will see a change.

3. **Loss-and-damage weight compounding.** `vulnerability.py` applies its
   damage weighting to the whole panel once per month iteration, so a
   tehsil's damage values compound as later months are processed. This is
   reproduced (`compound_damage_weights: true`, the default) to keep scores
   comparable with the published model; setting it to `false` in the state
   YAML applies the weighting once, which is what the code appears to have
   intended.
