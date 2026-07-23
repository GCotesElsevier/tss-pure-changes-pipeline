# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 — Merge pre-existing base snapshot into today's enriched tables (Ajman)
# MAGIC **One-time notebook, NOT part of the regular pipeline.** Ajman has no
# MAGIC data in FAR yet, so the very first delivery needs to combine:
# MAGIC 1. The pre-existing `processed_{scope}_{cutoff}` snapshot tables
# MAGIC    (produced separately by `ip-pure2far-integration`, pure-Pure data,
# MAGIC    never matched against FAR) — everything up to each scope's cutoff.
# MAGIC 2. Whatever `enrich_changes.py` already produced TODAY from the real
# MAGIC    Changes Endpoint delta since that cutoff (`changes_<scope>_<date>`
# MAGIC    from Part 1).
# MAGIC
# MAGIC Run this AFTER `enrich_changes.py` has already run for today (for both
# MAGIC scopes), and BEFORE `postprocess_changes.py`. It treats the base
# MAGIC snapshot as if it were itself a Part 2 output — enriches it the same
# MAGIC way (email -> `faculty_id`) — then folds it into the SAME
# MAGIC `enriched_<scope>_<CURRENT_DAY>` / `enriched_<scope>_authors_<CURRENT_DAY>`
# MAGIC tables `enrich_changes.py` already wrote, so `postprocess_changes.py`
# MAGIC runs completely UNCHANGED afterwards — it has no idea some of its
# MAGIC input came from a one-time snapshot instead of a real Changes event.
# MAGIC
# MAGIC **Overlap rule (confirmed with the user 2026-07-23):** if a `uuid`
# MAGIC appears in BOTH the base snapshot and today's real enriched output,
# MAGIC the real one wins (it reflects Pure's current state; the snapshot is
# MAGIC frozen as of its cutoff) — the snapshot's row for that `uuid` is
# MAGIC dropped before merging, not overwritten after.
# MAGIC
# MAGIC **`changeType` is forced to `"CREATE"` for the ENTIRE merged batch,
# MAGIC not just the base snapshot's rows** (added 2026-07-23, after Research
# MAGIC Output's real Changes Endpoint delta came back as 1332 `UPDATE`
# MAGIC events, vs. Grants' 232, which happened to all be `CREATE`). This is
# MAGIC Ajman's very first delivery — FAR has nothing to update yet, so a
# MAGIC record Pure calls `UPDATE` (because it already existed and was
# MAGIC edited in Pure) is still brand-new from FAR's point of view. Left
# MAGIC alone, `postprocess_changes.py` would route those rows to the
# MAGIC `updates/` SFTP subfolder instead of `new/`.
# MAGIC
# MAGIC **Run once, then never again** — after this, the regular pipeline
# MAGIC (`fetch_changes.py` -> `enrich_changes.py` -> `postprocess_changes.py`)
# MAGIC handles everything going forward on its own.
# MAGIC
# MAGIC **Schema confirmed 2026-07-23** against real output of
# MAGIC `part3_load/ajman/discover_initial_load_tables.py`:
# MAGIC - The author tables have NO `email` and NO `person_uuid` column —
# MAGIC   only `primary_id` (Pure's own "PrimaryId" identifier type,
# MAGIC   confirmed the same concept `entity_transforms.py`'s `process_person`
# MAGIC   already extracts into `sync_persons.primary_id` — NOT FAR's
# MAGIC   `faculty_id`, a different thing entirely despite the name).
# MAGIC   `normalize_base_authors` below joins on `primary_id` (cast to string
# MAGIC   on both sides — the raw values look numeric) against
# MAGIC   `sync_persons.primary_id` to get `email`, then resolves
# MAGIC   `faculty_id` from there. External authors (`internal=0`) have a
# MAGIC   null `primary_id` and correctly end up with no `faculty_id` — same
# MAGIC   as the regular pipeline already does for external participants.
# MAGIC - A few column names differ from what `enrich_changes.py` produces —
# MAGIC   `main_column_renames` / `author_column_renames` per scope below fix
# MAGIC   the ones that matter for the FAR templates (`far_templates.py`
# MAGIC   reads `sponsor`/`contractid`/`internal`, the base tables have
# MAGIC   `sponser`/`shortTitle`/`internal_flag`). Anything else that doesn't
# MAGIC   line up gets logged by `log_column_diff` instead of silently
# MAGIC   leaving NaNs — check the logs after running even though it won't
# MAGIC   crash.
# MAGIC - Grants' author table has no `sort_order` at all — assigned
# MAGIC   sequentially per grant (order within the source table) since there
# MAGIC   is no better signal available; harmless in practice (only affects
# MAGIC   the co-author display order on the FAR CSV, not who's included).
# MAGIC
# MAGIC **`journal_impact_factor` backfill for Research Output's base snapshot**
# MAGIC (added 2026-07-23): `processed_researchoutputs_20260723` never
# MAGIC computed this field — it requires a live per-record Pure API call
# MAGIC (`journals/{id}/metrics/webOfScienceJournal`) that
# MAGIC `ip-pure2far-integration`'s pipeline never made. Confirmed with the
# MAGIC user to backfill it anyway despite the cost (~5800 extra API calls) —
# MAGIC `backfill_journal_impact_factor` below fetches it in parallel
# MAGIC (same `fetch_records_parallel` pattern as `enrich_changes.py`) for
# MAGIC every base-snapshot row that has a `journal_id`. Expect this cell to
# MAGIC take several minutes.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../spark_utils

# COMMAND ----------

# MAGIC %run ../pure_api_client

# COMMAND ----------

# MAGIC %run ../far_users_client

# COMMAND ----------

# MAGIC %run ./far_users_source

# COMMAND ----------

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# Per-scope table names and column renames, confirmed against real output
# of discover_initial_load_tables.py (2026-07-23) — see module docstring.
BASE_SNAPSHOTS = {
    "grants": {
        "base_main_table": "processed_grants_20260625",
        "base_author_table": "processed_grant_author_20260625",
        "enriched_main_table": f"enriched_grants_{CURRENT_DAY}",
        "enriched_authors_table": f"enriched_grants_authors_{CURRENT_DAY}",
        "main_column_renames": {"sponser": "sponsor", "shortTitle": "contractid"},
        "author_column_renames": {"internal_flag": "internal"},
    },
    "research_output": {
        "base_main_table": "processed_researchoutputs_20260723",
        "base_author_table": "processed_author_20260723",
        "enriched_main_table": f"enriched_research_output_{CURRENT_DAY}",
        "enriched_authors_table": f"enriched_research_output_authors_{CURRENT_DAY}",
        "main_column_renames": {},
        "author_column_renames": {},
    },
}

# COMMAND ----------

# See far_users_source.py / config.py's FAR_USERS_SOURCE — Ajman's FAR API
# isn't provisioned yet, so this currently reads a CSV bypass instead of
# calling the real API.
email_to_faculty_id = get_email_to_faculty_id(spark, logger)
logger.info("Loaded %d FAR users for email -> faculty_id lookup", len(email_to_faculty_id))

persons_df = spark.table(f"{DATABASE}.{PERSON_TABLE}").toPandas()
pure_api = PureAPI(base_url=API_URL, api_key=API_KEY)

# COMMAND ----------

def fetch_journal_impact_factor(pure_api, journal_id, year):
    """
    Same as enrich_changes.py's version: looks up the journal's Web of
    Science impact factor for the record's publication year. A lookup
    failure (common -- many journals have no WoS metrics) just means no
    impact factor for that record, not a batch failure.
    """
    try:
        items = pure_api.read_related(f"journals/{journal_id}/metrics/webOfScienceJournal")
    except Exception:
        return None

    try:
        target_year = int(year)
    except (TypeError, ValueError):
        return None

    for item in items:
        if item.get("year") == target_year:
            for metric in item.get("metricValues", []):
                if metric.get("metricId") == "impactFactor":
                    return metric.get("decimalValue")
    return None


def fetch_records_parallel(fetch_fn, items: list, label: str, max_workers: int = 8, log_every: int = 200) -> list:
    """Same pattern as enrich_changes.py -- parallelizes independent Pure API GETs, preserves order, logs progress."""
    total = len(items)
    if total == 0:
        return []

    logger.info("[%s] fetching %d records (%d concurrent workers)...", label, total, max_workers)
    results = [None] * total
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {executor.submit(fetch_fn, item): i for i, item in enumerate(items)}
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            results[index] = future.result()
            completed += 1
            if completed % log_every == 0 or completed == total:
                logger.info("[%s] fetched %d/%d records", label, completed, total)

    return results


def backfill_journal_impact_factor(df: pd.DataFrame, scope_label: str) -> pd.DataFrame:
    """
    Research Output's base snapshot never computed journal_impact_factor
    (see module docstring) -- backfill it for every row with a journal_id,
    even though this means one extra Pure API call per row. No-op for
    scopes without a journal_id column (Grants).
    """
    if "journal_id" not in df.columns:
        return df

    df = df.copy()
    if "journal_impact_factor" not in df.columns:
        df["journal_impact_factor"] = None

    needs_lookup = df[df["journal_id"].notna() & df["journal_impact_factor"].isna()]
    if needs_lookup.empty:
        return df

    impact_factors = fetch_records_parallel(
        lambda row: fetch_journal_impact_factor(pure_api, row["journal_id"], row.get("statusYear")),
        needs_lookup.to_dict("records"),
        label=f"{scope_label}-journal-impact-factor-backfill",
    )
    df.loc[needs_lookup.index, "journal_impact_factor"] = impact_factors
    logger.info(
        "[%s] backfilled journal_impact_factor for %d/%d base-snapshot rows with a journal_id",
        scope_label, sum(v is not None for v in impact_factors), len(needs_lookup),
    )
    return df


def read_table_or_empty(table_name: str) -> pd.DataFrame:
    full_table_name = f"{DATABASE}.{table_name}"
    try:
        df = spark.table(full_table_name).toPandas()
        logger.info("Read %d rows from %s", len(df), full_table_name)
        return df
    except Exception:
        logger.info("Table %s not found — treating as empty.", full_table_name)
        return pd.DataFrame()


def log_column_diff(label: str, base_df: pd.DataFrame, existing_df: pd.DataFrame) -> None:
    """
    Surfaces column mismatches between the base snapshot and today's real
    enriched output as a warning instead of letting a silent pd.concat
    leave NaNs for the base snapshot's rows on any column that doesn't
    line up — see module docstring, this is NOT yet confirmed to match.
    """
    if existing_df.empty:
        return
    base_only = set(base_df.columns) - set(existing_df.columns)
    existing_only = set(existing_df.columns) - set(base_df.columns)
    if base_only or existing_only:
        logger.warning(
            "[%s] column mismatch between base snapshot and enriched output — "
            "only in base snapshot: %s | only in enriched output: %s. "
            "Rows from the base snapshot will have NaN in the columns they're missing.",
            label, sorted(base_only), sorted(existing_only),
        )


def _stringify_primary_id(value) -> str:
    """
    primary_id values look numeric (5903, 10003, ...) on both sides of the
    join — cast to string consistently so an int64 vs string dtype mismatch
    doesn't silently fail every match. Nulls stay null (never becomes the
    literal string "nan"/"None", which could accidentally collide with a
    stray value on the other side).
    """
    if pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def normalize_base_authors(authors_df: pd.DataFrame, author_column_renames: dict) -> pd.DataFrame:
    """
    Resolves faculty_id on the base snapshot's author table. Confirmed
    2026-07-23: these tables have neither `email` nor `person_uuid` — only
    `primary_id` (Pure's own "PrimaryId" identifier, the same thing
    `sync_persons.primary_id` already holds — see module docstring). Joins
    on that (string-normalized) to pick up email, then resolves
    faculty_id. Also adds `sort_order` if the table doesn't have one
    (Grants' author table doesn't).
    """
    if authors_df.empty:
        return authors_df

    df = authors_df.rename(columns=author_column_renames).copy()

    if "sort_order" not in df.columns:
        df["sort_order"] = df.groupby("uuid").cumcount() + 1

    if "email" in df.columns:
        logger.info("Author table has 'email' directly — using it.")
    elif "primary_id" in df.columns:
        logger.info("No 'email' column — joining 'primary_id' against %s.%s for email.", DATABASE, PERSON_TABLE)
        person_lookup = persons_df[["primary_id", "email"]].copy()
        person_lookup["primary_id"] = person_lookup["primary_id"].apply(_stringify_primary_id)
        person_lookup = person_lookup.dropna(subset=["primary_id"]).drop_duplicates(subset=["primary_id"])
        df["primary_id"] = df["primary_id"].apply(_stringify_primary_id)
        df = df.merge(person_lookup, how="left", on="primary_id")
    else:
        raise ValueError(
            f"Base author table has none of 'email', 'primary_id' — "
            f"can't resolve faculty_id. Columns present: {list(df.columns)}. "
            f"Run discover_initial_load_tables.py again and adjust normalize_base_authors."
        )

    df["faculty_id"] = df["email"].map(
        lambda email: email_to_faculty_id.get(email.strip().lower()) if isinstance(email, str) else None
    )
    return df


def merge_scope(scope_label: str, cfg: dict) -> None:
    logger.info("=== %s ===", scope_label)

    base_main_df = read_table_or_empty(cfg["base_main_table"])
    base_authors_df = read_table_or_empty(cfg["base_author_table"])
    existing_main_df = read_table_or_empty(cfg["enriched_main_table"])
    existing_authors_df = read_table_or_empty(cfg["enriched_authors_table"])

    if base_main_df.empty:
        logger.info("[%s] no base snapshot found — nothing to merge.", scope_label)
        return

    base_main_df = base_main_df.rename(columns=cfg["main_column_renames"]).copy()
    log_column_diff(scope_label, base_main_df, existing_main_df)

    base_main_df["changeType"] = "CREATE"

    # Real enrich_changes.py output wins on any overlapping uuid (see
    # module docstring) — drop those uuids from the base snapshot instead
    # of overwriting after the fact.
    existing_uuids = set(existing_main_df["uuid"]) if not existing_main_df.empty else set()
    overlap = set(base_main_df["uuid"]) & existing_uuids
    if overlap:
        logger.info("[%s] %d uuid(s) present in both base snapshot and today's real changes — keeping the real ones.", scope_label, len(overlap))
    base_main_df = base_main_df[~base_main_df["uuid"].isin(existing_uuids)]

    base_main_df = backfill_journal_impact_factor(base_main_df, scope_label)

    merged_main_df = pd.concat([existing_main_df, base_main_df], ignore_index=True)

    # This is Ajman's very first delivery to FAR -- nothing exists there
    # yet, so EVERYTHING in this merged batch is new to FAR, regardless of
    # what changeType Pure itself assigned. Real changes can legitimately
    # be "UPDATE" from Pure's own point of view (a record that already
    # existed in Pure got edited) even though it's still a brand-new
    # record from FAR's point of view -- confirmed 2026-07-23 when
    # Research Output's real changes came back as 1332 UPDATE events (vs.
    # Grants' 232, which happened to all be CREATE). Left uncorrected,
    # postprocess_changes.py would route those to the updates/ SFTP
    # subfolder, where FAR has nothing to match them against.
    if not merged_main_df.empty:
        overridden = (merged_main_df["changeType"] != "CREATE").sum()
        if overridden:
            logger.info(
                "[%s] overriding changeType to CREATE for %d row(s) that Pure reported as something else "
                "(this is a first delivery -- nothing exists in FAR yet to update).",
                scope_label, overridden,
            )
        merged_main_df["changeType"] = "CREATE"

    base_authors_df = normalize_base_authors(base_authors_df, cfg["author_column_renames"])
    if not base_authors_df.empty:
        base_authors_df = base_authors_df[~base_authors_df["uuid"].isin(existing_uuids)]
    merged_authors_df = pd.concat([existing_authors_df, base_authors_df], ignore_index=True)

    safe_save_table(spark, logger, merged_main_df, f"{DATABASE}.{cfg['enriched_main_table']}")
    safe_save_table(spark, logger, merged_authors_df, f"{DATABASE}.{cfg['enriched_authors_table']}")
    logger.info(
        "[%s] merged: %d total main rows (%d from base snapshot), %d total author rows",
        scope_label, len(merged_main_df), len(base_main_df), len(merged_authors_df),
    )

# COMMAND ----------

for scope_label, cfg in BASE_SNAPSHOTS.items():
    merge_scope(scope_label, cfg)
