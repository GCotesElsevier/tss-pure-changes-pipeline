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
# MAGIC way (email -> `faculty_id`, tag `changeType="CREATE"` since none of it
# MAGIC has ever reached FAR) — then folds it into the SAME
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
# MAGIC **Run once, then never again** — after this, the regular pipeline
# MAGIC (`fetch_changes.py` -> `enrich_changes.py` -> `postprocess_changes.py`)
# MAGIC handles everything going forward on its own.
# MAGIC
# MAGIC **TODO(user) before running — schema NOT yet confirmed:**
# MAGIC run `part3_load/ajman/discover_initial_load_tables.py` first and check
# MAGIC `BASE_SNAPSHOTS` below against the real column names, in particular:
# MAGIC - Does the main table's linking column really match `uuid` (the same
# MAGIC   name `enriched_<scope>_<date>` uses)?
# MAGIC - Does the author table have an `email` column directly, or only a
# MAGIC   person-reference column that needs joining against `sync_persons`?
# MAGIC   `_normalize_base_authors` below tries `email` first and falls back
# MAGIC   to a configurable person-reference column — set
# MAGIC   `person_ref_column` per scope once confirmed.
# MAGIC - Do the OTHER column names (title, sponsor, awardStatus, etc.) line
# MAGIC   up with what `enrich_changes.py` produces? `log_column_diff` below
# MAGIC   surfaces any mismatch as a warning instead of silently leaving NaNs
# MAGIC   — check the logs after running, even if it doesn't crash.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../spark_utils

# COMMAND ----------

# MAGIC %run ../far_users_client

# COMMAND ----------

import logging
import sys

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

# Per-scope table names. TODO(user): confirm against discover_initial_load_tables.py
# before trusting this — see module docstring.
BASE_SNAPSHOTS = {
    "grants": {
        "base_main_table": "processed_grants_20260625",
        "base_author_table": "processed_grant_author_20260625",  # TODO(user): confirm exact name (singular vs plural — see project memory)
        "enriched_main_table": f"enriched_grants_{CURRENT_DAY}",
        "enriched_authors_table": f"enriched_grants_authors_{CURRENT_DAY}",
        "person_ref_column": "person_uuid",  # TODO(user): confirm — only used if the author table has no "email" column directly
    },
    "research_output": {
        "base_main_table": "processed_researchoutputs_20260723",
        "base_author_table": "processed_author_20260723",
        "enriched_main_table": f"enriched_research_output_{CURRENT_DAY}",
        "enriched_authors_table": f"enriched_research_output_authors_{CURRENT_DAY}",
        "person_ref_column": "person_uuid",  # TODO(user): confirm — only used if the author table has no "email" column directly
    },
}

# COMMAND ----------

far_client = FARUsersClient(public_key=FAR_PUBLIC_KEY, private_key=FAR_PRIVATE_KEY, database=FAR_DATABASE)
far_users = far_client.fetch_all_users()
email_to_faculty_id = {
    user["email"].strip().lower(): user["userid"]
    for user in far_users
    if user.get("email") and user.get("userid") is not None
}
logger.info("Loaded %d FAR users for email -> faculty_id lookup", len(email_to_faculty_id))

persons_df = spark.table(f"{DATABASE}.{PERSON_TABLE}").toPandas()

# COMMAND ----------

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


def normalize_base_authors(authors_df: pd.DataFrame, person_ref_column: str) -> pd.DataFrame:
    """
    Resolves faculty_id on the base snapshot's author table. Tries a direct
    `email` column first; falls back to joining `person_ref_column` against
    `sync_persons` (same Pure uuids regardless of which pipeline synced
    them) if there's no `email` column. Raises loudly if neither is
    available — see module docstring, this needs confirming before
    trusting it silently.
    """
    if authors_df.empty:
        return authors_df

    df = authors_df.copy()
    if "email" in df.columns:
        logger.info("Author table has 'email' directly — using it.")
    elif person_ref_column in df.columns:
        logger.info("No 'email' column — joining '%s' against %s.%s for email.", person_ref_column, DATABASE, PERSON_TABLE)
        person_emails = persons_df[["uuid", "email"]].rename(columns={"uuid": person_ref_column})
        df = df.merge(person_emails, how="left", on=person_ref_column)
    else:
        raise ValueError(
            f"Base author table has neither 'email' nor '{person_ref_column}' — "
            f"can't resolve faculty_id. Columns present: {list(df.columns)}. "
            f"Run discover_initial_load_tables.py and fix person_ref_column in BASE_SNAPSHOTS."
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

    log_column_diff(scope_label, base_main_df, existing_main_df)

    base_main_df = base_main_df.copy()
    base_main_df["changeType"] = "CREATE"

    # Real enrich_changes.py output wins on any overlapping uuid (see
    # module docstring) — drop those uuids from the base snapshot instead
    # of overwriting after the fact.
    existing_uuids = set(existing_main_df["uuid"]) if not existing_main_df.empty else set()
    overlap = set(base_main_df["uuid"]) & existing_uuids
    if overlap:
        logger.info("[%s] %d uuid(s) present in both base snapshot and today's real changes — keeping the real ones.", scope_label, len(overlap))
    base_main_df = base_main_df[~base_main_df["uuid"].isin(existing_uuids)]

    merged_main_df = pd.concat([existing_main_df, base_main_df], ignore_index=True)

    base_authors_df = normalize_base_authors(base_authors_df, cfg["person_ref_column"])
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
