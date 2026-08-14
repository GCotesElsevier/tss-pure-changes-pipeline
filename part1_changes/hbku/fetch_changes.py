# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Fetch Pure Changes
# MAGIC Pulls change events (`CREATE` / `UPDATE` / `DELETE`) for one scope at a
# MAGIC time — Research Outputs (Scholarly Activities), Custom Sections and
# MAGIC Grants each run their own full pass over Pure's changes stream, with
# MAGIC their own resumption token (see `SYNC_STATE_TABLES` /
# MAGIC `DEFAULT_SINCE_DATES` in `config.py`), so each can run as its own
# MAGIC independent recurring Databricks job. Same `SCOPE` widget pattern as
# MAGIC `part1_changes/ajman/fetch_changes.py`. The `change_types` allow-list
# MAGIC per scope (see `cfgs/HBKU_cfg_changes.py`) is unchanged — it still
# MAGIC filters within whichever scope's pass is currently running.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../changes_client

# COMMAND ----------

# MAGIC %run ../sync_state

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_changes

# COMMAND ----------

import logging
import sys

import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Arrow-optimized createDataFrame hit a "Cannot grow BufferHolder by size -32"
# error converting one of the small per-scope pandas DataFrames below (Grants,
# Custom Sections) — it silently fell back and wrote a single fully-null row
# instead of raising, even though the pandas data itself was correct (verified
# directly). tss-dedup disables Arrow for exactly this kind of conversion;
# doing the same here instead of relying on the fallback path.
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

cfg = CHANGES_CONFIG
client = PureChangesClient(base_url=LEGACY_URL, api_key=LEGACY_API_KEY)
logger.info("Scopes: %s", list(cfg.keys()))

# COMMAND ----------

# Defaults to running all 3 scopes together, but can be narrowed to a single
# scope — this is what lets Research Outputs, Custom Sections and Grants
# run as independent recurring Databricks jobs. Same pattern as
# part1_changes/ajman/fetch_changes.py.
dbutils.widgets.text("SCOPE", "ALL", "Scope to run (or ALL)")
scope_widget = dbutils.widgets.get("SCOPE")
scopes_to_run = cfg if scope_widget == "ALL" else {scope_widget: cfg[scope_widget]}

# COMMAND ----------

# Collected across the loop and displayed as a table in the next cell —
# with 3 scopes the log stream above can get long enough that Databricks
# truncates it, burying the per-scope changeType groupby in the cut-off
# part (same issue hit on Ajman's 2-scope run, 2026-08-14). A `display()`'d
# table in its own cell survives that regardless of how noisy the log cell
# above gets.
#
# Unlike Ajman (no change_types filter), this scope_summaries' per-
# changeType "count" is taken AFTER the allow-list filter below, not
# straight off the raw dedup like the log line above — Scholarly
# Activities/Custom Sections only ever keep DELETE, so a pre-filter count
# would overstate what was actually saved to output_table. raw_events/
# unique_events stay pre-filter (same numbers as the log lines above) so
# the table can still show how much the filter dropped.
scope_summaries = []

for scope_name, scope_cfg in scopes_to_run.items():
    families = scope_cfg["pure_families"]
    sync_state_table = SYNC_STATE_TABLES[scope_name]
    default_since_date = DEFAULT_SINCE_DATES[scope_name]

    logger.info("=== %s (families: %s) ===", scope_name, families)

    start_token = get_last_resumption_token(spark, DATABASE, sync_state_table, default_since_date)
    logger.info("Starting from: %s", start_token)

    raw_events, next_token = client.fetch_changes(start_token_or_date=start_token, families=families)
    logger.info("Raw change events: %d", len(raw_events))
    logger.info("Next resumption token: %s", next_token)

    deduped_events = dedupe_last_event_per_uuid(raw_events)
    logger.info("Unique records after de-duplication: %d", len(deduped_events))

    changes_df = pd.DataFrame(deduped_events)
    if not changes_df.empty:
        logger.info("\n%s", changes_df.groupby("changeType").size().to_string())

    # change_types allow-list per scope, unchanged from before this
    # per-scope restructure (see cfgs/HBKU_cfg_changes.py): Scholarly
    # Activities/Custom Sections only keep DELETE (new records come via
    # tss-dedup), Grants keeps CREATE+UPDATE+DELETE.
    allowed_types = scope_cfg.get("change_types")
    if allowed_types is not None and not changes_df.empty:
        changes_df = changes_df[changes_df["changeType"].isin(allowed_types)]

    saved_type_counts = changes_df.groupby("changeType").size() if not changes_df.empty else pd.Series(dtype="int64")

    scope_slug = scope_name.lower().replace(" ", "_").replace(":", "")
    output_table = f"{DATABASE}.changes_{scope_slug}_{CURRENT_DAY}"

    if not changes_df.empty:
        spark_df = spark.createDataFrame(changes_df.astype(str))
        spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
        logger.info("Saved %d records to %s", spark_df.count(), output_table)
    else:
        logger.info("No change events for scope %s — nothing saved.", scope_name)

    # Only advance THIS scope's token after its own output has been saved
    # successfully — a failed run for one scope must not affect the other
    # scopes' tokens, and must not skip this scope's events on retry.
    save_resumption_token(spark, DATABASE, sync_state_table, next_token)
    logger.info("Persisted resumption token for %s: %s", scope_name, next_token)

    if saved_type_counts.empty:
        scope_summaries.append(
            {
                "scope": scope_name,
                "raw_events": len(raw_events),
                "unique_events": len(deduped_events),
                "changeType": None,
                "count": 0,
                "output_table": None,
                "next_token": next_token,
            }
        )
    else:
        for change_type, count in saved_type_counts.items():
            scope_summaries.append(
                {
                    "scope": scope_name,
                    "raw_events": len(raw_events),
                    "unique_events": len(deduped_events),
                    "changeType": change_type,
                    "count": int(count),
                    "output_table": output_table,
                    "next_token": next_token,
                }
            )

# COMMAND ----------

# Survives log truncation on the cell above — see comment where
# scope_summaries is initialized.
display(pd.DataFrame(scope_summaries))
