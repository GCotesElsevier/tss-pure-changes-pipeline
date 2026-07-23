# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Fetch Pure Changes (Ajman)
# MAGIC Unlike HBKU (a single combined pass over the changes stream, split by
# MAGIC scope afterwards, using one shared resumptionToken), Ajman needs each
# MAGIC scope to resume from its OWN starting point — Scholarly Activities and
# MAGIC Grants each have a different pre-existing snapshot cutoff (see
# MAGIC `ajman/config.py` and the initial load in `part3_load/ajman/`), and
# MAGIC Custom Sections has none. So this notebook does one full pass over the
# MAGIC changes stream **per scope**, each with its own starting
# MAGIC token/date and its own sync-state table.
# MAGIC
# MAGIC This costs more Pure API traffic than HBKU's combined pass (each of
# MAGIC the 3 passes re-walks the same underlying shared stream, filtered
# MAGIC client-side to just one scope's families) — accepted trade-off so that
# MAGIC each scope can safely start from its own cutoff. Once all 3 scopes
# MAGIC have caught up to the same point in the stream, their resumption
# MAGIC tokens converge again in practice, even though they're stored and
# MAGIC advanced independently.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../changes_client

# COMMAND ----------

# MAGIC %run ../sync_state

# COMMAND ----------

# MAGIC %run ../cfgs/AJMAN_cfg_changes

# COMMAND ----------

import logging
import sys

import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# See part1_changes/hbku/fetch_changes.py for why this is needed: Arrow's
# optimized createDataFrame silently corrupted small pandas -> Spark
# conversions in this same pipeline (HBKU's Grants/Custom Sections tables).
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

# Defaults to running all 3 scopes together (the regular pipeline mode), but
# can be narrowed to a single scope — useful when one scope's cutoff is
# closer to Pure's 30-day /changes limit than the others' and needs to run
# NOW without also advancing (and later having to reset) another scope's
# resumption token before it's ready (e.g. Scholarly Activities, still
# waiting on a fresh processed_* snapshot as of 2026-07-23 — see project
# memory).
dbutils.widgets.text("SCOPE", "ALL", "Scope to run (or ALL)")
scope_widget = dbutils.widgets.get("SCOPE")
scopes_to_run = cfg if scope_widget == "ALL" else {scope_widget: cfg[scope_widget]}

# COMMAND ----------

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

    # NOTE: destination table name/schema mirrors HBKU's
    # changes_<scope_slug>_<fecha> — consumed by Part 2 the same way.
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
