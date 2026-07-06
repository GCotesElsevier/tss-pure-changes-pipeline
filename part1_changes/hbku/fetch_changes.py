# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Fetch Pure Changes
# MAGIC Pulls change events (`CREATE` / `UPDATE` / `DELETE`) for **every
# MAGIC scope** in a single run — one pass over Pure's changes stream, split
# MAGIC by scope afterwards — and saves the result for Part 2 (enrichment) to
# MAGIC pick up. There is no per-scope widget: each pipeline run is expected
# MAGIC to process new records, updates, and deletes for all scopes and
# MAGIC subtypes together.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../changes_client

# COMMAND ----------

# MAGIC %run ../sync_state

# COMMAND ----------

import json
import logging
import os
import sys

import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# One level up (part1_changes/), not two: Databricks Repos in this
# workspace only exposes plain filesystem access (open/os.listdir) within
# the executing notebook's own top-level folder, not sibling folders at
# the repo root — confirmed by direct diagnostics against a real repo
# clone. cfgs/ lives inside part1_changes/ for that reason, not at the
# repo root.
part_root = os.path.normpath(os.path.join(os.getcwd(), ".."))

with open(os.path.join(part_root, "cfgs", "HBKU_cfg_changes.json")) as f:
    cfg = json.load(f)

# Union of every family we care about, plus the reverse lookup used to tag
# each event with its scope after the single fetch below.
family_to_scope = {}
for scope_name, scope_cfg in cfg.items():
    for family in scope_cfg["pure_families"]:
        family_to_scope[family] = scope_name

target_families = list(family_to_scope.keys())
logger.info("Scopes: %s", list(cfg.keys()))
logger.info("Pure families tracked: %s", target_families)

# COMMAND ----------

start_token = get_last_resumption_token(spark, DATABASE, SYNC_STATE_TABLE, DEFAULT_SINCE_DATE)
logger.info("Starting from: %s", start_token)

client = PureChangesClient(base_url=LEGACY_URL, api_key=LEGACY_API_KEY)
raw_events, next_token = client.fetch_changes(start_token_or_date=start_token, families=target_families)

logger.info("Total raw change events across all scopes: %d", len(raw_events))
logger.info("Next resumption token: %s", next_token)

# COMMAND ----------

deduped_events = dedupe_last_event_per_uuid(raw_events)
logger.info("Total unique records after de-duplication: %d", len(deduped_events))

changes_df = pd.DataFrame(deduped_events)
if not changes_df.empty:
    changes_df["scope"] = changes_df["familySystemName"].map(family_to_scope)
    logger.info("\n%s", changes_df.groupby(["scope", "changeType"]).size().to_string())

changes_df

# COMMAND ----------

# NOTE: destination table name/schema is provisional — to be finalized
# together with Part 2, which is what actually consumes this output.
for scope_name in cfg.keys():
    scope_slug = scope_name.lower().replace(" ", "_").replace(":", "")
    output_table = f"{DATABASE}.changes_{scope_slug}_{CURRENT_DAY}"

    scope_df = changes_df[changes_df["scope"] == scope_name] if not changes_df.empty else changes_df

    if not scope_df.empty:
        spark_df = spark.createDataFrame(scope_df.drop(columns=["scope"]).astype(str))
        spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
        logger.info("Saved %d records to %s", spark_df.count(), output_table)
    else:
        logger.info("No change events for scope %s — nothing saved.", scope_name)

# COMMAND ----------

# Only advance the token after every scope's output has been saved
# successfully, so a failed run resumes from the same place next time
# instead of silently skipping events.
save_resumption_token(spark, DATABASE, SYNC_STATE_TABLE, next_token)
logger.info("Persisted resumption token for the next run: %s", next_token)
