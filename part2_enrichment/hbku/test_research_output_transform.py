# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 — Test: research output transform (diagnostic)
# MAGIC One-off check: fetches a handful of REAL research-output records
# MAGIC (using uuids from Part 1's latest Scholarly Activities changes table)
# MAGIC via `PureAPI`, runs them through `flatten_dataframe` + the
# MAGIC `research_output` transform config, and displays the result for
# MAGIC manual inspection — before building the Activity and Grants configs,
# MAGIC to catch any wrong assumption about Pure's real JSON shape while
# MAGIC there is only one config to fix.
# MAGIC
# MAGIC Not part of the regular pipeline; safe to delete once
# MAGIC `hbku/enrich_changes.py` covers the same ground for real.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../pure_api_client

# COMMAND ----------

# MAGIC %run ../transform_engine

# COMMAND ----------

import json
import os

# One level up (part2_enrichment/), not two: Databricks Repos in this
# workspace only exposes plain filesystem access (open/os.listdir) within
# the executing notebook's own top-level folder, not sibling folders at
# the repo root — confirmed by direct diagnostics against a real repo
# clone. cfgs/ lives inside part2_enrichment/ for that reason, not at the
# repo root.
part_root = os.path.normpath(os.path.join(os.getcwd(), ".."))
with open(os.path.join(part_root, "cfgs", "HBKU_cfg_transform_research_output.json")) as f:
    research_output_config = json.load(f)

# COMMAND ----------

# Grab a handful of real uuids from Part 1's latest Scholarly Activities
# changes table (same naming Part 1's fetch_changes.py writes:
# changes_<scope_slug>_<date>).
tables = [t.name for t in spark.catalog.listTables(DATABASE)]
scholarly_tables = sorted(t for t in tables if t.startswith("changes_scholarly_activities_"))
latest_table = scholarly_tables[-1]
print(f"Using {latest_table}")

sample_rows = spark.table(f"{DATABASE}.{latest_table}").select("uuid").limit(15).collect()
sample_uuids = [row["uuid"] for row in sample_rows]
print(f"Sample uuids ({len(sample_uuids)}): {sample_uuids}")

# COMMAND ----------

pure_api = PureAPI(base_url=API_URL, api_key=API_KEY)
raw_records = [pure_api.read_record("research-outputs", uuid) for uuid in sample_uuids]
print(f"Fetched {len(raw_records)} full records")

# COMMAND ----------

flat = flatten_dataframe(raw_records)
result = apply_transforms(flat, research_output_config)

print(f"Subtypes in this sample: {sorted(result['subtype'].dropna().unique().tolist())}")
display(result)
