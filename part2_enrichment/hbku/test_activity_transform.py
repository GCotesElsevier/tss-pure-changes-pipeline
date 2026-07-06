# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 — Test: activity transform (diagnostic)
# MAGIC One-off check: fetches the real activity records (using uuids from
# MAGIC Part 1's latest Custom Sections changes table) via `PureAPI`, runs
# MAGIC them through `flatten_dataframe` + the `activity` transform config,
# MAGIC and displays the result for manual inspection.
# MAGIC
# MAGIC Custom Sections is low-volume (2 events seen in Part 1's discovery
# MAGIC run), so this may only have a couple of real records to check against
# MAGIC — better than none.
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

# MAGIC %run ../cfgs/HBKU_cfg_transform_activity

# COMMAND ----------

activity_config = ACTIVITY_TRANSFORM_CONFIG

# COMMAND ----------

tables = [t.name for t in spark.catalog.listTables(DATABASE)]
custom_sections_tables = sorted(t for t in tables if t.startswith("changes_custom_sections_"))
latest_table = custom_sections_tables[-1]
print(f"Using {latest_table}")

sample_rows = spark.table(f"{DATABASE}.{latest_table}").select("uuid").limit(15).collect()
sample_uuids = [row["uuid"] for row in sample_rows]
print(f"Sample uuids ({len(sample_uuids)}): {sample_uuids}")

# COMMAND ----------

pure_api = PureAPI(base_url=API_URL, api_key=API_KEY)
raw_records = [pure_api.read_record("activities", uuid) for uuid in sample_uuids]
print(f"Fetched {len(raw_records)} full records")

# COMMAND ----------

flat = flatten_dataframe(raw_records)
result = apply_transforms(flat, activity_config)

print(f"Types in this sample: {sorted(result['type'].dropna().unique().tolist())}")
display(result)
