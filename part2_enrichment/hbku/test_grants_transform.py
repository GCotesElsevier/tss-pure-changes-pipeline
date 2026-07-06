# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 — Test: grants transform (diagnostic)
# MAGIC One-off check: fetches the real Grants record(s) from Part 1's
# MAGIC latest changes table, merges Project with its linked Award (if any —
# MAGIC see `grants_merge.py`), runs the merged record through
# MAGIC `flatten_dataframe` + the `grants` transform config, and displays the
# MAGIC result.
# MAGIC
# MAGIC Grants is low-volume (1 real change seen so far, a `Project`), so
# MAGIC this also serves as the first real look at whether that Project has a
# MAGIC linked Award at all.
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

# MAGIC %run ../grants_merge

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_transform_grants

# COMMAND ----------

grants_config = GRANTS_TRANSFORM_CONFIG

# COMMAND ----------

tables = [t.name for t in spark.catalog.listTables(DATABASE)]
grants_tables = sorted(t for t in tables if t.startswith("changes_grants_"))
latest_table = grants_tables[-1]
print(f"Using {latest_table}")

sample_rows = (
    spark.table(f"{DATABASE}.{latest_table}")
    .select("uuid", "familySystemName")
    .limit(15)
    .collect()
)
print(f"Sample rows ({len(sample_rows)}): {[(r['uuid'], r['familySystemName']) for r in sample_rows]}")

# COMMAND ----------

pure_api = PureAPI(base_url=API_URL, api_key=API_KEY)

merged_records = [
    fetch_and_merge_grant(pure_api, row["uuid"], row["familySystemName"]) for row in sample_rows
]
print(f"Merged {len(merged_records)} grant record(s)")

# COMMAND ----------

external_orgs_df = spark.table(f"{DATABASE}.{EXTERNAL_ORG_TABLE}").toPandas()

flat = flatten_dataframe(merged_records)
result = apply_transforms(flat, grants_config, context={"external_organizations": external_orgs_df})

display(result)
