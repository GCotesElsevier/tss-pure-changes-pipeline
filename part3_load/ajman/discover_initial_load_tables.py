# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 — Discover Ajman's pre-existing `processed_*` tables (Ajman)
# MAGIC One-off diagnostic, run BEFORE writing/finalizing `initial_load_*.py`.
# MAGIC
# MAGIC Ajman (unlike HBKU) already has data loaded by `ip-pure2far-integration`
# MAGIC into `processed_{scope}_{cutoff_date}` tables — the source for the
# MAGIC initial FAR load (see `initial_load_research_output.py` /
# MAGIC `initial_load_grants.py`). Before writing that code we need to confirm,
# MAGIC against the real catalog, not memory or guesses:
# MAGIC 1. The exact table names (HBKU's own `ip-pure2far-integration` clone
# MAGIC    used `processed_researchoutputs` + `processed_author` for Research
# MAGIC    Output; Grants' exact companion author table name — singular
# MAGIC    `processed_grant_author` vs plural `processed_grant_authors` — was
# MAGIC    never fully pinned down even for HBKU, since that code lives on a
# MAGIC    branch (`grants_integration_upd`) not currently checked out
# MAGIC    locally).
# MAGIC 2. Their columns — do they match the `research_final_df`/`author_df`
# MAGIC    shape `ip-pure2far-integration`'s own `process_research_output_data`
# MAGIC    produces (per `ajman_research_output/initial_load.py`), i.e. no
# MAGIC    `faculty_id` yet (pure-Pure, pre-FAR-match) — confirms the initial
# MAGIC    load still needs its own email -> faculty_id join, same as
# MAGIC    `enrich_changes.py` does for the incremental pipeline.
# MAGIC
# MAGIC Does not save anything.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

tables_df = spark.sql(f"SHOW TABLES IN {DATABASE}").toPandas()
processed_tables = tables_df[tables_df["tableName"].str.startswith("processed_")]
print(f"processed_* tables in {DATABASE}:")
processed_tables[["tableName"]]

# COMMAND ----------

# Full schema + row count + a small sample for every processed_* table found,
# so the real column names/types can be compared against what
# process_research_output_data / GRANTS_CONFIG expect before initial_load_*.py
# is finalized.
for table_name in processed_tables["tableName"]:
    full_name = f"{DATABASE}.{table_name}"
    df = spark.table(full_name)
    print(f"\n=== {full_name} ===")
    print(f"columns: {df.columns}")
    print(f"row count: {df.count()}")
    display(df.limit(5))
