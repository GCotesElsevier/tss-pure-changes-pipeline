# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Reset sync state (dev utility, Ajman)
# MAGIC Drops one or all of Ajman's per-scope resumptionToken control tables
# MAGIC (`SYNC_STATE_TABLES` in `ajman/config.py`) so the next
# MAGIC `fetch_changes.py` run starts that scope fresh from its own
# MAGIC `DEFAULT_SINCE_DATES` entry instead of resuming from wherever the last
# MAGIC run left off.
# MAGIC
# MAGIC Not something to run routinely in production — only when a run's
# MAGIC *output* got corrupted or lost even though the events were already
# MAGIC consumed from the stream (resuming normally would skip them forever,
# MAGIC since Pure's changes stream has no way to "replay" a range twice).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

dbutils.widgets.text("SCOPE", "ALL", "Scope to reset (or ALL)")
scope_widget = dbutils.widgets.get("SCOPE")

scopes_to_reset = list(SYNC_STATE_TABLES.keys()) if scope_widget == "ALL" else [scope_widget]

for scope_name in scopes_to_reset:
    table_name = SYNC_STATE_TABLES[scope_name]
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{table_name}")
    print(
        f"Dropped {DATABASE}.{table_name} — the next fetch_changes.py run for "
        f"'{scope_name}' will start from DEFAULT_SINCE_DATES['{scope_name}'] "
        f"({DEFAULT_SINCE_DATES[scope_name]}) instead of a persisted resumptionToken."
    )
