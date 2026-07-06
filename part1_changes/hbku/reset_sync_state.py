# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Reset sync state (dev utility)
# MAGIC Drops the resumptionToken control table so the next
# MAGIC `fetch_changes.py` run starts fresh from `DEFAULT_SINCE_DATE` instead
# MAGIC of resuming from wherever the last run left off.
# MAGIC
# MAGIC Useful after a run's *output* got corrupted or lost for some reason
# MAGIC (e.g. a bug in the save step) even though the events were already
# MAGIC consumed from the stream — resuming normally would skip them forever,
# MAGIC since Pure's changes stream has no way to "replay" a range twice.
# MAGIC
# MAGIC Not something to run routinely in production — only when you
# MAGIC deliberately need to reprocess from `DEFAULT_SINCE_DATE` again.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{SYNC_STATE_TABLE}")
print(
    f"Dropped {DATABASE}.{SYNC_STATE_TABLE} — the next fetch_changes.py run "
    f"will start from DEFAULT_SINCE_DATE ({DEFAULT_SINCE_DATE}) instead of "
    f"a persisted resumptionToken."
)
