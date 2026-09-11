# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Reset sync state (dev utility, Ajman)
# MAGIC Appends a "reset" row to one or all of Ajman's per-scope resumptionToken
# MAGIC control tables (`SYNC_STATE_TABLES` in `ajman/config.py`) pointing back
# MAGIC at that scope's own `DEFAULT_SINCE_DATES` entry, so the next
# MAGIC `fetch_changes.py` run resumes from there instead of from wherever the
# MAGIC last run left off.
# MAGIC
# MAGIC The control tables are append-only history (see `../sync_state`), so a
# MAGIC reset does **not** drop them — the earlier rows stay as an audit trail.
# MAGIC If a truly clean slate is ever needed, drop the table by hand.
# MAGIC
# MAGIC Not something to run routinely in production — only when a run's
# MAGIC *output* got corrupted or lost even though the events were already
# MAGIC consumed from the stream (resuming normally would skip them forever,
# MAGIC since Pure's changes stream has no way to "replay" a range twice).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../sync_state

# COMMAND ----------

dbutils.widgets.text("SCOPE", "ALL", "Scope to reset (or ALL)")
scope_widget = dbutils.widgets.get("SCOPE")

scopes_to_reset = list(SYNC_STATE_TABLES.keys()) if scope_widget == "ALL" else [scope_widget]

for scope_name in scopes_to_reset:
    table_name = SYNC_STATE_TABLES[scope_name]
    cutoff = DEFAULT_SINCE_DATES[scope_name]
    save_resumption_token(spark, DATABASE, table_name, cutoff)
    print(
        f"Appended a reset row to {DATABASE}.{table_name} — the next "
        f"fetch_changes.py run for '{scope_name}' will resume from {cutoff} "
        f"instead of the previously persisted resumptionToken. Earlier "
        f"history rows are kept."
    )
