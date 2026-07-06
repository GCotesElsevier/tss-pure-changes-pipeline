# Databricks notebook source
# MAGIC %md
# MAGIC ### Sync state
# MAGIC Persists the single **global** `resumptionToken` for Pure's changes
# MAGIC stream between pipeline runs, so each run only pulls events that
# MAGIC happened since the last one instead of re-reading from a fixed start
# MAGIC date every time.
# MAGIC
# MAGIC The token is global, not per scope: it reflects a position in Pure's
# MAGIC one shared changes stream, not a per-family cursor, since a single
# MAGIC pipeline run fetches events for every scope in one pass.

# COMMAND ----------

from datetime import datetime


def get_last_resumption_token(spark, database: str, table_name: str, default_since_date: str) -> str:
    """
    Returns the resumptionToken saved by the previous run, or
    `default_since_date` if the control table does not exist yet (first run).
    """
    full_table_name = f"{database}.{table_name}"
    try:
        row = spark.table(full_table_name).collect()[0]
        return row["resumption_token"]
    except Exception:
        return default_since_date


def save_resumption_token(spark, database: str, table_name: str, token: str) -> None:
    """
    Overwrites the single-row control table with the new resumptionToken.
    Call this only after every scope's output for the current run has been
    saved successfully — advancing the token before that would make a
    failed run silently skip those events on the next retry.
    """
    full_table_name = f"{database}.{table_name}"
    spark.createDataFrame(
        [(1, token, datetime.now())],
        schema="id INT, resumption_token STRING, updated_at TIMESTAMP",
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
