# Databricks notebook source
# MAGIC %md
# MAGIC ### Sync state
# MAGIC Persists Pure's changes-stream `resumptionToken` between pipeline runs,
# MAGIC so each run only pulls events that happened since the last one instead
# MAGIC of re-reading from a fixed start date every time.
# MAGIC
# MAGIC The control table is **append-only history**: one row per save, with a
# MAGIC monotonically increasing `id`. Each run reads the most recent row and
# MAGIC appends a new one; older rows are never overwritten, so there is an
# MAGIC audit trail of which token was active and from when (`updated_at` is
# MAGIC the wall-clock time of the save, `run_date` the calendar date of the
# MAGIC run). This also feeds the coverage window of the changes dashboard.
# MAGIC
# MAGIC Both clients call this once per scope with a different `table_name`
# MAGIC (HBKU and Ajman each keep a per-scope control table); the token is a
# MAGIC position in Pure's one shared changes stream, filtered client-side by
# MAGIC family within each scope's pass.

# COMMAND ----------

from datetime import date, datetime


def _next_resumption_id(spark, full_table_name: str) -> int:
    """
    Returns MAX(id) + 1 for the control table, or 1 if the table does not
    exist yet or has no rows.
    """
    try:
        row = spark.sql(f"SELECT MAX(id) AS max_id FROM {full_table_name}").collect()[0]
    except Exception:
        return 1
    return (row["max_id"] or 0) + 1


def get_last_resumption_token(spark, database: str, table_name: str, default_since_date: str) -> str:
    """
    Returns the resumptionToken saved by the most recent run, or
    `default_since_date` if the control table does not exist yet or has no
    rows (first run for this scope).
    """
    full_table_name = f"{database}.{table_name}"
    try:
        rows = spark.sql(
            f"SELECT resumption_token FROM {full_table_name} ORDER BY id DESC LIMIT 1"
        ).collect()
    except Exception:
        return default_since_date
    if not rows:
        return default_since_date
    return rows[0]["resumption_token"]


def save_resumption_token(spark, database: str, table_name: str, token: str) -> None:
    """
    Appends a new row with the given resumptionToken, keeping every previous
    row as history (`id` = current MAX + 1).

    Call this only after every scope's output for the current run has been
    saved successfully — advancing the token before that would make a
    failed run silently skip those events on the next retry.

    The first append onto a pre-existing (single-row, no `run_date`) table
    relies on `mergeSchema` to add the column; that old row keeps
    `run_date = NULL`, which is acceptable.
    """
    full_table_name = f"{database}.{table_name}"
    new_id = _next_resumption_id(spark, full_table_name)
    spark.createDataFrame(
        [(new_id, token, datetime.now(), date.today())],
        schema="id INT, resumption_token STRING, updated_at TIMESTAMP, run_date DATE",
    ).write.mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)
