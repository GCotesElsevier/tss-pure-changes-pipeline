# Databricks notebook source
# MAGIC %md
# MAGIC ### Entity sync
# MAGIC One reusable, idempotent sync function for Part 2's supporting
# MAGIC entities (Person, Event, Publisher, InternalOrganization,
# MAGIC ExternalOrganization). The same function handles both the very first
# MAGIC run (target table does not exist yet -> full load) and every run
# MAGIC after that (incremental, based on the table's own `ingest_ts`
# MAGIC column) — there is no separate initial-load notebook to remember to
# MAGIC run only once.
# MAGIC
# MAGIC These tables are prefixed `sync_` (e.g. `sync_persons`) so they never
# MAGIC collide with `ip-pure2far-integration`'s own un-prefixed `persons` /
# MAGIC `events` / `publishers` / `internal_organizations` /
# MAGIC `external_organizations` tables living in the same Databricks catalog
# MAGIC — this pipeline keeps its own copy instead of depending on that
# MAGIC repo's jobs continuing to run (see part2_enrichment/README.md).
# MAGIC
# MAGIC `InternalOrganization` has no incremental endpoint in Pure's legacy
# MAGIC API (confirmed in `ip-pure2far-integration`), so it always does a full
# MAGIC reload — acceptable given how small and slow-changing that table is.

# COMMAND ----------

from datetime import timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

def _table_exists(spark, full_table_name: str) -> bool:
    try:
        spark.table(full_table_name)
        return True
    except Exception:
        return False


def _get_since_datetime(spark, full_table_name: str, default_since_datetime: str) -> str:
    if not _table_exists(spark, full_table_name):
        return default_since_datetime

    max_ts = spark.table(full_table_name).selectExpr("max(ingest_ts) as max_ts").collect()[0]["max_ts"]
    if max_ts is None:
        return default_since_datetime

    # 1 day of overlap, the same margin ip-pure2far-integration uses, in
    # case a record's ingest_ts landed slightly before another one that was
    # still in flight during the previous run.
    return (max_ts - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _upsert(spark, records: list, database: str, table_name: str, key_col: str = "uuid") -> None:
    if not records:
        return

    full_table_name = f"{database}.{table_name}"

    # All-string schema derived from the first record's keys — same
    # approach as `load_data_stage` in ip-pure2far-integration's utils.py,
    # kept for consistency rather than risking a new schema-inference bug.
    columns = list(records[0].keys())
    schema = StructType([StructField(col, StringType(), True) for col in columns])
    spark_df = spark.createDataFrame(records, schema=schema).withColumn("ingest_ts", F.current_timestamp())

    if not _table_exists(spark, full_table_name):
        spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            full_table_name
        )
        return

    staging_view = f"_staging_{table_name}"
    spark_df.createOrReplaceTempView(staging_view)
    spark.sql(
        f"""
        MERGE INTO {full_table_name} AS target
        USING {staging_view} AS source
        ON target.{key_col} = source.{key_col}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# COMMAND ----------

def sync_entity(
    spark,
    pure_api,
    legacy_api,
    database: str,
    table_name: str,
    end_point: str,
    legacy_end_point: str,
    query_field: str,
    process_fn,
    default_since_datetime: str,
) -> int:
    """
    Syncs one supporting entity end to end and returns the number of
    records written.

    `process_fn(raw_record) -> dict` flattens a single raw Pure record into
    the flat shape saved to the table (see `entity_transforms.py`).

    Always does a full reload when `legacy_end_point` is empty (Pure's
    legacy API has no incremental query for that entity) or when the target
    table does not exist yet; otherwise pulls only the uuids created since
    the table's own last `ingest_ts` and re-fetches just those.
    """
    full_table_name = f"{database}.{table_name}"

    if not legacy_end_point or not _table_exists(spark, full_table_name):
        raw_records = pure_api.read_all(end_point)
    else:
        since_datetime = _get_since_datetime(spark, full_table_name, default_since_datetime)
        uuids = legacy_api.read_uuids_since(legacy_end_point, query_field, since_datetime)
        raw_records = [pure_api.read_record(end_point, uuid) for uuid in uuids]

    processed_records = [process_fn(record) for record in raw_records]
    _upsert(spark, processed_records, database, table_name)
    return len(processed_records)
