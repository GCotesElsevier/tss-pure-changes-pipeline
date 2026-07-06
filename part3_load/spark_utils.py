# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark save utility
# MAGIC `safe_save_table` — same function as `part2_enrichment/spark_utils.py`
# MAGIC (copied here rather than shared across Parts, per this repo's
# MAGIC per-Part self-contained convention — see the root README's
# MAGIC "Conventions" section). See that file's docstring for the full
# MAGIC rationale (ported from `tss-dedup`'s `safe_save_table`, handles Spark
# MAGIC schema-inference failures on heterogeneous/object-dtype columns).
# MAGIC
# MAGIC If this ever needs to change, change both copies (here and in
# MAGIC `part2_enrichment/`) — Part 1's `fetch_changes.py` still uses its own
# MAGIC simpler `.astype(str)` approach, not this function, as a separate
# MAGIC follow-up.

# COMMAND ----------

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

def safe_save_table(spark, logger, df, table_name: str) -> None:
    if isinstance(df, pd.DataFrame):
        is_empty = df.empty
        if not is_empty:
            df = df.copy()

            # Pass 1: any object-dtype value that is not a list and not None
            # becomes a string. Lists are left alone here — list-of-scalars
            # columns are common and Spark infers those fine.
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].apply(lambda x: x if isinstance(x, (list, type(None))) else str(x))

            # Pass 2: a column is only left as-is if EVERY non-null value in
            # it is a list (a "pure" list column). Anything else (mixed
            # types, or a column that turned out not to be a real list
            # column) gets stringified too.
            for col in df.columns:
                if df[col].dtype in ("int64", "float64", "int32", "float32"):
                    continue
                sample = df[col].dropna()
                if sample.empty:
                    df[col] = df[col].astype(object)
                elif sample.apply(lambda x: isinstance(x, list)).all():
                    pass
                else:
                    df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

            # Final fallback: if createDataFrame still fails (e.g. a "pure"
            # list column holds lists of structurally different dicts row
            # to row, which passed pass 2 unchanged but still breaks
            # Spark's inference), force every column to string and give
            # Spark an explicit all-string schema so it never has to infer
            # anything.
            try:
                spark_df = spark.createDataFrame(df)
            except Exception as exc:
                logger.warning("createDataFrame failed (%s), forcing all columns to string", exc)
                for col in df.columns:
                    df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
                explicit_schema = StructType([StructField(c, StringType(), True) for c in df.columns])
                spark_df = spark.createDataFrame(df, schema=explicit_schema)

            incoming_schema = spark_df.schema
        else:
            spark_df = None
            incoming_schema = StructType([StructField(col, StringType(), True) for col in df.columns])

    elif isinstance(df, SparkDataFrame):
        is_empty = df.isEmpty()
        spark_df = df if not is_empty else None
        incoming_schema = df.schema
    else:
        raise TypeError(f"DataFrame type not supported: {type(df)}")

    if not is_empty:
        spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        logger.info("Table saved: %s (%d records)", table_name, spark_df.count())
    else:
        try:
            existing_schema = spark.table(table_name).schema
            logger.info("Table %s already exists — using its schema for the empty write", table_name)
        except Exception:
            existing_schema = incoming_schema
            logger.info("Table %s does not exist — using the input DataFrame's schema", table_name)

        spark.createDataFrame([], existing_schema).write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_name)
        logger.warning("Empty DataFrame received — table %s saved empty with schema preserved", table_name)
