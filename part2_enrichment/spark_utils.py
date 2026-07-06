# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark save utility
# MAGIC `safe_save_table` — ported from `tss-dedup`'s `safe_save_table`
# MAGIC (`Step0_Fetcher.ipynb` / `Step3_Postprocessor.ipynb`, functionally
# MAGIC identical in both). Handles a real, recurring failure mode: a pandas
# MAGIC DataFrame built from raw, only-partially-transformed Pure JSON often
# MAGIC has object-dtype columns mixing types across rows (or list-of-dict
# MAGIC columns whose items are shaped differently row to row — e.g. a batch
# MAGIC mixing Research Output subtypes whose `keywordGroups` isn't shaped
# MAGIC the same way for every subtype), which breaks Spark's row-by-row
# MAGIC schema inference (`CANNOT_MERGE_TYPE` / `CANNOT_INFER_TYPE_FOR_FIELD`
# MAGIC — hit in practice on a real 5,383-record Scholarly Activities batch
# MAGIC in `enrich_changes.py`).
# MAGIC
# MAGIC Ported as-is (not improved) since the point is reusing an
# MAGIC already-validated function, not rewriting it. Planned to be reused
# MAGIC the same way from Part 1 (`fetch_changes.py` currently sidesteps this
# MAGIC with its own `.astype(str)`, which works today but isn't this) and
# MAGIC from Part 3 once it exists — for now it only lives here, in
# MAGIC `part2_enrichment/`, matching this repo's per-Part self-contained
# MAGIC convention (see the root README's "Conventions" section). Copy/adapt
# MAGIC it into the other Parts when they're wired up to use it too, rather
# MAGIC than reaching across folders.
# MAGIC
# MAGIC **Known trade-off, carried over from the original:** the final
# MAGIC fallback stringifies every column with Python's `str()`, not
# MAGIC `json.dumps()` — so a fallback-stringified list/dict column (e.g.
# MAGIC `keywordGroups`) ends up as Python-repr text (`"[{'a': 1}]"`), not
# MAGIC valid JSON. A future consumer would need `ast.literal_eval()`, not
# MAGIC `json.loads()`, to parse it back.

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
