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
# MAGIC
# MAGIC **Real bug fixed 2026-07-24:** all three stringify passes checked
# MAGIC `x is not None` before calling `str(x)` — but a missing value in
# MAGIC pandas is almost always `float('nan')`, not `None` itself, and
# MAGIC `float('nan') is not None` is `True`. So a genuinely missing value
# MAGIC (e.g. Ajman's `journal_impact_factor`, which Pure never returns any
# MAGIC data for) got stringified to the literal 3-character text `"nan"`
# MAGIC instead of staying a real SQL NULL — invisible in row counts
# MAGIC (`IS NOT NULL` matched every row) and would have shown up as the
# MAGIC literal word "nan" in the FAR CSV instead of a blank cell. Every
# MAGIC `is not None` check below is now `_is_missing(x)`, which uses
# MAGIC `pd.isna()` (catches `None`, `float('nan')`, and `NaT` alike) and is
# MAGIC guarded against lists/dicts, which `pd.isna()` doesn't accept as a
# MAGIC single scalar.
# MAGIC
# MAGIC **Real bug fixed 2026-08-13:** a second, distinct missing-value bug —
# MAGIC `_is_missing(x)` correctly returned `True` for a missing cell and
# MAGIC Pass 1/2 correctly reassigned it to Python `None`, but on this
# MAGIC cluster's pandas version (3.0.5, `future.infer_string` on by
# MAGIC default) a plain string column built by `pd.DataFrame(list_of_dicts)`
# MAGIC is NOT legacy `object` dtype — it's pandas' newer Arrow-backed
# MAGIC `string`/`str` dtype (`pandas.arrays.ArrowStringArray`). Pass 1's
# MAGIC trigger check was `df[col].dtype == object`, which is `False` for
# MAGIC that dtype, so Pass 1 silently skipped every such column entirely.
# MAGIC Pass 2 did reach it and reassigned `None` for missing cells — but
# MAGIC `df[col] = df[col].apply(...)` on a DataFrame with `future.infer_string`
# MAGIC enabled gets silently re-inferred right back into an
# MAGIC ArrowStringArray, which stores the `None` as its own NA sentinel
# MAGIC instead of a plain Python `None`. `spark.createDataFrame(df)` (Arrow
# MAGIC disabled on the Spark side — see `enrich_changes.py`'s Arrow-bug
# MAGIC comment) doesn't unpack that sentinel as a real null through its
# MAGIC legacy pandas conversion path; it surfaces as a raw NaN, which then
# MAGIC renders as the literal string `"NaN"` (capitalized — this is Java's
# MAGIC `Double.toString(NaN)`, not Python's `str()`, which is what gave this
# MAGIC bug's output a different capitalization than the 2026-07-24 one and
# MAGIC made the two easy to mistake for the same root cause). Confirmed via
# MAGIC a real repro against this exact cluster (not just read from a
# MAGIC production table) that this reproduces with ONLY a missing value in
# MAGIC an otherwise-string column, no heterogeneous list/dict shapes
# MAGIC required. Fixed by (a) widening Pass 1's trigger to
# MAGIC `pd.api.types.is_string_dtype(df[col])`, which is `True` for both
# MAGIC legacy `object` dtype (unchanged behavior) and the new `string`/`str`
# MAGIC dtype, and (b) appending `.astype(object)` after every Pass 1/2/
# MAGIC fallback reassignment, which pins the result to a plain numpy object
# MAGIC array so pandas can't re-infer it back into an Arrow-backed
# MAGIC extension array and lose the real `None`. Numeric (float64-with-NaN)
# MAGIC columns were NOT affected by this bug — a native numpy float64 array
# MAGIC converts its NaN to a real Spark SQL NULL correctly through the same
# MAGIC legacy conversion path; only the Arrow-backed string extension array
# MAGIC has this gap.

# COMMAND ----------

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

def _is_missing(x) -> bool:
    """
    True for None/NaN/NaT — anything pandas considers "no value". Guarded
    against lists/dicts, which pd.isna() doesn't accept as a single scalar
    (it either raises or returns an array instead of a bool for those).
    """
    if isinstance(x, (list, dict)):
        return False
    try:
        return pd.isna(x)
    except (TypeError, ValueError):
        return False


def safe_save_table(spark, logger, df, table_name: str) -> None:
    if isinstance(df, pd.DataFrame):
        is_empty = df.empty
        if not is_empty:
            df = df.copy()

            # Pass 1: any string-dtype value that is not a list and not
            # missing becomes a string. Lists are left alone here —
            # list-of-scalars columns are common and Spark infers those
            # fine. Missing values are normalized to a real None (not just
            # left as whatever flavor of "missing" they arrived as) so
            # only one representation of "no value" flows downstream.
            # `is_string_dtype` (not `== object`) so this also catches
            # pandas' Arrow-backed `string`/`str` dtype, not just legacy
            # `object` — see the "Real bug fixed 2026-08-13" note above.
            # `.astype(object)` pins the result to a plain numpy object
            # array so pandas can't silently re-infer it back into that
            # Arrow-backed dtype and lose the real None.
            for col in df.columns:
                if pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].apply(
                        lambda x: x if isinstance(x, list) else (None if _is_missing(x) else str(x))
                    ).astype(object)

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
                    df[col] = df[col].apply(lambda x: None if _is_missing(x) else str(x)).astype(object)

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
                    df[col] = df[col].apply(lambda x: None if _is_missing(x) else str(x)).astype(object)
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
