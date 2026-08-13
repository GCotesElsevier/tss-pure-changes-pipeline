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
# MAGIC **Real bug fixed 2026-08-13 (round 1, INCOMPLETE — see round 2
# MAGIC below):** on this cluster's pandas version (3.0.5, `future.infer_string`
# MAGIC on by default), a plain string column built by
# MAGIC `pd.DataFrame(list_of_dicts)` is NOT legacy `object` dtype — it's
# MAGIC pandas' newer Arrow-backed `string`/`str` dtype
# MAGIC (`pandas.arrays.ArrowStringArray`). Pass 1's trigger check was
# MAGIC `df[col].dtype == object`, `False` for that dtype, so Pass 1 silently
# MAGIC skipped every such column. Widened the trigger to
# MAGIC `pd.api.types.is_string_dtype(df[col])` and appended `.astype(object)`
# MAGIC after every reassignment. This fixed the specific repro tested at the
# MAGIC time (a column built directly by `pd.DataFrame(list_of_dicts)`,
# MAGIC missing values from an absent dict key) — but QA's follow-up
# MAGIC verification against the real re-run found the production bug was
# MAGIC UNCHANGED, and traced it to a different missing-value source not
# MAGIC covered by this round's repro. See round 2.
# MAGIC
# MAGIC **Real bug fixed 2026-08-13 (round 2 — the actual fix):** the missing
# MAGIC piece was `.apply()`/`.map()`/`.where()` themselves. On a pandas
# MAGIC Arrow-backed extension Series (the `string` dtype above, but this
# MAGIC applies to any extension array), these three don't reliably invoke
# MAGIC their callback — or don't use its return value — for a cell the
# MAGIC array's own null-bitmap already marks missing; they just keep
# MAGIC whatever the original cell held, silently discarding what the
# MAGIC callback returned. `_is_missing(x)` correctly said `True` and the
# MAGIC lambda correctly returned `None`, but that `None` never made it into
# MAGIC the output — confirmed with a real repro (not just read from a
# MAGIC table): tracing every value immediately before and after
# MAGIC `.apply(...)` on a column produced by `sync_persons_df.toPandas()`
# MAGIC (a Spark NULL survives `.toPandas()` as this same kind of cell) then
# MAGIC `.merge(how="left")` (`participant_explode.py`'s `attach_faculty_id`,
# MAGIC exactly the code path behind `enriched_grants_authors_<date>`'s
# MAGIC `email`) — the value went in as `float('nan')`, `_is_missing()` said
# MAGIC `True`, and it STILL came out as the same `float('nan')`, unchanged,
# MAGIC after `.apply()`. Round 1's dict-flattening repro happened not to
# MAGIC trigger this because `pd.DataFrame(list_of_dicts)`'s own missing-fill
# MAGIC apparently marks the array's null-bitmap in a way `.apply()` DOES
# MAGIC still run its callback against — the failure is specific to a value
# MAGIC that entered the array via `.toPandas()` / `.merge()`, not to the
# MAGIC dtype alone. `.where(pd.notnull(df), None)` was also tried as a
# MAGIC simpler fix and has the exact same gap — confirmed with the same
# MAGIC repro before ruling it out. Fixed by never calling
# MAGIC `.apply()`/`.map()`/`.where()` on a Series that might hold a missing
# MAGIC value at all: `_unpack()` explicitly unpacks every value (missing or
# MAGIC not) via `Series.to_numpy(dtype=object, na_value=None)`, which
# MAGIC bypasses that per-element callback path entirely and returns a plain
# MAGIC Python list with a real `None` for every flavor of missing this
# MAGIC cluster produces, regardless of source. `_stringify()`/`_fix_missing()`
# MAGIC then operate on that plain list, never back on the original Series.
# MAGIC
# MAGIC Same repro also turned up a second, previously undocumented gap:
# MAGIC `faculty_id` (a `float64` column derived via `.map()` over `email`)
# MAGIC came out as a genuine Spark `Double NaN` — NOT a string, but also NOT
# MAGIC a real SQL NULL (`IS NULL` = 0 on it) — because Pass 2 explicitly
# MAGIC `continue`s past numeric-dtype columns entirely, on the (now known to
# MAGIC be wrong for this cluster) assumption that a native float64 NaN
# MAGIC always converts to a real Spark NULL on its own. It doesn't, at least
# MAGIC not for a float64 column built through this same `.toPandas()`/
# MAGIC `.merge()` path. Fixed by running every numeric column through
# MAGIC `_fix_missing()` too (real `None` for anything `_is_missing()`, every
# MAGIC other value untouched — no stringification, so Spark still infers a
# MAGIC numeric column, just with real nulls in it) instead of skipping it.

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


def _unpack(series: pd.Series) -> list:
    """
    Converts a Series to a plain Python list with a real None for every
    missing value, regardless of the Series' backing dtype/extension
    array or how the missingness was introduced. Deliberately NOT
    .apply()/.map()/.where() — see the "Real bug fixed 2026-08-13 (round
    2)" note above: those don't reliably run their callback (or don't use
    its return value) against a cell an extension array's own
    null-bitmap already marks missing, silently keeping the original
    value instead — including a value that arrived via `.toPandas()` or a
    pandas `.merge()`. `to_numpy(na_value=None)` unpacks explicitly
    instead, sidestepping that per-element callback path entirely.
    """
    return series.to_numpy(dtype=object, na_value=None).tolist()


def _stringify(series: pd.Series, keep_lists: bool = False) -> pd.Series:
    """
    Turns every non-missing value into a string; missing values become a
    real None. When keep_lists is True, list values are left untouched
    instead of being stringified — list-of-scalars columns are common and
    Spark infers those fine.
    """
    def convert(x):
        if keep_lists and isinstance(x, list):
            return x
        return None if _is_missing(x) else str(x)

    return pd.Series([convert(x) for x in _unpack(series)], index=series.index, dtype=object)


def _fix_missing(series: pd.Series) -> pd.Series:
    """
    Replaces every missing value with a real None; every other value is
    left exactly as-is (no stringification). Used for numeric columns,
    where the native int/float type should be preserved so Spark infers a
    numeric column, not a string one — only how "no value" is represented
    needs fixing.
    """
    return pd.Series([None if _is_missing(x) else x for x in _unpack(series)], index=series.index, dtype=object)


def safe_save_table(spark, logger, df, table_name: str) -> None:
    if isinstance(df, pd.DataFrame):
        is_empty = df.empty
        if not is_empty:
            df = df.copy()

            # Pass 1: any string-dtype value that is not a list and not
            # missing becomes a string. `is_string_dtype` (not `== object`)
            # so this also catches pandas' Arrow-backed `string`/`str`
            # dtype, not just legacy `object`.
            for col in df.columns:
                if pd.api.types.is_string_dtype(df[col]):
                    df[col] = _stringify(df[col], keep_lists=True)

            # Pass 2: numeric columns keep their native type — only their
            # missing values get normalized to a real None, not stringified
            # (see the "round 2" note above for why this couldn't just be
            # skipped). Everything else: a column is only left as-is if
            # EVERY non-null value in it is a list (a "pure" list column);
            # anything else (mixed types, or a column that turned out not
            # to be a real list column) gets stringified too.
            for col in df.columns:
                if df[col].dtype in ("int64", "float64", "int32", "float32"):
                    df[col] = _fix_missing(df[col])
                    continue
                sample = df[col].dropna()
                if sample.empty:
                    df[col] = df[col].astype(object)
                elif sample.apply(lambda x: isinstance(x, list)).all():
                    pass
                else:
                    df[col] = _stringify(df[col])

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
                    df[col] = _stringify(df[col])
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
