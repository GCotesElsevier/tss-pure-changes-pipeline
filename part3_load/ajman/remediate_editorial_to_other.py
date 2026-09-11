# Databricks notebook source
# MAGIC %md
# MAGIC # One-time remediation: Ajman "Editorial" -> "Other" (TSSH-1087 AC2)
# MAGIC **Not part of the regular pipeline — run this ONCE, manually, cell by
# MAGIC cell, then don't run it again.** Same spirit as
# MAGIC `hbku/migrate_sftp_layout.py` / `part1_changes/ajman/reset_sync_state.py`:
# MAGIC a utility notebook, not an orchestration step. Does not touch
# MAGIC `AJMAN_cfg_far_templates.py` or any business logic — reuses the same
# MAGIC `Pure_Other_Transformer` (`far_templates.py`) and collaborator-shaping
# MAGIC logic `postprocess_changes.py` already runs for a normal "Other" delivery.
# MAGIC
# MAGIC TSSH-1087 (commit `4571845`) stopped routing Pure "Editorial" research
# MAGIC outputs to their own FAR type and started sending them through "Other
# MAGIC Scholarly Work" instead — Ajman FAR has no "Editorial" subtype. This
# MAGIC remediates what was already loaded/attempted as Editorial in the 2
# MAGIC runs before that fix: **2026-07-23** (initial load) and **2026-08-14**.
# MAGIC
# MAGIC ## What this does
# MAGIC 1. Reads `far_results_editorial_20260723` / `far_results_editorial_20260814`
# MAGIC    only to get each date's set of affected `uuid`s (their actual FAR
# MAGIC    column content is NOT reused — see step 3).
# MAGIC 2. Deduplicates the 2 dates' uuid sets, keeping the **2026-08-14** state
# MAGIC    for any uuid present in both.
# MAGIC 3. For a uuid whose latest editorial state is **2026-07-23 only** (not
# MAGIC    reloaded as Editorial again on 08-14), checks
# MAGIC    `enriched_research_output_deletes_20260814` — if Pure deleted that
# MAGIC    record before the next run, the remediation is a **DELETE**, not a
# MAGIC    rebuilt Other record. **Confirmed with the user 2026-09-11** (this was
# MAGIC    flagged as an interpretation, not something explicitly requested).
# MAGIC 4. Everything else gets rebuilt as "Other Scholarly Work" from
# MAGIC    `enriched_research_output_<source_date>` +
# MAGIC    `enriched_research_output_authors_<source_date>` (the source date being
# MAGIC    whichever of the 2 dates holds that uuid's latest state), through the
# MAGIC    exact same `Pure_Other_Transformer` + collaborator-building code
# MAGIC    `postprocess_changes.py` uses for a normal "Other" run.
# MAGIC 5. Splits the rebuilt records into `new` (changeType CREATE) /
# MAGIC    `updates` (changeType UPDATE) — same convention as the regular
# MAGIC    pipeline's `CHANGE_TYPE_TO_STATUS_FOLDER` — and uploads up to 5 CSVs
# MAGIC    (record + collaborators for new, record + collaborators for update,
# MAGIC    one delete file), skipping any group with no data.
# MAGIC
# MAGIC ## Known limitation, deliberately out of scope
# MAGIC The only cross-check done against `enriched_research_output_deletes_*`
# MAGIC is the DELETE case above. A uuid whose subtype changed from "Editorial"
# MAGIC to something else entirely between the 2 runs (still exists in Pure, but
# MAGIC no longer Editorial) is NOT specially detected — it would just rebuild
# MAGIC as Other from its last known Editorial-tagged state. Not something the
# MAGIC user asked to handle; flagged here so it isn't assumed covered.
# MAGIC
# MAGIC ## SFTP note — read before running
# MAGIC This uploads into the SAME `pure2far_scholarly/{new,updates,deletes}/`
# MAGIC folders the regular daily pipeline uses, just with a distinguishing
# MAGIC `Fixed_` filename prefix. Since `sftp_utils.py`'s
# MAGIC `_archive_all_existing` (commit `010b38b`, 2026-09-10) sweeps **every**
# MAGIC existing file in a status folder into `old_files/` on that folder's
# MAGIC first upload each notebook run — do not run this concurrently with
# MAGIC `postprocess_changes.py`. Whichever of the two runs later today will
# MAGIC archive the other's freshly-uploaded CSVs into `old_files/` (nothing is
# MAGIC lost, just moved) — if that matters, run this at a time isolated from
# MAGIC the regular pipeline's schedule.
# MAGIC
# MAGIC ## Table naming
# MAGIC Intermediate tables use a `_remediation_<today>` suffix (today = the day
# MAGIC this notebook actually runs, not either source date) — never overwrites
# MAGIC `far_results_editorial_*` / `far_results_other_*` / any real daily table.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../spark_utils

# COMMAND ----------

# MAGIC %run ../far_templates

# COMMAND ----------

# MAGIC %run ../sftp_utils

# COMMAND ----------

# MAGIC %run ../cfgs/AJMAN_cfg_far_templates

# COMMAND ----------

import logging
import sys

import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

EDITORIAL_DATES = ["20260723", "20260814"]
LATEST_EDITORIAL_DATE = "20260814"
CSV_PREFIX = "Fixed_"
scholarly_cfg = FAR_TEMPLATES_CONFIG["Scholarly Activities"]


def read_table(table_name: str) -> pd.DataFrame:
    full_table_name = f"{DATABASE}.{table_name}"
    try:
        df = spark.table(full_table_name).toPandas()
        logger.info("Read %d rows from %s", len(df), full_table_name)
        return df
    except Exception:
        logger.info("Table %s not found -- treating as empty.", full_table_name)
        return pd.DataFrame()

# COMMAND ----------

# Step 1: read inputs for both dates.

far_results_editorial = {d: read_table(f"far_results_editorial_{d}") for d in EDITORIAL_DATES}
far_collaborators_editorial = {d: read_table(f"far_collaborators_editorial_{d}") for d in EDITORIAL_DATES}
enriched_main = {d: read_table(f"enriched_research_output_{d}") for d in EDITORIAL_DATES}
enriched_authors = {d: read_table(f"enriched_research_output_authors_{d}") for d in EDITORIAL_DATES}
enriched_deletes = {d: read_table(f"enriched_research_output_deletes_{d}") for d in EDITORIAL_DATES}

# far_collaborators_editorial_* is read only for the audit log below -- it is
# never used as a rebuild source (step 4 of the docstring rebuilds from the
# enriched_* tables directly, through the real Pure_Other_Transformer path).
for d in EDITORIAL_DATES:
    logger.info(
        "[audit] %s: far_results_editorial rows=%d, far_collaborators_editorial rows=%d",
        d, len(far_results_editorial[d]), len(far_collaborators_editorial[d]),
    )

# COMMAND ----------

# Step 2: dedup uuids across the 2 dates, keeping the most recent state.

uuid_col = "uuid_output"
uuids_by_date = {
    d: set(far_results_editorial[d][uuid_col].dropna().unique()) if not far_results_editorial[d].empty else set()
    for d in EDITORIAL_DATES
}
uuids_723 = uuids_by_date["20260723"]
uuids_814 = uuids_by_date["20260814"]
union_uuids = uuids_723 | uuids_814

# Present at 08-14 -> 08-14 IS the latest state, regardless of 07-23.
rebuild_from_814 = set(uuids_814)

# Present only at 07-23 (not reloaded as Editorial again on 08-14) -- latest
# state is 07-23, UNLESS Pure deleted the record before the 08-14 run (step 3).
carried_from_723_only = uuids_723 - uuids_814

# COMMAND ----------

# Step 3: DELETE cross-check (confirmed with the user 2026-09-11) -- a uuid
# carried over from 07-23 whose record was deleted in Pure by the time of the
# 08-14 run gets remediated as a DELETE, not rebuilt as Other.

deletes_814_df = enriched_deletes[LATEST_EDITORIAL_DATE]
deletes_814_uuids = set(deletes_814_df["uuid"].dropna().unique()) if not deletes_814_df.empty else set()

delete_uuids = carried_from_723_only & deletes_814_uuids
rebuild_from_723 = carried_from_723_only - delete_uuids

logger.info(
    "uuids_723=%d uuids_814=%d union=%d | rebuild_from_723=%d rebuild_from_814=%d delete=%d",
    len(uuids_723), len(uuids_814), len(union_uuids),
    len(rebuild_from_723), len(rebuild_from_814), len(delete_uuids),
)
if delete_uuids:
    logger.info("uuid(s) remediated as DELETE (loaded 07-23 as Editorial, deleted from Pure by 08-14): %s", sorted(delete_uuids))

# COMMAND ----------

# Step 4: rebuild as "Other Scholarly Work" -- same shape build_far_template()
# / build_collaborators() produce in postprocess_changes.py for a normal
# "Other" run, ported here unchanged and pre-filtered to this remediation's
# uuid set per source date.

def filter_to_internal_faculty(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["internal_num"] = pd.to_numeric(out["internal"], errors="coerce").fillna(0).astype(int)
    mask = (out["internal_num"] == 1) & out["faculty_id"].notna() & (out["faculty_id"].astype(str).str.strip() != "")
    return out[mask].drop(columns=["internal_num"])


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[ ,;{}()\n\t=]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


def build_collaborators(authors_df: pd.DataFrame, results_df: pd.DataFrame) -> pd.DataFrame:
    if authors_df.empty or results_df.empty:
        return pd.DataFrame()
    link_cols = ["uuid_output", "record_id"]
    if "changetype" in results_df.columns:
        link_cols.append("changetype")
    uuid_to_record = results_df[link_cols].drop_duplicates().rename(columns={"uuid_output": "uuid"})
    out_df = authors_df.merge(uuid_to_record, on="uuid", how="inner")
    out_df["pure_id"] = out_df["record_id"]
    out_df["middle_initial"] = None
    out_df["percent_effort"] = None
    out_df["custom_coauthor_classifications"] = None
    out_df = out_df.drop(columns=["internal"], errors="ignore")
    cols = [
        "record_id", "faculty_id", "first_name", "middle_initial", "last_name",
        "role", "percent_effort", "sort_order", "custom_coauthor_classifications",
        "pure_id", "uuid",
    ]
    if "changetype" in out_df.columns:
        cols.append("changetype")
    return out_df[cols].drop_duplicates()


def rebuild_as_other(main_df: pd.DataFrame, authors_df: pd.DataFrame, target_uuids: set):
    """Rebuilds `target_uuids` (mis-routed to Editorial at the time) as Other
    Scholarly Work, from one source date's own enriched tables."""
    if main_df.empty or not target_uuids:
        return pd.DataFrame(), pd.DataFrame()

    type_df = main_df[main_df["uuid"].isin(target_uuids) & (main_df["subtype"] == "Editorial")]
    if type_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    internal_authors_df = filter_to_internal_faculty(authors_df) if not authors_df.empty else authors_df
    if internal_authors_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_all_data = type_df.merge(internal_authors_df, on="uuid", how="inner")
    if df_all_data.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_template = Pure_Other_Transformer().build(df_all_data)
    if df_template.empty:
        return pd.DataFrame(), pd.DataFrame()

    if "changeType" in df_all_data.columns:
        change_type_by_uuid = df_all_data[["uuid", "changeType"]].drop_duplicates(subset="uuid")
        df_template = df_template.merge(
            change_type_by_uuid, left_on="uuid_output", right_on="uuid", how="left"
        ).drop(columns=["uuid"])

    df_template["Publication Status"] = "Completed/Published"
    df_template["Review"] = "To be Reviewed"
    df_template = normalize_columns(df_template).drop_duplicates()

    collaborators_df = build_collaborators(authors_df, df_template)

    return df_template, collaborators_df


results_723, collabs_723 = rebuild_as_other(enriched_main["20260723"], enriched_authors["20260723"], rebuild_from_723)
results_814, collabs_814 = rebuild_as_other(enriched_main["20260814"], enriched_authors["20260814"], rebuild_from_814)

combined_results = pd.concat([results_723, results_814], ignore_index=True)
combined_collaborators = pd.concat([collabs_723, collabs_814], ignore_index=True)

deletes_source_df = enriched_deletes[LATEST_EDITORIAL_DATE]
deletes_export_df = (
    deletes_source_df[deletes_source_df["uuid"].isin(delete_uuids)].drop(columns=["scope"], errors="ignore")
    if not deletes_source_df.empty and delete_uuids
    else pd.DataFrame()
)

# COMMAND ----------

# Step 5 (part 1): count validation -- must equal the deduplicated union
# before anything gets saved/uploaded. Hard stop if it doesn't: better to
# investigate here than to deliver a silently incomplete remediation.

expected_rebuild_uuids = rebuild_from_723 | rebuild_from_814
produced_uuids = set(combined_results["uuid_output"].unique()) if not combined_results.empty else set()
missing_rebuild_uuids = expected_rebuild_uuids - produced_uuids

if missing_rebuild_uuids:
    logger.warning(
        "%d uuid(s) expected to rebuild as Other produced no FAR row (no internal "
        "author with a resolved faculty_id, most likely): %s",
        len(missing_rebuild_uuids), sorted(missing_rebuild_uuids),
    )

unrecognized_changetypes = (
    set(combined_results["changetype"].dropna().unique()) - {"CREATE", "UPDATE"}
    if "changetype" in combined_results.columns else set()
)
if unrecognized_changetypes:
    logger.warning("Unrecognized changeType value(s) in rebuilt records, excluded from new/updates: %s", unrecognized_changetypes)

total_produced = len(produced_uuids) + len(delete_uuids)
total_union = len(union_uuids)

print(f"Union of far_results_editorial_20260723 + far_results_editorial_20260814 uuids: {total_union}")
print(f"Rebuilt as Other (unique uuids): {len(produced_uuids)}")
print(f"Remediated as DELETE: {len(delete_uuids)}")
print(f"Total produced: {total_produced}")

assert total_produced == total_union, (
    f"Count mismatch: union of editorial uuids ({total_union}) != rebuilt Other "
    f"({len(produced_uuids)}) + deletes ({len(delete_uuids)}) = {total_produced}. "
    f"Missing uuids expected to rebuild: {sorted(missing_rebuild_uuids)}. "
    f"Investigate before saving/uploading anything."
)
print("Count check OK -- union of editorial uuids matches new + update + delete exactly.")

# COMMAND ----------

# Step 5 (part 2): save intermediate tables (remediation-suffixed, never the
# real daily table names).

def save_table(df: pd.DataFrame, table_name: str) -> None:
    full_table_name = f"{DATABASE}.{table_name}"
    if df.empty:
        logger.info("Nothing to save for %s -- skipping.", full_table_name)
        return
    safe_save_table(spark, logger, df, full_table_name)


save_table(combined_results, f"far_results_other_remediation_{CURRENT_DAY}")
save_table(combined_collaborators, f"far_collaborators_other_remediation_{CURRENT_DAY}")
save_table(deletes_export_df, f"far_deletes_other_remediation_{CURRENT_DAY}")

# COMMAND ----------

# Step 5 (part 3): upload to SFTP -- see module docstring's SFTP note before
# running this cell. Only non-empty groups are uploaded (up to 5 files total).

CHANGE_TYPE_TO_STATUS_FOLDER = {"CREATE": "new", "UPDATE": "updates"}

for change_type, status_folder in CHANGE_TYPE_TO_STATUS_FOLDER.items():
    results_subset = combined_results[combined_results["changetype"] == change_type].drop(columns=["changetype"]) if not combined_results.empty else pd.DataFrame()
    if not results_subset.empty:
        filename = f"{CSV_PREFIX}Faculty180_other_{YEAR}-{MONTH}-{DAY}_01.csv"
        remote_path = upload_df_to_sftp(
            csv_ready(results_subset), SFTP_BASE, scholarly_cfg["sftp_folder"], status_folder, filename, logger,
            secret_scope=SFTP_SECRET_SCOPE,
        )
        logger.info("Uploaded %d rows to %s", len(results_subset), remote_path)

    collaborators_subset = combined_collaborators[combined_collaborators["changetype"] == change_type].drop(columns=["changetype"]) if not combined_collaborators.empty else pd.DataFrame()
    if not collaborators_subset.empty:
        filename = f"{CSV_PREFIX}Faculty180_other_collaborator_{YEAR}-{MONTH}-{DAY}_01.csv"
        remote_path = upload_df_to_sftp(
            csv_ready(collaborators_subset), SFTP_BASE, scholarly_cfg["sftp_folder"], status_folder, filename, logger,
            secret_scope=SFTP_SECRET_SCOPE,
        )
        logger.info("Uploaded %d rows to %s", len(collaborators_subset), remote_path)

if not deletes_export_df.empty:
    filename = f"{CSV_PREFIX}Faculty180_deletes_{YEAR}-{MONTH}-{DAY}_01.csv"
    remote_path = upload_df_to_sftp(
        csv_ready(deletes_export_df), SFTP_BASE, scholarly_cfg["sftp_folder"], "deletes", filename, logger,
        secret_scope=SFTP_SECRET_SCOPE,
    )
    logger.info("Uploaded %d rows to %s", len(deletes_export_df), remote_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final summary

# COMMAND ----------

for change_type, status_folder in CHANGE_TYPE_TO_STATUS_FOLDER.items():
    subset = combined_results[combined_results["changetype"] == change_type] if not combined_results.empty else pd.DataFrame()
    print(f"{status_folder}: {len(subset)} record row(s), {subset['uuid_output'].nunique() if not subset.empty else 0} unique uuid(s)")
print(f"deletes: {len(deletes_export_df)} uuid(s)")
print(f"TOTAL unique uuids remediated: {total_produced} (matches union of {total_union})")
