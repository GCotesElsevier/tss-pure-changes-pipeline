# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 — Postprocess changes into FAR upload templates
# MAGIC Reads today's `enriched_<scope>_<CURRENT_DAY>` tables from Part 2
# MAGIC (same `CURRENT_DAY`, same reasoning as Part 2 reading Part 1's
# MAGIC tables — all 3 notebooks run in the same pipeline execution), builds
# MAGIC one row per (record, internal author), and runs it through the
# MAGIC matching `far_templates.py` transformer to produce Faculty180's
# MAGIC upload column shape.
# MAGIC
# MAGIC **Why this is simpler than `tss-dedup`'s `Step3_Postprocessor`:**
# MAGIC that notebook's `unmatched_full_{type}` join and
# MAGIC `explode_by_internal_authors` step exist because `tss-dedup` has a
# MAGIC Step2 PURE-vs-FAR matching stage that decides which record+faculty
# MAGIC combinations still need to be pushed. This pipeline has no matching
# MAGIC stage — Part 1's Changes Endpoint already says new/update/delete
# MAGIC directly — so the equivalent of `df_all_data` is just: the enriched
# MAGIC main table INNER JOINed with its authors table filtered to internal +
# MAGIC resolved `faculty_id` rows. That join is already one row per
# MAGIC (record, internal author), so there is nothing left to explode.
# MAGIC
# MAGIC **Custom Sections is the one exception:** Part 2 explodes its
# MAGIC participants IN PLACE (no separate authors table — see
# MAGIC `enrich_changes.py`), so the internal-row filter is applied directly
# MAGIC to the main table here instead of to a joined authors table.
# MAGIC
# MAGIC **DELETE records are not handled here** — Part 2 only passes through
# MAGIC minimal `uuid`/`scope`/`changeType` for deletes, for audit purposes;
# MAGIC there is no enriched record to build a FAR template from.
# MAGIC
# MAGIC **Not done yet, on purpose:** no SFTP upload. The folder structure
# MAGIC (`new` / `updates` / `deletes` subfolders per scope, replacing
# MAGIC `tss-dedup`'s single-folder + `old_files` archive) is still to be
# MAGIC designed together — this notebook only builds and saves the
# MAGIC `far_results_*` / `far_collaborators_*` tables for now.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../spark_utils

# COMMAND ----------

# MAGIC %run ../far_templates

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_far_templates

# COMMAND ----------

import logging
import sys

import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# See enrich_changes.py's module docstring for why logging.basicConfig()
# doesn't work in this workspace — same fix applied here.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

TRANSFORMER_MAP = {
    "Book": Pure_Books_Transformer,
    "Chapter": Pure_Chapter_Transformer,
    "Journal": Pure_Journal_Article_Transformer,
    "Proceeding": Pure_Conference_Transformer,
    "Other": Pure_Other_Transformer,
    "Patent": Pure_Patent_Transformer,
    "Editorial": Pure_Editorial_Transformer,
    "Award": Pure_Grants_Transformer,
    "Service: Professional": Pure_Custom_SP_Transformer,
    "Service: University - other than Committees": Pure_Custom_SU_Transformer,
    "Other: Professional Membership": Pure_Custom_OPM_Transformer,
    "Other: Consulting": Pure_Custom_Consulting_Transformer,
}

# COMMAND ----------

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    far_templates.py builds readable "Record ID" / "Faculty ID"-style
    column names; the actual FAR upload format wants snake_case headers.
    Ported from Step3_Postprocessor.ipynb's normalize_columns.
    """
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


def filter_to_internal_faculty(df: pd.DataFrame) -> pd.DataFrame:
    """Rows with a resolved internal faculty_id — the granularity Faculty180 needs."""
    if df.empty:
        return df
    out = df.copy()
    out["internal_num"] = pd.to_numeric(out["internal"], errors="coerce").fillna(0).astype(int)
    mask = (out["internal_num"] == 1) & out["faculty_id"].notna() & (out["faculty_id"].astype(str).str.strip() != "")
    return out[mask].drop(columns=["internal_num"])


def type_table_suffix(type_name: str, type_slug_map: dict = None) -> str:
    if type_slug_map and type_name in type_slug_map:
        return type_slug_map[type_name]
    return type_name.lower().replace(" ", "_").replace(":", "").replace("-", "_")


def build_far_template(primary_df, type_name, transformer_cls, authors_df=None, subtype_filter_col="subtype"):
    """
    Builds df_all_data (one row per record x internal author) for one
    output type, then runs it through the matching far_templates.py
    transformer.

    If `authors_df` is given (Scholarly Activities / Grants), joins
    `primary_df` (filtered to this type) against `authors_df` filtered to
    internal, resolved-faculty_id rows, on uuid. If `authors_df` is None
    (Custom Sections), `primary_df` already IS one row per (record,
    participant) with faculty_id inline (Part 2 explodes it in place) —
    just filter it to internal rows directly. `subtype_filter_col=None`
    skips the type filter entirely (Grants has a single output type
    regardless of Pure's own Project/Award split).
    """
    if primary_df.empty:
        return pd.DataFrame()

    type_df = primary_df if subtype_filter_col is None else primary_df[primary_df[subtype_filter_col] == type_name]
    if type_df.empty:
        return pd.DataFrame()

    if authors_df is not None:
        if authors_df.empty:
            return pd.DataFrame()
        internal_authors_df = filter_to_internal_faculty(authors_df)
        if internal_authors_df.empty:
            return pd.DataFrame()
        df_all_data = type_df.merge(internal_authors_df, on="uuid", how="inner")
    else:
        df_all_data = filter_to_internal_faculty(type_df)

    if df_all_data.empty:
        return pd.DataFrame()

    return transformer_cls().build(df_all_data)


def build_collaborators(authors_df: pd.DataFrame, results_df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (record, ANY author — internal or external) for every
    record that made it into today's results — the full collaborator
    list, not just the internal-faculty rows the main template explodes
    on. Ported from Step3_Postprocessor.ipynb's split_author + the
    author-file column shaping in its final cell.
    """
    if authors_df.empty or results_df.empty:
        return pd.DataFrame()

    uuid_to_record = (
        results_df[["uuid_output", "record_id"]]
        .drop_duplicates()
        .rename(columns={"uuid_output": "uuid"})
    )

    out_df = authors_df.merge(uuid_to_record, on="uuid", how="inner")
    # Unlike the original (which re-derived pure_id from record_id via a
    # "_"-split that was a no-op for this pipeline's actual record_id
    # values — plain pureId strings / grant uuids, neither ever contains
    # "_"), pure_id is just record_id directly here.
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
    return out_df[cols].drop_duplicates()


def read_enriched_table(table_name: str) -> pd.DataFrame:
    full_table_name = f"{DATABASE}.{table_name}"
    try:
        df = spark.table(full_table_name).toPandas()
        logger.info("Read %d rows from %s", len(df), full_table_name)
        return df
    except Exception:
        logger.info("Table %s not found — treating as empty.", full_table_name)
        return pd.DataFrame()


def save_table(df: pd.DataFrame, table_name: str) -> None:
    full_table_name = f"{DATABASE}.{table_name}"
    if df.empty:
        logger.info("Nothing to save for %s — skipping (no dated table created for today).", full_table_name)
        return
    safe_save_table(spark, logger, df, full_table_name)


def log_unmapped_subtypes(df: pd.DataFrame, subtype_column: str, known_types, scope_label: str) -> None:
    """
    Flags real records whose subtype isn't one of this scope's known FAR
    types (e.g. a Custom Sections `typeDiscriminator` tss-dedup's original
    config never covered) -- these are silently skipped by the per-type
    loop below with no template to run them through, so without this they'd
    just vanish with no visibility. Real case hit: a Custom Sections record
    with subtype "EditorialWork: Publication Peer-review", not one of the
    4 known Service/Other types.
    """
    if df.empty:
        return
    unmapped = df[~df[subtype_column].isin(known_types)]
    if unmapped.empty:
        return
    unmapped_subtypes = sorted(unmapped[subtype_column].dropna().unique().tolist())
    logger.warning(
        "[%s] %d record(s) changed today have a subtype with no FAR template mapped yet -- "
        "skipped, not exported: %s",
        scope_label, len(unmapped), unmapped_subtypes,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scholarly Activities

# COMMAND ----------

scholarly_cfg = FAR_TEMPLATES_CONFIG["Scholarly Activities"]

research_output_df = read_enriched_table(f"enriched_research_output_{CURRENT_DAY}")
research_output_authors_df = read_enriched_table(f"enriched_research_output_authors_{CURRENT_DAY}")

if not research_output_df.empty:
    log_unmapped_subtypes(research_output_df, "subtype", scholarly_cfg["subtype_to_type"].keys(), "scholarly_activities")
    research_output_df = research_output_df.assign(type=research_output_df["subtype"].map(scholarly_cfg["subtype_to_type"]))

for type_name in scholarly_cfg["types"]:
    df_template = build_far_template(
        research_output_df, type_name, TRANSFORMER_MAP[type_name],
        authors_df=research_output_authors_df, subtype_filter_col="type",
    )
    if df_template.empty:
        logger.info("[scholarly_activities] no records for type %s today.", type_name)
        continue

    df_template["Publication Status"] = "Completed/Published"
    df_template["Review"] = "To be Reviewed"
    df_template = normalize_columns(df_template).drop_duplicates()

    suffix = type_table_suffix(type_name)
    save_table(df_template, f"far_results_{suffix}_{CURRENT_DAY}")
    save_table(df_template.sample(min(50, len(df_template))), f"far_sample_results_{suffix}_{CURRENT_DAY}")

    collaborators_df = build_collaborators(research_output_authors_df, df_template)
    save_table(collaborators_df, f"far_collaborators_{suffix}_{CURRENT_DAY}")

    logger.info(
        "[scholarly_activities] %s: %d rows exported, %d collaborators",
        type_name, len(df_template), len(collaborators_df),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grants

# COMMAND ----------

grants_cfg = FAR_TEMPLATES_CONFIG["Grants"]

grants_df = read_enriched_table(f"enriched_grants_{CURRENT_DAY}")
grants_authors_df = read_enriched_table(f"enriched_grants_authors_{CURRENT_DAY}")

for type_name in grants_cfg["types"]:  # just ["Award"] -- Pure's own Project/Award split is not exposed to FAR
    df_template = build_far_template(
        grants_df, type_name, TRANSFORMER_MAP[type_name],
        authors_df=grants_authors_df, subtype_filter_col=None,
    )
    if df_template.empty:
        logger.info("[grants] no records for type %s today.", type_name)
        continue

    # Ported as-is: the original also drops this column before saving/upload.
    df_template = df_template.drop(columns=["Co-Investigator(s)"], errors="ignore")
    df_template["Review"] = "To be Reviewed"
    df_template = normalize_columns(df_template).drop_duplicates()

    suffix = type_table_suffix(type_name)
    save_table(df_template, f"far_results_{suffix}_{CURRENT_DAY}")
    save_table(df_template.sample(min(50, len(df_template))), f"far_sample_results_{suffix}_{CURRENT_DAY}")

    collaborators_df = build_collaborators(grants_authors_df, df_template)
    save_table(collaborators_df, f"far_collaborators_{suffix}_{CURRENT_DAY}")

    logger.info(
        "[grants] %s: %d rows exported, %d collaborators",
        type_name, len(df_template), len(collaborators_df),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Sections

# COMMAND ----------

custom_cfg = FAR_TEMPLATES_CONFIG["Custom Sections"]
type_slug_map = custom_cfg["type_slug"]

custom_sections_df = read_enriched_table(f"enriched_custom_sections_{CURRENT_DAY}")
log_unmapped_subtypes(custom_sections_df, "subtype", custom_cfg["types"], "custom_sections")

for type_name in custom_cfg["types"]:
    # No authors_df: Part 2 already explodes participants in place, so
    # build_far_template just filters custom_sections_df to internal rows
    # directly instead of joining a separate authors table.
    df_template = build_far_template(
        custom_sections_df, type_name, TRANSFORMER_MAP[type_name],
        authors_df=None, subtype_filter_col="subtype",
    )
    if df_template.empty:
        logger.info("[custom_sections] no records for type %s today.", type_name)
        continue

    df_template["Review"] = "To be Reviewed"
    df_template = normalize_columns(df_template).drop_duplicates()

    suffix = type_table_suffix(type_name, type_slug_map)
    save_table(df_template, f"far_results_{suffix}_{CURRENT_DAY}")
    save_table(df_template.sample(min(50, len(df_template))), f"far_sample_results_{suffix}_{CURRENT_DAY}")

    # No collaborator file for Custom Sections -- same as the original
    # (it has no author data at all, internal or external).
    logger.info("[custom_sections] %s: %d rows exported", type_name, len(df_template))
