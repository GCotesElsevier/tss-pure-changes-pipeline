# Databricks notebook source
# MAGIC %md
# MAGIC ### Initial load helpers (Ajman)
# MAGIC Shared by `initial_load_research_output.py` and `initial_load_grants.py`
# MAGIC only — a small colocated helper module, same spirit as
# MAGIC `hbku/migrate_sftp_layout.py` being a one-off utility next to the
# MAGIC regular pipeline notebooks rather than a new cross-client `common/`
# MAGIC folder (this repo's existing convention: no shared code across Parts
# MAGIC or clients, see root README).
# MAGIC
# MAGIC Every function here is copied **unchanged** from
# MAGIC `hbku/postprocess_changes.py` (the regular incremental Part 3
# MAGIC pipeline) — not reimplemented. They already handle the "no
# MAGIC `changeType` column" case gracefully (`build_far_template` /
# MAGIC `build_collaborators` only touch `changeType`/`changetype` if present),
# MAGIC which is exactly the initial load's situation: every row here is a
# MAGIC one-time bulk "new" load, not a delta from Part 1's Changes Endpoint,
# MAGIC so there is no `changeType` to carry through. Kept as a copy rather
# MAGIC than importing from `hbku/` so this file is never at risk of an
# MAGIC accidental edit to the already-validated HBKU production notebook.

# COMMAND ----------

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    far_templates.py builds readable "Record ID" / "Faculty ID"-style
    column names; the actual FAR upload format wants snake_case headers.
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
    transformer. See hbku/postprocess_changes.py for the full docstring —
    identical logic here.
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

    df_template = transformer_cls().build(df_all_data)

    # No changeType here (see module docstring) — this block only runs if a
    # caller ever does add one, kept for parity with postprocess_changes.py.
    if not df_template.empty and "changeType" in df_all_data.columns:
        change_type_by_uuid = df_all_data[["uuid", "changeType"]].drop_duplicates(subset="uuid")
        df_template = df_template.merge(
            change_type_by_uuid, left_on="uuid_output", right_on="uuid", how="left"
        ).drop(columns=["uuid"])

    return df_template


def build_collaborators(authors_df: pd.DataFrame, results_df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (record, ANY author — internal or external) for every
    record that made it into today's results. Identical to
    hbku/postprocess_changes.py's version.
    """
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


def log_unmapped_subtypes(df: pd.DataFrame, subtype_column: str, known_types, scope_label: str, logger) -> None:
    """
    Flags real records whose subtype isn't one of this scope's known FAR
    types — see hbku/postprocess_changes.py's docstring for the real case
    that motivated this (HBKU's "EditorialWork: Publication Peer-review").
    """
    if df.empty:
        return
    unmapped = df[~df[subtype_column].isin(known_types)]
    if unmapped.empty:
        return
    unmapped_subtypes = sorted(unmapped[subtype_column].dropna().unique().tolist())
    logger.warning(
        "[%s] %d record(s) have a subtype with no FAR template mapped yet -- "
        "skipped, not exported: %s",
        scope_label, len(unmapped), unmapped_subtypes,
    )
