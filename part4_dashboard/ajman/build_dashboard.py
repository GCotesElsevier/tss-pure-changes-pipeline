# Databricks notebook source
# MAGIC %md
# MAGIC # Part 4 — Build client dashboard (Ajman)
# MAGIC Runs **live, right after a normal changes run** (Part 1 → Part 2 →
# MAGIC Part 3), over the **same `CURRENT_DAY`**.
# MAGIC
# MAGIC Same `SCOPE` widget convention as the rest of the pipeline
# MAGIC (`part1_changes/ajman/fetch_changes.py`): a dropdown with `ALL` /
# MAGIC `Scholarly Activities` / `Grants`. `ALL` builds both scopes' reports in
# MAGIC one run; picking one builds only that scope's.
# MAGIC
# MAGIC Part 4 only ever **reads** the tables Parts 1-3 already wrote for today
# MAGIC (`changes_<scope>_<date>`, `enriched_<scope>_<date>`,
# MAGIC `enriched_<scope>_authors_<date>`, `enriched_<scope>_deletes_<date>`,
# MAGIC `far_results_<type>_<date>`, `far_collaborators_<type>_<date>`) — it
# MAGIC never writes to or modifies anything of theirs. It writes its own
# MAGIC `dashboard_*` tables and uploads one HTML report per scope to
# MAGIC `{SFTP_BASE}/{sftp_folder}/reports/`.
# MAGIC
# MAGIC What it produces per scope:
# MAGIC 1. `dashboard_metrics_<date>` — curated named KPIs, long format
# MAGIC    (`scope`/`metric`/`dimension`/`value`), formulas per
# MAGIC    `project_ajman_data_analyst_dashboard` (memory).
# MAGIC 2. `dashboard_summary_detail_<date>` — raw per-type/status counts.
# MAGIC 3. `dashboard_<scope>_dropped_<date>` — the actionable list of records
# MAGIC    that changed but never reached FAR (no internal author resolved).
# MAGIC 4. `dashboard_records_<date>` — one row per (record × internal
# MAGIC    participant), plus delete rows; feeds the report's record-level table.
# MAGIC 5. `dashboard_delivery_log` (append-only) — one row per successful
# MAGIC    delivery; drives the coverage window of the next run.
# MAGIC 6. The HTML report itself, uploaded to SFTP.
# MAGIC
# MAGIC `dashboard_metrics_<date>` / `dashboard_summary_detail_<date>` /
# MAGIC `dashboard_records_<date>` are shared across both scopes — each scope's
# MAGIC build replaces only its own rows in those tables (see
# MAGIC `upsert_scope_table`), so an `ALL` run and 2 separate single-scope runs
# MAGIC leave the same end state.
# MAGIC
# MAGIC Reconciliation identities (logged as warnings if they don't close):
# MAGIC - `received(CREATE) == enriched(CREATE) == dropped(CREATE) + delivered(CREATE)`
# MAGIC - `received(UPDATE) == enriched(UPDATE) == dropped(UPDATE) + delivered(UPDATE)`
# MAGIC - `received(DELETE) == deletes_delivered`

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./dashboard_report

# COMMAND ----------

# MAGIC %run ../../part3_load/spark_utils

# COMMAND ----------

# MAGIC %run ../../part3_load/sftp_utils

# COMMAND ----------

# MAGIC %run ../../part3_load/cfgs/AJMAN_cfg_far_templates

# COMMAND ----------

import logging
import sys
from datetime import date, datetime

import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# Same logging fix as the rest of the pipeline — logging.basicConfig() is a
# no-op in this workspace.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

RUN_TS = datetime.now()

# COMMAND ----------

# Same widget name/values as part1_changes/ajman/fetch_changes.py — the
# widget label ("Scholarly Activities"/"Grants") is this repo's Pure-facing
# scope name; SCOPE_CONFIG (config.py) maps it to Part 4's own internal
# table-slug key ("research_output"/"grants" — same slug Part 2/3 already
# use for the Scholarly Activities scope's tables).
WIDGET_TO_SCOPE_KEY = {"Scholarly Activities": "research_output", "Grants": "grants"}

dbutils.widgets.dropdown("SCOPE", "ALL", ["ALL", "Scholarly Activities", "Grants"], "Scope to build (or ALL)")
scope_widget = dbutils.widgets.get("SCOPE")
scopes_to_run = (
    list(WIDGET_TO_SCOPE_KEY.values()) if scope_widget == "ALL" else [WIDGET_TO_SCOPE_KEY[scope_widget]]
)

logger.info("=== Part 4 dashboard: SCOPE=%s -> %s, CURRENT_DAY=%s ===", scope_widget, scopes_to_run, CURRENT_DAY)

# COMMAND ----------

def read_table(table_name: str) -> pd.DataFrame:
    """Read a table as pandas, or return an empty DataFrame if it doesn't
    exist (a scope with no changes that day simply has no table)."""
    full = f"{DATABASE}.{table_name}"
    try:
        df = spark.table(full).toPandas()
        logger.info("Read %d rows from %s", len(df), full)
        return df
    except Exception:
        logger.info("Table %s not found — treating as empty.", full)
        return pd.DataFrame()


def _type_slug(type_name: str) -> str:
    """Same slug rule as part3_load/*/postprocess_changes.py type_table_suffix()."""
    return type_name.lower().replace(" ", "_").replace(":", "").replace("-", "_")


def _far_display(far_type: str) -> str:
    """Internal FAR type name -> client-facing label ("Other" -> "Other Scholarly Work")."""
    return FAR_TYPE_DISPLAY.get(far_type, far_type)


def _clean_fid(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text == "" or text.lower() in ("nan", "none"):
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _full_name(first, last) -> str:
    parts = [str(x).strip() for x in (first, last) if x is not None and str(x).strip() not in ("", "nan", "None")]
    return " ".join(parts)

# COMMAND ----------

def _counts_by_change_type(df: pd.DataFrame, column: str) -> dict:
    if df.empty or column not in df.columns:
        return {}
    return {str(k): int(v) for k, v in df.groupby(column).size().items()}


def faculty_match_rate(df: pd.DataFrame):
    """
    Share of internal participants with a resolved faculty_id. None (not 0)
    when there were no internal participants at all this run.
    """
    if df.empty or "internal" not in df.columns:
        return None
    internal_mask = pd.to_numeric(df["internal"], errors="coerce").fillna(0).astype(int) == 1
    denom = int(internal_mask.sum())
    if denom == 0:
        return None
    resolved = df["faculty_id"].map(lambda v: _clean_fid(v) != "")
    num = int((internal_mask & resolved).sum())
    return num / denom


def build_delivered_frame(far_results: dict, slug_to_type: dict) -> pd.DataFrame:
    """
    Every row across all of today's far_results_<slug>_<date> tables for this
    scope, narrowed to the columns Part 4 needs, plus `far_type` (the FAR
    type name that table encodes). Row-level (fan-out by internal co-author).
    """
    frames = []
    for slug, df in far_results.items():
        if df.empty:
            continue
        keep = {}
        for want in ("uuid_output", "record_id", "changetype", "faculty_id"):
            keep[want] = df[want] if want in df.columns else None
        part = pd.DataFrame(keep)
        part["far_type"] = slug_to_type.get(slug, slug.title())
        frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["uuid_output", "record_id", "changetype", "faculty_id", "far_type"])
    return pd.concat(frames, ignore_index=True)


_INVALID_UUID_TOKENS = {"", "nan", "none", "null", "<na>"}


def _is_invalid_uuid(value) -> bool:
    """
    Same check as part2_enrichment/ajman/enrich_changes.py's
    _is_invalid_uuid (own copy, per this repo's per-part self-contained
    convention). Catches a malformed /changes event that reached
    changes_<scope>_<date> with no real uuid (seen 2026-09-11 — see
    project_ajman_fix_nan_uuid_crash_20260911, memory) — Part 1/2 now filter
    these before enrichment, but Part 4 reads the RAW changes table
    independently, so it detects the same condition on its own instead of
    depending on that fix having run.
    """
    if value is None:
        return True
    return str(value).strip().lower() in _INVALID_UUID_TOKENS


def split_malformed_events(changes_df: pd.DataFrame) -> tuple:
    """
    Splits changes_<scope>_<date> into (valid_df, malformed_df). A
    malformed row (no real uuid) is excluded from every metric/KPI computed
    from `valid_df` — it was never a real, actionable Pure change — and
    reported separately (Sankey branch + record-level table row) ONLY when
    at least one exists this run.
    """
    if changes_df.empty or "uuid" not in changes_df.columns:
        return changes_df, pd.DataFrame(columns=changes_df.columns)
    malformed_mask = changes_df["uuid"].map(_is_invalid_uuid)
    if not malformed_mask.any():
        return changes_df, pd.DataFrame(columns=changes_df.columns)
    return changes_df[~malformed_mask].reset_index(drop=True), changes_df[malformed_mask].reset_index(drop=True)


def build_dropped_list(scope: str, enriched_df: pd.DataFrame, subtype_to_type: dict, delivered_uuids: set) -> pd.DataFrame:
    """Non-delete records that changed but produced no FAR row at all."""
    cols = ["uuid", "changeType", "subtype_pure", "subtype_far", "title"]
    if enriched_df.empty:
        return pd.DataFrame(columns=cols)
    nd = enriched_df[enriched_df["changeType"] != "DELETE"].copy()
    if nd.empty:
        return pd.DataFrame(columns=cols)
    dropped = nd[~nd["uuid"].map(str).isin(delivered_uuids)].copy()
    if dropped.empty:
        return pd.DataFrame(columns=cols)
    if scope == "research_output":
        dropped["subtype_pure"] = dropped["subtype"]
        dropped["subtype_far"] = dropped["subtype"].map(lambda s: _far_display(subtype_to_type.get(s, s)))
    else:
        # Grants only ever has one FAR-side type ("Award" in
        # FAR_TEMPLATES_CONFIG), but the PURE-side record can be either a
        # Project or an Award change -- read the real value instead of
        # assuming "Award" for both (found 2026-09-16: most Ajman Grants
        # changes are Projects, not Awards).
        dropped["subtype_pure"] = dropped["typeDisc"].fillna("Award") if "typeDisc" in dropped.columns else "Award"
        dropped["subtype_far"] = "Award"
    if "title" not in dropped.columns:
        dropped["title"] = None
    return dropped[cols].drop_duplicates(subset="uuid").reset_index(drop=True)

# COMMAND ----------

# --- record-level table (one row per record × internal participant, + deletes) ---
EVENT_LABEL = {"CREATE": "New", "UPDATE": "Updated", "DELETE": "Deleted"}
EVENT_SLUG = {"CREATE": "new", "UPDATE": "updated", "DELETE": "deleted"}
OUTCOME_LABEL = {
    "delivered": "Delivered",
    "retracted": "Retracted from FAR",
    "dropped_no_fid": "Dropped - no Faculty ID match",
    "dropped_no_internal": "Dropped - no internal participant",
}
STATUS_MAP = {"CREATE": "new", "UPDATE": "update", "DELETE": "delete"}


def build_records(
    scope: str, enriched_df: pd.DataFrame, authors_df: pd.DataFrame, deletes_df: pd.DataFrame,
    subtype_to_type: dict, delivered_pairs: set,
) -> list:
    records = []

    if not enriched_df.empty:
        base = enriched_df[["uuid", "changeType"]].copy()
        base["title"] = enriched_df["title"] if "title" in enriched_df.columns else None
        if scope == "research_output":
            base["subtype_pure"] = enriched_df["subtype"]
            base["subtype_far"] = base["subtype_pure"].map(lambda s: _far_display(subtype_to_type.get(s, s)))
        else:
            # See build_dropped_list's comment above -- same real
            # Project/Award distinction instead of assuming "Award".
            base["subtype_pure"] = enriched_df["typeDisc"].fillna("Award") if "typeDisc" in enriched_df.columns else "Award"
            base["subtype_far"] = "Award"
        nd = base[base["changeType"] != "DELETE"]

        if not authors_df.empty and "internal" in authors_df.columns:
            a = authors_df.copy()
            a["_int"] = pd.to_numeric(a["internal"], errors="coerce").fillna(0).astype(int)
            a_int = a[a["_int"] == 1][["uuid", "first_name", "last_name", "faculty_id"]]
        else:
            a_int = pd.DataFrame(columns=["uuid", "first_name", "last_name", "faculty_id"])

        merged = nd.merge(a_int, on="uuid", how="left", indicator=True)
        for _, r in merged.iterrows():
            uuid_str = str(r["uuid"])
            event = EVENT_SLUG.get(r["changeType"], "updated")
            fid = _clean_fid(r.get("faculty_id"))
            has_participant = r["_merge"] == "both"

            if not has_participant:
                name, fid, outcome = "", "", "dropped_no_internal"
            elif fid and (uuid_str, fid) in delivered_pairs:
                name, outcome = _full_name(r.get("first_name"), r.get("last_name")), "delivered"
            else:
                name = _full_name(r.get("first_name"), r.get("last_name"))
                outcome = "dropped_no_fid"
                if not fid:
                    fid = ""

            title = r.get("title")
            title_ok = has_real_title(title)
            records.append({
                "name": name,
                "fid": fid,
                "title": str(title) if title_ok else "",
                "titleMissing": not title_ok,
                "uuid": uuid_str,
                "subtypePure": str(r["subtype_pure"]) if r.get("subtype_pure") is not None else "",
                "subtypeFar": str(r["subtype_far"]) if r.get("subtype_far") is not None else "",
                "event": event,
                "outcome": outcome,
            })

    for _, r in deletes_df.iterrows():
        records.append({
            "name": "", "fid": "", "title": "", "titleMissing": True, "uuid": str(r["uuid"]),
            "subtypePure": "", "subtypeFar": "", "event": "deleted", "outcome": "retracted",
        })

    return records


def build_subtypes(scope: str, delivered_df: pd.DataFrame, enriched_df: pd.DataFrame) -> list:
    """Per-Pure-subtype new/updated delivered counts, for the bar chart /
    donut / Sankey leaves."""
    if delivered_df.empty or "changetype" not in delivered_df.columns:
        return []
    deliv = delivered_df.dropna(subset=["changetype"])[["uuid_output", "changetype", "far_type"]].drop_duplicates()
    if scope == "research_output" and not enriched_df.empty:
        sub_lookup = enriched_df[["uuid", "subtype"]].rename(columns={"uuid": "uuid_output"})
        deliv = deliv.merge(sub_lookup, on="uuid_output", how="left")
        deliv["subtype"] = deliv["subtype"].fillna(deliv["far_type"])
    elif not enriched_df.empty and "typeDisc" in enriched_df.columns:
        # Same real Project/Award distinction as build_records/
        # build_dropped_list, instead of assuming every Grants change is
        # an "Award".
        sub_lookup = enriched_df[["uuid", "typeDisc"]].rename(columns={"uuid": "uuid_output"})
        deliv = deliv.merge(sub_lookup, on="uuid_output", how="left")
        deliv["subtype"] = deliv["typeDisc"].fillna("Award")
        deliv = deliv.drop(columns=["typeDisc"])
    else:
        deliv["subtype"] = "Award"

    rows = []
    grouped = deliv.groupby(["subtype", "far_type", "changetype"])["uuid_output"].nunique()
    for (pure, far_type, ct), n in grouped.items():
        row = next((x for x in rows if x["pure"] == pure and x["far_type"] == far_type), None)
        if row is None:
            row = {
                "pure": pure,
                "far_type": far_type,
                "far": FAR_TYPE_DISPLAY.get(far_type, far_type),
                "far_slug": _type_slug(far_type),
                "color_var": FAR_TYPE_COLOR_VAR.get(far_type, "--seq-400"),
                "new": 0,
                "updated": 0,
            }
            rows.append(row)
        if ct == "CREATE":
            row["new"] += int(n)
        elif ct == "UPDATE":
            row["updated"] += int(n)
    return [{k: v for k, v in r.items() if k != "far_type"} for r in sorted(rows, key=lambda r: -(r["new"] + r["updated"]))]

# COMMAND ----------

def _check(label: str, left, right) -> bool:
    if left != right:
        logger.warning("RECONCILIATION MISMATCH [%s]: %s != %s", label, left, right)
        return False
    logger.info("Reconciliation OK [%s]: %s == %s", label, left, right)
    return True


def upsert_scope_table(df: pd.DataFrame, table_name: str, scope: str) -> None:
    """
    dashboard_metrics_/dashboard_summary_detail_/dashboard_records_ are
    shared by both scopes. Replace only this scope's rows, keep the other
    scope's (so ALL and 2 single-scope runs leave the same end state).
    """
    full = f"{DATABASE}.{table_name}"
    existing = read_table(table_name)
    kept = existing[existing["scope"] != scope] if (not existing.empty and "scope" in existing.columns) else pd.DataFrame()
    combined = pd.concat([kept, df], ignore_index=True) if not df.empty else kept
    if combined.empty:
        logger.info("Nothing to persist for %s (scope=%s) — skipping.", full, scope)
        return
    safe_save_table(spark, logger, combined, full)


def compute_coverage_start(scope: str) -> str:
    log_df = read_table(DELIVERY_LOG_TABLE)
    candidate = None
    if not log_df.empty and "scope" in log_df.columns:
        scope_log = log_df[log_df["scope"] == scope]
        if not scope_log.empty:
            candidate = pd.to_datetime(scope_log["delivered_at"], errors="coerce").max()
    if candidate is None or pd.isna(candidate):
        candidate = pd.to_datetime(SNAPSHOT_CUTOFFS[scope])
    floor = pd.Timestamp(RUN_TS) - pd.Timedelta(days=COVERAGE_MAX_LOOKBACK_DAYS)
    start = max(pd.Timestamp(candidate), floor)
    return start.strftime("%Y-%m-%d")


def upload_html_to_sftp(html_content: str, remote_folder: str, filename: str) -> str:
    """Uploads the report to {SFTP_BASE}/{remote_folder}/reports/{filename},
    reusing sftp_utils.py's connection helpers (not modified). No old_files
    archiving — the filename is date-stamped."""
    remote_dir = f"{SFTP_BASE}/{remote_folder}/reports"
    remote_path = f"{remote_dir}/{filename}"
    client = _connect_sftp(SFTP_SECRET_SCOPE)
    sftp = client.open_sftp()
    try:
        _ensure_remote_dir(sftp, remote_dir)
        with sftp.open(remote_path, "w") as remote_file:
            remote_file.write(html_content)
    finally:
        sftp.close()
        client.close()
    return remote_path

# COMMAND ----------

def run_scope(scope: str) -> None:
    """Builds, persists and uploads the full dashboard report for one scope
    ("research_output" or "grants")."""
    cfg = SCOPE_CONFIG[scope]
    far_cfg = FAR_TEMPLATES_CONFIG[cfg["far_templates_key"]]
    subtype_to_type = far_cfg.get("subtype_to_type", {})  # Grants has none — single "Award" type
    sftp_folder = far_cfg["sftp_folder"]

    logger.info("--- Part 4 dashboard: scope=%s ---", scope)

    # --- READ-ONLY reads of Part 1/2/3 output for today ---
    changes_df, malformed_df = split_malformed_events(read_table(f"{cfg['changes_table']}_{CURRENT_DAY}"))
    if not malformed_df.empty:
        logger.error(
            "[%s] %d malformed /changes event(s) detected (no valid uuid) — excluded from "
            "'received' and reported separately in the dashboard: %s",
            scope, len(malformed_df), malformed_df.to_dict("records"),
        )
    enriched_df = read_table(f"{cfg['enriched_table']}_{CURRENT_DAY}")
    authors_df = read_table(f"{cfg['enriched_authors_table']}_{CURRENT_DAY}")
    deletes_df = read_table(f"{cfg['enriched_deletes_table']}_{CURRENT_DAY}")

    slug_to_type = {_type_slug(t): t for t in far_cfg["types"]}
    far_results = {slug: read_table(f"far_results_{slug}_{CURRENT_DAY}") for slug in cfg["far_result_slugs"]}
    far_collaborators = {slug: read_table(f"far_collaborators_{slug}_{CURRENT_DAY}") for slug in cfg["far_result_slugs"]}

    delivered_df = build_delivered_frame(far_results, slug_to_type)
    delivered_uuids = set(delivered_df["uuid_output"].dropna().map(str)) if not delivered_df.empty else set()
    delivered_pairs = set()
    if not delivered_df.empty:
        for _, r in delivered_df.iterrows():
            delivered_pairs.add((str(r["uuid_output"]), _clean_fid(r.get("faculty_id"))))

    received_by_ct = _counts_by_change_type(changes_df, "changeType")
    enriched_by_ct = _counts_by_change_type(enriched_df, "changeType")

    distinct_delivered_by_ct = {}
    delivered_rows_by_ct = {}
    if not delivered_df.empty and "changetype" in delivered_df.columns:
        grp = delivered_df.dropna(subset=["changetype"])
        distinct_delivered_by_ct = {str(k): int(v) for k, v in grp.groupby("changetype")["uuid_output"].nunique().items()}
        delivered_rows_by_ct = {str(k): int(v) for k, v in grp.groupby("changetype").size().items()}

    deletes_delivered = int(len(deletes_df))
    collaborators_exported = int(sum(len(df) for df in far_collaborators.values()))
    match_rate = faculty_match_rate(authors_df)

    dropped_list_df = build_dropped_list(scope, enriched_df, subtype_to_type, delivered_uuids)
    dropped_by_ct = {}
    if not dropped_list_df.empty:
        dropped_by_ct = {str(k): int(v) for k, v in dropped_list_df.groupby("changeType").size().items()}

    # Malformed events are NOT added to `records` — the user asked for them
    # to show up in the Sankey only, not clutter the record-level table
    # (they carry no faculty/title/subtype information worth a row anyway).
    records = build_records(scope, enriched_df, authors_df, deletes_df, subtype_to_type, delivered_pairs)
    subtypes_list = build_subtypes(scope, delivered_df, enriched_df)

    # --- reconciliation identities ---
    recon_ok = True
    for ct in ("CREATE", "UPDATE"):
        recv, enr = received_by_ct.get(ct, 0), enriched_by_ct.get(ct, 0)
        drp, dlv = dropped_by_ct.get(ct, 0), distinct_delivered_by_ct.get(ct, 0)
        recon_ok = _check(f"received==enriched ({ct})", recv, enr) and recon_ok
        recon_ok = _check(f"enriched==dropped+delivered ({ct})", enr, drp + dlv) and recon_ok
    recon_ok = _check("received(DELETE)==deletes_delivered", received_by_ct.get("DELETE", 0), deletes_delivered) and recon_ok

    # --- persist Part 4's own tables ---
    metrics_rows = []

    def add_metric(metric, dimension, value):
        metrics_rows.append({"scope": scope, "metric": metric, "dimension": dimension, "value": value})

    for ct, n in received_by_ct.items():
        add_metric("received", ct, n)                                    # 1
    for ct, n in enriched_by_ct.items():
        add_metric("enriched", ct, n)                                    # 2
    add_metric("faculty_match_rate", "(n/a)", match_rate)                # 3
    for ct, n in distinct_delivered_by_ct.items():
        add_metric("distinct_delivered", ct, n)                          # 4
    for ct, n in delivered_rows_by_ct.items():
        add_metric("delivered_rows", ct, n)                              # 5
    for ct in ("CREATE", "UPDATE"):
        add_metric("silently_dropped", ct, dropped_by_ct.get(ct, 0))     # 6
    add_metric("deletes_delivered", "(n/a)", deletes_delivered)          # 7
    add_metric("collaborators_exported", "(n/a)", collaborators_exported)  # 8

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_df["value"] = pd.to_numeric(metrics_df["value"], errors="coerce")
    upsert_scope_table(metrics_df, f"dashboard_metrics_{CURRENT_DAY}", scope)

    # dashboard_summary_detail_<date> — raw per-type/status counts
    summary_rows = []
    if not enriched_df.empty:
        tmp = enriched_df[enriched_df["changeType"] != "DELETE"].copy()
        if scope == "research_output":
            tmp["resolved_type"] = tmp["subtype"].map(lambda s: subtype_to_type.get(s, s))
        else:
            tmp["resolved_type"] = "Award"
        for (rt, ct), n in tmp.groupby(["resolved_type", "changeType"]).size().items():
            summary_rows.append({"scope": scope, "status": STATUS_MAP.get(ct, ct), "subtype": rt, "count": int(n)})
    if deletes_delivered:
        summary_rows.append({"scope": scope, "status": "delete", "subtype": "(n/a)", "count": deletes_delivered})

    summary_detail_df = pd.DataFrame(summary_rows, columns=["scope", "status", "subtype", "count"])
    upsert_scope_table(summary_detail_df, f"dashboard_summary_detail_{CURRENT_DAY}", scope)

    # dashboard_records_<date> — persisted form of the record-level table
    records_table_rows = []
    for r in records:
        ev_code = {"new": "CREATE", "updated": "UPDATE", "deleted": "DELETE"}.get(r["event"], r["event"])
        records_table_rows.append({
            "scope": scope,
            "faculty_member": r["name"],
            "faculty_id": r["fid"],
            "pure_record": r["title"] if not r["titleMissing"] else "",
            "pure_uuid": r["uuid"],
            "subtype_pure": r["subtypePure"],
            "subtype_far": r["subtypeFar"],
            "event_type": EVENT_LABEL.get(ev_code, ev_code),
            "outcome": OUTCOME_LABEL[r["outcome"]],
        })
    records_df = pd.DataFrame(
        records_table_rows,
        columns=["scope", "faculty_member", "faculty_id", "pure_record", "pure_uuid",
                 "subtype_pure", "subtype_far", "event_type", "outcome"],
    )
    upsert_scope_table(records_df, f"dashboard_records_{CURRENT_DAY}", scope)

    # dashboard_<scope>_dropped_<date> — scope in the name, no upsert needed
    if not dropped_list_df.empty:
        safe_save_table(spark, logger, dropped_list_df.assign(scope=scope), f"dashboard_{scope}_dropped_{CURRENT_DAY}")
    else:
        logger.info("No silently-dropped records for %s today — no dashboard_%s_dropped table.", scope, scope)

    # dashboard_<scope>_malformed_<date> — only created when it actually
    # happens (see split_malformed_events); scope in the name, no upsert needed.
    if not malformed_df.empty:
        safe_save_table(spark, logger, malformed_df.assign(scope=scope), f"dashboard_{scope}_malformed_{CURRENT_DAY}")

    # --- coverage window (see config.py) ---
    coverage_start = compute_coverage_start(scope)
    coverage_end = RUN_TS.strftime("%Y-%m-%d")
    logger.info("Coverage window for %s: %s -> %s", scope, coverage_start, coverage_end)

    # --- render + upload ---
    faculty_affected = len({r["fid"] for r in records if r["fid"]})

    context = {
        "client_name": "Ajman",
        "scope_label": cfg["report_scope_label"],
        "eyebrow": f"{cfg['report_scope_label']} · Pure → FAR",
        "delivery_date": coverage_end,
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
        "faculty_affected": faculty_affected,
        "received": {k: received_by_ct.get(k, 0) for k in ("CREATE", "UPDATE", "DELETE")},
        "dropped": {k: dropped_by_ct.get(k, 0) for k in ("CREATE", "UPDATE")},
        "deletes_delivered": deletes_delivered,
        "match_rate": match_rate,
        "subtypes": subtypes_list,
        "records": records,
        "malformed_count": len(malformed_df),
    }

    report_html = render_report_html(context, scope)
    report_filename = f"Ajman_{scope}_changes_report_{coverage_end}.html"
    remote_path = upload_html_to_sftp(report_html, sftp_folder, report_filename)
    logger.info("Uploaded %s dashboard report to %s", scope, remote_path)

    # --- record the delivery (append-only), only after a successful upload ---
    spark.createDataFrame(
        [(scope, RUN_TS, date.today())],
        schema="scope STRING, delivered_at TIMESTAMP, run_date DATE",
    ).write.mode("append").option("mergeSchema", "true").saveAsTable(f"{DATABASE}.{DELIVERY_LOG_TABLE}")
    logger.info("Appended delivery-log row for %s (delivered_at=%s)", scope, RUN_TS)

    print(f"scope={scope}  coverage {coverage_start} -> {coverage_end}")
    print(metrics_df.to_string(index=False))
    if not dropped_list_df.empty:
        print(f"\n{len(dropped_list_df)} record(s) silently dropped (no internal author resolved):")
        print(dropped_list_df.to_string(index=False))
    if not malformed_df.empty:
        print(f"\n{len(malformed_df)} malformed /changes event(s) detected today (see ERROR logs above):")
        print(malformed_df.to_string(index=False))
    if not recon_ok:
        print(f"\n*** [{scope}] one or more reconciliation identities did NOT close — see WARNING logs above ***")

# COMMAND ----------

for scope in scopes_to_run:
    run_scope(scope)
