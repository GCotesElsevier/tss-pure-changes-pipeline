# Databricks notebook source
# MAGIC %md
# MAGIC # Part 4 — Build client dashboard (Ajman)
# MAGIC Runs **live, right after a normal changes run** (Part 1 → Part 2 →
# MAGIC Part 3), over the **same `CURRENT_DAY`**. Manual sequence:
# MAGIC `SCOPE=research_output`, then `SCOPE=grants`.
# MAGIC
# MAGIC Part 4 only ever **reads** the tables Parts 1-3 already wrote for today
# MAGIC (`changes_<scope>_<date>`, `enriched_<scope>_<date>`,
# MAGIC `enriched_<scope>_authors_<date>`, `enriched_<scope>_deletes_<date>`,
# MAGIC `far_results_<type>_<date>`, `far_collaborators_<type>_<date>`) — it
# MAGIC never writes to or modifies anything of theirs. It writes its own
# MAGIC `dashboard_*` tables and uploads one HTML report per scope to
# MAGIC `{SFTP_BASE}/{sftp_folder}/reports/`.
# MAGIC
# MAGIC What it produces per run:
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

dbutils.widgets.text("SCOPE", "research_output", "Scope to build (research_output | grants)")
scope = dbutils.widgets.get("SCOPE").strip()

if scope not in SCOPE_CONFIG:
    raise ValueError(
        f"SCOPE={scope!r} is not valid — expected one of {list(SCOPE_CONFIG)}. "
        "Part 4 runs one scope at a time, after the changes run for that same day."
    )

cfg = SCOPE_CONFIG[scope]
far_cfg = FAR_TEMPLATES_CONFIG[cfg["far_templates_key"]]
subtype_to_type = far_cfg.get("subtype_to_type", {})  # Grants has none — single "Award" type
sftp_folder = far_cfg["sftp_folder"]

logger.info("=== Part 4 dashboard: scope=%s, CURRENT_DAY=%s ===", scope, CURRENT_DAY)

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

# COMMAND ----------

# --- READ-ONLY reads of Part 1/2/3 output for today ---
changes_df = read_table(f"{cfg['changes_table']}_{CURRENT_DAY}")
enriched_df = read_table(f"{cfg['enriched_table']}_{CURRENT_DAY}")
authors_df = read_table(f"{cfg['enriched_authors_table']}_{CURRENT_DAY}")
deletes_df = read_table(f"{cfg['enriched_deletes_table']}_{CURRENT_DAY}")

# far_results_<slug>_<date> / far_collaborators_<slug>_<date>, one per FAR type
slug_to_type = {_type_slug(t): t for t in far_cfg["types"]}
far_results = {slug: read_table(f"far_results_{slug}_{CURRENT_DAY}") for slug in cfg["far_result_slugs"]}
far_collaborators = {slug: read_table(f"far_collaborators_{slug}_{CURRENT_DAY}") for slug in cfg["far_result_slugs"]}

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


def build_delivered_frame() -> pd.DataFrame:
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


delivered_df = build_delivered_frame()
delivered_uuids = set(delivered_df["uuid_output"].dropna().map(str)) if not delivered_df.empty else set()
delivered_pairs = set()
if not delivered_df.empty:
    for _, r in delivered_df.iterrows():
        delivered_pairs.add((str(r["uuid_output"]), _clean_fid(r.get("faculty_id"))))

# COMMAND ----------

received_by_ct = _counts_by_change_type(changes_df, "changeType")
enriched_by_ct = _counts_by_change_type(enriched_df, "changeType")

# distinct records delivered, per changeType (nunique on uuid_output so it
# reconciles against enriched, which is keyed by uuid)
distinct_delivered_by_ct = {}
delivered_rows_by_ct = {}
if not delivered_df.empty and "changetype" in delivered_df.columns:
    grp = delivered_df.dropna(subset=["changetype"])
    distinct_delivered_by_ct = {
        str(k): int(v) for k, v in grp.groupby("changetype")["uuid_output"].nunique().items()
    }
    delivered_rows_by_ct = {str(k): int(v) for k, v in grp.groupby("changetype").size().items()}

deletes_delivered = int(len(deletes_df))
collaborators_exported = int(sum(len(df) for df in far_collaborators.values()))
match_rate = faculty_match_rate(authors_df)

# COMMAND ----------

# --- silently dropped: non-delete records that produced no FAR row at all ---
def build_dropped_list() -> pd.DataFrame:
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
        dropped["subtype_pure"] = "Award"
        dropped["subtype_far"] = "Award"
    if "title" not in dropped.columns:
        dropped["title"] = None
    return dropped[cols].drop_duplicates(subset="uuid").reset_index(drop=True)


dropped_list_df = build_dropped_list()
dropped_by_ct = {}
if not dropped_list_df.empty:
    dropped_by_ct = {str(k): int(v) for k, v in dropped_list_df.groupby("changeType").size().items()}

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


def _full_name(first, last) -> str:
    parts = [str(x).strip() for x in (first, last) if x is not None and str(x).strip() not in ("", "nan", "None")]
    return " ".join(parts)


def build_records() -> list:
    records = []

    if not enriched_df.empty:
        base = enriched_df[["uuid", "changeType"]].copy()
        base["title"] = enriched_df["title"] if "title" in enriched_df.columns else None
        if scope == "research_output":
            base["subtype_pure"] = enriched_df["subtype"]
            base["subtype_far"] = base["subtype_pure"].map(lambda s: _far_display(subtype_to_type.get(s, s)))
        else:
            base["subtype_pure"] = "Award"
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


records = build_records()

# COMMAND ----------

# --- subtype breakdown for the bar chart / donut / Sankey leaves ---
def build_subtypes() -> list:
    if delivered_df.empty or "changetype" not in delivered_df.columns:
        return []
    deliv = delivered_df.dropna(subset=["changetype"])[["uuid_output", "changetype", "far_type"]].drop_duplicates()
    if scope == "research_output" and not enriched_df.empty:
        sub_lookup = enriched_df[["uuid", "subtype"]].rename(columns={"uuid": "uuid_output"})
        deliv = deliv.merge(sub_lookup, on="uuid_output", how="left")
        deliv["subtype"] = deliv["subtype"].fillna(deliv["far_type"])
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


subtypes_list = build_subtypes()

# COMMAND ----------

# --- reconciliation identities ---
def _check(label, left, right):
    if left != right:
        logger.warning("RECONCILIATION MISMATCH [%s]: %s != %s", label, left, right)
        return False
    logger.info("Reconciliation OK [%s]: %s == %s", label, left, right)
    return True


recon_ok = True
for ct in ("CREATE", "UPDATE"):
    recv = received_by_ct.get(ct, 0)
    enr = enriched_by_ct.get(ct, 0)
    drp = dropped_by_ct.get(ct, 0)
    dlv = distinct_delivered_by_ct.get(ct, 0)
    recon_ok &= _check(f"received==enriched ({ct})", recv, enr)
    recon_ok &= _check(f"enriched==dropped+delivered ({ct})", enr, drp + dlv)
recon_ok &= _check("received(DELETE)==deletes_delivered", received_by_ct.get("DELETE", 0), deletes_delivered)

# COMMAND ----------

# --- persist Part 4's own tables ---
def upsert_scope_table(df: pd.DataFrame, table_name: str) -> None:
    """
    dashboard_metrics_/dashboard_summary_detail_/dashboard_records_ are
    shared by both scopes (Part 4 runs one scope at a time). Replace only
    this scope's rows, keep the other scope's.
    """
    full = f"{DATABASE}.{table_name}"
    existing = read_table(table_name)
    kept = existing[existing["scope"] != scope] if (not existing.empty and "scope" in existing.columns) else pd.DataFrame()
    combined = pd.concat([kept, df], ignore_index=True) if not df.empty else kept
    if combined.empty:
        logger.info("Nothing to persist for %s (scope=%s) — skipping.", full, scope)
        return
    safe_save_table(spark, logger, combined, full)


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
upsert_scope_table(metrics_df, f"dashboard_metrics_{CURRENT_DAY}")

# COMMAND ----------

# dashboard_summary_detail_<date> — raw per-type/status counts (the number
# Ajman's postprocess_changes.py only ever print()s)
STATUS_MAP = {"CREATE": "new", "UPDATE": "update", "DELETE": "delete"}
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
upsert_scope_table(summary_detail_df, f"dashboard_summary_detail_{CURRENT_DAY}")

# COMMAND ----------

# dashboard_records_<date> — persisted form of the record-level table
records_table_rows = []
for r in records:
    ev_code = {"new": "CREATE", "updated": "UPDATE", "deleted": "DELETE"}[r["event"]]
    records_table_rows.append({
        "scope": scope,
        "faculty_member": r["name"],
        "faculty_id": r["fid"],
        "pure_record": r["title"] if not r["titleMissing"] else "",
        "pure_uuid": r["uuid"],
        "subtype_pure": r["subtypePure"],
        "subtype_far": r["subtypeFar"],
        "event_type": EVENT_LABEL[ev_code],
        "outcome": OUTCOME_LABEL[r["outcome"]],
    })
records_df = pd.DataFrame(
    records_table_rows,
    columns=["scope", "faculty_member", "faculty_id", "pure_record", "pure_uuid",
             "subtype_pure", "subtype_far", "event_type", "outcome"],
)
upsert_scope_table(records_df, f"dashboard_records_{CURRENT_DAY}")

# dashboard_<scope>_dropped_<date> — scope in the name, no upsert needed
if not dropped_list_df.empty:
    safe_save_table(spark, logger, dropped_list_df.assign(scope=scope), f"dashboard_{scope}_dropped_{CURRENT_DAY}")
else:
    logger.info("No silently-dropped records for %s today — no dashboard_%s_dropped table.", scope, scope)

# COMMAND ----------

# --- coverage window (see config.py) ---
def compute_coverage_start() -> str:
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


coverage_start = compute_coverage_start()
coverage_end = RUN_TS.strftime("%Y-%m-%d")
logger.info("Coverage window for %s: %s -> %s", scope, coverage_start, coverage_end)

# COMMAND ----------

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
}

report_html = render_report_html(context, scope)


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


report_filename = f"Ajman_{scope}_changes_report_{coverage_end}.html"
remote_path = upload_html_to_sftp(report_html, sftp_folder, report_filename)
logger.info("Uploaded %s dashboard report to %s", scope, remote_path)

# COMMAND ----------

# --- record the delivery (append-only), only after a successful upload ---
spark.createDataFrame(
    [(scope, RUN_TS, date.today())],
    schema="scope STRING, delivered_at TIMESTAMP, run_date DATE",
).write.mode("append").option("mergeSchema", "true").saveAsTable(f"{DATABASE}.{DELIVERY_LOG_TABLE}")
logger.info("Appended delivery-log row for %s (delivered_at=%s)", scope, RUN_TS)

# COMMAND ----------

print(f"scope={scope}  coverage {coverage_start} -> {coverage_end}")
print(metrics_df.to_string(index=False))
if not dropped_list_df.empty:
    print(f"\n{len(dropped_list_df)} record(s) silently dropped (no internal author resolved):")
    print(dropped_list_df.to_string(index=False))
if not recon_ok:
    print("\n*** one or more reconciliation identities did NOT close — see WARNING logs above ***")
