# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 — Client dashboard report (Grants)
# MAGIC Renders the client-facing HTML execution report for the **Grants**
# MAGIC scope and uploads it next to the CSV exports on SFTP, and uploads it
# MAGIC to SFTP. Reads the 2 tables `postprocess_changes.py` already persists
# MAGIC at the end of its run (same `CURRENT_DAY`, same pipeline execution):
# MAGIC `dashboard_metrics_<CURRENT_DAY>` (curated KPIs, long format) and
# MAGIC `dashboard_grants_dropped_<CURRENT_DAY>` (silently-dropped grants
# MAGIC list). See `project_hbku_dev_dashboard_metrics_20260814` in this
# MAGIC repo's memory for how those 2 tables are built.
# MAGIC
# MAGIC **Grants is the simple case**: this repo is the only data source for
# MAGIC it (new + update + delete, no `tss-dedup` dependency — see
# MAGIC `project_dashboard_feature_architecture.md`'s 2026-08-14 scoping
# MAGIC correction). Research Output / Custom Sections are explicitly **out of
# MAGIC scope here** — their combined dedup+changes report is a separate,
# MAGIC future piece of work that also needs to read `tss-dedup`'s
# MAGIC `dashboard_run_summary` table.
# MAGIC
# MAGIC **Design**: CSS variables/classes are ported 1:1 from the approved
# MAGIC mockup (https://claude.ai/code/artifact/36b5279f-2b02-4345-ae85-329715e37301,
# MAGIC built with the `dataviz` skill's validated default palette — not
# MAGIC re-validated here since the colors are unchanged, only which
# MAGIC components are used). Per that mockup's own spec-notes for a
# MAGIC Grants-only job: no dedup section (no match-score histogram, no
# MAGIC borderline-matches table), and the volume card is New/Updated/Deleted
# MAGIC instead of Deletes-only. The rendered report is 100% English —
# MAGIC client-facing deliverable, distinct from this repo's Spanish-first
# MAGIC internal conventions.
# MAGIC
# MAGIC `render_grants_report_html` below has no Spark/`dbutils` dependency —
# MAGIC it's plain Python + `html.escape`, so it can be unit-tested with
# MAGIC synthetic dicts/lists outside Databricks, same as the metric helpers
# MAGIC in `postprocess_changes.py`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../sftp_utils

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_far_templates

# COMMAND ----------

import html as html_lib
import logging
import sys
from datetime import datetime, timezone

import pandas as pd

# Same fix as postprocess_changes.py / enrich_changes.py — logging.basicConfig()
# doesn't work in this workspace, so the handler is wired up by hand.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# Same SCOPE widget as the rest of the Part 3 chain. This notebook is
# %run'd from postprocess_changes.py *after* its Grants section and its
# "Dashboard metrics" section have both run (that's what creates the 2
# source tables read below), but the widget is declared here too so this
# notebook stays independently runnable for testing.
dbutils.widgets.text("SCOPE", "ALL", "Scope to run (or ALL)")
scope_widget = dbutils.widgets.get("SCOPE")
run_grants = scope_widget in ("ALL", "Grants")

# COMMAND ----------

CHANGE_TYPE_LABELS = {"CREATE": "New", "UPDATE": "Updated", "DELETE": "Deleted"}
STATUS_LABELS = {"new": "New", "updates": "Updated"}

REPORT_CSS = """
  .viz-root {
    color-scheme: light;
    --surface-1:      #fcfcfb;
    --page:           #f9f9f7;
    --text-primary:   #0b0b0b;
    --text-secondary: #52514e;
    --text-muted:     #898781;
    --grid:           #e1e0d9;
    --border:         rgba(11,11,11,0.10);
    --series-1:       #2a78d6;
    --status-good:      #0ca30c;
    --status-good-text: #006300;
    --status-warning:   #fab219;
  }
  @media (prefers-color-scheme: dark) {
    .viz-root {
      color-scheme: dark;
      --surface-1:      #1a1a19;
      --page:           #0d0d0d;
      --text-primary:   #ffffff;
      --text-secondary: #c3c2b7;
      --text-muted:     #898781;
      --grid:           #2c2c2a;
      --border:         rgba(255,255,255,0.10);
      --series-1:       #3987e5;
      --status-good:      #0ca30c;
      --status-good-text: #0ca30c;
      --status-warning:   #fab219;
    }
  }

  * { box-sizing: border-box; }
  body { background: var(--page); margin: 0; }
  .viz-root {
    background: var(--page);
    color: var(--text-primary);
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    max-width: 1040px;
    margin: 0 auto;
    padding: 28px 20px 64px;
  }

  .report-header {
    display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between;
    gap: 16px; padding-bottom: 20px; border-bottom: 1px solid var(--border); margin-bottom: 28px;
  }
  .report-header h1 { font-size: 22px; font-weight: 600; margin: 0 0 6px; }
  .report-header .meta { color: var(--text-secondary); font-size: 13px; line-height: 1.6; }
  .report-header .meta b { color: var(--text-primary); font-weight: 600; }
  .badge {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 6px 12px; border-radius: 999px; font-size: 13px; font-weight: 600;
    white-space: nowrap;
  }
  .badge .dot { width: 8px; height: 8px; border-radius: 50%; flex: none; }
  .badge.status-good {
    background: color-mix(in srgb, var(--status-good) 16%, transparent);
    border: 1px solid color-mix(in srgb, var(--status-good) 45%, transparent);
  }
  .badge.status-good .dot { background: var(--status-good); }
  .badge.status-warning {
    background: color-mix(in srgb, var(--status-warning) 16%, transparent);
    border: 1px solid color-mix(in srgb, var(--status-warning) 45%, transparent);
  }
  .badge.status-warning .dot { background: var(--status-warning); }

  section.card {
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px;
    padding: 20px 24px 24px; margin-bottom: 20px;
  }
  section.card h2 { font-size: 15px; font-weight: 600; margin: 0 0 2px; }
  section.card .subtitle { color: var(--text-secondary); font-size: 13px; margin: 0 0 18px; }

  .stat-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 8px; }
  .stat-tile { background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px; padding: 16px 18px; }
  .stat-tile .label { font-size: 12.5px; color: var(--text-secondary); margin-bottom: 8px; }
  .stat-tile .value { font-size: 26px; font-weight: 600; color: var(--text-primary); }
  .stat-tile .sub { font-size: 12px; color: var(--text-muted); margin-top: 4px; }
  @media (max-width: 720px) { .stat-row { grid-template-columns: repeat(2, 1fr); } }

  .bar-chart-wrap { display: flex; flex-direction: column; gap: 10px; }
  .bar-row { display: grid; grid-template-columns: 168px 1fr 56px; align-items: center; gap: 10px; }
  .bar-row .cat { font-size: 13px; color: var(--text-secondary); text-align: right; }
  .bar-track { position: relative; height: 24px; background: var(--grid); border-radius: 4px; overflow: hidden; }
  .bar-fill { position: absolute; left: 0; top: 0; bottom: 0; background: var(--series-1); border-radius: 4px; }
  .bar-row .val { font-size: 13px; color: var(--text-primary); font-variant-numeric: tabular-nums; }

  table.data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.data-table th {
    text-align: left; font-weight: 600; color: var(--text-secondary);
    font-size: 12px; padding: 8px 10px; border-bottom: 1px solid var(--grid);
  }
  table.data-table td {
    padding: 9px 10px; border-bottom: 1px solid var(--grid);
    color: var(--text-primary); font-variant-numeric: tabular-nums;
  }
  table.data-table td.mono, span.mono { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; color: var(--text-secondary); }
  table.data-table tr:last-child td { border-bottom: none; }
  .tag {
    display: inline-flex; align-items: center; gap: 5px;
    font-size: 11.5px; font-weight: 600; padding: 3px 8px; border-radius: 999px;
    background: color-mix(in srgb, var(--status-warning) 18%, transparent);
    color: var(--text-primary);
  }
  .tag .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--status-warning); }

  details.table-toggle { margin-top: 12px; }
  details.table-toggle summary { cursor: pointer; font-size: 12.5px; color: var(--text-secondary); list-style: none; }
  details.table-toggle summary::-webkit-details-marker { display: none; }
  details.table-toggle summary:before { content: "View as table  \\203A"; }
  details.table-toggle[open] summary:before { content: "Hide table  \\2304"; }
  details.table-toggle .data-table { margin-top: 10px; }

  .callout-row { display: grid; grid-template-columns: 1fr; gap: 14px; }
  .callout {
    border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px;
    display: flex; align-items: center; justify-content: space-between;
  }
  .callout .callout-label { font-size: 12.5px; color: var(--text-secondary); }
  .callout .callout-value { font-size: 18px; font-weight: 600; }
  .callout .callout-value.good { color: var(--status-good-text); }

  footer.report-footer {
    margin-top: 8px; padding-top: 16px; border-top: 1px solid var(--border);
    font-size: 11.5px; color: var(--text-muted); text-align: center;
  }
"""

# COMMAND ----------

def _fmt_int(value) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


def _fmt_rate(value) -> str:
    """None (no internal participants that day) renders as "n/a", never "0%" -- see faculty_match_rate()'s docstring in postprocess_changes.py for why that distinction matters."""
    if value is None:
        return "n/a"
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if rate != rate:  # NaN
        return "n/a"
    return f"{rate * 100:.1f}%"


def _has_real_title(title) -> bool:
    """
    grants_merge.py fills a missing title with the literal string "None"
    (a deliberate placeholder, not a NaN) when an Award-only change has no
    linked Project resolved -- see project_hbku_qa_dashboard_grants_dropped_title_20260814
    in this repo's memory. Treated the same as an actually-missing title here.
    """
    if title is None:
        return False
    text = str(title).strip()
    return text != "" and text.lower() not in ("none", "nan")


def _stat_tile(label: str, value: str, sub: str) -> str:
    return (
        '<div class="stat-tile">'
        f'<div class="label">{html_lib.escape(label)}</div>'
        f'<div class="value">{html_lib.escape(value)}</div>'
        f'<div class="sub">{html_lib.escape(sub)}</div>'
        "</div>"
    )

# COMMAND ----------

def render_grants_report_html(
    client_name: str,
    run_date: str,
    report_generated: str,
    received_by_type: dict,
    enriched_count,
    match_rate,
    delivered_by_status: dict,
    distinct_delivered,
    dropped_rows: list,
    deletes_delivered,
) -> str:
    """
    Builds the full standalone HTML document for the Grants client
    dashboard report. `received_by_type` keys are Pure changeType codes
    (CREATE/UPDATE/DELETE); `delivered_by_status` keys are "new"/"updates"
    (dashboard_metrics_<date>'s "delivered" dimension, see G4 in
    postprocess_changes.py). `dropped_rows` is dashboard_grants_dropped_<date>
    as a list of {"uuid", "changeType", "title"} dicts.
    """
    total_received = sum(received_by_type.values()) if received_by_type else 0
    has_warnings = len(dropped_rows) > 0
    badge_label = "Completed with warnings" if has_warnings else "Completed"
    badge_class = "status-warning" if has_warnings else "status-good"

    received_sub = " · ".join(
        f"{_fmt_int(received_by_type.get(code, 0))} {label.lower()}"
        for code, label in CHANGE_TYPE_LABELS.items()
    )
    delivered_sub = " · ".join(
        f"{_fmt_int(delivered_by_status.get(dim, 0))} {label.lower()}"
        for dim, label in STATUS_LABELS.items()
    )
    match_rate_sub = (
        "No internal participants today" if match_rate is None
        else "Internal participants resolved to a Faculty ID"
    )

    stat_tiles = "".join([
        _stat_tile("Grants received", _fmt_int(total_received), received_sub),
        _stat_tile("Grants enriched", _fmt_int(enriched_count), "Passed enrichment, ready for FAR matching"),
        _stat_tile("Distinct grants delivered", _fmt_int(distinct_delivered), delivered_sub),
        _stat_tile("Faculty match rate", _fmt_rate(match_rate), match_rate_sub),
    ])

    max_received = max(received_by_type.values(), default=0) if received_by_type else 0
    bar_rows_html = []
    volume_table_rows = []
    for code, label in CHANGE_TYPE_LABELS.items():
        count = received_by_type.get(code, 0)
        width_pct = round(count / max_received * 100) if max_received else 0
        bar_rows_html.append(
            f'<div class="bar-row"><div class="cat">{html_lib.escape(label)}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{width_pct}%"></div></div>'
            f'<div class="val">{_fmt_int(count)}</div></div>'
        )
        volume_table_rows.append(f"<tr><td>{html_lib.escape(label)}</td><td>{_fmt_int(count)}</td></tr>")

    if dropped_rows:
        dropped_row_html = []
        for row in dropped_rows:
            uuid_val = str(row.get("uuid") or "")
            raw_change_type = row.get("changeType")
            change_type_label = CHANGE_TYPE_LABELS.get(raw_change_type, raw_change_type or "")
            title = row.get("title")
            if _has_real_title(title):
                grant_cell = html_lib.escape(str(title))
            else:
                grant_cell = (
                    f'<span class="mono">{html_lib.escape(uuid_val)}</span> '
                    '<span class="tag"><span class="dot"></span>Title unavailable</span>'
                )
            dropped_row_html.append(
                f"<tr><td>{grant_cell}</td><td>{html_lib.escape(change_type_label)}</td>"
                f'<td class="mono">{html_lib.escape(uuid_val)}</td></tr>'
            )
        dropped_section_html = (
            '<table class="data-table">'
            "<thead><tr><th>Grant</th><th>Change type</th><th>Pure UUID</th></tr></thead>"
            f'<tbody>{"".join(dropped_row_html)}</tbody>'
            "</table>"
        )
    else:
        dropped_section_html = (
            '<div class="callout">'
            '<span class="callout-label">No grants were silently dropped today</span>'
            '<span class="callout-value good">0</span>'
            "</div>"
        )

    deletes_section_html = ""
    if deletes_delivered:
        deletes_section_html = f"""
  <section class="card">
    <h2>Deletions</h2>
    <p class="subtitle">Grants removed in Pure and retracted from Faculty180, this run</p>
    <div class="callout-row">
      <div class="callout">
        <span class="callout-label">Deletions delivered</span>
        <span class="callout-value">{_fmt_int(deletes_delivered)}</span>
      </div>
    </div>
  </section>"""

    client_esc = html_lib.escape(client_name)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{client_esc} — Grants — Execution Report</title>
<style>{REPORT_CSS}</style>
</head>
<body>
<div class="viz-root">

  <div class="report-header">
    <div>
      <h1>{client_esc} — Grants — Execution Report</h1>
      <div class="meta">
        <b>Run date:</b> {html_lib.escape(run_date)} &nbsp;·&nbsp; <b>Report generated:</b> {html_lib.escape(report_generated)}<br>
        <b>Job:</b> Grants pipeline (changes only — no dedup step) &nbsp;·&nbsp; <b>Pipeline steps:</b> Pure Changes Endpoint → enrichment → Faculty180 upload
      </div>
    </div>
    <span class="badge {badge_class}"><span class="dot"></span>{badge_label}</span>
  </div>

  <div class="stat-row">{stat_tiles}</div>

  <section class="card">
    <h2>Grants received by change type</h2>
    <p class="subtitle">All grant changes detected in Pure this run</p>
    <div class="bar-chart-wrap" role="img" aria-label="Grants received by change type">
      {"".join(bar_rows_html)}
    </div>
    <details class="table-toggle">
      <summary></summary>
      <table class="data-table">
        <thead><tr><th>Change type</th><th>Grants</th></tr></thead>
        <tbody>{"".join(volume_table_rows)}</tbody>
      </table>
    </details>
  </section>

  <section class="card">
    <h2>Grants dropped — no internal author resolved</h2>
    <p class="subtitle">Changed grants that did not reach Faculty180 because no internal participant could be matched to a Faculty ID</p>
    {dropped_section_html}
  </section>
{deletes_section_html}
  <footer class="report-footer">
    Generated automatically from the {client_esc} Pure → FAR migration pipeline. For questions about this report, contact your Elsevier project team.
  </footer>

</div>
</body>
</html>"""

# COMMAND ----------

def read_table(table_name: str) -> pd.DataFrame:
    full_table_name = f"{DATABASE}.{table_name}"
    try:
        df = spark.table(full_table_name).toPandas()
        logger.info("Read %d rows from %s", len(df), full_table_name)
        return df
    except Exception:
        logger.info("Table %s not found — treating as empty.", full_table_name)
        return pd.DataFrame()


def _metric_value(df: pd.DataFrame, metric: str, dimension: str = "(n/a)"):
    if df.empty or "metric" not in df.columns:
        return None
    match = df[(df["metric"] == metric) & (df["dimension"] == dimension)]
    if match.empty:
        return None
    value = match.iloc[0]["value"]
    return None if pd.isna(value) else value


def _metric_by_dimension(df: pd.DataFrame, metric: str) -> dict:
    if df.empty or "metric" not in df.columns:
        return {}
    match = df[df["metric"] == metric]
    return {row["dimension"]: row["value"] for _, row in match.iterrows() if pd.notna(row["value"])}


def upload_html_to_sftp(
    html_content: str, base_remote_dir: str, scope_folder: str, filename: str, logger,
    secret_scope: str = "sftp_scope",
) -> str:
    """
    Uploads the report to {base_remote_dir}/{scope_folder}/reports/{filename}
    -- same SFTP server/base path as the CSV exports (`_connect_sftp` /
    `_ensure_remote_dir` reused from sftp_utils.py, already in this
    notebook's namespace via %run above), new "reports/" subfolder decided
    alongside new/updates/deletes (see project_dashboard_feature_architecture.md).
    No old_files archiving here (unlike upload_df_to_sftp) -- the filename
    is date-stamped, so each day's report already has its own unique path.
    """
    remote_dir = f"{base_remote_dir}/{scope_folder}/reports"
    remote_path = f"{remote_dir}/{filename}"

    client = _connect_sftp(secret_scope)
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

if run_grants:
    metrics_df = read_table(f"dashboard_metrics_{CURRENT_DAY}")
    grants_metrics_df = metrics_df[metrics_df["scope"] == "grants"] if not metrics_df.empty else metrics_df

    dropped_df = read_table(f"dashboard_grants_dropped_{CURRENT_DAY}")
    dropped_rows = dropped_df.to_dict("records") if not dropped_df.empty else []

    report_html = render_grants_report_html(
        client_name="HBKU",
        run_date=f"{YEAR}-{MONTH}-{DAY}",
        report_generated=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        received_by_type=_metric_by_dimension(grants_metrics_df, "received"),
        enriched_count=_metric_value(grants_metrics_df, "enriched") or 0,
        match_rate=_metric_value(grants_metrics_df, "faculty_match_rate"),
        delivered_by_status=_metric_by_dimension(grants_metrics_df, "delivered"),
        distinct_delivered=_metric_value(grants_metrics_df, "distinct_delivered") or 0,
        dropped_rows=dropped_rows,
        deletes_delivered=_metric_value(grants_metrics_df, "deletes_delivered") or 0,
    )

    report_filename = f"HBKU_grants_report_{YEAR}-{MONTH}-{DAY}.html"
    remote_path = upload_html_to_sftp(
        report_html, SFTP_BASE, FAR_TEMPLATES_CONFIG["Grants"]["sftp_folder"], report_filename, logger,
        secret_scope=SFTP_SECRET_SCOPE,
    )
    logger.info("Uploaded Grants client dashboard report to %s", remote_path)
else:
    logger.info("Skipping Grants dashboard report — SCOPE=%s", scope_widget)
