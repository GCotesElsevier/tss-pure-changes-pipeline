# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 — Client dashboard report
# MAGIC Renders the client-facing HTML execution report for each scope and
# MAGIC uploads it next to that scope's CSV exports on SFTP. Two independent
# MAGIC pieces, added in 2 phases:
# MAGIC
# MAGIC **Grants** (phase 1, `render_grants_report_html`): this repo is the
# MAGIC only data source (new + update + delete, no `tss-dedup` dependency —
# MAGIC see `project_dashboard_feature_architecture.md`'s 2026-08-14 scoping
# MAGIC correction). Reads `dashboard_metrics_<CURRENT_DAY>` (curated KPIs,
# MAGIC long format) and `dashboard_grants_dropped_<CURRENT_DAY>`
# MAGIC (silently-dropped grants list) — both persisted by
# MAGIC `postprocess_changes.py`, see `project_hbku_dev_dashboard_metrics_20260814`
# MAGIC in this repo's memory.
# MAGIC
# MAGIC **Scholarly Activities / Custom Sections** (phase 2,
# MAGIC `render_ro_cs_report_html`): combines this repo's own deletes-only data
# MAGIC with `tss-dedup`'s matching/dedup instrumentation
# MAGIC (`dashboard_run_summary` for Scholarly Activities,
# MAGIC `custom_dashboard_run_summary` for Custom Sections) — both tables live
# MAGIC in the SAME Databricks catalog/schema this repo already reads/writes
# MAGIC (`academicinformationsystems_technicalservices.hbku`), so it's a plain
# MAGIC same-catalog Spark read, no cross-repo plumbing. See
# MAGIC `project_hbku_dashboard_ro_cs_renderer_20260814` for the exact schema
# MAGIC assumptions (scope label matching, `run_date` format, per-`type` grain)
# MAGIC that still need validating against a real `tss-dedup` row — none of the
# MAGIC 3 recurring Jobs exist yet, so today there is normally NO row for
# MAGIC today's `run_date`, and this renderer is built to degrade gracefully
# MAGIC (a "not available for this run" state) rather than fail when that's
# MAGIC the case. The match-score histogram / borderline-matches table from
# MAGIC the original mockup are NOT built here — `dashboard_run_summary`'s
# MAGIC given schema only has aggregate counts, not per-bucket/per-record score
# MAGIC detail; that's flagged as a gap for `tss-dedup`'s `dev-hbku`, not
# MAGIC guessed at.
# MAGIC
# MAGIC **Design**: CSS variables/classes are ported 1:1 from the approved
# MAGIC mockup (https://claude.ai/code/artifact/36b5279f-2b02-4345-ae85-329715e37301,
# MAGIC built with the `dataviz` skill's validated default palette — not
# MAGIC re-validated here since the colors are unchanged, only which
# MAGIC components are used per scope). The rendered report is 100% English —
# MAGIC client-facing deliverable, distinct from this repo's Spanish-first
# MAGIC internal conventions.
# MAGIC
# MAGIC Both `render_*_report_html` functions have no Spark/`dbutils`
# MAGIC dependency — plain Python + `html.escape`, unit-testable with synthetic
# MAGIC dicts/lists outside Databricks, same as the metric helpers in
# MAGIC `postprocess_changes.py`.

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
run_scholarly_activities = scope_widget in ("ALL", "Scholarly Activities")
run_custom_sections = scope_widget in ("ALL", "Custom Sections")

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

  .callout-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; }
  .callout {
    border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px;
    display: flex; align-items: center; justify-content: space-between;
  }
  .callout .callout-label { font-size: 12.5px; color: var(--text-secondary); }
  .callout .callout-value { font-size: 18px; font-weight: 600; }
  .callout .callout-value.good { color: var(--status-good-text); }

  .notice {
    border: 1px dashed var(--border); border-radius: 10px; padding: 14px 16px;
    font-size: 13px; color: var(--text-secondary); text-align: center;
  }

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

def render_ro_cs_report_html(
    client_name: str,
    scope_label: str,
    run_date: str,
    report_generated: str,
    dedup_available: bool,
    dedup: dict,
    deletes_delivered,
    deletions_by_subtype: list,
    cross_type_label: str,
    cross_type_value,
) -> str:
    """
    Builds the full standalone HTML document for a Scholarly Activities /
    Custom Sections client dashboard report. `dedup` is the aggregated
    dashboard_run_summary/custom_dashboard_run_summary row(s) for today
    (see aggregate_dedup_rows) -- empty/ignored when `dedup_available` is
    False (no matching row for today's run_date, e.g. because tss-dedup's
    recurring Job hasn't run yet). `deletions_by_subtype` is a list of
    {"subtype", "count"} dicts from this repo's own
    dashboard_summary_detail_<date> (status == "delete").

    Unlike Grants, the match-score histogram / borderline-matches cards
    from the original mockup are NOT rendered -- the given
    dashboard_run_summary schema (aggregate counts only) has no per-bucket
    or per-record score detail to show. That section always renders as an
    explicit gap notice instead of an invented/misleading chart.
    """
    no_facultyid_skipped = dedup.get("no_facultyid_skipped") if dedup_available else None
    has_warnings = (not dedup_available) or ((no_facultyid_skipped or 0) > 0)
    badge_label = "Completed with warnings" if has_warnings else "Completed"
    badge_class = "status-warning" if has_warnings else "status-good"

    if dedup_available:
        evaluated = dedup.get("total_source", 0)
        matched = dedup.get("matched", 0)
        match_rate = dedup.get("match_rate")
        exported_new = dedup.get("exported_step3", 0)
        evaluated_sub = "Pure records compared against FAR"
        matched_sub = f"{_fmt_rate(match_rate)} match rate"
        exported_sub = "No existing FAR counterpart found"
    else:
        evaluated = matched = exported_new = None
        evaluated_sub = matched_sub = exported_sub = "Dedup data not available for this run"

    stat_tiles = "".join([
        _stat_tile("Records evaluated", _fmt_int(evaluated) if dedup_available else "n/a", evaluated_sub),
        _stat_tile("Matched to existing FAR record", _fmt_int(matched) if dedup_available else "n/a", matched_sub),
        _stat_tile("New records exported", _fmt_int(exported_new) if dedup_available else "n/a", exported_sub),
        _stat_tile("Records deleted", _fmt_int(deletes_delivered), "Removed in Pure since last run"),
    ])

    sorted_deletions = sorted(deletions_by_subtype, key=lambda row: row["count"], reverse=True)
    max_deletion = max((row["count"] for row in sorted_deletions), default=0)
    if sorted_deletions:
        bar_rows_html = []
        deletion_table_rows = []
        for row in sorted_deletions:
            width_pct = round(row["count"] / max_deletion * 100) if max_deletion else 0
            bar_rows_html.append(
                f'<div class="bar-row"><div class="cat">{html_lib.escape(str(row["subtype"]))}</div>'
                f'<div class="bar-track"><div class="bar-fill" style="width:{width_pct}%"></div></div>'
                f'<div class="val">{_fmt_int(row["count"])}</div></div>'
            )
            deletion_table_rows.append(
                f'<tr><td>{html_lib.escape(str(row["subtype"]))}</td><td>{_fmt_int(row["count"])}</td></tr>'
            )
        deletions_section_html = f"""
    <div class="bar-chart-wrap" role="img" aria-label="Deletions by record type">
      {"".join(bar_rows_html)}
    </div>
    <details class="table-toggle">
      <summary></summary>
      <table class="data-table">
        <thead><tr><th>Record type</th><th>Deletions</th></tr></thead>
        <tbody>{"".join(deletion_table_rows)}</tbody>
      </table>
    </details>"""
    else:
        deletions_section_html = '<div class="notice">No deletions today.</div>'

    if dedup_available:
        quality_callouts = [
            f'<div class="callout"><span class="callout-label">Skipped — missing Faculty ID</span>'
            f'<span class="callout-value">{_fmt_int(no_facultyid_skipped)}</span></div>'
        ]
        if cross_type_value is not None:
            quality_callouts.append(
                f'<div class="callout"><span class="callout-label">{html_lib.escape(cross_type_label)}</span>'
                f'<span class="callout-value">{_fmt_int(cross_type_value)}</span></div>'
            )
        quality_section_html = f'<div class="callout-row">{"".join(quality_callouts)}</div>'
    else:
        quality_section_html = '<div class="notice">Dedup data quality metrics not available for this run.</div>'

    client_esc = html_lib.escape(client_name)
    scope_esc = html_lib.escape(scope_label)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{client_esc} — {scope_esc} — Execution Report</title>
<style>{REPORT_CSS}</style>
</head>
<body>
<div class="viz-root">

  <div class="report-header">
    <div>
      <h1>{client_esc} — {scope_esc} — Execution Report</h1>
      <div class="meta">
        <b>Run date:</b> {html_lib.escape(run_date)} &nbsp;·&nbsp; <b>Report generated:</b> {html_lib.escape(report_generated)}<br>
        <b>Job:</b> {scope_esc} pipeline (dedup → changes) &nbsp;·&nbsp; <b>Pipeline steps:</b> tss-dedup (new) + changes pipeline (deletes)
      </div>
    </div>
    <span class="badge {badge_class}"><span class="dot"></span>{badge_label}</span>
  </div>

  <div class="stat-row">{stat_tiles}</div>

  <section class="card">
    <h2>Deletions by record type</h2>
    <p class="subtitle">Records removed in Pure and retracted from FAR, this run</p>
    {deletions_section_html}
  </section>

  <section class="card">
    <h2>Match score detail</h2>
    <p class="subtitle">Matched records only — score distribution and borderline matches</p>
    <div class="notice">Score-level detail is not available in this report yet — tss-dedup's dashboard_run_summary only exposes aggregate counts (matched / unmatched / match_rate), not a per-record or per-bucket score breakdown. Showing this section requires additional instrumentation on tss-dedup's side.</div>
  </section>

  <section class="card">
    <h2>Data quality</h2>
    <p class="subtitle">Records skipped or adjusted during matching</p>
    {quality_section_html}
  </section>

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


DEDUP_SUM_COLS = [
    "total_source", "total_target", "matched", "unmatched",
    "no_facultyid_skipped", "cross_type_removed", "cross_type_matched",
    "had_candidate_below_threshold", "no_candidate", "initial_step3", "exported_step3",
]


def read_dedup_scope_rows(table_name: str, scope_value: str) -> pd.DataFrame:
    """
    tss-dedup writes dashboard_run_summary / custom_dashboard_run_summary
    to the SAME catalog/schema this repo already reads/writes
    (academicinformationsystems_technicalservices.hbku) -- a direct Spark
    read via the existing `read_table`, no cross-repo plumbing needed.

    Neither the exact stored `scope` label nor `run_date`'s type/format
    were confirmed against a real row (the 3 recurring Databricks Jobs that
    would produce one don't exist yet -- still manually chained) -- see
    project_hbku_dashboard_ro_cs_renderer_20260814 in this repo's memory.
    `run_date` is parsed defensively via pandas (handles a DATE column,
    "YYYYMMDD", or "YYYY-MM-DD" alike) instead of assuming a Spark-side
    string/DATE comparison would work; `scope` is matched case/whitespace
    -insensitively for the same reason. Returns an empty DataFrame (with a
    logged reason) for any failure mode -- missing table, missing columns,
    scope not found, or no row for today -- so the caller can render a
    single "not available for this run" state without distinguishing why.
    """
    df = read_table(table_name)
    if df.empty:
        return df
    if "scope" not in df.columns or "run_date" not in df.columns:
        logger.warning(
            "%s is missing expected columns (scope/run_date) -- dedup section will show as not available.",
            table_name,
        )
        return pd.DataFrame()

    scope_rows = df[df["scope"].astype(str).str.strip().str.casefold() == scope_value.strip().casefold()]
    if scope_rows.empty:
        logger.warning(
            "No rows for scope=%r in %s -- check the exact scope label tss-dedup writes.", scope_value, table_name,
        )
        return scope_rows

    run_dates = pd.to_datetime(scope_rows["run_date"], errors="coerce").dt.strftime("%Y%m%d")
    todays_rows = scope_rows[run_dates == CURRENT_DAY]
    if todays_rows.empty:
        logger.warning(
            "No %s row for run_date=%s (scope=%s) -- tss-dedup's recurring Job may not have run yet today.",
            table_name, CURRENT_DAY, scope_value,
        )
    return todays_rows


def aggregate_dedup_rows(rows: pd.DataFrame) -> dict:
    """
    dashboard_run_summary/custom_dashboard_run_summary has one row per
    `type` per scope+run_date (exact grain not confirmed -- see
    read_dedup_scope_rows's docstring); summed to scope-level totals since
    none of this report's stat tiles break down by type. `match_rate` is
    recomputed from the summed matched/total_source (not averaged from
    each row's own match_rate) so it stays a true ratio, same reasoning as
    faculty_match_rate() in postprocess_changes.py.
    """
    if rows.empty:
        return {}
    totals = {
        col: pd.to_numeric(rows[col], errors="coerce").fillna(0).sum()
        for col in DEDUP_SUM_COLS if col in rows.columns
    }
    totals["match_rate"] = (totals["matched"] / totals["total_source"]) if totals.get("total_source") else None
    if "threshold_used" in rows.columns:
        threshold_series = rows["threshold_used"].dropna()
        totals["threshold_used"] = float(threshold_series.iloc[0]) if not threshold_series.empty else None
    else:
        totals["threshold_used"] = None
    return totals


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

# COMMAND ----------

# Per-scope config for the RO/CS report loop below. `dedup_scope_value` is
# this repo's own scope naming ("Scholarly Activities" / "Custom
# Sections") -- assumed to match what tss-dedup writes to its `scope`
# column, not confirmed yet (see read_dedup_scope_rows's docstring).
RO_CS_SCOPE_CONFIG = {
    "scholarly_activities": {
        "label": "Scholarly Activities",
        "should_run": run_scholarly_activities,
        "dedup_table": "dashboard_run_summary",
        "dedup_scope_value": "Scholarly Activities",
        "cross_type_field": "cross_type_matched",
        "cross_type_label": "Cross-type matches (title override)",
        "sftp_folder": FAR_TEMPLATES_CONFIG["Scholarly Activities"]["sftp_folder"],
        "report_prefix": "HBKU_scholarly_activities_report",
    },
    "custom_sections": {
        "label": "Custom Sections",
        "should_run": run_custom_sections,
        "dedup_table": "custom_dashboard_run_summary",
        "dedup_scope_value": "Custom Sections",
        "cross_type_field": "cross_type_removed",
        "cross_type_label": "Cross-type duplicate pairs removed",
        "sftp_folder": FAR_TEMPLATES_CONFIG["Custom Sections"]["sftp_folder"],
        "report_prefix": "HBKU_custom_sections_report",
    },
}

# COMMAND ----------

for metrics_scope_value, cfg in RO_CS_SCOPE_CONFIG.items():
    if not cfg["should_run"]:
        logger.info("Skipping %s dashboard report — SCOPE=%s", cfg["label"], scope_widget)
        continue

    dedup_rows = read_dedup_scope_rows(cfg["dedup_table"], cfg["dedup_scope_value"])
    dedup_available = not dedup_rows.empty
    dedup = aggregate_dedup_rows(dedup_rows) if dedup_available else {}

    metrics_df = read_table(f"dashboard_metrics_{CURRENT_DAY}")
    scope_metrics_df = metrics_df[metrics_df["scope"] == metrics_scope_value] if not metrics_df.empty else metrics_df
    deletes_delivered = _metric_value(scope_metrics_df, "deletes_delivered") or 0

    summary_detail_df = read_table(f"dashboard_summary_detail_{CURRENT_DAY}")
    if not summary_detail_df.empty:
        deletion_rows_df = summary_detail_df[
            (summary_detail_df["scope"] == metrics_scope_value) & (summary_detail_df["status"] == "delete")
        ]
        deletions_by_subtype = [
            {"subtype": row["subtype"], "count": int(row["count"])} for _, row in deletion_rows_df.iterrows()
        ]
    else:
        deletions_by_subtype = []

    report_html = render_ro_cs_report_html(
        client_name="HBKU",
        scope_label=cfg["label"],
        run_date=f"{YEAR}-{MONTH}-{DAY}",
        report_generated=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        dedup_available=dedup_available,
        dedup=dedup,
        deletes_delivered=deletes_delivered,
        deletions_by_subtype=deletions_by_subtype,
        cross_type_label=cfg["cross_type_label"],
        cross_type_value=dedup.get(cfg["cross_type_field"]) if dedup_available else None,
    )

    report_filename = f"{cfg['report_prefix']}_{YEAR}-{MONTH}-{DAY}.html"
    remote_path = upload_html_to_sftp(
        report_html, SFTP_BASE, cfg["sftp_folder"], report_filename, logger, secret_scope=SFTP_SECRET_SCOPE,
    )
    logger.info("Uploaded %s client dashboard report to %s", cfg["label"], remote_path)
