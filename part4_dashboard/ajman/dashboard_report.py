# Databricks notebook source
# MAGIC %md
# MAGIC # Part 4 — Client dashboard renderer (Ajman)
# MAGIC `render_report_html(context, scope)` builds the full standalone HTML
# MAGIC report for one scope (`research_output` or `grants`) and returns it as
# MAGIC a string. Pure Python — no Spark, no `dbutils` — so it is unit-testable
# MAGIC with a synthetic context outside Databricks, same split as
# MAGIC `part3_load/hbku/dashboard_report.py` and `tss-dedup`'s
# MAGIC `reconciliation_report.render_report_html`.
# MAGIC
# MAGIC The HTML (CSS + section layout + the client-side sort/filter/paginate
# MAGIC and Sankey-layout JS) is the approved changes-dashboard mockup
# MAGIC (artifact `ba55c1b9-2b85-475c-af64-290e05e067c1`), which is itself the
# MAGIC "Migration Ledger" look of `tss-dedup`'s reconciliation report adapted
# MAGIC to the incremental-changes scenario. Only the data is swapped: the
# MAGIC mockup's synthetic figures/rows are replaced with the real values
# MAGIC `build_dashboard.py` computes from Part 1/2/3's tables.
# MAGIC
# MAGIC The report is 100% English — client-facing deliverable, distinct from
# MAGIC this repo's Spanish-first internal conventions.

# COMMAND ----------

# MAGIC %run ./assets

# COMMAND ----------

import html as _html
import json as _json

# COMMAND ----------

_CSS = """
  @import url('https://fonts.googleapis.com/css2?family=Source+Serif+4:opsz,wght@8..60,500;8..60,600&family=Archivo:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

  :root {
    color-scheme: light;
    --page-bg:      #F2F4F6;
    --surface:      #ffffff;
    --surface-2:    #ffffff;
    --ink-primary:  #14171A;
    --ink-secondary:#4D4D4D;
    --ink-muted:    #7C7F83;
    --gridline:     #E6E6E6;
    --baseline:     #CCCCCC;
    --border:       #E2E5E9;

    --brand-orange:      #FF4203;
    --brand-orange-tint: #FFF7EF;
    --brand-orange-border: #FFD8C2;
    --brand-blue:        #0056D6;
    --brand-blue-tint:   #EBF1F9;
    --brand-blue-border: #D3E1F7;

    --status-good:       #0C8930;
    --status-good-tint:  #F4FAF6;
    --status-good-border:#CFEAD8;
    --status-info:       #0056D6;
    --status-info-tint:  #F2F6FC;
    --status-info-border:#D3E1F7;
    --status-notice:     #B8720A;
    --status-notice-tint:#FFF8E0;
    --status-notice-border:#F5DFA6;
    --status-error:      #AF1D1D;
    --status-neutral:       #8A6A2F;
    --status-neutral-tint:  #FBF3E3;
    --status-neutral-border:#EFDFC0;

    --neutral-chip-bg: #F2F2F2;
    --neutral-chip-ink: #4D4D4D;
    --neutral-chip-border: #D8D8D8;

    --cat-journal:  #2a78d6;
    --cat-chapter:  #eb6834;
    --cat-proceeding:#1baf7a;
    --cat-book:     #eda100;
    --cat-other:    #e87ba4;
    --cat-patent:   #4a3aa7;
    --cat-award:    #7a5ea8;

    --seq-400:#3987e5;
  }

  * { box-sizing: border-box; }
  html, body { margin:0; padding:0; }
  body {
    background: var(--page-bg);
    color: var(--ink-primary);
    font-family: 'Archivo', system-ui, -apple-system, "Segoe UI", sans-serif;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
  }
  h1, h2 { font-family: 'Source Serif 4', Georgia, serif; text-wrap: balance; margin: 0; }
  h3 { font-family: 'Archivo', sans-serif; font-weight: 600; margin: 0; }
  .mono { font-family: 'IBM Plex Mono', ui-monospace, monospace; }
  .tnum { font-variant-numeric: tabular-nums; }

  .page { max-width: 1180px; margin: 0 auto; padding: 0 24px 96px; }
  .wide-page { max-width: 1440px; margin: 0 auto; padding: 0 24px; }

  .masthead { background: var(--surface); border-bottom: 1px solid var(--border); }
  .masthead-inner { max-width: 1180px; margin: 0 auto; padding: 14px 24px; display:flex; align-items:center; justify-content:space-between; gap: 20px; flex-wrap: wrap; }
  .elsevier-logo { height: 90px; width: auto; display:block; }
  .ajman-logo { height: 78px; width: auto; display:block; }

  .identity { background: var(--surface); border-bottom: 3px solid var(--brand-orange); }
  .identity-inner { max-width: 1180px; margin: 0 auto; padding: 26px 24px 24px; display:flex; align-items:flex-end; justify-content:space-between; gap: 24px; flex-wrap: wrap; }
  .identity-eyebrow { font-family: 'Archivo', sans-serif; font-weight: 600; font-size: 11px; letter-spacing: 0.12em; text-transform: uppercase; color: var(--brand-orange); margin-bottom: 10px; }
  .identity h1 { font-size: 26px; font-weight: 600; color: var(--ink-primary); }
  .identity-meta { font-family: 'IBM Plex Mono', monospace; font-size: 12.5px; color: var(--ink-secondary); margin-top: 10px; display:flex; gap:18px; flex-wrap:wrap; }
  .identity-meta .cov { color: var(--brand-orange); }
  .badge-fullload { display:inline-flex; align-items:center; gap:7px; background: var(--brand-orange-tint); color: var(--ink-secondary); border: 1px solid var(--brand-orange-border); padding: 7px 13px; border-radius: 3px; font-family: 'Archivo', sans-serif; font-weight: 600; font-size: 11.5px; letter-spacing: 0.01em; }
  .badge-fullload::before { content: "\\25CF"; font-size: 8px; color: var(--brand-orange); }
  .badge-fullload strong { color: var(--ink-primary); font-weight: 700; }

  section { margin-bottom: 40px; }
  .section-head { display:flex; align-items:baseline; justify-content:space-between; gap:16px; margin-bottom: 16px; flex-wrap:wrap; }
  .section-head h2 { font-size: 26px; font-weight: 600; }
  .section-sub { font-size: 12.5px; color: var(--ink-muted); }

  .kpi-row { display:grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
  .kpi-tile { background: var(--surface-2); border: 1px solid var(--border); border-radius: 6px; padding: 18px 18px 16px; }
  .kpi-label { font-size: 12px; color: var(--ink-muted); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 10px; }
  .kpi-value { font-size: 30px; font-weight: 600; font-family: 'IBM Plex Mono', monospace; }
  .kpi-sub { font-size: 12.5px; color: var(--ink-secondary); margin-top: 6px; }
  .kpi-tile.accent-tile { border-color: var(--brand-orange-border); background: var(--brand-orange-tint); }
  .kpi-tile.accent-tile .kpi-value { color: var(--brand-orange); }

  .chart-row { display:grid; grid-template-columns: 1.3fr 1fr; gap: 16px; align-items: stretch; }
  .chart-row.even { grid-template-columns: 1fr 1fr; }
  .chart-card { background: var(--surface-2); border: 1px solid var(--border); border-radius: 6px; padding: 20px 22px 16px; }
  .chart-title { font-size: 13.5px; font-weight: 600; margin-bottom: 2px; }
  .chart-caption { font-size: 12px; color: var(--ink-muted); margin-bottom: 16px; }

  .bar-chart { display:flex; flex-direction:column; gap: 12px; }
  .bar-row { display:grid; grid-template-columns: 170px 1fr 54px; align-items:center; gap: 12px; cursor: pointer; }
  .bar-row:hover .bar-track { background: var(--baseline); }
  .bar-label { font-size: 12px; color: var(--ink-secondary); }
  .bar-track { position:relative; height: 14px; background: var(--gridline); border-radius: 3px; overflow:hidden; }
  .bar-fill { position:absolute; left:0; top:0; bottom:0; border-radius: 3px 0 0 3px; }
  .bar-value { font-size: 12px; text-align:right; color: var(--ink-secondary); }

  .donut-wrap { display:flex; align-items:center; gap: 20px; }
  .donut { width: 132px; height: 132px; border-radius: 50%; flex-shrink: 0; position: relative; }
  .donut-svg { position:absolute; inset:0; width:100%; height:100%; }
  .donut-seg { cursor: pointer; transition: opacity .12s ease; }
  .donut-seg:hover { opacity: 0.82; }
  .donut-center { position:absolute; inset:22px; display:flex; align-items:center; justify-content:center; flex-direction:column; pointer-events:none; }
  .donut-center b { font-family:'IBM Plex Mono',monospace; font-size: 15px; }
  .donut-center span { font-size: 10px; color: var(--ink-muted); text-transform:uppercase; letter-spacing:0.04em; }
  .donut-legend { display:flex; flex-direction:column; gap: 6px; font-size: 12.5px; flex: 1; }
  .donut-legend .lg-row { display:flex; align-items:center; gap: 8px; color: var(--ink-secondary); }
  .donut-legend .lg-swatch { width: 9px; height: 9px; border-radius: 2px; flex-shrink:0; }
  .donut-legend .lg-name { flex:1; color: var(--ink-primary); }
  .donut-legend .lg-pct { font-family:'IBM Plex Mono',monospace; color: var(--ink-secondary); }

  .sankey-card { background: var(--surface-2); border: 1px solid var(--border); border-radius: 6px; padding: 22px 26px 20px; overflow-x: auto; }
  .sankey-card svg { display:block; max-width: 100%; height:auto; margin: 0 auto; }
  .sankey-node-label { font-family: 'Archivo', sans-serif; font-size: 11.5px; font-weight: 700; fill: #fff; paint-order: stroke; stroke: rgba(0,0,0,0.38); stroke-width: 3px; stroke-linejoin: round; pointer-events:none; }
  .sankey-node-label .sk-val { font-family: 'IBM Plex Mono', monospace; font-weight: 500; }
  .sankey-group-label { font-family: 'Archivo', sans-serif; font-size: 11px; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; fill: var(--ink-muted); }
  .sankey-node { cursor: pointer; transition: opacity .12s ease; }
  .sankey-node:hover { opacity: 0.85; }
  .sankey-ribbon { cursor: pointer; transition: opacity .12s ease; }

  .chart-tooltip { display:none; position:fixed; z-index:1000; max-width: 240px; background: var(--ink-primary); color: #fff; font-family:'Archivo',sans-serif; font-size: 12.5px; line-height:1.4; padding: 9px 12px; border-radius: 5px; box-shadow: 0 6px 18px rgba(0,0,0,0.22); pointer-events: none; }
  .chart-tooltip .tt-title { font-weight: 600; margin-bottom: 3px; }
  .chart-tooltip .tt-value { font-family:'IBM Plex Mono',monospace; font-size: 12.5px; }
  .chart-tooltip .tt-pct { color: #C9CDD2; }
  .chart-tooltip .tt-sub { color: #C9CDD2; margin-top: 2px; font-family:'IBM Plex Mono',monospace; font-size: 11.5px; }

  .recon-card { background: var(--surface-2); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
  .recon-toolbar { padding: 16px 20px; border-bottom: 1px solid var(--border); display:flex; align-items:center; gap: 14px; flex-wrap:wrap; background: var(--surface); }
  .search-wrap { position:relative; flex: 1 1 260px; max-width: 340px; }
  .search-wrap input { width:100%; padding: 9px 12px 9px 34px; border: 1px solid var(--border); border-radius: 5px; background: var(--surface-2); color: var(--ink-primary); font-family: 'Archivo', sans-serif; font-size: 13.5px; }
  .search-wrap input:focus { outline: 2px solid var(--brand-orange); outline-offset: 1px; }
  .search-icon { position:absolute; left: 11px; top: 50%; transform: translateY(-50%); opacity: 0.5; font-size: 13px; }
  th .col-filter { display:block; margin-top: 5px; font-size: 10.5px; font-weight: 400; text-transform: none; letter-spacing: 0; padding: 3px 5px; border: 1px solid var(--border); border-radius: 3px; background: var(--surface-2); color: var(--ink-secondary); max-width: 150px; }
  .pagination-bar { display:flex; align-items:center; justify-content:center; gap: 14px; padding: 14px 20px; border-top: 1px solid var(--border); background: var(--surface); }
  .page-btn { padding: 6px 12px; border: 1px solid var(--border); border-radius: 5px; background: var(--surface-2); color: var(--ink-primary); font-family:'Archivo',sans-serif; font-size: 12.5px; cursor: pointer; }
  .page-btn:disabled { opacity: 0.4; cursor: not-allowed; }
  .page-btn:not(:disabled):hover { border-color: var(--brand-orange); }
  .page-info { font-size: 12.5px; color: var(--ink-secondary); }
  select.page-size { padding: 6px 10px; border: 1px solid var(--border); border-radius: 5px; background: var(--surface-2); color: var(--ink-primary); font-family:'Archivo',sans-serif; font-size: 12.5px; }
  .recon-toolbar .hint { font-size: 12.5px; color: var(--ink-muted); margin-left: auto; }
  .clear-btn { font-size: 12.5px; color: var(--brand-blue); background:none; border:none; cursor:pointer; font-family:'Archivo',sans-serif; text-decoration: underline; padding: 4px 2px; }

  .person-summary { margin: 0; padding: 14px 20px; background: var(--brand-orange-tint); border-bottom: 1px solid var(--border); font-size: 13.5px; display: none; }
  .person-summary.show { display:flex; gap: 22px; flex-wrap:wrap; align-items:baseline; }
  .person-summary b { font-family:'IBM Plex Mono',monospace; font-weight:600; }
  .person-summary .ps-item { color: var(--ink-secondary); }
  .person-summary .ps-item strong { color: var(--ink-primary); font-family:'IBM Plex Mono',monospace; }

  table.recon { width:100%; border-collapse: collapse; font-size: 13px; }
  table.recon th { text-align:left; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--ink-muted); font-weight: 600; padding: 10px 14px; border-bottom: 1px solid var(--border); white-space: nowrap; cursor: pointer; user-select: none; }
  table.recon th:hover { color: var(--ink-primary); }
  table.recon th .sort-arrow { display:inline-block; margin-left: 4px; opacity: 0.5; font-size: 10px; }
  table.recon th.sorted .sort-arrow { opacity: 1; color: var(--brand-orange); }
  table.recon td { padding: 12px 14px; border-bottom: 1px solid var(--gridline); vertical-align: top; }
  table.recon tbody tr:last-child td { border-bottom: none; }
  table.recon tbody tr:hover { background: var(--page-bg); }
  .rec-title { max-width: 260px; }
  .rec-title .t { color: var(--ink-primary); }
  .rec-person { white-space: nowrap; }
  .rec-person .name { font-weight: 600; }
  .rec-person .fid { font-family:'IBM Plex Mono',monospace; font-size: 11.5px; color: var(--ink-muted); }
  .subtype-cell .sp { color: var(--ink-primary); }
  .subtype-cell .sf { font-size: 11.5px; color: var(--ink-muted); margin-top: 2px; }
  .subtype-cell .sf.diff { color: var(--brand-orange); }
  .title-missing { display:inline-block; margin-left: 6px; font-size: 10.5px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; color: var(--status-notice); background: var(--status-notice-tint); border: 1px solid var(--status-notice-border); border-radius: 3px; padding: 1px 6px; }

  .chip { display:inline-flex; align-items:center; gap: 6px; padding: 4px 10px; border-radius: 100px; font-size: 12px; font-weight: 600; white-space: nowrap; border: 1px solid transparent; }
  .chip::before { content:""; width:6px; height:6px; border-radius: 50%; }
  .chip-delivered { background: var(--status-good-tint); color: var(--status-good); border-color: var(--status-good-border); }
  .chip-delivered::before { background: var(--status-good); }
  .chip-dropped { background: var(--status-neutral-tint); color: var(--status-neutral); border-color: var(--status-neutral-border); }
  .chip-dropped::before { background: var(--status-neutral); }
  .chip-retracted { background: var(--brand-blue-tint); color: var(--brand-blue); border-color: var(--brand-blue-border); }
  .chip-retracted::before { background: var(--brand-blue); }
  .chip-malformed { background: var(--status-notice-tint); color: var(--status-notice); border-color: var(--status-notice-border); }
  .chip-malformed::before { background: var(--status-notice); }

  .evt { display:inline-block; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; font-family:'Archivo',sans-serif; }
  .evt-new { background: var(--status-good-tint); color: var(--status-good); }
  .evt-updated { background: var(--brand-blue-tint); color: var(--brand-blue); }
  .evt-deleted { background: var(--neutral-chip-bg); color: var(--ink-secondary); }
  .evt-malformed { background: var(--status-notice-tint); color: var(--status-notice); }

  .dash { color: var(--ink-muted); }
  .recon-footer { padding: 12px 20px; font-size: 12px; color: var(--ink-muted); border-top: 1px solid var(--border); background: var(--surface); }

  @media (max-width: 760px) {
    .kpi-row { grid-template-columns: repeat(1,1fr); }
    .chart-row { grid-template-columns: 1fr; }
    .donut-wrap { flex-direction: column; align-items: flex-start; }
  }

  ::selection { background: var(--brand-orange-tint); }
"""

# COMMAND ----------

def _esc(value) -> str:
    if value is None:
        return ""
    return _html.escape(str(value))


def _fmt_int(value) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return "0"


def _fmt_rate(value) -> str:
    """None (no internal participants) renders as "n/a", never "0%"."""
    if value is None:
        return "n/a"
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if rate != rate:  # NaN
        return "n/a"
    return f"{rate * 100:.1f}%"


def has_real_title(title) -> bool:
    """
    grants_merge.py can fill a missing title with the literal string "None"
    (a placeholder, not a NaN) when an Award-only change has no linked
    Project resolved (fix c298256, not validated against Ajman's Pure yet).
    Treated the same as an actually-missing title here so the renderer shows
    the uuid + a "Title unavailable" tag instead.
    """
    if title is None:
        return False
    text = str(title).strip()
    return text != "" and text.lower() not in ("none", "nan")

# COMMAND ----------

# Client-side rendering + interaction, lifted from the approved mockup. Only
# the data source changed: SUBTYPES / RECEIVED / DROPPED / DELETES_DELIVERED
# / RECORDS are injected as JSON (real values from build_dashboard.py)
# instead of the mockup's synthetic figures. Layout/sort/filter/paginate are
# byte-for-byte the mockup's; the donut/bar/Sankey chart hover tooltips are a
# post-mockup addition (not in the approved artifact) -- see
# project_ajman_dashboard_chart_tooltips_20260916 in the repo's memory.
_JS_TEMPLATE = r"""
<script>
  const cs = getComputedStyle(document.documentElement);
  const col = (v) => cs.getPropertyValue(v).trim();

  // ---------- shared chart tooltip (donuts, bars, sankey) ----------
  const tooltipEl = document.createElement('div');
  tooltipEl.className = 'chart-tooltip';
  document.body.appendChild(tooltipEl);
  function moveTooltip(evt) {
    const pad = 14;
    let x = evt.clientX + pad, y = evt.clientY + pad;
    const r = tooltipEl.getBoundingClientRect();
    if (x + r.width > window.innerWidth - pad) x = evt.clientX - r.width - pad;
    if (y + r.height > window.innerHeight - pad) y = evt.clientY - r.height - pad;
    tooltipEl.style.left = x + 'px';
    tooltipEl.style.top = y + 'px';
  }
  function showTooltip(evt, html) {
    tooltipEl.innerHTML = html;
    tooltipEl.style.display = 'block';
    moveTooltip(evt);
  }
  function hideTooltip() { tooltipEl.style.display = 'none'; }
  function ttShare(title, value, pct) {
    return `<div class="tt-title">${esc(title)}</div><div class="tt-value">${Number(value).toLocaleString()}<span class="tt-pct"> (${Number(pct).toFixed(1)}%)</span></div>`;
  }
  // Jumps from a donut slice / bar row / Sankey node or ribbon to the
  // record-level table below, set to exactly the records behind it.
  // `renderTable`/the filter <select>s are defined further down in this
  // script, but this only ever runs from a later click, once the whole
  // script (and that section) has already run.
  function applyChartFilter(filter) {
    document.getElementById('facSearch').value = '';
    document.getElementById('eventFilter').value = filter.event || 'all';
    document.getElementById('outcomeFilter').value = filter.outcome || 'all';
    document.getElementById('subtypeFilter').value = filter.subtypeFar || 'all';
    renderTable(true);
    document.querySelector('.recon-card').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  // DELIVERED distinct records per Pure subtype, split new / updated.
  const SUBTYPES = __SUBTYPES_JSON__;
  // Change events RECEIVED this run, by Pure changeType.
  const RECEIVED = __RECEIVED_JSON__;
  // Records that changed but never reached FAR (no internal author resolved),
  // by changeType.
  const DROPPED = __DROPPED_JSON__;
  const DELETES_DELIVERED = __DELETES_DELIVERED_JSON__;
  // Malformed /changes events (no valid uuid) detected this run -- 0 on a
  // normal run; only ever shown when it actually happens (see
  // project_ajman_fix_nan_uuid_crash_20260911 in the repo's memory).
  const MALFORMED_COUNT = __MALFORMED_COUNT_JSON__;
  // One row per (record x internal participant), plus delete rows and any
  // malformed-event rows.
  const RECORDS = __RECORDS_JSON__;

  const deliveredNew = SUBTYPES.reduce((a,s) => a + s.new, 0);
  const deliveredUpd = SUBTYPES.reduce((a,s) => a + s.updated, 0);

  // aggregate to FAR-type level (for the FAR-type donut + sankey leaves)
  const FAR_TYPES = [];
  SUBTYPES.forEach(s => {
    let f = FAR_TYPES.find(x => x.far_slug === s.far_slug);
    if (!f) { f = { far: s.far, far_slug: s.far_slug, color_var: s.color_var, new: 0, updated: 0 }; FAR_TYPES.push(f); }
    f.new += s.new; f.updated += s.updated;
  });

  // ---------- helper: build a donut + legend ----------
  // Drawn as stacked SVG arc segments (not a CSS conic-gradient) so each
  // slice is a real element that can carry its own hover tooltip with the
  // absolute count behind the percentage.
  function buildDonut(donutId, legendId, rows) {
    const grand = rows.reduce((a,r) => a + r.value, 0) || 1;
    const donutEl = document.getElementById(donutId);
    const legendEl = document.getElementById(legendId);
    const old = donutEl.querySelector('svg.donut-svg');
    if (old) old.remove();
    const svgNS = 'http://www.w3.org/2000/svg';
    const R = 55, C = 2 * Math.PI * R;
    const svg = document.createElementNS(svgNS, 'svg');
    svg.setAttribute('viewBox', '0 0 132 132');
    svg.setAttribute('class', 'donut-svg');
    let cum = 0;
    rows.forEach(r => {
      const frac = r.value / grand;
      const dash = frac * C;
      const seg = document.createElementNS(svgNS, 'circle');
      seg.setAttribute('cx', '66'); seg.setAttribute('cy', '66'); seg.setAttribute('r', String(R));
      seg.setAttribute('fill', 'none');
      seg.setAttribute('stroke', r.color);
      seg.setAttribute('stroke-width', '22');
      seg.setAttribute('stroke-dasharray', `${dash.toFixed(2)} ${(C - dash).toFixed(2)}`);
      seg.setAttribute('stroke-dashoffset', (-cum).toFixed(2));
      seg.setAttribute('transform', 'rotate(-90 66 66)');
      seg.classList.add('donut-seg');
      if (frac > 0) {
        seg.addEventListener('mouseenter', (e) => showTooltip(e, ttShare(r.label, r.value, frac * 100)));
        seg.addEventListener('mousemove', moveTooltip);
        seg.addEventListener('mouseleave', hideTooltip);
        seg.addEventListener('click', () => { hideTooltip(); applyChartFilter(r.filter || {}); });
      }
      svg.appendChild(seg);
      cum += dash;
    });
    donutEl.insertBefore(svg, donutEl.firstChild);
    legendEl.innerHTML = rows.map(r =>
      `<div class="lg-row"><span class="lg-swatch" style="background:${r.color}"></span><span class="lg-name">${r.label}</span><span class="lg-pct tnum">${(r.value/grand*100).toFixed(1)}%</span></div>`
    ).join('');
  }

  // ---------- bar chart : delivered by FAR type ----------
  (function() {
    const chartEl = document.getElementById('subtype-chart');
    if (!chartEl) return;
    const rows = FAR_TYPES.map(f => ({ ...f, total: f.new + f.updated })).sort((a,b) => b.total - a.total);
    const maxV = Math.max(...rows.map(r => r.total), 1);
    chartEl.innerHTML = rows.map(r => `
      <div class="bar-row">
        <div class="bar-label">${r.far}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${(r.total/maxV*100).toFixed(1)}%; background:${col(r.color_var)};"></div></div>
        <div class="bar-value tnum">${r.total.toLocaleString()}</div>
      </div>`).join('');
    chartEl.querySelectorAll('.bar-row').forEach((el, i) => {
      const r = rows[i];
      const html = `<div class="tt-title">${esc(r.far)}</div><div class="tt-value">${r.total.toLocaleString()} total</div><div class="tt-sub">${r.new.toLocaleString()} new &middot; ${r.updated.toLocaleString()} updated</div>`;
      el.addEventListener('mouseenter', (e) => showTooltip(e, html));
      el.addEventListener('mousemove', moveTooltip);
      el.addEventListener('mouseleave', hideTooltip);
      el.addEventListener('click', () => { hideTooltip(); applyChartFilter({ outcome: 'delivered', subtypeFar: r.far }); });
    });
  })();

  // ---------- donut : share by FAR type ----------
  (function() {
    const nEl = document.getElementById('donut-far-n');
    if (!nEl) return;
    nEl.textContent = FAR_TYPES.length;
    buildDonut('donut-far', 'donut-far-legend',
      FAR_TYPES.map(f => ({ label: f.far, value: f.new + f.updated, color: col(f.color_var), filter: { outcome: 'delivered', subtypeFar: f.far } }))
               .sort((a,b) => b.value - a.value));
  })();

  // ---------- donut : share by event type ----------
  buildDonut('donut-evt', 'donut-evt-legend', [
    { label: 'New',     value: RECEIVED.CREATE || 0, color: col('--status-good'), filter: { event: 'new' } },
    { label: 'Updated', value: RECEIVED.UPDATE || 0, color: col('--brand-blue'), filter: { event: 'updated' } },
    { label: 'Deleted', value: RECEIVED.DELETE || 0, color: '#9AA0A6', filter: { event: 'deleted' } },
  ]);

  // ---------- sankey : routed by change type ----------
  (function() {
    const svg = document.getElementById('sankey');
    const GREY_DARK = "#8A8F96", GREY_MID = "#A9AEB4", GREY_LIGHT = "#C7CBD1";
    // Tighter than the mockup's original constants -- with Ajman's real,
    // heavily-skewed volumes (a handful of New/Deleted next to thousands of
    // Updated) the taller floor and wider gaps made the small flows look
    // like a rendering glitch. Matches the scale tss-dedup's reconciliation
    // report already uses successfully at similar real volumes.
    const yStart = 24, minH = 30, leafGap = 8, bigGap = 20, groupGap = 40;
    const totalEvents = (RECEIVED.CREATE || 0) + (RECEIVED.UPDATE || 0) + (RECEIVED.DELETE || 0);
    const k = Math.min(0.05, 900 / (totalEvents || 1));
    const wCol = 120, gapX = 270;

    const x0 = 20, x1 = x0 + wCol + gapX, x2 = x1 + wCol + gapX, x3 = x2 + wCol + gapX;

    const columns = [
      [ { id:'events', value: totalEvents, color: GREY_DARK, labelLines:['Total change','events'], x:x0, w:wCol, filter:{} } ],
      [ { id:'create', value: RECEIVED.CREATE || 0, color: GREY_MID, labelLines:['New in','Pure'],     x:x1, w:wCol, gapAfter:bigGap, filter:{event:'new'} },
        { id:'update', value: RECEIVED.UPDATE || 0, color: GREY_MID, labelLines:['Updated in','Pure'], x:x1, w:wCol, gapAfter:bigGap, filter:{event:'updated'} },
        { id:'delete', value: RECEIVED.DELETE || 0, color: GREY_MID, labelLines:['Deleted in','Pure'], x:x1, w:wCol, filter:{event:'deleted'} } ],
      [ { id:'c-drop', value: DROPPED.CREATE || 0, color: GREY_LIGHT, labelLines:['Dropped'], x:x2, w:wCol, filter:{event:'new', outcome:'dropped'} },
        { id:'c-del',  value: deliveredNew,   color: col('--status-good'), labelLines:['Delivered','new'],   x:x2, w:wCol, gapAfter:bigGap, filter:{event:'new', outcome:'delivered'} },
        { id:'u-drop', value: DROPPED.UPDATE || 0, color: GREY_LIGHT, labelLines:['Dropped'], x:x2, w:wCol, filter:{event:'updated', outcome:'dropped'} },
        { id:'u-del',  value: deliveredUpd,   color: col('--brand-blue'),  labelLines:['Delivered','updated'], x:x2, w:wCol, gapAfter:bigGap, filter:{event:'updated', outcome:'delivered'} },
        { id:'d-del',  value: DELETES_DELIVERED, color: col('--status-neutral'), labelLines:['Retracted','from FAR'], x:x2, w:wCol, filter:{event:'deleted', outcome:'retracted'} } ],
      []
    ];

    const leaves = columns[3];
    FAR_TYPES.forEach(t => { if (t.new > 0) leaves.push({ id:'n-'+t.far_slug, value:t.new, color: col(t.color_var), label:t.far, x:x3, w:230, group:'n', filter:{event:'new', outcome:'delivered', subtypeFar:t.far} }); });
    if (leaves.length) leaves[leaves.length-1].gapAfter = groupGap;
    FAR_TYPES.forEach(t => { if (t.updated > 0) leaves.push({ id:'u-'+t.far_slug, value:t.updated, color: col(t.color_var), label:t.far, x:x3, w:230, group:'u', filter:{event:'updated', outcome:'delivered', subtypeFar:t.far} }); });

    const links = [
      ['events','create'], ['events','update'], ['events','delete'],
      ['create','c-drop'], ['create','c-del'],
      ['update','u-drop'], ['update','u-del'],
      ['delete','d-del'],
    ];
    FAR_TYPES.forEach(t => { if (t.new > 0) links.push(['c-del','n-'+t.far_slug]); });
    FAR_TYPES.forEach(t => { if (t.updated > 0) links.push(['u-del','u-'+t.far_slug]); });
    if (MALFORMED_COUNT > 0) links.push(['malformed','excluded-malformed']);

    const greyRibbonTargets = new Set(['create','update','delete','c-drop','u-drop']);

    const nodes = {};
    columns.forEach(defs => {
      let y = yStart;
      defs.forEach(n => {
        const h = Math.max(minH, n.value * k);
        nodes[n.id] = Object.assign({}, n, { y0: y, y1: y + h });
        y += h + (n.gapAfter !== undefined ? n.gapAfter : leafGap);
      });
    });

    // Malformed /changes events (no valid uuid) -- a data-quality artifact,
    // not part of the received/delivered/dropped accounting. Placed as its
    // OWN row, pinned below every other node (source and target share the
    // same y-band) instead of joining columns[0]/leaves' per-column
    // stacking -- a source-to-leaf ribbon spans the full diagram width, and
    // stacking it alongside "Total change events" (near the top) made that
    // long ribbon cut across the delete/retracted band in the middle. Only
    // added when MALFORMED_COUNT > 0 (see build_dashboard.py's
    // split_malformed_events -- this should not happen on a normal run).
    if (MALFORMED_COUNT > 0) {
      const bottomY = Math.max(...Object.values(nodes).map(n => n.y1));
      const mfY0 = bottomY + bigGap;
      const mfH = Math.max(minH, MALFORMED_COUNT * k);
      const mfColor = col('--status-notice');
      nodes['malformed'] = {
        id:'malformed', value: MALFORMED_COUNT, color: mfColor, labelLines:['Malformed','events'],
        x:x0, w:wCol, y0:mfY0, y1:mfY0+mfH, filter:{event:'malformed'},
      };
      nodes['excluded-malformed'] = {
        id:'excluded-malformed', value: MALFORMED_COUNT, color: mfColor, label:'Excluded — malformed event',
        x:x3, w:230, y0:mfY0, y1:mfY0+mfH, filter:{event:'malformed', outcome:'excluded_malformed'},
      };
    }

    function ribbon(x1,y1a,y1b,x2,y2a,y2b,color,opacity) {
      const mx = (x1+x2)/2;
      return `<path class="sankey-ribbon" d="M${x1},${y1a} C${mx},${y1a} ${mx},${y2a} ${x2},${y2a} L${x2},${y2b} C${mx},${y2b} ${mx},${y1b} ${x1},${y1b} Z" fill="${color}" opacity="${opacity}"/>`;
    }

    const bySource = {};
    links.forEach(([s,t]) => { (bySource[s] = bySource[s] || []).push(t); });

    let out = '';
    const ribbonMeta = [];
    Object.keys(bySource).forEach(srcId => {
      const src = nodes[srcId];
      const targets = bySource[srcId];
      const totalVal = targets.reduce((a,id) => a + nodes[id].value, 0);
      if (!totalVal) return;
      let cursor = src.y0;
      const availH = src.y1 - src.y0;
      targets.forEach(id => {
        const tgt = nodes[id];
        const sliceH = availH * (tgt.value / totalVal);
        const y1a = cursor, y1b = cursor + sliceH;
        cursor += sliceH;
        const isGrey = greyRibbonTargets.has(id);
        out += ribbon(src.x + src.w, y1a, y1b, tgt.x, tgt.y0, tgt.y1, isGrey ? GREY_LIGHT : tgt.color, isGrey ? 0.45 : 0.32);
        ribbonMeta.push({ filter: tgt.filter });
      });
    });

    Object.values(nodes).forEach(n => {
      if ((n.y1 - n.y0) <= 0) return;
      out += `<rect class="sankey-node" data-node-id="${n.id}" x="${n.x}" y="${n.y0}" width="${n.w}" height="${(n.y1-n.y0).toFixed(1)}" rx="3" fill="${n.color}"/>`;
      const midX = n.x + n.w / 2, midY = (n.y0 + n.y1) / 2;
      const lines = (n.labelLines || [n.label]).concat([n.value.toLocaleString()]);
      const lh = 14, startDy = -((lines.length - 1) * lh) / 2 + 4;
      out += `<text class="sankey-node-label tnum" x="${midX}" y="${midY}" text-anchor="middle">`;
      lines.forEach((ln, i) => {
        const isVal = i === lines.length - 1;
        out += `<tspan x="${midX}" dy="${i === 0 ? startDy : lh}"${isVal ? ' class="sk-val"' : ''}>${ln}</tspan>`;
      });
      out += `</text>`;
    });

    const nNodes = leaves.filter(n => n.group === 'n').map(n => nodes[n.id]);
    const uNodes = leaves.filter(n => n.group === 'u').map(n => nodes[n.id]);
    const leafCX = x3 + 115;
    if (nNodes.length) out += `<text class="sankey-group-label" x="${leafCX}" y="${nNodes[0].y0 - 16}" text-anchor="middle" style="fill:${col('--status-good')}">New &mdash; by FAR type</text>`;
    if (uNodes.length) out += `<text class="sankey-group-label" x="${leafCX}" y="${uNodes[0].y0 - 16}" text-anchor="middle" style="fill:${col('--brand-blue')}">Updated &mdash; by FAR type</text>`;
    if (nodes['excluded-malformed']) out += `<text class="sankey-group-label" x="${leafCX}" y="${nodes['excluded-malformed'].y0 - 16}" text-anchor="middle" style="fill:${col('--status-notice')}">Data quality</text>`;

    const all = Object.values(nodes);
    const maxX = Math.max(...all.map(n => n.x + n.w)) + 28;
    const maxY = Math.max(...all.map(n => n.y1)) + 24;
    svg.setAttribute('viewBox', `0 0 ${maxX} ${maxY}`);
    svg.innerHTML = out;

    // Hover is just a "this is clickable" hint -- the numbers themselves are
    // already printed on every node, so a tooltip repeating them would add
    // nothing. Clicking jumps to the record-level table below, filtered to
    // exactly the records behind that node/ribbon.
    const CLICK_HINT = '<div class="tt-title">Click to filter the record table below</div>';
    svg.querySelectorAll('.sankey-ribbon').forEach((el, i) => {
      const m = ribbonMeta[i];
      const baseOpacity = el.getAttribute('opacity');
      el.addEventListener('mouseenter', (e) => {
        el.style.opacity = Math.min(1, parseFloat(baseOpacity) + 0.4);
        showTooltip(e, CLICK_HINT);
      });
      el.addEventListener('mousemove', moveTooltip);
      el.addEventListener('mouseleave', () => { el.style.opacity = baseOpacity; hideTooltip(); });
      el.addEventListener('click', () => { hideTooltip(); applyChartFilter(m.filter); });
    });
    svg.querySelectorAll('.sankey-node').forEach((el) => {
      const n = nodes[el.dataset.nodeId];
      el.addEventListener('mouseenter', (e) => showTooltip(e, CLICK_HINT));
      el.addEventListener('mousemove', moveTooltip);
      el.addEventListener('mouseleave', hideTooltip);
      el.addEventListener('click', () => { hideTooltip(); applyChartFilter(n.filter); });
    });
  })();

  // ---------- record-level detail ----------
  const OUTCOME_META = {
    delivered:           { cls: 'chip-delivered', txt: 'Delivered' },
    retracted:           { cls: 'chip-retracted', txt: 'Retracted from FAR' },
    dropped_no_fid:      { cls: 'chip-dropped',   txt: 'Dropped — no Faculty ID match' },
    dropped_no_internal: { cls: 'chip-dropped',   txt: 'Dropped — no internal participant' },
    excluded_malformed:  { cls: 'chip-malformed', txt: 'Excluded — malformed event' },
  };
  const EVT_META = { new: ['evt-new','New'], updated: ['evt-updated','Updated'], deleted: ['evt-deleted','Deleted'], malformed: ['evt-malformed','Malformed'] };

  function esc(s) { return s === null || s === undefined ? '' : String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

  // Filter by the FAR subtype (subtypeFar), not the raw Pure subtype -- FAR
  // is what the client actually files by. Defaults to the first FAR
  // subtype with data this run instead of "All subtypes" (same convention
  // as tss-dedup's reconciliation report).
  const subtypeSelect = document.getElementById('subtypeFilter');
  const subtypeOptions = Array.from(new Set(RECORDS.map(r => r.subtypeFar).filter(Boolean))).sort();
  subtypeSelect.innerHTML = '<option value="all">All subtypes</option>' +
    subtypeOptions.map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join('');
  if (subtypeOptions.length) subtypeSelect.value = subtypeOptions[0];

  const body = document.getElementById('reconBody');
  const TOTAL_RECORDS = RECORDS.length;
  document.getElementById('totalCount').textContent = TOTAL_RECORDS.toLocaleString();
  let sortState = { key: null, dir: 1 };
  let currentPage = 1;

  function renderTable(resetPage) {
    if (resetPage) currentPage = 1;
    const q = document.getElementById('facSearch').value.trim().toLowerCase();
    const ev = document.getElementById('eventFilter').value;
    const oc = document.getElementById('outcomeFilter').value;
    const st = subtypeSelect.value;

    let rows = RECORDS.filter(r => {
      if (q && !(r.name || '').toLowerCase().includes(q) && !(r.fid || '').includes(q)) return false;
      if (ev !== 'all' && r.event !== ev) return false;
      if (oc !== 'all') {
        if (oc === 'dropped' && !r.outcome.startsWith('dropped')) return false;
        if (oc !== 'dropped' && r.outcome !== oc) return false;
      }
      if (st !== 'all' && r.subtypeFar !== st) return false;
      return true;
    });

    if (sortState.key) {
      const key = sortState.key, dir = sortState.dir;
      rows = rows.slice().sort((a,b) => {
        let av = a[key] ?? '', bv = b[key] ?? '';
        if (typeof av === 'string') { av = av.toLowerCase(); bv = bv.toLowerCase(); }
        if (av < bv) return -dir;
        if (av > bv) return dir;
        return 0;
      });
    }

    const totalFiltered = rows.length;
    const pageSize = parseInt(document.getElementById('pageSize').value, 10);
    const totalPages = Math.max(1, Math.ceil(totalFiltered / pageSize));
    currentPage = Math.min(Math.max(1, currentPage), totalPages);
    const startI = (currentPage - 1) * pageSize;
    const pageRows = rows.slice(startI, startI + pageSize);

    body.innerHTML = pageRows.map(r => {
      const om = OUTCOME_META[r.outcome] || OUTCOME_META.delivered;
      const em = EVT_META[r.event] || EVT_META.updated;
      const diff = r.subtypePure && r.subtypeFar && r.subtypePure !== r.subtypeFar;
      const titleCell = r.titleMissing
        ? (r.event === 'deleted'
            ? `<span class="mono">${esc(r.uuid)}</span>`
            : `<span class="mono">${esc(r.uuid)}</span><span class="title-missing">Title unavailable</span>`)
        : (esc(r.title) || '<span class="dash">&mdash;</span>');
      const subtypeCell = r.subtypePure
        ? `<div class="sp">${esc(r.subtypePure)}</div><div class="sf${diff ? ' diff' : ''}">&rarr; ${esc(r.subtypeFar)}</div>`
        : '<span class="dash">&mdash;</span>';
      return `<tr>
        <td class="rec-person"><div class="name">${esc(r.name) || '<span class="dash">&mdash;</span>'}</div></td>
        <td class="rec-person"><span class="fid">${esc(r.fid) || '<span class="dash">&mdash;</span>'}</span></td>
        <td class="rec-title"><div class="t">${titleCell}</div></td>
        <td class="subtype-cell">${subtypeCell}</td>
        <td><span class="evt ${em[0]}">${em[1]}</span></td>
        <td><span class="chip ${om.cls}">${om.txt}</span></td></tr>`;
    }).join('');

    document.getElementById('rowCount').textContent = totalFiltered.toLocaleString();
    document.getElementById('pageInfo').textContent = `Page ${currentPage} of ${totalPages}`;
    document.getElementById('prevPage').disabled = currentPage <= 1;
    document.getElementById('nextPage').disabled = currentPage >= totalPages;

    const summary = document.getElementById('personSummary');
    const uniq = new Set(rows.filter(r => r.fid).map(r => r.fid));
    if (q && uniq.size === 1 && rows.length) {
      const r0 = rows.find(r => r.fid) || rows[0];
      const nDel = rows.filter(r => r.outcome === 'delivered').length;
      const nDrop = rows.filter(r => r.outcome.startsWith('dropped')).length;
      const nRet = rows.filter(r => r.outcome === 'retracted').length;
      summary.className = 'person-summary show';
      summary.innerHTML = `
        <span class="ps-item"><b>${esc(r0.name)}</b> &middot; Faculty ID <b class="mono">${esc(r0.fid)}</b></span>
        <span class="ps-item">${rows.length} change rows &rarr; <strong>${nDel}</strong> delivered, <strong>${nDrop}</strong> dropped, <strong>${nRet}</strong> retracted</span>`;
    } else {
      summary.className = 'person-summary';
    }
  }

  document.querySelectorAll('table.recon th[data-key]').forEach(th => {
    th.addEventListener('click', (e) => {
      if (e.target.closest('select')) return;
      const key = th.dataset.key;
      if (sortState.key === key) sortState.dir *= -1; else sortState = { key, dir: 1 };
      document.querySelectorAll('table.recon th').forEach(t => t.classList.remove('sorted'));
      th.classList.add('sorted');
      const a = th.querySelector('.sort-arrow');
      if (a) a.textContent = sortState.dir === 1 ? '↑' : '↓';
      renderTable(true);
    });
  });
  ['facSearch','eventFilter','outcomeFilter','subtypeFilter'].forEach(id =>
    document.getElementById(id).addEventListener(id === 'facSearch' ? 'input' : 'change', () => renderTable(true)));
  document.getElementById('pageSize').addEventListener('change', () => renderTable(true));
  document.getElementById('prevPage').addEventListener('click', () => { currentPage--; renderTable(false); });
  document.getElementById('nextPage').addEventListener('click', () => { currentPage++; renderTable(false); });
  document.getElementById('clearBtn').addEventListener('click', () => {
    document.getElementById('facSearch').value = '';
    document.getElementById('eventFilter').value = 'all';
    document.getElementById('outcomeFilter').value = 'all';
    subtypeSelect.value = 'all';
    renderTable(true);
  });

  renderTable(true);
</script>
"""

# COMMAND ----------

def render_report_html(context: dict, scope: str) -> str:
    """
    `context` keys:
      client_name, scope_label, eyebrow, delivery_date, coverage_start,
      coverage_end, faculty_affected,
      received      -> {"CREATE": int, "UPDATE": int, "DELETE": int}
      dropped       -> {"CREATE": int, "UPDATE": int}
      deletes_delivered -> int
      match_rate    -> float 0-1, or None
      subtypes      -> [{"pure","far","far_slug","color_var","new","updated"}]
      records       -> [{"name","fid","title","titleMissing","uuid",
                         "subtypePure","subtypeFar","event","outcome"}]
      malformed_count -> int, 0 on a normal run (see
                         project_ajman_fix_nan_uuid_crash_20260911, memory) —
                         when > 0 the Sankey gets an extra "Data quality"
                         branch and the record table an "Malformed"/
                         "Excluded — malformed event" filter option; both
                         stay entirely absent when it's 0.
    `scope` is "research_output" or "grants" (only used for the <title>).
    """
    received = context["received"]
    dropped = context.get("dropped", {})
    subtypes = context.get("subtypes", [])
    malformed_count = int(context.get("malformed_count") or 0)

    received_total = sum(int(v or 0) for v in received.values())
    delivered_new = sum(int(s.get("new") or 0) for s in subtypes)
    delivered_upd = sum(int(s.get("updated") or 0) for s in subtypes)
    delivered_distinct = delivered_new + delivered_upd
    deletes_delivered = int(context.get("deletes_delivered") or 0)
    dropped_total = sum(int(v or 0) for v in dropped.values())

    received_sub = (
        f"{_fmt_int(received.get('CREATE', 0))} new"
        f" · {_fmt_int(received.get('UPDATE', 0))} updated"
        f" · {_fmt_int(received.get('DELETE', 0))} deleted"
    )
    delivered_sub = (
        f"{_fmt_int(delivered_new)} new · {_fmt_int(delivered_upd)} updated"
        f" (+ {_fmt_int(deletes_delivered)} retracted)"
    )

    client = _esc(context.get("client_name", "Ajman"))
    scope_label = _esc(context.get("scope_label", scope))
    eyebrow = _esc(context.get("eyebrow", f"{scope_label} · Pure → FAR"))

    # Hardcoded to "Manual Execution" for now -- every run today is a
    # one-off someone kicks off by hand. Once Part 4 is wired to a
    # recurring Databricks Job, this should read that context instead and
    # say "Recurrent Job Execution" (flagged as a next step, not built yet).
    execution_mode_label = _esc(context.get("execution_mode_label", "Manual Execution"))

    # The "Malformed"/"Excluded — malformed event" filter options only exist
    # in the markup at all when this run actually had one — never an
    # always-there-but-empty affordance.
    event_option_malformed = '<option value="malformed">Malformed</option>' if malformed_count else ""
    outcome_option_malformed = (
        '<option value="excluded_malformed">Excluded — malformed event</option>' if malformed_count else ""
    )

    js = (
        _JS_TEMPLATE
        .replace("__SUBTYPES_JSON__", _json.dumps(subtypes))
        .replace("__RECEIVED_JSON__", _json.dumps({k: int(received.get(k, 0) or 0) for k in ("CREATE", "UPDATE", "DELETE")}))
        .replace("__DROPPED_JSON__", _json.dumps({k: int(dropped.get(k, 0) or 0) for k in ("CREATE", "UPDATE")}))
        .replace("__DELETES_DELIVERED_JSON__", _json.dumps(deletes_delivered))
        .replace("__MALFORMED_COUNT_JSON__", _json.dumps(malformed_count))
        .replace("__RECORDS_JSON__", _json.dumps(context.get("records", [])))
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{client} — {scope_label} — Changes Delivery Report</title>
<style>{_CSS}</style>
</head>
<body>

<div class="masthead">
  <div class="masthead-inner">
    <img class="elsevier-logo" src="{ELSEVIER_LOGO_DATA_URI}" alt="Elsevier">
    <img class="ajman-logo" src="{AJMAN_LOGO_DATA_URI}" alt="Ajman University">
  </div>
</div>

<div class="identity">
  <div class="identity-inner">
    <div>
      <div class="identity-eyebrow">{eyebrow}</div>
      <h1>Incremental Changes Delivery Report</h1>
      <div class="identity-meta">
        <span>Delivery date <b class="mono">{_esc(context.get('delivery_date', ''))}</b></span>
        <span class="cov">Coverage <b class="mono">{_esc(context.get('coverage_start', ''))}</b> &rarr; <b class="mono">{_esc(context.get('coverage_end', ''))}</b></span>
        <span>Faculty members affected <b class="mono tnum">{_fmt_int(context.get('faculty_affected', 0))}</b></span>
      </div>
    </div>
    <div class="badge-fullload"><strong>{execution_mode_label.upper()}</strong></div>
  </div>
</div>

<div class="page">
  <section>
    <div class="section-head">
      <h2>Summary</h2>
      <div class="section-sub">All figures cover changes in Pure since your last delivery.</div>
    </div>
    <div class="kpi-row">
      <div class="kpi-tile">
        <div class="kpi-label">Changes received</div>
        <div class="kpi-value tnum">{_fmt_int(received_total)}</div>
        <div class="kpi-sub">{received_sub}</div>
      </div>
      <div class="kpi-tile accent-tile">
        <div class="kpi-label">Records delivered</div>
        <div class="kpi-value tnum">{_fmt_int(delivered_distinct)}</div>
        <div class="kpi-sub">{delivered_sub}</div>
      </div>
      <div class="kpi-tile">
        <div class="kpi-label">Dropped &mdash; no internal author</div>
        <div class="kpi-value tnum">{_fmt_int(dropped_total)}</div>
        <div class="kpi-sub">changed in Pure but never reached FAR</div>
      </div>
      <div class="kpi-tile">
        <div class="kpi-label">Faculty match rate</div>
        <div class="kpi-value tnum">{_fmt_rate(context.get('match_rate'))}</div>
        <div class="kpi-sub">internal Pure authors linked to a Faculty ID in FAR</div>
      </div>
    </div>
  </section>
</div>

<div class="page">
  <section>
    <div class="section-head">
      <h2>Where the changes landed</h2>
    </div>
    <div class="chart-card" style="margin-bottom:16px;">
      <div class="chart-title">Delivered records by FAR type</div>
      <div class="chart-caption">{_fmt_int(delivered_distinct)} distinct records delivered to FAR this run</div>
      <div class="bar-chart" id="subtype-chart"></div>
    </div>
    <div class="chart-row even">
      <div class="chart-card">
        <div class="chart-title">Share by FAR type</div>
        <div class="chart-caption">% of records delivered this run</div>
        <div class="donut-wrap">
          <div class="donut" id="donut-far">
            <div class="donut-center"><b class="tnum" id="donut-far-n"></b><span>types</span></div>
          </div>
          <div class="donut-legend" id="donut-far-legend"></div>
        </div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Share by event type</div>
        <div class="chart-caption">% of change events received this run</div>
        <div class="donut-wrap">
          <div class="donut" id="donut-evt">
            <div class="donut-center"><b class="tnum">{_fmt_int(received_total)}</b><span>events</span></div>
          </div>
          <div class="donut-legend" id="donut-evt-legend"></div>
        </div>
      </div>
    </div>
  </section>
</div>

<div class="wide-page">
  <section>
    <div class="section-head">
      <h2>Data journey</h2>
    </div>
    <div class="sankey-card">
      <svg id="sankey" viewBox="0 0 1600 900" xmlns="http://www.w3.org/2000/svg"></svg>
    </div>
  </section>
</div>

<div class="page">
  <section>
    <div class="section-head">
      <h2>Record-level detail</h2>
    </div>
    <div class="recon-card">
      <div class="recon-toolbar">
        <div class="search-wrap">
          <span class="search-icon">&#8981;</span>
          <input type="text" id="facSearch" placeholder="Search by faculty name or Faculty ID&hellip;" autocomplete="off">
        </div>
        <span class="hint">Showing <span id="rowCount" class="mono tnum"></span> of <span id="totalCount" class="mono tnum"></span> rows</span>
        <button class="clear-btn" id="clearBtn">Clear</button>
      </div>
      <div class="person-summary" id="personSummary"></div>
      <div style="overflow-x:auto;">
        <table class="recon" id="reconTable">
          <thead>
            <tr>
              <th data-key="name">Faculty member<span class="sort-arrow">&updownarrow;</span></th>
              <th data-key="fid">Faculty ID<span class="sort-arrow">&updownarrow;</span></th>
              <th data-key="title">Pure record<span class="sort-arrow">&updownarrow;</span></th>
              <th data-key="subtypePure">Subtype<span class="sort-arrow">&updownarrow;</span>
                <select class="col-filter" id="subtypeFilter" onclick="event.stopPropagation()"></select>
              </th>
              <th data-key="event">Event type<span class="sort-arrow">&updownarrow;</span>
                <select class="col-filter" id="eventFilter" onclick="event.stopPropagation()">
                  <option value="all">All events</option>
                  <option value="new">New</option>
                  <option value="updated">Updated</option>
                  <option value="deleted">Deleted</option>
                  {event_option_malformed}
                </select>
              </th>
              <th data-key="outcome">Outcome<span class="sort-arrow">&updownarrow;</span>
                <select class="col-filter" id="outcomeFilter" onclick="event.stopPropagation()">
                  <option value="all">All outcomes</option>
                  <option value="delivered">Delivered</option>
                  <option value="dropped">Dropped</option>
                  <option value="retracted">Retracted</option>
                  {outcome_option_malformed}
                </select>
              </th>
            </tr>
          </thead>
          <tbody id="reconBody"></tbody>
        </table>
      </div>
      <div class="pagination-bar">
        <select class="page-size" id="pageSize">
          <option value="25" selected>25 / page</option>
          <option value="50">50 / page</option>
          <option value="100">100 / page</option>
          <option value="250">250 / page</option>
        </select>
        <button class="page-btn" id="prevPage">&larr; Previous</button>
        <span class="page-info" id="pageInfo"></span>
        <button class="page-btn" id="nextPage">Next &rarr;</button>
      </div>
      <div class="recon-footer">One row per record &times; internal participant. External participants are not shown. "Dropped" rows never reached FAR.</div>
    </div>
  </section>
</div>

{js}
</body>
</html>"""
