# Databricks notebook source
# MAGIC %md
# MAGIC ### Part 4 config (Ajman)
# MAGIC Part 4 (the client dashboard) is a top-level section, sibling to
# MAGIC part1_changes / part2_enrichment / part3_load. It runs live, right
# MAGIC after a normal changes run, over the SAME `CURRENT_DAY`, and only ever
# MAGIC READS the tables Parts 1-3 already wrote for that day — it never
# MAGIC writes to or modifies anything of theirs. Its own tables use the
# MAGIC `dashboard_*` prefix.
# MAGIC
# MAGIC Same `DATABASE` / `CURRENT_DAY` / `SFTP_*` values as
# MAGIC `part3_load/ajman/config.py` (kept in sync by hand, same as the other
# MAGIC per-part configs already are).

# COMMAND ----------

from datetime import datetime

DATABASE = "academicinformationsystems_technicalservices.ajman"

# Same formula as the other parts' configs. Part 4 runs in the same manual
# execution as Parts 1-3, so it reads the `_<CURRENT_DAY>` tables directly.
CURRENT_DAY = datetime.now().strftime("%Y%m%d")

date_object = datetime.strptime(CURRENT_DAY, "%Y%m%d")
YEAR = date_object.strftime("%Y")
MONTH = date_object.strftime("%m")
DAY = date_object.strftime("%d")

# Report is uploaded to {SFTP_BASE}/{sftp_folder}/reports/ — a sibling of the
# new/ updates/ deletes/ folders postprocess_changes.py already writes to.
# Same server/secret as Part 3 (Ajman is on transfer.eu1.interfolio.com,
# scope "sftp_scope_ajman" — NOT the same server as HBKU).
SFTP_BASE = "/ajman/incoming/pure2far/ajman_dev"
SFTP_SECRET_SCOPE = "sftp_scope_ajman"

# Part 4's own SCOPE widget. Values are the scope slugs used in this repo's
# table names, not Part 1/2/3's "Scholarly Activities"/"Grants" widget
# labels. The manual sequence is: run with SCOPE=research_output, then
# SCOPE=grants, both in the same execution as the changes run.
SCOPE_CONFIG = {
    "research_output": {
        # Part 1 writes the changes table under the "scholarly_activities"
        # slug; Part 2 writes the enriched tables under "research_output".
        "changes_table": "changes_scholarly_activities",
        "enriched_table": "enriched_research_output",
        "enriched_authors_table": "enriched_research_output_authors",
        "enriched_deletes_table": "enriched_research_output_deletes",
        # far_results_<slug>_<date> / far_collaborators_<slug>_<date>, one per
        # FAR output type — these are type_table_suffix() of each entry in
        # AJMAN_cfg_far_templates["Scholarly Activities"]["types"].
        "far_result_slugs": ["book", "chapter", "journal", "proceeding", "patent", "other"],
        "far_templates_key": "Scholarly Activities",
        "report_scope_label": "Research Outputs",
        "report_noun": "records",
    },
    "grants": {
        "changes_table": "changes_grants",
        "enriched_table": "enriched_grants",
        "enriched_authors_table": "enriched_grants_authors",
        "enriched_deletes_table": "enriched_grants_deletes",
        "far_result_slugs": ["award"],
        "far_templates_key": "Grants",
        "report_scope_label": "Grants",
        "report_noun": "grants",
    },
}

# --- Coverage window ---
# `<start>` = MAX(delivered_at) from dashboard_delivery_log for the scope, or
# — on the very first delivery of a scope, when that table has no prior row —
# the one-time initial-load snapshot cutoff below (from
# part1_changes/ajman/config.py's PROCESSED_SNAPSHOT_CUTOFFS; these never
# change). Whatever that candidate is, it is then floored at
# `today - COVERAGE_MAX_LOOKBACK_DAYS`: Pure's /changes endpoint physically
# can't return events older than ~30 days, so the report must not claim to
# cover a longer window than that. `<end>` = the timestamp of this run.
# No COVERAGE_START widget — decided with the user 2026-09-10, source is
# dashboard_delivery_log only (a background Part 1 under the future on-demand
# design would make changes_sync_state_<scope>.updated_at reflect the last
# poll, not the last delivery — the delivery log doesn't have that problem).
SNAPSHOT_CUTOFFS = {
    "research_output": "2026-07-23",
    "grants": "2026-06-25",
}
COVERAGE_MAX_LOOKBACK_DAYS = 30

DELIVERY_LOG_TABLE = "dashboard_delivery_log"

# Timezone for RUN_TS (build_dashboard.py) -- the "now" used for
# coverage_end/delivery_date in the report and for the delivered_at row
# written to dashboard_delivery_log. Pinned instead of relying on the
# Databricks cluster's own clock/timezone: 2026-09-11 the cluster had
# already rolled over to the next calendar day while it was still the 10th
# for the person running it, so a same-day run showed a 1-day-wide coverage
# window instead of "same day". Ajman's own timezone, not whoever happens
# to run this or which cluster it lands on — a client report's date should
# read as the client's calendar day, and it stays correct once Part 1 runs
# in the background (see project_hbku_on_demand_delivery_design, memory):
# RUN_TS is computed once, when Part 4 itself runs, independent of how many
# times Part 1 polled beforehand.
REPORT_TIMEZONE = "Asia/Dubai"

# The internal FAR type "Other" IS "Other Scholarly Work" on the FAR side
# (TSSH-1087) — the report shows the client-facing label.
FAR_TYPE_DISPLAY = {
    "Other": "Other Scholarly Work",
}

# FAR output type -> a categorical colour token from the approved mockup's
# palette (dataviz skill's validated defaults). Used by the renderer for the
# bar chart / donut / Sankey leaves. Keyed by the internal FAR type name
# (subtype_to_type's values), not the display label.
FAR_TYPE_COLOR_VAR = {
    "Journal": "--cat-journal",
    "Chapter": "--cat-chapter",
    "Proceeding": "--cat-proceeding",
    "Book": "--cat-book",
    "Other": "--cat-other",
    "Patent": "--cat-patent",
    "Award": "--cat-award",
}
