# Databricks notebook source
from datetime import datetime

LEGACY_API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku_legacy-prod-api-key')
LEGACY_URL = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku_legacy-base-url')

DATABASE = "academicinformationsystems_technicalservices.hbku"

INGEST_TS = datetime.now()
CURRENT_DAY = INGEST_TS.strftime("%Y%m%d")

# One resumptionToken control table per scope, so Research Outputs, Custom
# Sections and Grants can each run as an independent recurring Databricks
# job (see fetch_changes.py's SCOPE widget) without one scope's run
# advancing — or resetting — another scope's token. Unlike Ajman
# (part1_changes/ajman/config.py), HBKU doesn't need a per-scope
# _resolve_since_date/OLDEST_ALLOWED_SINCE_DATE dance: all 3 scopes were
# synced by tss-dedup's full load on the same day (2026-08-11), so a single
# fixed DEFAULT_SINCE_DATES entry per scope is enough — no divergent
# historical cutoffs to reconcile against Pure's 30-day /changes window.
SYNC_STATE_TABLES = {
    "Scholarly Activities": "changes_sync_state_scholarly_activities",
    "Custom Sections": "changes_sync_state_custom_sections",
    "Grants": "changes_sync_state_grants",
}

# Start date used only the very first time each scope's stream is polled,
# i.e. before that scope's entry in SYNC_STATE_TABLES has a saved
# resumptionToken. Every run after that resumes from that scope's own
# persisted token instead of this date — see sync_state.py.
#
# All 3 scopes start from 2026-08-12 — the same day already used (and
# validated by QA, no findings) for the single shared token this replaces,
# see project_hbku_scope_specific_changetype_routing_implementation memory.
# 2026-08-12 is the day after tss-dedup's full load + initial sync between
# Pure and FAR completed for all 3 scopes (2026-08-11: Grants at ~19:00 UTC,
# Scholarly Activities/Custom Sections at ~19:25-20:24 UTC). Starting from
# 2026-08-12 rather than 2026-08-11 is deliberate: the changes endpoint only
# accepts day granularity, not time-of-day, so 2026-08-11 would re-pull the
# same evening's events the full load already covered — that leaves an
# uncaptured gap between ~20:24 UTC on 2026-08-11 and midnight, accepted as
# the one-time cost of this cutover. Splitting the token into one table per
# scope re-walks this same already-validated day once per scope instead of
# once total — low cost, no duplicate risk (already proven safe).
DEFAULT_SINCE_DATES = {
    "Scholarly Activities": "2026-08-12",
    "Custom Sections": "2026-08-12",
    "Grants": "2026-08-12",
}

# Fallback used only by generic/diagnostic scripts (discover_families.py)
# that need a single representative date rather than a per-scope one.
DEFAULT_SINCE_DATE = min(DEFAULT_SINCE_DATES.values())
