# Databricks notebook source
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

LEGACY_API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-ajman_legacy-prod-api-key')
LEGACY_URL = dbutils.secrets.get(scope='integration-delivery-services', key='pure-ajman_legacy-base-url')

DATABASE = "academicinformationsystems_technicalservices.ajman"

INGEST_TS = datetime.now()
CURRENT_DAY = INGEST_TS.strftime("%Y%m%d")

# Unlike HBKU (one shared resumptionToken across all 3 scopes, since every
# scope needed to resume from roughly the same point), Ajman already has
# pre-existing data loaded separately up to a per-scope cutoff (see the
# initial load in part3_load/ajman/) — so each scope must resume its Changes
# stream from its OWN cutoff, not a shared one. This only means calling
# get_last_resumption_token / save_resumption_token once per scope with a
# different table_name each time — sync_state.py itself needed no changes.
SYNC_STATE_TABLES = {
    "Scholarly Activities": "changes_sync_state_scholarly_activities",
    "Grants": "changes_sync_state_grants",
    "Custom Sections": "changes_sync_state_custom_sections",
}

# The processed_* snapshot cutoffs the user gave us when this was designed
# (2026-07-09) — the actual initial-load cutoff dates, NOT necessarily still
# usable as a `/changes` start date (see PROCESSED_SNAPSHOT_CUTOFFS below).
PROCESSED_SNAPSHOT_CUTOFFS = {
    "Scholarly Activities": "2026-06-11",
    "Grants": "2026-06-25",
}

# Pure's /changes endpoint rejects a tokenOrDate more than 30 days old,
# relative to whenever this actually runs — not to whenever this file was
# written. A fixed literal date goes stale the moment real time passes it,
# which is exactly what happened here: this was designed on 2026-07-09, but
# by the time it was picked back up (2026-07-23) the Scholarly Activities
# cutoff (2026-06-11) had been unusable for 12 days already, and Grants'
# (2026-06-25) was about to follow. So instead of a literal date, each
# scope's start date is computed at import time as MAX(its own snapshot
# cutoff, the oldest date Pure will currently accept) — uses the real
# cutoff if it's still inside the 30-day window, otherwise falls back to the
# oldest allowed date and logs a warning, since falling back means a real
# gap: any changes to a record between its cutoff and the fallback date that
# never happened again later will never be seen by this pipeline.
OLDEST_ALLOWED_SINCE_DATE = INGEST_TS - timedelta(days=29)  # 1 day of safety margin under Pure's 30-day limit


def _resolve_since_date(scope_name: str, cutoff_str: str) -> str:
    cutoff_date = datetime.strptime(cutoff_str, "%Y-%m-%d")
    if cutoff_date < OLDEST_ALLOWED_SINCE_DATE:
        logger.warning(
            "[%s] snapshot cutoff %s is older than Pure's 30-day /changes window (oldest allowed: %s) — "
            "starting from the oldest allowed date instead. Any changes to a %s record between %s and "
            "%s that were never repeated after that will NOT be captured by this pipeline.",
            scope_name, cutoff_str, OLDEST_ALLOWED_SINCE_DATE.strftime("%Y-%m-%d"),
            scope_name, cutoff_str, OLDEST_ALLOWED_SINCE_DATE.strftime("%Y-%m-%d"),
        )
        return OLDEST_ALLOWED_SINCE_DATE.strftime("%Y-%m-%d")
    return cutoff_str


# Start date used only the very first time each scope's stream is polled,
# i.e. before that scope's entry in SYNC_STATE_TABLES has a saved
# resumptionToken. Every run after that resumes from that scope's own
# persisted token instead of this date — see part1_changes/sync_state.py.
#
# - Scholarly Activities / Grants: resolved dynamically against their real
#   snapshot cutoff, see _resolve_since_date above.
# - Custom Sections: no prior snapshot exists for this scope (starts from
#   zero, no initial load) — so it starts from whenever this pipeline first
#   runs, computed dynamically rather than a fixed date.
DEFAULT_SINCE_DATES = {
    "Scholarly Activities": _resolve_since_date("Scholarly Activities", PROCESSED_SNAPSHOT_CUTOFFS["Scholarly Activities"]),
    "Grants": _resolve_since_date("Grants", PROCESSED_SNAPSHOT_CUTOFFS["Grants"]),
    "Custom Sections": INGEST_TS.strftime("%Y-%m-%d"),
}

# Fallback used only by generic/diagnostic scripts (discover_families.py,
# reset_sync_state.py) that need a single representative date rather than
# a per-scope one.
DEFAULT_SINCE_DATE = min(DEFAULT_SINCE_DATES.values())
