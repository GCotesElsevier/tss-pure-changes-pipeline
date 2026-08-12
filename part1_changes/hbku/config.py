# Databricks notebook source
from datetime import datetime

LEGACY_API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku_legacy-prod-api-key')
LEGACY_URL = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku_legacy-base-url')

DATABASE = "academicinformationsystems_technicalservices.hbku"
SYNC_STATE_TABLE = "changes_sync_state"

INGEST_TS = datetime.now()
CURRENT_DAY = INGEST_TS.strftime("%Y%m%d")

# Start date used only the very first time the changes stream is polled, i.e.
# before `SYNC_STATE_TABLE` has a saved resumptionToken (ISO date,
# "YYYY-MM-DD"). Every run after that resumes from the persisted token
# instead of this date — see sync_state.py.
#
# 2026-08-12 is the day after tss-dedup's full load + initial sync between
# Pure and FAR completed for all 3 scopes (2026-08-11: Grants at ~19:00 UTC,
# Scholarly Activities/Custom Sections at ~19:25-20:24 UTC), which is also
# when this table's resumptionToken was reset via reset_sync_state.py — this
# date only matters if that control table is ever dropped again. Starting
# from 2026-08-12 rather than 2026-08-11 is deliberate: the changes endpoint
# only accepts day granularity, not time-of-day, so 2026-08-11 would re-pull
# the same evening's events the full load already covered — that leaves an
# uncaptured gap between ~20:24 UTC on 2026-08-11 and midnight, accepted as
# the one-time cost of this cutover.
DEFAULT_SINCE_DATE = "2026-08-12"
