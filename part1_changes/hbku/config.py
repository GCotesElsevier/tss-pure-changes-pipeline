# Databricks notebook source
from datetime import datetime

LEGACY_API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku_legacy-prod-api-key')
LEGACY_URL = 'https://elmi.hbku.edu.qa/ws/api/524'

DATABASE = "academicinformationsystems_technicalservices.hbku"
SYNC_STATE_TABLE = "changes_sync_state"

INGEST_TS = datetime.now()
CURRENT_DAY = INGEST_TS.strftime("%Y%m%d")

# Start date used only the very first time the changes stream is polled, i.e.
# before `SYNC_STATE_TABLE` has a saved resumptionToken (ISO date,
# "YYYY-MM-DD"). Every run after that resumes from the persisted token
# instead of this date — see sync_state.py.
#
# 2026-07-01 is the day after the last confirmed fully-current state across
# all 3 scopes: Scholarly Activities (research outputs) last ran through
# 2026-06-30, and Grants / Custom Sections last ran on 2026-06-22 but had no
# new changes when checked again on 2026-06-30 — so all 3 scopes were already
# current as of 2026-06-30.
DEFAULT_SINCE_DATE = "2026-07-01"
