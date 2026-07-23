# Databricks notebook source
from datetime import datetime

LEGACY_API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-ajman_legacy-prod-api-key')
LEGACY_URL = dbutils.secrets.get(scope='integration-delivery-services', key='pure-ajman_legacy-base-url')

# New secret, mirroring pure-hbku-prod-api-key — Ajman's Pure REST API key
# (not the legacy one used in Part 1 for the changes stream).
API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-ajman-prod-api-key')
API_URL = dbutils.secrets.get(scope='integration-delivery-services', key='pure-ajman-base-url')

# Ajman's Faculty180 API access is still being provisioned (support ticket
# in progress, 2026-07-23) — meanwhile, far_users_source.py reads a CSV the
# user exported directly from Faculty180 (has faculty_id + email columns)
# instead of calling the real API. Switch this to "api" once Ajman's FAR
# API access is confirmed working — nothing else needs to change,
# enrich_changes.py / initial_load_merge_base_snapshot.py both go through
# far_users_source.get_email_to_faculty_id() either way.
FAR_USERS_SOURCE = "csv_bypass"  # "csv_bypass" or "api"

# TODO(user): confirm the real path after uploading the CSV to Databricks
# (DBFS or a Unity Catalog Volume).
FAR_USERS_CSV_PATH = "dbfs:/FileStore/ajman/far_users_bypass.csv"

if FAR_USERS_SOURCE == "api":
    # New secrets, mirroring hbku-far-api-public-key/private-key — Ajman's
    # Faculty180 HMAC-signed API credentials. Only resolved when actually
    # needed — fetching them eagerly would break the CSV bypass path too,
    # since these secrets don't exist yet.
    FAR_PUBLIC_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='ajman-far-api-public-key')
    FAR_PRIVATE_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='ajman-far-api-private-key')
else:
    FAR_PUBLIC_KEY = None
    FAR_PRIVATE_KEY = None

# TODO(user): confirm Ajman's real Faculty180 database identifier
# (HBKU's is "hbku_dev") before switching FAR_USERS_SOURCE to "api".
FAR_DATABASE = "REPLACE_ME_AJMAN_FAR_DATABASE"

# Confirmed against ip-pure2far-integration/ajman_research_output/config.py
# (that pipeline already ran successfully against real Ajman data) — NOT
# "en_GB" like HBKU. This also required fixing every AJMAN_cfg_transform_*.py
# config (they hardcode the locale as a literal JSON path segment, e.g.
# "abstract.en_GB" -> "abstract.en_US" — not something this LANGUAGE
# constant parameterizes on its own).
LANGUAGE = "en_US"

DATABASE = "academicinformationsystems_technicalservices.ajman"

# Prefixed `sync_` so these never collide with ip-pure2far-integration's own
# un-prefixed entity tables living in the same catalog — see
# entity_sync.py's module docstring. No sync_internal_organizations for
# Ajman — it only ever fed Custom Sections' org name resolution, which is
# out of scope for this client.
PERSON_TABLE = "sync_persons"
EVENT_TABLE = "sync_events"
PUBLISHER_TABLE = "sync_publishers"
EXTERNAL_ORG_TABLE = "sync_external_organizations"

# Only used the very first time an entity is synced (a missing table always
# triggers a full reload regardless — see entity_sync.py). Set to the
# earliest of Ajman's Part 1 per-scope cutoffs (part1_changes/ajman/config.py's
# DEFAULT_SINCE_DATES), same rationale HBKU used tying this to its own
# Part 1 DEFAULT_SINCE_DATE.
DEFAULT_SINCE_DATETIME = "2026-06-11T00:00:00.000Z"

# Same formula as part1_changes/ajman/config.py's CURRENT_DAY. enrich_changes.py
# runs right after fetch_changes.py in the same pipeline execution, same day,
# so it reads changes_<scope>_<CURRENT_DAY> directly instead of searching for
# the "latest" table by listing and sorting.
CURRENT_DAY = datetime.now().strftime("%Y%m%d")
