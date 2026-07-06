# Databricks notebook source
LEGACY_API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku_legacy-prod-api-key')
LEGACY_URL = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku_legacy-base-url')

# Same secret already used by ip-pure2far-integration for the current Pure
# REST API — reused here, not recreated.
API_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku-prod-api-key')
# New secret, mirroring how LEGACY_URL was moved out of code in Part 1 (see
# part1_changes/hbku/config.py) instead of hardcoding the Pure hostname again.
API_URL = dbutils.secrets.get(scope='integration-delivery-services', key='pure-hbku-base-url')

# Same secrets already used by tss-dedup's Step 0 for the FAR (Interfolio
# Faculty180) HMAC-signed API.
FAR_PUBLIC_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='hbku-far-api-public-key')
FAR_PRIVATE_KEY = dbutils.secrets.get(scope='integration-delivery-services', key='hbku-far-api-private-key')
FAR_DATABASE = "hbku_dev"

LANGUAGE = "en_GB"

DATABASE = "academicinformationsystems_technicalservices.hbku"

# Prefixed `sync_` so these never collide with ip-pure2far-integration's own
# un-prefixed entity tables living in the same catalog — see
# entity_sync.py's module docstring.
PERSON_TABLE = "sync_persons"
EVENT_TABLE = "sync_events"
PUBLISHER_TABLE = "sync_publishers"
INTERNAL_ORG_TABLE = "sync_internal_organizations"
EXTERNAL_ORG_TABLE = "sync_external_organizations"

# Only used the very first time an entity is synced (its table does not
# exist yet triggers a full reload regardless, but LegacyPureAPI still needs
# some starting point the first time an incremental entity's table exists
# without any ingest_ts rows). Same rationale as
# part1_changes/hbku/config.py's DEFAULT_SINCE_DATE.
DEFAULT_SINCE_DATETIME = "2026-07-01T00:00:00.000Z"
