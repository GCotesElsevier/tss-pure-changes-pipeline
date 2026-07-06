# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 — Sync supporting entities
# MAGIC Keeps Person, Event, Publisher, InternalOrganization, and
# MAGIC ExternalOrganization up to date in this repo's own `sync_*` tables.
# MAGIC Safe to run every time the pipeline runs: `entity_sync.sync_entity`
# MAGIC does a full load the first time (or for InternalOrganization, every
# MAGIC time — Pure's legacy API has no incremental query for it) and an
# MAGIC incremental pull after that.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../pure_api_client

# COMMAND ----------

# MAGIC %run ../entity_transforms

# COMMAND ----------

# MAGIC %run ../entity_sync

# COMMAND ----------

import logging
import sys

# Preemptive: Arrow-optimized createDataFrame silently corrupted a row
# during a similar pandas -> Spark write in part1_changes/hbku/fetch_changes.py
# ("Cannot grow BufferHolder by size -32"). No failure seen here yet, but
# entity_sync.py's _upsert does the same kind of conversion, so disabling
# Arrow here too rather than waiting to hit the same bug.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

pure_api = PureAPI(base_url=API_URL, api_key=API_KEY)
legacy_api = LegacyPureAPI(base_url=LEGACY_URL, api_key=LEGACY_API_KEY)

# (table_name, end_point, legacy_end_point, query_field, process_fn)
# InternalOrganization's legacy_end_point is intentionally empty: Pure's
# legacy API has no incremental query for it, so entity_sync.sync_entity
# always does a full reload for it.
entities = [
    (PERSON_TABLE, "persons", "persons", "personsQuery", process_person),
    (EVENT_TABLE, "events", "events", "eventsQuery", process_event),
    (PUBLISHER_TABLE, "publishers", "publishers", "publishersQuery", process_publisher),
    (INTERNAL_ORG_TABLE, "organizations", "", "", process_organization),
    (EXTERNAL_ORG_TABLE, "external-organizations", "external-organisations", "externalOrganisationsQuery", process_organization),
]

for table_name, end_point, legacy_end_point, query_field, process_fn in entities:
    logger.info("Syncing %s...", table_name)
    count = sync_entity(
        spark=spark,
        pure_api=pure_api,
        legacy_api=legacy_api,
        database=DATABASE,
        table_name=table_name,
        end_point=end_point,
        legacy_end_point=legacy_end_point,
        query_field=query_field,
        process_fn=lambda record, fn=process_fn: fn(record, LANGUAGE),
        default_since_datetime=DEFAULT_SINCE_DATETIME,
    )
    logger.info("Synced %d records into %s.%s", count, DATABASE, table_name)
