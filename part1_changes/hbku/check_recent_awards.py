# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Check recent Award activity (diagnostic)
# MAGIC `discover_families.py` found no `Award` events in the changes stream
# MAGIC between 2026-07-01 and 2026-07-06. Pure's `/changes` endpoint has no
# MAGIC server-side family filter, so re-running it over a longer window would
# MAGIC be just as expensive without telling us anything new faster.
# MAGIC
# MAGIC Instead, this checks directly against Pure's legacy `awards` endpoint
# MAGIC (server-side filtered by `createdAfter`, uuid-only response — cheap)
# MAGIC whether any award was created recently at all. If none were, the
# MAGIC absence of `Award` events in the changes stream is explained by low
# MAGIC volume rather than a wrong family name.
# MAGIC
# MAGIC One-off diagnostic; not part of the regular pipeline.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import logging
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

def build_incremental_payload(query_field: str, date_filter: str, size: int = 100) -> str:
    # `createdAfter` is hardcoded here, not a parameter: Pure's legacy XML
    # query for this endpoint rejected `modifiedAfter` with a 400 — mirrors
    # `LegacyPureAPI.build_payload` in ip-pure2far-integration's
    # `grants_integration_upd` branch, which hardcodes the same tag.
    return f"""
        <{query_field}>
        <size>{size}</size>
        <offset>0</offset>
        <fields>
            <field>uuid</field>
        </fields>
        <createdAfter>{date_filter}</createdAfter>
        </{query_field}>
    """


def count_recent_records(base_url: str, api_key: str, end_point: str, query_field: str, since_date: str) -> int:
    """
    Lightweight check against Pure's legacy incremental query for a single
    entity endpoint (e.g. `awards`) — server-side filtered by
    `createdAfter` and uuid-only, so it is much cheaper than paging the
    full, unfiltered changes stream.
    """
    url = f"{base_url}/{end_point}"
    headers = {"accept": "application/json", "Content-Type": "application/xml", "api-key": api_key}
    payload = build_incremental_payload(query_field, since_date)
    response = requests.post(url, headers=headers, data=payload, verify=False)
    response.raise_for_status()
    return response.json().get("count", 0)

# COMMAND ----------

# Pure's changes endpoint rejects tokens/dates older than ~30 days, so this
# is close to the earliest date worth checking.
dbutils.widgets.text("SINCE_DATE", "2026-06-07", "Since date (YYYY-MM-DD, up to ~30 days back)")
since_date = dbutils.widgets.get("SINCE_DATE")

award_count = count_recent_records(LEGACY_URL, LEGACY_API_KEY, "awards", "awardsQuery", since_date)
logger.info("Awards modified since %s: %d", since_date, award_count)
