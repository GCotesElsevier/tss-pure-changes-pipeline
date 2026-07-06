# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 — Discover Pure change families
# MAGIC One-off diagnostic: pulls the **unfiltered** changes stream over a
# MAGIC short window and reports the distinct `familySystemName` values seen,
# MAGIC so the scope -> family mapping in `cfgs/HBKU_cfg_changes.json` can be
# MAGIC confirmed against real data instead of inferred from REST endpoint
# MAGIC names.
# MAGIC
# MAGIC Run this once per environment to confirm the mapping; it is not part
# MAGIC of the regular pipeline and does not save anything.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../changes_client

# COMMAND ----------

import logging
import sys

import pandas as pd
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

# Start with a short window (a few days) if you just want a quick sanity
# check — the unfiltered stream includes every family Pure tracks
# (ImportResult, Fingerprint, Concept, etc.), not just the ones we care about,
# so it can be a lot noisier than the per-scope pulls in fetch_changes.py.
dbutils.widgets.text("SINCE_DATE", DEFAULT_SINCE_DATE, "Since date (YYYY-MM-DD)")
since_date = dbutils.widgets.get("SINCE_DATE")

# COMMAND ----------

client = PureChangesClient(base_url=LEGACY_URL, api_key=LEGACY_API_KEY)

# No `families` filter on purpose: we want to see every family Pure reports.
raw_events, next_token = client.fetch_changes(start_token_or_date=since_date, families=None)

logger.info("Total raw change events since %s: %d", since_date, len(raw_events))
logger.info("Next resumption token: %s", next_token)

# COMMAND ----------

events_df = pd.DataFrame(raw_events)

family_counts = (
    events_df["familySystemName"].value_counts().rename_axis("familySystemName").reset_index(name="count")
    if not events_df.empty
    else pd.DataFrame(columns=["familySystemName", "count"])
)

logger.info("\n%s", family_counts.to_string(index=False))
family_counts

# COMMAND ----------

# Breakdown by family AND changeType, to sanity-check volumes per action too.
if not events_df.empty:
    family_changetype_counts = (
        events_df.groupby(["familySystemName", "changeType"]).size().reset_index(name="count")
    )
else:
    family_changetype_counts = pd.DataFrame(columns=["familySystemName", "changeType", "count"])

family_changetype_counts
