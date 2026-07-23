# Databricks notebook source
# MAGIC %md
# MAGIC ### Changes config — scope to Pure family homologation (Ajman)
# MAGIC **Ajman only needs Grants and Scholarly Activities** — Custom Sections
# MAGIC is explicitly out of scope for this client (confirmed with the user
# MAGIC 2026-07-23; it stays HBKU-only for now). Family names copied from
# MAGIC HBKU (`HBKU_cfg_changes.py`) as a starting assumption per the
# MAGIC onboarding checklist. `Grants` is now CONFIRMED against real data —
# MAGIC `fetch_changes.py` pulled 232 real CREATE events for it on 2026-07-23.
# MAGIC `ResearchOutput` (Scholarly Activities) is still unconfirmed — run
# MAGIC `ajman/discover_families.py` if it doesn't behave as expected.

# COMMAND ----------

CHANGES_CONFIG = {
    "Scholarly Activities": {"pure_families": ["ResearchOutput"]},
    "Grants": {"pure_families": ["Award", "Project"]},
}
