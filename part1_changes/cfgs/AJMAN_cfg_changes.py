# Databricks notebook source
# MAGIC %md
# MAGIC ### Changes config — scope to Pure family homologation (Ajman)
# MAGIC Same family names as HBKU (`HBKU_cfg_changes.py`), copied as a
# MAGIC starting assumption per the onboarding checklist — **not yet
# MAGIC confirmed against Ajman's real Pure instance**. Run
# MAGIC `ajman/discover_families.py` once connected to Databricks and update
# MAGIC this file if the real `familySystemName` values differ (HBKU's own
# MAGIC `Award` family, for example, took extra diagnosis to confirm — see
# MAGIC part1_changes/README.md).

# COMMAND ----------

CHANGES_CONFIG = {
    "Scholarly Activities": {"pure_families": ["ResearchOutput"]},
    "Grants": {"pure_families": ["Award", "Project"]},
    "Custom Sections": {"pure_families": ["Activity"]},
}
