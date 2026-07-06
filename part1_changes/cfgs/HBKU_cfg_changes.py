# Databricks notebook source
# MAGIC %md
# MAGIC ### Changes config — scope to Pure family homologation
# MAGIC A plain data file, no logic — same content as a JSON config would
# MAGIC hold, just loaded via `%run` instead of `open()`. This workspace's
# MAGIC Databricks Repos only reliably resolves files Databricks recognizes
# MAGIC as notebooks (`.py` with this header, `.ipynb`, `.sql`, `.r`); plain
# MAGIC files like `.json` are visible in the Repos UI but are not reliably
# MAGIC readable via `open()` / `os.listdir()` from a running notebook in
# MAGIC this workspace (confirmed directly against a real repo clone).

# COMMAND ----------

CHANGES_CONFIG = {
    "Scholarly Activities": {"pure_families": ["ResearchOutput"]},
    "Grants": {"pure_families": ["Award", "Project"]},
    "Custom Sections": {"pure_families": ["Activity"]},
}
