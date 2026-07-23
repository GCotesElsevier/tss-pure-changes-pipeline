# Databricks notebook source
# MAGIC %md
# MAGIC ### FAR templates config (Ajman)
# MAGIC **Grants + Scholarly Activities only** — Custom Sections is
# MAGIC explicitly out of scope for Ajman (confirmed with the user
# MAGIC 2026-07-23; stays HBKU-only). `subtype_to_type` / `types` copied from
# MAGIC `HBKU_cfg_far_templates.py` unchanged — these encode Pure's own
# MAGIC standard Scholarly Activities taxonomy, not anything HBKU-specific,
# MAGIC so they're expected to apply as-is (per the onboarding checklist
# MAGIC assumption B).
# MAGIC
# MAGIC `sftp_folder` values are PLACEHOLDERS (`ajman_pure2far_*`, not copied
# MAGIC from HBKU's `pure2far_*`) — TODO(user): confirm Ajman's real SFTP
# MAGIC folder naming convention before this is used for an actual upload.

# COMMAND ----------

FAR_TEMPLATES_CONFIG = {
    "Scholarly Activities": {
        "subtype_to_type": {
            "Article": "Journal",
            "Book": "Book",
            "Chapter": "Chapter",
            "Comment/debate": "Other",
            "Commissioned report": "Other",
            "Conference article": "Journal",
            "Conference contribution": "Proceeding",
            "Editorial": "Editorial",
            "Foreword/postscript": "Other",
            "Letter": "Other",
            "Meeting Abstract": "Other",
            "Other contribution": "Other",
            "Paper": "Other",
            "Patent": "Patent",
            "Review article": "Journal",
            "Short survey": "Other",
        },
        "types": ["Book", "Chapter", "Journal", "Proceeding", "Patent", "Other", "Editorial"],
        # TODO(user): confirm real SFTP folder name for Ajman.
        "sftp_folder": "ajman_pure2far_scholarly",
    },
    "Grants": {
        "types": ["Award"],
        # TODO(user): confirm real SFTP folder name for Ajman.
        "sftp_folder": "ajman_pure2far_grants",
    },
}
