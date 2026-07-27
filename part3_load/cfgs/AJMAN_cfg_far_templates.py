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
# MAGIC `sftp_folder` values confirmed with the user 2026-07-24: same naming
# MAGIC convention as HBKU (`pure2far_scholarly`/`pure2far_grants`, no
# MAGIC `ajman_` prefix needed) — the client separation already comes from
# MAGIC `SFTP_BASE` (`/ajman/incoming/pure2far/ajman_dev` vs HBKU's
# MAGIC `/hbku/incoming/pure2far/hbku_dev`), so the folder names themselves
# MAGIC don't need to repeat it. Corrected 2026-07-27: Ajman is also on a
# MAGIC different SFTP server than HBKU (`transfer.eu1.interfolio.com` vs
# MAGIC `transfer.ops.interfolio.com`) — see `ajman/config.py`'s
# MAGIC `SFTP_SECRET_SCOPE`.

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
        "sftp_folder": "pure2far_scholarly",
    },
    "Grants": {
        "types": ["Award"],
        "sftp_folder": "pure2far_grants",
    },
}
