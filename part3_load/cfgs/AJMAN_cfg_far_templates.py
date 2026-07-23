# Databricks notebook source
# MAGIC %md
# MAGIC ### FAR templates config (Ajman)
# MAGIC `subtype_to_type` / `types` / `type_slug` copied from
# MAGIC `HBKU_cfg_far_templates.py` unchanged — these encode Pure's own
# MAGIC standard Scholarly Activities/Custom Sections taxonomy, not anything
# MAGIC HBKU-specific, so they're expected to apply as-is (per the onboarding
# MAGIC checklist assumption B). **Known risk carried over:** HBKU's real
# MAGIC data surfaced a Custom Sections `typeDiscriminator` ("EditorialWork")
# MAGIC outside these 4 known types — `postprocess_changes.py`'s
# MAGIC `log_unmapped_subtypes` will warn (not silently drop) if Ajman has an
# MAGIC equivalent surprise; don't assume this list is exhaustive.
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
    "Custom Sections": {
        "types": [
            "Service: Professional",
            "Service: University - other than Committees",
            "Other: Professional Membership",
            "Other: Consulting",
        ],
        "type_slug": {
            "Service: Professional": "service_professional",
            "Service: University - other than Committees": "service_university",
            "Other: Professional Membership": "other_professional_membership",
            "Other: Consulting": "other_consulting",
        },
        # TODO(user): confirm real SFTP folder name for Ajman.
        "sftp_folder": "ajman_pure2far_custom",
    },
}
