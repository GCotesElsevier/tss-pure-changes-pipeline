# Databricks notebook source
# MAGIC %md
# MAGIC ### FAR templates config
# MAGIC Ported from `tss-dedup`'s `cfgs/HBKU_cfg_postprocessor.json`. Drops
# MAGIC `source_primary` / `source_authors` — those pointed at tss-dedup's
# MAGIC dated-alias tables (`processed_research_outputs`, etc.); this pipeline
# MAGIC computes its own source table names directly from `CURRENT_DAY`
# MAGIC (`enriched_<scope>_<date>` / `enriched_<scope>_authors_<date>`), same
# MAGIC as Part 1/Part 2 already do — no separate config needed for that.
# MAGIC
# MAGIC `sftp_folder` is kept as a placeholder for each scope but not wired up
# MAGIC to anything yet — the SFTP upload step (and its new/updates/deletes
# MAGIC subfolder structure, different from tss-dedup's single-folder +
# MAGIC `old_files` archive) is still to be designed together.

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
        "sftp_folder": "pure2far_custom",
    },
}
