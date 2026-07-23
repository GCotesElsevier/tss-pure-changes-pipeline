# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 — Initial load: Research Outputs (Ajman)
# MAGIC **One-time notebook, NOT part of the regular pipeline.** Run once,
# MAGIC before `part1_changes/ajman/fetch_changes.py` starts polling Scholarly
# MAGIC Activities changes from `DEFAULT_SINCE_DATES["Scholarly Activities"]`
# MAGIC (2026-06-11).
# MAGIC
# MAGIC Ajman already has Research Output data loaded up to 2026-06-11 by
# MAGIC `ip-pure2far-integration` (a separate repo, run once/separately — see
# MAGIC `ajman_research_output/initial_load.py`) into
# MAGIC `processed_researchoutputs_20260611` / `processed_author_20260611`.
# MAGIC That data was never matched against FAR or uploaded — `initial_load.py`
# MAGIC only talks to Pure — so this notebook:
# MAGIC 1. Reads those two tables.
# MAGIC 2. Fetches FAR's user directory and joins `faculty_id` onto the author
# MAGIC    table by email (same join `enrich_changes.py` does for the regular
# MAGIC    incremental pipeline, done fresh here since this data was never
# MAGIC    matched).
# MAGIC 3. Runs the result through the SAME `far_templates.py` transformers as
# MAGIC    `postprocess_changes.py` (via `initial_load_helpers.py`).
# MAGIC 4. Uploads every non-empty type's CSV to the `new/` SFTP subfolder —
# MAGIC    per the user's decision, this bulk historical load is treated
# MAGIC    exactly like any other batch of new records (see project memory).
# MAGIC
# MAGIC **TODO(user) before running:**
# MAGIC - Run `discover_initial_load_tables.py` first and confirm
# MAGIC   `SOURCE_RESEARCH_OUTPUT_TABLE` / `SOURCE_AUTHOR_TABLE` below are the
# MAGIC   real table names (assumed here: `ip-pure2far-integration`'s own
# MAGIC   `processed_researchoutputs` / `processed_author`, suffixed with the
# MAGIC   2026-06-11 cutoff) and that the author table really has an `email`
# MAGIC   column.
# MAGIC - Fill in `config.py`'s FAR_DATABASE, FAR secrets, and SFTP_BASE.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../spark_utils

# COMMAND ----------

# MAGIC %run ../far_templates

# COMMAND ----------

# MAGIC %run ../sftp_utils

# COMMAND ----------

# MAGIC %run ../far_users_client

# COMMAND ----------

# MAGIC %run ../cfgs/AJMAN_cfg_far_templates

# COMMAND ----------

# MAGIC %run ./initial_load_helpers

# COMMAND ----------

import logging
import sys

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

SOURCE_CUTOFF = "20260611"
SOURCE_RESEARCH_OUTPUT_TABLE = f"processed_researchoutputs_{SOURCE_CUTOFF}"
SOURCE_AUTHOR_TABLE = f"processed_author_{SOURCE_CUTOFF}"

TRANSFORMER_MAP = {
    "Book": Pure_Books_Transformer,
    "Chapter": Pure_Chapter_Transformer,
    "Journal": Pure_Journal_Article_Transformer,
    "Proceeding": Pure_Conference_Transformer,
    "Other": Pure_Other_Transformer,
    "Patent": Pure_Patent_Transformer,
    "Editorial": Pure_Editorial_Transformer,
}


def save_table(df, table_name: str) -> None:
    full_table_name = f"{DATABASE}.{table_name}"
    if df.empty:
        logger.info("Nothing to save for %s — skipping.", full_table_name)
        return
    safe_save_table(spark, logger, df, full_table_name)

# COMMAND ----------

research_output_df = spark.table(f"{DATABASE}.{SOURCE_RESEARCH_OUTPUT_TABLE}").toPandas()
author_df = spark.table(f"{DATABASE}.{SOURCE_AUTHOR_TABLE}").toPandas()
logger.info(
    "Read %d research outputs from %s, %d author rows from %s",
    len(research_output_df), SOURCE_RESEARCH_OUTPUT_TABLE, len(author_df), SOURCE_AUTHOR_TABLE,
)

# COMMAND ----------

far_client = FARUsersClient(public_key=FAR_PUBLIC_KEY, private_key=FAR_PRIVATE_KEY, database=FAR_DATABASE)
far_users = far_client.fetch_all_users()
email_to_faculty_id = {
    user["email"].strip().lower(): user["userid"]
    for user in far_users
    if user.get("email") and user.get("userid") is not None
}
logger.info("Loaded %d FAR users for email -> faculty_id lookup", len(email_to_faculty_id))

author_df["faculty_id"] = author_df["email"].astype(str).str.strip().str.lower().map(email_to_faculty_id)
logger.info(
    "Resolved faculty_id for %d/%d author rows",
    author_df["faculty_id"].notna().sum(), len(author_df),
)

# COMMAND ----------

scholarly_cfg = FAR_TEMPLATES_CONFIG["Scholarly Activities"]

log_unmapped_subtypes(
    research_output_df, "subtype", scholarly_cfg["subtype_to_type"].keys(),
    "scholarly_activities (initial load)", logger,
)
research_output_df = research_output_df.assign(
    type=research_output_df["subtype"].map(scholarly_cfg["subtype_to_type"])
)

# COMMAND ----------

for type_name in scholarly_cfg["types"]:
    df_template = build_far_template(
        research_output_df, type_name, TRANSFORMER_MAP[type_name],
        authors_df=author_df, subtype_filter_col="type",
    )
    if df_template.empty:
        logger.info("[initial_load research_output] no records for type %s.", type_name)
        continue

    df_template["Publication Status"] = "Completed/Published"
    df_template["Review"] = "To be Reviewed"
    df_template = normalize_columns(df_template).drop_duplicates()

    suffix = type_table_suffix(type_name)
    save_table(df_template, f"far_initial_load_results_{suffix}")

    collaborators_df = build_collaborators(author_df, df_template)
    save_table(collaborators_df, f"far_initial_load_collaborators_{suffix}")

    remote_path = upload_df_to_sftp(
        csv_ready(df_template), SFTP_BASE, scholarly_cfg["sftp_folder"], "new",
        f"Faculty180_{suffix}_{YEAR}-{MONTH}-{DAY}_01_initial_load.csv", logger, secret_scope=SFTP_SECRET_SCOPE,
    )
    logger.info(
        "[initial_load research_output] %s: %d rows uploaded to %s",
        type_name, len(df_template), remote_path,
    )

    if not collaborators_df.empty:
        remote_path = upload_df_to_sftp(
            csv_ready(collaborators_df), SFTP_BASE, scholarly_cfg["sftp_folder"], "new",
            f"Faculty180_{suffix}_collaborator_{YEAR}-{MONTH}-{DAY}_01_initial_load.csv",
            logger, secret_scope=SFTP_SECRET_SCOPE,
        )
        logger.info(
            "[initial_load research_output] %s: %d collaborators uploaded to %s",
            type_name, len(collaborators_df), remote_path,
        )
