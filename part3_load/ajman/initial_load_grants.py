# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 — Initial load: Grants (Ajman)
# MAGIC **One-time notebook, NOT part of the regular pipeline.** Run once,
# MAGIC before `part1_changes/ajman/fetch_changes.py` starts polling Grants
# MAGIC changes from `DEFAULT_SINCE_DATES["Grants"]` (2026-06-25).
# MAGIC
# MAGIC Same shape as `initial_load_research_output.py` — see that notebook's
# MAGIC docstring for the overall design (read pre-existing `processed_*`
# MAGIC tables -> fresh email -> faculty_id join against FAR -> same
# MAGIC `far_templates.py` transformer -> upload to SFTP `new/`).
# MAGIC
# MAGIC **Materially LESS confirmed than the Research Output initial load —
# MAGIC read before running:**
# MAGIC - Research Output's source tables were confirmed directly against
# MAGIC   `ip-pure2far-integration/ajman_research_output/initial_load.py`
# MAGIC   (`main` branch, available locally). Grants' equivalent
# MAGIC   (`grants_initial_load.py`, per project memory) lives on the
# MAGIC   `grants_integration_upd` branch, which is **not** currently checked
# MAGIC   out locally — so `SOURCE_GRANTS_TABLE` / `SOURCE_GRANTS_AUTHOR_TABLE`
# MAGIC   below, and the assumption that the columns line up with
# MAGIC   `far_templates.py`'s `Pure_Grants_Transformer`, are inferred from
# MAGIC   HBKU's `GRANTS_TRANSFORM_CONFIG` (a close port of
# MAGIC   `ip-pure2far-integration`'s own `GRANTS_CONFIG`) — **not verified
# MAGIC   against a real Ajman table.**
# MAGIC - The author companion table name itself was never fully pinned down
# MAGIC   even for HBKU (project memory flags `processed_grant_author` vs
# MAGIC   `processed_grant_authors` as an open discrepancy) — confirm the real
# MAGIC   name via `discover_initial_load_tables.py` before running this.
# MAGIC
# MAGIC **TODO(user) before running:**
# MAGIC - Run `discover_initial_load_tables.py` first; fix `SOURCE_GRANTS_TABLE`
# MAGIC   / `SOURCE_GRANTS_AUTHOR_TABLE` and re-check every column referenced
# MAGIC   below (`uuid`, `subtype`/`type` equivalent — Grants has a single
# MAGIC   output type "Award" so there may be none — `awardStatus`,
# MAGIC   `awardStatusDate`, `title`, `sponsor`, `contractid`, `awardDate`,
# MAGIC   `startDate`, `endDate`, `fundingType`, `totalAwardAmount`,
# MAGIC   `currency`, `abstract`, `description`, `url`, `type` (grant type),
# MAGIC   plus the author table's `email`/`internal`/`sort_order`/`role`/
# MAGIC   `first_name`/`last_name`).
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

SOURCE_CUTOFF = "20260625"
# TODO(user): confirm both names against discover_initial_load_tables.py —
# unlike research output's, these are inferred, not confirmed (see module
# docstring).
SOURCE_GRANTS_TABLE = f"processed_grants_{SOURCE_CUTOFF}"
SOURCE_GRANTS_AUTHOR_TABLE = f"processed_grant_author_{SOURCE_CUTOFF}"


def save_table(df, table_name: str) -> None:
    full_table_name = f"{DATABASE}.{table_name}"
    if df.empty:
        logger.info("Nothing to save for %s — skipping.", full_table_name)
        return
    safe_save_table(spark, logger, df, full_table_name)

# COMMAND ----------

grants_df = spark.table(f"{DATABASE}.{SOURCE_GRANTS_TABLE}").toPandas()
grants_author_df = spark.table(f"{DATABASE}.{SOURCE_GRANTS_AUTHOR_TABLE}").toPandas()
logger.info(
    "Read %d grants from %s, %d author rows from %s",
    len(grants_df), SOURCE_GRANTS_TABLE, len(grants_author_df), SOURCE_GRANTS_AUTHOR_TABLE,
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

grants_author_df["faculty_id"] = (
    grants_author_df["email"].astype(str).str.strip().str.lower().map(email_to_faculty_id)
)
logger.info(
    "Resolved faculty_id for %d/%d author rows",
    grants_author_df["faculty_id"].notna().sum(), len(grants_author_df),
)

# COMMAND ----------

grants_cfg = FAR_TEMPLATES_CONFIG["Grants"]

for type_name in grants_cfg["types"]:  # just ["Award"] — no subtype filter, same as postprocess_changes.py
    df_template = build_far_template(
        grants_df, type_name, Pure_Grants_Transformer,
        authors_df=grants_author_df, subtype_filter_col=None,
    )
    if df_template.empty:
        logger.info("[initial_load grants] no records for type %s.", type_name)
        continue

    df_template = df_template.drop(columns=["Co-Investigator(s)"], errors="ignore")
    df_template["Review"] = "To be Reviewed"
    df_template = normalize_columns(df_template).drop_duplicates()

    suffix = type_table_suffix(type_name)
    save_table(df_template, f"far_initial_load_results_{suffix}")

    collaborators_df = build_collaborators(grants_author_df, df_template)
    save_table(collaborators_df, f"far_initial_load_collaborators_{suffix}")

    remote_path = upload_df_to_sftp(
        csv_ready(df_template), SFTP_BASE, grants_cfg["sftp_folder"], "new",
        f"Faculty180_{suffix}_{YEAR}-{MONTH}-{DAY}_01_initial_load.csv", logger, secret_scope=SFTP_SECRET_SCOPE,
    )
    logger.info("[initial_load grants] %s: %d rows uploaded to %s", type_name, len(df_template), remote_path)

    if not collaborators_df.empty:
        remote_path = upload_df_to_sftp(
            csv_ready(collaborators_df), SFTP_BASE, grants_cfg["sftp_folder"], "new",
            f"Faculty180_{suffix}_collaborator_{YEAR}-{MONTH}-{DAY}_01_initial_load.csv",
            logger, secret_scope=SFTP_SECRET_SCOPE,
        )
        logger.info(
            "[initial_load grants] %s: %d collaborators uploaded to %s",
            type_name, len(collaborators_df), remote_path,
        )
