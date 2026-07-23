# Databricks notebook source
# MAGIC %md
# MAGIC ### FAR users source (Ajman)
# MAGIC Single switch point for where the email -> faculty_id lookup comes
# MAGIC from, controlled by `config.py`'s `FAR_USERS_SOURCE`:
# MAGIC - `"csv_bypass"` (current, 2026-07-23): Ajman's Faculty180 API access
# MAGIC   isn't provisioned yet, so this reads a one-time CSV export the user
# MAGIC   pulled directly from Faculty180 (has `faculty_id` + `email` columns
# MAGIC   among others — only those two are used here).
# MAGIC - `"api"`: calls the real `FARUsersClient.fetch_all_users()`, same as
# MAGIC   HBKU already does.
# MAGIC
# MAGIC `enrich_changes.py` and `initial_load_merge_base_snapshot.py` both
# MAGIC call `get_email_to_faculty_id(spark, logger)` instead of building the
# MAGIC dict inline — switching `FAR_USERS_SOURCE` to `"api"` later needs no
# MAGIC changes to either of those notebooks.

# COMMAND ----------

import pandas as pd


def get_email_to_faculty_id(spark, logger) -> dict:
    if FAR_USERS_SOURCE == "csv_bypass":
        logger.info("FAR API not yet provisioned for Ajman — reading email -> faculty_id from CSV bypass: %s", FAR_USERS_CSV_PATH)
        df = spark.read.option("header", True).csv(FAR_USERS_CSV_PATH).toPandas()

        missing = {"email", "faculty_id"} - set(df.columns)
        if missing:
            raise ValueError(
                f"FAR users CSV bypass ({FAR_USERS_CSV_PATH}) is missing column(s) {missing}. "
                f"Columns present: {list(df.columns)}."
            )

        return {
            str(row["email"]).strip().lower(): row["faculty_id"]
            for _, row in df.iterrows()
            if pd.notna(row["email"]) and pd.notna(row["faculty_id"])
        }

    far_client = FARUsersClient(public_key=FAR_PUBLIC_KEY, private_key=FAR_PRIVATE_KEY, database=FAR_DATABASE)
    far_users = far_client.fetch_all_users()
    return {
        user["email"].strip().lower(): user["userid"]
        for user in far_users
        if user.get("email") and user.get("userid") is not None
    }
