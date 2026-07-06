# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 — Enrich changes (main orchestration)
# MAGIC For each of the 3 scopes (Scholarly Activities / Research Output,
# MAGIC Custom Sections / Activity, Grants), reads today's
# MAGIC `changes_<scope>_<CURRENT_DAY>` table written by Part 1's
# MAGIC `fetch_changes.py` (same `CURRENT_DAY`, since both notebooks run in the
# MAGIC same pipeline execution — no need to search for the "latest" table),
# MAGIC then per non-DELETE record:
# MAGIC 1. Fetches the full Pure record (Grants uses `grants_merge.py` to pair
# MAGIC    a changed Project/Award with its counterpart).
# MAGIC 2. Applies the scope's transform config
# MAGIC    (`RESEARCH_OUTPUT_TRANSFORM_CONFIG` / `ACTIVITY_TRANSFORM_CONFIG` /
# MAGIC    `GRANTS_TRANSFORM_CONFIG`).
# MAGIC 3. Joins with the support entity tables (`sync_publishers`,
# MAGIC    `sync_events`, `sync_internal_organizations` +
# MAGIC    `sync_external_organizations`).
# MAGIC 4. Explodes `contributors` / `persons` / `participants` (kept raw by
# MAGIC    the transform configs on purpose — see `participant_explode.py`)
# MAGIC    and resolves each internal participant's FAR `faculty_id` via
# MAGIC    email (Pure never carries a FAR id directly).
# MAGIC
# MAGIC DELETE records are NOT enriched — the Pure record is already gone by
# MAGIC the time the change event shows up, so there is nothing to fetch and
# MAGIC no reliable way to know which faculty it belonged to. They are saved
# MAGIC as a minimal `uuid` + `scope` + `changeType` table per scope, for
# MAGIC audit/log purposes — Part 3 decides what (if anything) to do with them.
# MAGIC
# MAGIC **Output tables** (mirrors `tss-dedup`'s `processed_*` shape, new
# MAGIC `enriched_` prefix so these never collide with `ip-pure2far-integration`'s
# MAGIC own un-prefixed tables in the same catalog):
# MAGIC - `enriched_research_output_<date>` + `enriched_research_output_authors_<date>`
# MAGIC - `enriched_custom_sections_<date>` (participants exploded IN PLACE — one
# MAGIC   row per activity+participant, no separate table, same as the original
# MAGIC   `ActivityDataProcessor`)
# MAGIC - `enriched_grants_<date>` + `enriched_grants_participants_<date>`
# MAGIC - `enriched_<scope>_deletes_<date>` (all 3 scopes, same minimal shape)
# MAGIC
# MAGIC **Known gaps / deliberately NOT replicated from the old pipeline**
# MAGIC (left for Part 3, or flagged as unconfirmed):
# MAGIC - No `title`+`subTitle` merge, no `status_date` construction, no numeric
# MAGIC   casts, no `role` uppercasing — these were cosmetic finishing touches
# MAGIC   in `ip-pure2far-integration`'s `processed_joined_data`; subtype-specific
# MAGIC   column selection already lives in Part 3, so this batch is left wider
# MAGIC   and less polished on purpose.
# MAGIC - Research Output's own `organizations` field stays as a joined uuid
# MAGIC   string (not resolved to names) — matches the original code's scope,
# MAGIC   which never resolved it either.
# MAGIC - Event `sponsorOrganizations` (on `sync_events`) is not resolved to a
# MAGIC   sponsor name here.
# MAGIC - Grants' `participants`/`awardHolders` item shape is ASSUMED to match
# MAGIC   contributors/persons (person/externalPerson + name + role) — not yet
# MAGIC   confirmed against a real Grants record with participants (none seen
# MAGIC   so far). Validate the first time one shows up.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../pure_api_client

# COMMAND ----------

# MAGIC %run ../transform_engine

# COMMAND ----------

# MAGIC %run ../far_users_client

# COMMAND ----------

# MAGIC %run ../grants_merge

# COMMAND ----------

# MAGIC %run ../participant_explode

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_transform_research_output

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_transform_activity

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_transform_grants

# COMMAND ----------

import logging
import sys

import pandas as pd

# Same Arrow bug Part 1 hit (see part1_changes/hbku/fetch_changes.py): a
# pandas -> Spark conversion can silently corrupt small/oddly-typed batches
# unless this is disabled first.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# logging.basicConfig() is a no-op here: Databricks pre-configures the root
# logger's handlers before this cell runs, so basicConfig's "only attach a
# handler if the root has none yet" check silently skips it. Configuring our
# own named logger directly (same pattern as fetch_changes.py) avoids that,
# and explicitly targets stdout since the default StreamHandler's stderr
# output doesn't reliably show in the notebook cell's output area.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# Support entity tables — loaded once, reused across all 3 scopes.
persons_df = spark.table(f"{DATABASE}.{PERSON_TABLE}").toPandas()
events_df = spark.table(f"{DATABASE}.{EVENT_TABLE}").toPandas()
publishers_df = spark.table(f"{DATABASE}.{PUBLISHER_TABLE}").toPandas()
internal_orgs_df = spark.table(f"{DATABASE}.{INTERNAL_ORG_TABLE}").toPandas()
external_orgs_df = spark.table(f"{DATABASE}.{EXTERNAL_ORG_TABLE}").toPandas()

orgs_df = pd.concat([internal_orgs_df, external_orgs_df], ignore_index=True)
org_name_by_uuid = orgs_df.set_index("uuid")["name"].to_dict()

publisher_lookup = publishers_df[["uuid", "name", "country"]].rename(
    columns={"uuid": "publisher_uuid", "name": "publisher_name", "country": "publisher_country"}
)
event_lookup = events_df[["uuid", "type", "title", "city", "country", "startDate", "endDate"]].rename(
    columns={
        "uuid": "event_uuid",
        "type": "event_type",
        "title": "event_title",
        "city": "event_city",
        "country": "event_country",
        "startDate": "event_startDate",
        "endDate": "event_endDate",
    }
)

# COMMAND ----------

# FAR (Faculty180) users, fetched once: bridges Pure's author email to a
# `faculty_id` — Pure has no native FAR id (see tss-dedup Step0's original
# email -> primary_id join, replicated here under the name `faculty_id` to
# avoid colliding with sync_persons.primary_id, an unrelated Pure-internal
# identifier).
far_client = FARUsersClient(public_key=FAR_PUBLIC_KEY, private_key=FAR_PRIVATE_KEY, database=FAR_DATABASE)
far_users = far_client.fetch_all_users()
email_to_faculty_id = {
    user["email"].strip().lower(): user["userid"]
    for user in far_users
    if user.get("email") and user.get("userid") is not None
}
logger.info("Loaded %d FAR users for email -> faculty_id lookup", len(email_to_faculty_id))

# COMMAND ----------

pure_api = PureAPI(base_url=API_URL, api_key=API_KEY)

# COMMAND ----------

def read_changes_table(scope_slug: str) -> pd.DataFrame:
    """
    Reads today's changes_<scope>_<CURRENT_DAY> table written by Part 1's
    fetch_changes.py. fetch_changes.py only saves a table when it has at
    least one event for that scope that day, so a missing table just means
    zero changes today — not an error.
    """
    table_name = f"{DATABASE}.changes_{scope_slug}_{CURRENT_DAY}"
    try:
        return spark.table(table_name).toPandas()
    except Exception:
        logger.info("No changes table for %s today (%s) — treating as zero events.", scope_slug, table_name)
        return pd.DataFrame(columns=["uuid", "changeType", "familySystemName", "version"])


def split_deletes(changes_df: pd.DataFrame):
    deletes = changes_df[changes_df["changeType"] == "DELETE"].copy()
    non_deletes = changes_df[changes_df["changeType"] != "DELETE"].copy()
    return non_deletes, deletes


def build_deletes_table(deletes_df: pd.DataFrame, scope_slug: str) -> pd.DataFrame:
    """
    Deletes are NOT enriched (see module docstring) — just the bare
    identifying info, for audit/log purposes.
    """
    if deletes_df.empty:
        return pd.DataFrame()
    return deletes_df[["uuid", "changeType", "version"]].assign(scope=scope_slug)


def build_org_name_map(uuid_series: pd.Series) -> pd.Series:
    return uuid_series.map(org_name_by_uuid)


def save_table(df: pd.DataFrame, table_name: str) -> None:
    full_table_name = f"{DATABASE}.{table_name}"
    if df.empty:
        logger.info("Nothing to save for %s — skipping.", full_table_name)
        return
    clean_df = df.astype(object).where(pd.notnull(df), None)
    spark_df = spark.createDataFrame(clean_df)
    spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
    logger.info("Saved %d rows to %s", len(df), full_table_name)

# COMMAND ----------

def process_research_output(uuids: list):
    if not uuids:
        return pd.DataFrame(), pd.DataFrame()

    raw_records = [pure_api.read_record("research-outputs", uuid) for uuid in uuids]
    flat = flatten_dataframe(raw_records)
    result = apply_transforms(flat, RESEARCH_OUTPUT_TRANSFORM_CONFIG)

    result = result.merge(publisher_lookup, how="left", on="publisher_uuid")
    result = result.merge(event_lookup, how="left", on="event_uuid")

    authors = explode_participants(result, id_column="uuid", list_column="contributors", language=LANGUAGE)
    authors = attach_faculty_id(authors, persons_df, email_to_faculty_id)

    result = result.drop(columns=["contributors"])
    return result, authors

# COMMAND ----------

def process_custom_sections(uuids: list) -> pd.DataFrame:
    if not uuids:
        return pd.DataFrame()

    raw_records = [pure_api.read_record("activities", uuid) for uuid in uuids]
    flat = flatten_dataframe(raw_records)
    result = apply_transforms(flat, ACTIVITY_TRANSFORM_CONFIG)

    result["managing_organization"] = build_org_name_map(result["managing_organization_uuid"])
    result["member_of_name"] = build_org_name_map(result["member_of_uuid"])
    result = result.drop(columns=["managing_organization_uuid", "member_of_uuid"])

    # Custom Sections has no separate participant table (unlike Research
    # Output / Grants) — `persons` is exploded IN PLACE, one row per
    # activity+participant, same as the original ActivityDataProcessor.
    participants = explode_participants(result, id_column="uuid", list_column="persons", language=LANGUAGE)
    participants = attach_faculty_id(participants, persons_df, email_to_faculty_id)

    return result.drop(columns=["persons"]).merge(participants, how="left", on="uuid")

# COMMAND ----------

def process_grants(changes_rows: pd.DataFrame):
    if changes_rows.empty:
        return pd.DataFrame(), pd.DataFrame()

    merged_records = [
        fetch_and_merge_grant(pure_api, row["uuid"], row["familySystemName"])
        for _, row in changes_rows.iterrows()
    ]
    flat = flatten_dataframe(merged_records)
    result = apply_transforms(flat, GRANTS_TRANSFORM_CONFIG, context={"external_organizations": external_orgs_df})

    participants = explode_participants(result, id_column="uuid", list_column="participants", language=LANGUAGE)
    participants = attach_faculty_id(participants, persons_df, email_to_faculty_id)

    result = result.drop(columns=["participants"])
    return result, participants

# COMMAND ----------

scholarly_changes = read_changes_table("scholarly_activities")
scholarly_non_deletes, scholarly_deletes = split_deletes(scholarly_changes)

research_output_df, research_output_authors_df = process_research_output(scholarly_non_deletes["uuid"].tolist())
research_output_deletes_df = build_deletes_table(scholarly_deletes, "scholarly_activities")

save_table(research_output_df, f"enriched_research_output_{CURRENT_DAY}")
save_table(research_output_authors_df, f"enriched_research_output_authors_{CURRENT_DAY}")
save_table(research_output_deletes_df, f"enriched_research_output_deletes_{CURRENT_DAY}")

# COMMAND ----------

custom_sections_changes = read_changes_table("custom_sections")
custom_sections_non_deletes, custom_sections_deletes = split_deletes(custom_sections_changes)

custom_sections_df = process_custom_sections(custom_sections_non_deletes["uuid"].tolist())
custom_sections_deletes_df = build_deletes_table(custom_sections_deletes, "custom_sections")

save_table(custom_sections_df, f"enriched_custom_sections_{CURRENT_DAY}")
save_table(custom_sections_deletes_df, f"enriched_custom_sections_deletes_{CURRENT_DAY}")

# COMMAND ----------

grants_changes = read_changes_table("grants")
grants_non_deletes, grants_deletes = split_deletes(grants_changes)

grants_df, grants_participants_df = process_grants(grants_non_deletes)
grants_deletes_df = build_deletes_table(grants_deletes, "grants")

save_table(grants_df, f"enriched_grants_{CURRENT_DAY}")
save_table(grants_participants_df, f"enriched_grants_participants_{CURRENT_DAY}")
save_table(grants_deletes_df, f"enriched_grants_deletes_{CURRENT_DAY}")
