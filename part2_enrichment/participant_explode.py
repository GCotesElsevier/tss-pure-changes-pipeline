# Databricks notebook source
# MAGIC %md
# MAGIC ### Participant explode
# MAGIC Shared explode logic for the 3 author/participant-like list fields the
# MAGIC transform configs deliberately leave untouched: `contributors`
# MAGIC (Research Output), `persons` (Activity / Custom Sections) and
# MAGIC `participants` (Grants). Pure represents all 3 with the same item
# MAGIC shape — an internal `person` ref OR an `externalPerson` ref, plus a
# MAGIC `name` (firstName/lastName) and a `role` — confirmed against
# MAGIC `ip-pure2far-integration`'s `_process_contributors` / `_get_persons`
# MAGIC (which duplicated this same extraction under two different names,
# MAGIC field-for-field identical otherwise). Grants' `participants` /
# MAGIC `awardHolders` is assumed to share this shape too, by analogy — not
# MAGIC yet confirmed against a real Grants record with participants, since
# MAGIC none has been seen in Part 1 so far. Validate this assumption the
# MAGIC first time a real Grants batch has a non-empty `participants` list.
# MAGIC
# MAGIC Also resolves each internal participant's FAR `faculty_id`: Pure only
# MAGIC ever carries an author's email (via `sync_persons`, joined here on
# MAGIC `person_uuid`), never a FAR id directly — the same email ->
# MAGIC `primary_id` bridge `tss-dedup`'s Step0 built for the old pipeline
# MAGIC (see the "puente" finding in project memory), named `faculty_id` here
# MAGIC instead to avoid colliding with `sync_persons.primary_id`, which is an
# MAGIC unrelated Pure-internal identifier (`typeDiscriminator == "PrimaryId"`).

# COMMAND ----------

import pandas as pd

# COMMAND ----------

_EXPLODE_COLUMNS = ["sort_order", "internal", "person_uuid", "first_name", "last_name", "role"]


def _extract_participant(item: dict, sort_order: int, language: str) -> dict:
    """Pulls the common fields out of one raw contributor/person/participant item."""
    if not isinstance(item, dict):
        return {"sort_order": sort_order, "internal": None, "person_uuid": None,
                "first_name": None, "last_name": None, "role": None}

    internal = "person" in item
    person_uuid = None
    if internal:
        person_uuid = (item.get("person") or {}).get("uuid")
    elif "externalPerson" in item:
        person_uuid = (item.get("externalPerson") or {}).get("uuid")

    name = item.get("name") or {}
    role = item.get("role") or {}

    return {
        "sort_order": sort_order,
        "internal": 1 if internal else 0,
        "person_uuid": person_uuid,
        "first_name": name.get("firstName"),
        "last_name": name.get("lastName"),
        "role": (role.get("term") or {}).get(language),
    }


def explode_participants(df: pd.DataFrame, id_column: str, list_column: str, language: str) -> pd.DataFrame:
    """
    Given a DataFrame with one row per record and a column holding a raw
    list of contributor/person/participant items (or None/NaN), returns a
    long DataFrame — one row per (record, participant) pair — with columns
    [id_column, sort_order, internal, person_uuid, first_name, last_name,
    role]. Records with no items produce no rows (unlike `pandas.explode`,
    which would keep one all-null row per empty list).
    """
    columns = [id_column] + _EXPLODE_COLUMNS

    if list_column not in df.columns:
        return pd.DataFrame(columns=columns)

    rows = []
    for record_id, items in zip(df[id_column], df[list_column]):
        if not isinstance(items, list):
            continue
        for position, item in enumerate(items, start=1):
            extracted = _extract_participant(item, position, language)
            extracted[id_column] = record_id
            rows.append(extracted)

    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows)[columns]


def attach_faculty_id(exploded_df: pd.DataFrame, persons_df: pd.DataFrame, email_to_faculty_id: dict) -> pd.DataFrame:
    """
    Left-joins each participant row against `sync_persons` (on
    `person_uuid`) to pick up the internal person's email, then resolves
    `faculty_id` from the FAR users lookup. External participants (no
    `person_uuid`) simply get null `email`/`faculty_id`.
    """
    person_emails = persons_df[["uuid", "email"]].rename(columns={"uuid": "person_uuid"})
    joined = exploded_df.merge(person_emails, how="left", on="person_uuid")
    joined["faculty_id"] = joined["email"].map(
        lambda email: email_to_faculty_id.get(email.strip().lower()) if isinstance(email, str) else None
    )
    return joined
