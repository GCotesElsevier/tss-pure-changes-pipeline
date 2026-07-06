# Databricks notebook source
# MAGIC %md
# MAGIC ### Entity transforms
# MAGIC Flattens a single raw Pure record for each supporting entity into the
# MAGIC flat dict shape saved by `entity_sync.sync_entity`.
# MAGIC
# MAGIC Ported from `Transformer.process_persons` / `process_publishers` /
# MAGIC `process_events` / `process_organizations` in
# MAGIC `ip-pure2far-integration`'s `transform.py`, as plain functions with an
# MAGIC explicit `language` parameter instead of the original's mutable
# MAGIC class-level `Transformer.language` (set once via `set_language` and
# MAGIC read implicitly everywhere) — same behavior, no hidden global state.
# MAGIC
# MAGIC `InternalOrganization` and `ExternalOrganization` share the same shape
# MAGIC in Pure (`process_organization` covers both).

# COMMAND ----------

def _join_list(values: list, separator: str = "; ") -> str:
    return separator.join(values) if values else None


def _find_person_ids(identifiers: list, language: str):
    primary_id = None
    scopus_id = None
    employee_id = None
    for item in identifiers:
        if item["typeDiscriminator"] == "PrimaryId":
            primary_id = item["value"]
        if item["typeDiscriminator"] == "ClassifiedId" and "type" in item:
            term = item["type"]["term"][language]
            if term == "Scopus Author ID":
                scopus_id = item["id"]
            if term == "Employee ID":
                employee_id = item["id"]
    return primary_id, scopus_id, employee_id


def process_person(data: dict, language: str) -> dict:
    primary_id, scopus_id, employee_id = (
        _find_person_ids(data["identifiers"], language) if "identifiers" in data else (None, None, None)
    )
    return {
        "pure_id": data["pureId"],
        "uuid": data["uuid"],
        "first_name": data["name"]["firstName"],
        "last_name": data["name"]["lastName"],
        "email": (
            data["staffOrganizationAssociations"][0]["emails"][0]["value"]
            if "staffOrganizationAssociations" in data and "emails" in data["staffOrganizationAssociations"][0]
            else None
        ),
        "employee_id": employee_id,
        "scopus_author_id": scopus_id,
        "primary_id": primary_id,
    }


def process_publisher(data: dict, language: str) -> dict:
    countries = [c["term"][language] for c in data.get("countries", []) if "term" in c]
    return {
        "pureId": data["pureId"],
        "uuid": data["uuid"],
        "name": data["name"],
        "country": _join_list(countries) if countries else None,
    }


def process_event(data: dict, language: str) -> dict:
    import json

    return {
        "pureId": data["pureId"],
        "uuid": data["uuid"],
        "type": data["type"]["term"][language],
        "title": data["title"][language],
        "city": data.get("city"),
        "country": data["country"]["term"][language] if "country" in data else None,
        "startDate": data.get("lifecycle", {}).get("startDate"),
        "endDate": data.get("lifecycle", {}).get("endDate"),
        "sponsorOrganizations": json.dumps(data["sponsorOrganizations"]) if "sponsorOrganizations" in data else None,
    }


def process_organization(data: dict, language: str) -> dict:
    return {
        "pureId": data["pureId"],
        "uuid": data["uuid"],
        "name": data["name"][language],
    }
