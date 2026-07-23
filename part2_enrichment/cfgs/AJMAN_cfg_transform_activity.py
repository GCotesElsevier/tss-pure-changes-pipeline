# Databricks notebook source
# MAGIC %md
# MAGIC ### Activity transform config (Ajman)
# MAGIC Copied from `HBKU_cfg_transform_activity.py` unchanged — maps Pure's
# MAGIC standard `activities` JSON shape, not anything client-customized.
# MAGIC **Caveat carried over from HBKU:** the resulting `type` value is
# MAGIC `"{typeDiscriminator}: {term}"`, and HBKU's real data surfaced a
# MAGIC `typeDiscriminator` ("EditorialWork") outside the 4 types
# MAGIC `AJMAN_cfg_far_templates.py` will initially know about (copied from
# MAGIC HBKU's own 4). `postprocess_changes.py`'s `log_unmapped_subtypes`
# MAGIC will warn instead of silently dropping records if Ajman has the same
# MAGIC kind of surprise — don't assume HBKU's 4 known types are exhaustive.
# MAGIC
# MAGIC Covers all 4 Custom Sections subtypes (Service: Professional,
# MAGIC Service: University, Other: Professional Membership, Other:
# MAGIC Consulting) — they share one raw JSON shape in Pure (the `activities`
# MAGIC endpoint); subtype-specific column selection happens in Part 3.
# MAGIC
# MAGIC Ported from `Transformer.process_activities` in
# MAGIC `ip-pure2far-integration`. `persons` (the participant list) is left
# MAGIC untouched here, same as `contributors` in the research output config —
# MAGIC handled as its own step in Part 2's orchestration notebook, not by
# MAGIC this per-record config. `managing_organization_uuid` / `member_of_uuid`
# MAGIC stay as uuids here too; resolving them to organization NAMES needs a
# MAGIC join against the `sync_internal_organizations` /
# MAGIC `sync_external_organizations` tables, which also happens in the
# MAGIC orchestration notebook, not here.

# COMMAND ----------

ACTIVITY_TRANSFORM_CONFIG = {
    "pureId": {"actions": [{"type": "cast", "to_type": "string"}]},

    "typeDiscriminator": {"actions": [{"type": "add"}]},

    "type.term.en_US": {
        "actions": [
            {"type": "add"},
            {
                "type": "concat_fields",
                "fields": ["typeDiscriminator", "type.term.en_US"],
                "separator": ": ",
                "to": "type",
            },
        ]
    },

    "title.en_US": {
        "actions": [
            {"type": "add"},
            {"type": "rename", "to": "title"},
        ]
    },

    "prettyUrlIdentifiers": {
        "actions": [
            {"type": "add"},
            {"type": "extract_from_list", "value_path": [], "to": "pretty_url_identifiers"},
            {"type": "drop"},
        ]
    },

    "managingOrganization.uuid": {
        "actions": [
            {"type": "add"},
            {"type": "rename", "to": "managing_organization_uuid"},
        ]
    },

    # memberOf can point at EITHER an external or an internal organization —
    # never both. Priority matches the original code: external first, then
    # internal as a fallback. A distinct intermediate name is used for the
    # first one so the final rename below doesn't collide with it (two
    # columns ending up with the same name was the exact bug fixed in the
    # research output config's flatten_dataframe fix).
    "memberOf.organization.uuid": {
        "actions": [
            {"type": "add"},
            {"type": "rename", "to": "member_of_uuid_fallback"},
        ]
    },
    "memberOf.externalOrganization.uuid": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "member_of_uuid_fallback"},
            {"type": "rename", "to": "member_of_uuid"},
        ]
    },

    "period.startDate.year": {
        "actions": [{"type": "add"}, {"type": "rename", "to": "start_date_year"}]
    },
    "period.startDate.month": {
        "actions": [{"type": "add"}, {"type": "rename", "to": "start_date_month"}]
    },
    "period.startDate.day": {
        "actions": [{"type": "add"}, {"type": "rename", "to": "start_date_day"}]
    },
    "period.endDate.year": {
        "actions": [{"type": "add"}, {"type": "rename", "to": "end_date_year"}]
    },
    "period.endDate.month": {
        "actions": [{"type": "add"}, {"type": "rename", "to": "end_date_month"}]
    },
    "period.endDate.day": {
        "actions": [{"type": "add"}, {"type": "rename", "to": "end_date_day"}]
    },

    # `to` matches the field name here (overwrite in place, not a rename),
    # so no trailing "drop" — unlike identifiers/links/etc. above, there is
    # no separate raw column left behind to clean up.
    "descriptions": {
        "actions": [
            {"type": "add"},
            {"type": "join_from_list", "value_path": ["value", "en_US"], "to": "descriptions"},
        ]
    },
}
