# Databricks notebook source
# MAGIC %md
# MAGIC ### Grants transform config (Ajman)
# MAGIC Copied from `HBKU_cfg_transform_grants.py` as a starting assumption
# MAGIC (per the onboarding checklist: same Grants structure — Project+Award
# MAGIC merged via `award-clusters` — until proven otherwise against real
# MAGIC Ajman data). Two fields were deliberately NOT copied verbatim because
# MAGIC they encode HBKU-specific (Qatar) business data, not shared Pure
# MAGIC schema — see the `fundingType` and `fundings` entries below.
# MAGIC
# MAGIC Expects a Project+Award **merged** record (see `grants_merge.py`):
# MAGIC any field present on both sides comes in suffixed `_project` /
# MAGIC `_award` (mirrors `pandas.merge(..., suffixes=(...))`, which is how
# MAGIC the original code built this same shape); fields unique to one side
# MAGIC (e.g. `participants` vs `awardHolders`) keep their own name.

# COMMAND ----------

GRANTS_TRANSFORM_CONFIG = {
    "pureId_project": {
        "actions": [{"type": "add"}, {"type": "cast", "to_type": "string"}]
    },
    "pureId_award": {
        "actions": [{"type": "add"}, {"type": "cast", "to_type": "string"}]
    },
    "uuid": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "uuid_project"},
            {"type": "fill_from", "source": "uuid_award"},
        ]
    },
    "pureId": {
        "actions": [
            {"type": "add"},
            {"type": "cast", "to_type": "string"},
            {"type": "fill_from", "source": "pureId_project"},
            {"type": "fill_from", "source": "pureId_award"},
        ]
    },

    "visibility_award.description.en_US": {
        "actions": [
            {"type": "add"},
            {"type": "lowercase"},
            {"type": "strip"},
            {"type": "rename", "to": "visible"},
        ]
    },

    "period.startDate": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "actualPeriod.startDate"},
            {"type": "rename", "to": "startDate"},
        ]
    },
    "period.endDate": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "actualPeriod.endDate"},
            {"type": "rename", "to": "endDate"},
        ]
    },

    "keywordGroups_project": {
        "actions": [
            {"type": "add"},
            {
                "type": "extract_from_list",
                "match": {"path": ["name", "en_US"], "equals": "Project Status"},
                "list_path": ["classifications"],
                "value_path": ["term", "en_US"],
            },
            {
                "type": "map_values",
                "mapping": {
                    "Award Active": "Funded - In Progress",
                    "Under Closure": "Completed",
                    "Completed": "Completed",
                    "Withdrawn": "Withdrawn",
                    "Award Suspended": "Withdrawn",
                    "Award Pending": "Funded - In Progress",
                    "Funded - In Progress": "Funded - In Progress",
                    "Terminated": "Terminated",
                },
                "default": "Completed",
            },
            {"type": "rename", "to": "awardStatus"},
        ]
    },
    "keywordGroups_award": {
        "actions": [
            {"type": "add"},
            {
                "type": "extract_from_list",
                "match": {"path": ["name", "en_US"], "equals": "Funding Type"},
                "list_path": ["classifications"],
                "value_path": ["term", "en_US"],
            },
            {"type": "rename", "to": "internal_external"},
        ]
    },

    "fundings": {
        "actions": [
            {"type": "add"},
            {"type": "extract_from_list", "value_path": ["awardedAmount", "value"], "to": "totalAwardAmount"},
            # TODO(user): HBKU's fallback was "QAR" (Qatar). "AED" is a guess
            # based on Ajman being UAE-based — confirm before relying on it.
            # Only matters when a record's raw `fundings` entry has no
            # currency at all, which should be rare.
            {"type": "extract_from_list", "value_path": ["awardedAmount", "currency"], "default": "AED", "to": "currency"},
            {"type": "extract_from_list", "value_path": ["funder", "uuid"], "to": "funder_uuid"},
        ]
    },
    "funder_uuid": {
        "actions": [
            {"type": "add"},
            {
                "type": "lookup_from_dataframe",
                "reference": "external_organizations",
                "lookup_key": "uuid",
                "value_column": "name",
                "to": "sponsor",
            },
            # TSSH-1113: FAR "Type of Funding" is derived from the funder
            # organization's OWN classification type (e.g. "University"),
            # not from its name -- see the "fundingType" entry below. Needs
            # `process_organization` (entity_transforms.py) to actually
            # capture a "type" column into sync_external_organizations;
            # unvalidated against a real Ajman external-organisation
            # payload (field name/shape assumed from the same
            # "type.term.en_US" pattern already used elsewhere in this file).
            {
                "type": "lookup_from_dataframe",
                "reference": "external_organizations",
                "lookup_key": "uuid",
                "value_column": "type",
                "to": "funder_org_type",
            },
        ]
    },
    "funder_org_type": {
        "actions": [
            {"type": "add"},
            {
                "type": "map_values",
                # TSSH-1113 AC1/AC2: a recognized funder org type passes
                # through as-is; anything else (unmatched org, or an org
                # with no type resolved) falls to "Other" -- never blank.
                # Only "University" is confirmed so far; add more real
                # values here as they're seen.
                "mapping": {"University": "University"},
                "default": "Other",
            },
            {"type": "rename", "to": "fundingType"},
        ]
    },

    "type_award.term.en_US": {
        # TSSH-1111 + TSSH-1114 (Ajman): both target FAR "Type of Grant",
        # confirmed via real Ajman data (2026-09-16, enriched_grants_*) to
        # be the SAME underlying Pure field -- besides the known research-
        # methodology values below, a real record was seen with the raw
        # value "Internal" (previously fell through unmapped via
        # "__SELF__"). TSSH-1111 wants "Internal"/"External" mapped to a
        # constant "Grants" + a separate "Classification" field
        # (Internal Grants/External Grants); TSSH-1114 wants everything
        # else to default to "Research" (confirmed with the user: a truly
        # unrecognized value defaults to "Research" rather than being
        # logged/flagged, overriding TSSH-1111 AC2's edge case on this
        # point). "External" itself is not yet confirmed against real
        # data -- only "Internal" has been observed.
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "type_project.term.en_US"},
            # Copy the raw value into "classification" BEFORE it gets
            # mapped below -- map_values/fill_from always write back into
            # this same field, so the raw value has to be preserved
            # elsewhere first.
            {"type": "cast", "to_type": "string", "to": "classification"},
            {
                "type": "map_values",
                "mapping": {
                    "Experimental Development/Translation Research": "Research",
                    "Applied Research": "Research",
                    "Basic Research": "Research",
                    "Others": "Research",
                    "Internal": "Grants",
                    "External": "Grants",
                },
                "default": "Research",
            },
            {"type": "rename", "to": "grantType"},
        ]
    },
    "classification": {
        "actions": [
            {"type": "add"},
            {
                "type": "map_values",
                # Blank (not "Other"/"__SELF__") for every value that isn't
                # Internal/External -- Classification only applies to
                # TSSH-1111's Internal/External Grants distinction.
                "mapping": {
                    "Internal": "Internal Grants",
                    "External": "External Grants",
                },
            },
        ]
    },

    "version_project": {"actions": [{"type": "add"}, {"type": "drop"}]},

    "descriptions_project": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "descriptions_award"},
            {
                "type": "extract_from_list",
                "match": {"path": ["type", "uri"], "equals": "/dk/atira/pure/upmproject/descriptions/abstract"},
                "value_path": ["value", "en_US"],
            },
            {"type": "rename", "to": "abstract"},
        ]
    },

    "awardStatusDate": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "awardDate"},
            {"type": "fill_from", "source": "startDate"},
        ]
    },

    # Kept as its own column too, not just as a fallback source for
    # awardStatusDate above -- far_templates.py's Pure_Grants_Transformer
    # reads "awardDate" directly for the "Award Date" FAR field, which was
    # always empty until this was added (found 2026-07-23 while
    # reconciling Ajman's initial load against real column names).
    "awardDate": {"actions": [{"type": "add"}]},

    "title_project.en_US": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "title_award.en_US"},
            {"type": "fill_null", "value": "None"},
            {"type": "rename", "to": "title"},
        ]
    },
    "shortTitle_project.en_US": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "shortTitle_award.en_US"},
            {"type": "rename", "to": "contractid"},
        ]
    },

    "participants": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "awardHolders"},
        ]
    },

    "typeDiscriminator_project": {
        "actions": [
            {"type": "add"},
            {
                "type": "if_null_else",
                "null_value": "Award",
                "else_value": "Project",
                "treat_empty_string_as_null": True,
                "to": "typeDisc",
            },
        ]
    },
    "fundedStatus": {
        # TSSH-1112: FAR "Funded Status" from whether the record is an
        # Award (Funded) or a Project (Not Funded). Copied from "typeDisc"
        # via fill_from rather than mapped in place, so "typeDisc" itself
        # stays intact for anything else that reads it (e.g. Part 4's
        # dashboard subtype display).
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "typeDisc"},
            {
                "type": "map_values",
                "mapping": {"Award": "Funded", "Project": "Not Funded"},
            },
        ]
    },

    "url": {"actions": [{"type": "add"}]},
    "description": {"actions": [{"type": "add"}]},
    "percentEffort": {"actions": [{"type": "add"}]},
    "periodLength": {"actions": [{"type": "add"}]},
    "periodUnit": {"actions": [{"type": "add"}]},
    "indirectFunding": {"actions": [{"type": "add"}]},
    "indirectCostRate": {"actions": [{"type": "add"}]},
    "totalDirectFunding": {"actions": [{"type": "add"}]},
    "NumPeriods": {"actions": [{"type": "add"}]},
}
