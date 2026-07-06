# Databricks notebook source
# MAGIC %md
# MAGIC ### Grants transform config
# MAGIC Ported from `GRANTS_CONFIG` in `ip-pure2far-integration`
# MAGIC (`grants_integration_upd` branch) — that config was already
# MAGIC declarative, using the same action vocabulary this engine was
# MAGIC generalized from, so this is close to a direct translation.
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

    "visibility_award.description.en_GB": {
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
                "match": {"path": ["name", "en_GB"], "equals": "Project Status"},
                "list_path": ["classifications"],
                "value_path": ["term", "en_GB"],
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
                "match": {"path": ["name", "en_GB"], "equals": "Funding Type"},
                "list_path": ["classifications"],
                "value_path": ["term", "en_GB"],
            },
            {"type": "rename", "to": "internal_external"},
        ]
    },

    "fundings": {
        "actions": [
            {"type": "add"},
            {"type": "extract_from_list", "value_path": ["awardedAmount", "value"], "to": "totalAwardAmount"},
            {"type": "extract_from_list", "value_path": ["awardedAmount", "currency"], "default": "QAR", "to": "currency"},
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
        ]
    },
    "fundingType": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "sponsor"},
            {
                "type": "map_values",
                "mapping": {
                    "American University of Beirut": "Institutional",
                    "Argus Cognitive Inc.": "Corporate",
                    "Education Above All": "Not for Profit",
                    "ExxonMobil": "Corporate",
                    "Facebook": "Corporate",
                    "Gates Foundation": "Foundation",
                    "Hamad Medical Corporation (HMC)": "Government",
                    "Iberdrola QSTP LLC": "Corporate",
                    "LEAN": "Corporate",
                    "MADA CENTER": "Not for Profit",
                    "Ministry of Public Health": "Government",
                    "Ministry of Transport (Qatar)": "Government",
                    "NORTH ATLANTIC TREATY ORGANIZATION (NATO)": "Government",
                    "Qatar Financial Centre Authority (QFCA)": "Government",
                    "QATAR NATIONAL RESEARCH FUND (QNRF)": "Institutional",
                    "Qatar Primary Materials Company (Al-Awalia) QPMC": "Corporate",
                    "QF-RDI": "Foundation",
                    "Silverstein Foundation for Parkinson's with GBA": "Foundation",
                    "The Norwegian Ministry of Foreign Affairs": "Government",
                    "WORLD CANCER RESEARCH FUND INTERNATIONAL (WCRF)": "Not for Profit",
                },
                "default": "__SELF__",
            },
        ]
    },

    "type_award.term.en_GB": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "type_project.term.en_GB"},
            {
                "type": "map_values",
                "mapping": {
                    "Experimental Development/Translation Research": "Research",
                    "Applied Research": "Research",
                    "Basic Research": "Research",
                    "Others": "Research",
                },
                "default": "__SELF__",
            },
            {"type": "rename", "to": "grantType"},
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
                "value_path": ["value", "en_GB"],
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

    "title_project.en_GB": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "title_award.en_GB"},
            {"type": "fill_null", "value": "None"},
            {"type": "rename", "to": "title"},
        ]
    },
    "shortTitle_project.en_GB": {
        "actions": [
            {"type": "add"},
            {"type": "fill_from", "source": "shortTitle_award.en_GB"},
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
