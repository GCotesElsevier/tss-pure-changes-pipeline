# Databricks notebook source
# MAGIC %md
# MAGIC ### Grants merge
# MAGIC Pure models a grant as two separate content types — `Project`
# MAGIC (title, description, status) and `Award` (funding amount, currency,
# MAGIC sponsor) — linked through an `award-clusters` bridge. Faculty180
# MAGIC needs both sides combined into one record, so a changed `Project` or
# MAGIC `Award` uuid from Part 1 has to be paired with its counterpart before
# MAGIC `HBKU_cfg_transform_grants.py` can run (that config expects a merged
# MAGIC record with `_project` / `_award` suffixes on any field present on
# MAGIC both sides — mirrors `pandas.merge(..., suffixes=(...))`, which is
# MAGIC what `ip-pure2far-integration` used to build the same shape).
# MAGIC
# MAGIC **Known gap:** only the `Project -> Award` direction is implemented
# MAGIC (via `projects/{uuid}/award-clusters`, the only bridge endpoint Pure
# MAGIC exposes). There is no equivalent `awards/{uuid}/award-clusters` to go
# MAGIC the other way, so a changed `Award` uuid (rare — 0 seen in Part 1's
# MAGIC last 30-day check) currently has no linked `Project` looked up;
# MAGIC revisit if `Award`-only changes turn out to matter in practice.

# COMMAND ----------

def find_linked_award_uuid(pure_api, project_uuid: str):
    """
    Looks up the Award uuid linked to a Project via Pure's award-clusters
    bridge. Returns None if no award is linked (a Project can exist without
    a matching Award).
    """
    clusters = pure_api.read_related(f"projects/{project_uuid}/award-clusters")
    for cluster in clusters:
        contained_awards = cluster.get("containedAwards") or []
        if contained_awards:
            return contained_awards[0].get("uuid")
    return None


def merge_project_and_award(project: dict, award: dict) -> dict:
    """
    Merges a Project and its linked Award into one dict, suffixing any key
    present on BOTH sides with `_project` / `_award` — mirrors
    `pandas.merge(df_project, df_award, suffixes=("_project", "_award"))`,
    which is the shape `HBKU_cfg_transform_grants.py` expects. Keys unique
    to one side keep their original name.

    Either `project` or `award` can be `None` (e.g. a Project with no
    linked Award yet).
    """
    project = project or {}
    award = award or {}
    shared_keys = set(project.keys()) & set(award.keys())

    merged = {}
    for key, value in project.items():
        merged[f"{key}_project" if key in shared_keys else key] = value
    for key, value in award.items():
        merged[f"{key}_award" if key in shared_keys else key] = value
    return merged


def fetch_and_merge_grant(pure_api, uuid: str, family: str) -> dict:
    """
    Given a changed uuid and its Pure family ("Project" or "Award"), fetches
    the full record and its counterpart (Project -> Award direction only —
    see the "Known gap" note above), and returns the merged dict ready for
    `flatten_dataframe` + `HBKU_cfg_transform_grants.GRANTS_TRANSFORM_CONFIG`.
    """
    if family == "Project":
        project = pure_api.read_record("projects", uuid)
        award_uuid = find_linked_award_uuid(pure_api, uuid)
        award = pure_api.read_record("awards", award_uuid) if award_uuid else None
        return merge_project_and_award(project, award)

    if family == "Award":
        award = pure_api.read_record("awards", uuid)
        return merge_project_and_award(None, award)

    raise ValueError(f"Unexpected grants family: {family!r} (expected 'Project' or 'Award')")
