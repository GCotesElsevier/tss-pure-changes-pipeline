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
# MAGIC **Both directions are implemented.** `Project -> Award` uses
# MAGIC `projects/{uuid}/award-clusters` (a list of cluster objects, each with
# MAGIC a `containedAwards` list). `Award -> Project` uses `awards/{uuid}/cluster`
# MAGIC (singular -- a different sub-resource, returning ONE cluster object
# MAGIC directly, with a `project` key pointing back). Two more-obvious
# MAGIC guesses for the reverse direction were tried and confirmed NOT to work
# MAGIC against the real HBKU Pure instance (2026-08-14): a top-level
# MAGIC `award-clusters/{uuid}` resource ("Service not found" -- `AwardCluster`
# MAGIC isn't independently fetchable) and a symmetric
# MAGIC `awards/{uuid}/award-clusters` relation ("Content not found" -- no such
# MAGIC relation registered under `awards`, unlike `projects`). The working
# MAGIC endpoint is proven in production by `ip-pure2far-integration`'s
# MAGIC `read_awards_cluster`/`get_award_clusters` (`utils.py`/`transform.py`)
# MAGIC against this same Pure instance -- ported here, not guessed.
# MAGIC
# MAGIC This was a "rare — 0 seen in Part 1's last 30-day check" gap when this
# MAGIC file was first written (2026-07-06); revisited because it turned out
# MAGIC to be common in practice (8 `Award` `UPDATE` events in a single day,
# MAGIC 2026-08-14 — see `project_hbku_qa_dashboard_grants_dropped_title_20260814`
# MAGIC in this repo's memory), each landing with a literal `"None"` title
# MAGIC placeholder for lack of a resolved `Project`.

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


def find_linked_project_uuid(pure_api, award_uuid: str):
    """
    Looks up the Project uuid linked to an Award via `awards/{uuid}/cluster`
    -- see the module docstring's "Both directions are implemented" note for
    why this endpoint (not the more obvious-looking guesses) and where it's
    already proven in production. Returns None if the award has no cluster
    linked (`cluster` object present but `project` key absent/empty --
    not observed in practice, but the response shape doesn't rule it out).
    """
    cluster = pure_api.read_record("awards", f"{award_uuid}/cluster")
    return (cluster.get("project") or {}).get("uuid")


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
    the full record and its counterpart (both directions — see the module
    docstring), and returns the merged dict ready for `flatten_dataframe` +
    `HBKU_cfg_transform_grants.GRANTS_TRANSFORM_CONFIG`.
    """
    if family == "Project":
        project = pure_api.read_record("projects", uuid)
        award_uuid = find_linked_award_uuid(pure_api, uuid)
        award = pure_api.read_record("awards", award_uuid) if award_uuid else None
        return merge_project_and_award(project, award)

    if family == "Award":
        award = pure_api.read_record("awards", uuid)
        # Only attempt the cluster lookup if the raw payload actually has
        # one -- every real Award seen so far does (124/124, 2026-08-14),
        # but skipping the extra API call for a genuinely cluster-less
        # Award avoids find_linked_project_uuid raising on a 404 that would
        # otherwise fail this record's whole fetch (via
        # enrich_changes.py's fetch_records_parallel retry wrapper).
        project_uuid = find_linked_project_uuid(pure_api, uuid) if award.get("cluster") else None
        project = pure_api.read_record("projects", project_uuid) if project_uuid else None
        return merge_project_and_award(project, award)

    raise ValueError(f"Unexpected grants family: {family!r} (expected 'Project' or 'Award')")
