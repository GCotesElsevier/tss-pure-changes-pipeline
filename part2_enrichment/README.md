# Part 2 — Enrichment

For every `uuid` from Part 1 that is not a `DELETE`, fetches the full record
from Pure's current API, joins it with supporting entities (persons,
organizations, publishers, events), and resolves the record's FAR
`primary_id` via an email lookup against FAR's user directory.

## Design decisions

- **No external dependency on `ip-pure2far-integration`.** Rather than
  reading that repo's `persons` / `organizations` / `publishers` / `events`
  tables (which would mean this pipeline depends on that repo's jobs running
  forever), Part 2 ports and keeps its own copy of that entity-sync logic
  (full load via `PureAPI.read_all`, incremental via `LegacyPureAPI` date
  filtering) so this repo is self-contained.
- **Transform engine unified across all 3 scopes.** Every scope/subtype uses
  the same declarative, config-driven engine (`transform_engine.py`),
  originally written only for Grants in `ip-pure2far-integration`. No more
  hardcoded Python classes per subtype.

## Files so far

- `pure_api_client.py` — `PureAPI` (current REST API: full sync pagination,
  single-record fetch by `uuid`) and `LegacyPureAPI` (legacy XML query API,
  used only for the supporting entities' incremental uuid lists).
- `transform_engine.py` — `flatten_dataframe` + `apply_transforms`, the
  generic config-driven engine (see its module docstring for the full list
  of supported actions).
- `far_users_client.py` — `FARUsersClient.fetch_all_users()`, trimmed from
  `tss-dedup`'s `FAR_API_Client` (no `fetch_user_activities`: there is no
  PURE-vs-FAR matching step in this pipeline anymore).
- `entity_transforms.py` — flattening functions for the 4 supporting entity
  shapes (`process_person`, `process_publisher`, `process_event`,
  `process_organization` — shared by Internal and External organizations),
  ported from `Transformer.process_persons` etc. in
  `ip-pure2far-integration`, with `language` as an explicit parameter
  instead of the original's mutable class-level state.
- `entity_sync.py` — `sync_entity(...)`, one reusable, idempotent sync
  function per entity: full load the first time (or always, for
  `InternalOrganization` — Pure's legacy API has no incremental query for
  it), incremental after that based on the table's own `ingest_ts`. No
  separate initial-load notebook to remember to run only once.
- `hbku/config.py` — secrets/constants for Part 2 (new Pure API key + base
  URL, FAR HMAC keys, database, `sync_*` table names).
- `hbku/sync_entities.py` — orchestration notebook: syncs all 5 supporting
  entities into `sync_persons` / `sync_events` / `sync_publishers` /
  `sync_internal_organizations` / `sync_external_organizations`. These are
  intentionally prefixed `sync_` so they never collide with
  `ip-pure2far-integration`'s own un-prefixed tables in the same catalog.

- `cfgs/HBKU_cfg_transform_research_output.py` — transform config
  (`RESEARCH_OUTPUT_TRANSFORM_CONFIG`) covering all 7 Scholarly Activities
  subtypes (they share one raw JSON shape in Pure; subtype-specific column
  selection happens in Part 3). A `.py` file, not `.json` — see the root
  README's "Conventions" section for why. **Validated end-to-end against
  real HBKU data** (Article, Review article, Conference contribution
  subtypes all extracted correctly). Also extracts `volume`, `edition`,
  `numberOfPages`, `journalNumber` (added while building Part 3 — these
  are simple top-level fields on the same record, needed by
  `far_templates.py`'s Book/Chapter/Journal/Proceeding templates).
  `journal_impact_factor` is NOT in this config — it needs a separate API
  call per record (see `enrich_changes.py`'s
  `fetch_journal_impact_factor`), so it's computed in the orchestration
  notebook instead of the declarative config.
- `hbku/test_research_output_transform.py` — one-off diagnostic: runs the
  config above against real records fetched via `PureAPI`, using uuids
  already sitting in Part 1's changes table.
- `cfgs/HBKU_cfg_transform_activity.py` — transform config
  (`ACTIVITY_TRANSFORM_CONFIG`) covering all 4 Custom Sections subtypes.
  Ported from `Transformer.process_activities`. `persons` (the participant
  list) and organization-name resolution are left for the orchestration
  notebook, same reasoning as `contributors` in the research output config.
  Validated locally against synthetic records and against the one real
  Custom Sections record available (Custom Sections is very low-volume).
- `hbku/test_activity_transform.py` — same diagnostic pattern, against
  Part 1's Custom Sections changes table (only ~2 real events seen so far,
  per Part 1's low-volume finding).
- `grants_merge.py` — `fetch_and_merge_grant(...)`: Pure models a grant as
  two linked content types (`Project` + `Award`, joined via an
  `award-clusters` bridge), and `HBKU_cfg_transform_grants.py` expects them
  already merged (fields present on both sides suffixed `_project` /
  `_award`, mirroring the `pandas.merge(..., suffixes=...)` shape
  `ip-pure2far-integration` used). Implements the `Project -> Award`
  direction (`projects/{uuid}/award-clusters`). **Known gap:** no
  `Award -> Project` reverse lookup exists yet — a changed `Award` uuid
  (rare; 0 seen in Part 1's 30-day check) is processed alone for now.
- `cfgs/HBKU_cfg_transform_grants.py` — transform config
  (`GRANTS_TRANSFORM_CONFIG`), close to a direct translation of
  `GRANTS_CONFIG` (already declarative in the original). **Validated
  end-to-end against real HBKU data**: the one real Grants change (a
  Project) does have a linked Award, found via `grants_merge.py`, and the
  full merge + transform produced correct results — `uuid`/`pureId`
  fallback, `awardStatus`/`internal_external`/`grantType` mappings,
  `fundings` extraction (a real ~1.8M QAR award), the `lookup_from_dataframe`
  sponsor lookup against the real `sync_external_organizations` table, and
  `typeDisc` all correct.
  **Inherited limitation:** `fundingType`'s mapping is an exact,
  case-sensitive string match against a hardcoded sponsor-name list from
  `ip-pure2far-integration` — any mismatch in casing/punctuation silently
  falls through to `"__SELF__"` (the sponsor's own name used as the
  funding type). Ported as-is, not something introduced here; happened to
  match correctly for the one real sponsor seen so far (QNRF).
- `hbku/test_grants_transform.py` — same diagnostic pattern, using Part 1's
  real Grants change (currently 1, a `Project`) — confirmed it has a real
  linked Award.
- `participant_explode.py` — shared explode logic for `contributors`
  (Research Output) / `persons` (Custom Sections) / `participants` (Grants):
  all 3 transform configs deliberately leave these raw (see each config's
  docstring), so extraction happens once here instead of 3 near-identical
  times. Also resolves each internal participant's FAR `faculty_id` by
  joining `sync_persons` on `person_uuid` to get an email, then looking it
  up against the FAR users directory. **Named `faculty_id`, not
  `primary_id`**: `sync_persons.primary_id` already means something else
  (Pure's own internal `PrimaryId` identifier type) — reusing the name
  would silently collide the two concepts. Validated locally with synthetic
  contributor/external-person records. **Grants' `participants`/
  `awardHolders` item shape is assumed** to match contributors/persons
  (`person`/`externalPerson` + `name` + `role`) by analogy — not yet
  confirmed against a real Grants record with participants (none seen in
  Part 1 so far).
- `hbku/enrich_changes.py` — the main orchestration notebook. Reads today's
  `changes_<scope>_<CURRENT_DAY>` table per scope (same `CURRENT_DAY` as
  Part 1's `fetch_changes.py`, since both run in the same pipeline
  execution — no "latest table" search needed). For non-DELETE records:
  fetches full records, applies the transform config, joins
  publisher/event/organization names from the `sync_*` tables, explodes
  participants + resolves `faculty_id`. DELETE records are **not**
  enriched (the Pure record is already gone — nothing to fetch, no
  reliable way to know whose it was) — saved as a minimal `uuid` + `scope`
  + `changeType` table per scope, for audit/log purposes only; Part 3
  decides what to do with them.

  Output tables (mirrors `tss-dedup`'s `processed_*` shape, `enriched_`
  prefix so nothing collides with `ip-pure2far-integration`'s tables):
  - `enriched_research_output_<date>` + `enriched_research_output_authors_<date>`
  - `enriched_custom_sections_<date>` — participants exploded **in place**
    (one row per activity+participant, no separate table — matches the
    original `ActivityDataProcessor`, confirmed with the user rather than
    assumed, since it's a real deviation from how Research Output/Grants
    handle their own author lists)
  - `enriched_grants_<date>` + `enriched_grants_authors_<date>`
  - `enriched_<scope>_deletes_<date>` (all 3 scopes, same minimal shape)

  **Deliberately not replicated from the old pipeline** (left for Part 3,
  or just not requested): `title`+`subTitle` merge, `status_date`
  construction, numeric casts, `role` uppercasing, Research Output's own
  `organizations` field resolved to names (stays a joined uuid string, same
  as the original), event `sponsorOrganizations` resolved to a sponsor
  name.

  Validated locally (no spark/dbutils needed) by running the real
  transform configs + `participant_explode.py` end-to-end against
  synthetic records for all 3 scopes before touching Databricks.

  **Confirmed end-to-end against real HBKU data (2026-07-06):** all 3
  scopes ran successfully — Research Output (5,383 records + 160,992
  authors), Custom Sections (1 record), Grants (1 record + 5 authors,
  the first real confirmation that Grants' `participants`/`awardHolders`
  shares contributors/persons' item shape). Two real bugs found and fixed
  along the way: a `CANNOT_MERGE_TYPE` schema-inference crash on
  `keywordGroups` (fixed by adopting `spark_utils.safe_save_table`, ported
  from `tss-dedup`), and a `DELTA_EMPTY_DATA` crash writing an empty,
  column-less deletes table (fixed by having `save_table` skip empty
  DataFrames entirely instead of trying to write them — this repo's
  date-suffixed tables don't need `safe_save_table`'s "preserve schema for
  an empty write" behavior, unlike `tss-dedup`'s fixed-name tables).

  `fetch_journal_impact_factor(pure_api, journal_id, year)`: added while
  building Part 3, since `far_templates.py`'s Journal/Editorial templates
  need it. One extra API call per record that has a `journal_id`
  (`journals/{journal_id}/metrics/webOfScienceJournal`, filtered by
  publication year) — ported from `ip-pure2far-integration`'s
  `get_journal_impact_factor`. Missing WoS metrics (common) just means no
  impact factor for that record, not a batch failure.

## Still to build

- Retrofit Part 1's `fetch_changes.py` to use `spark_utils.safe_save_table`
  too, for consistency (currently uses its own simpler `.astype(str)`,
  which works today but isn't this) — non-urgent follow-up.
- Part 3 (`part3_load/`) is now built (see its own README) but not yet
  validated against real Databricks data, and does not upload to SFTP yet
  (folder structure still to be designed together).
