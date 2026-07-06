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
  subtypes all extracted correctly).
- `hbku/test_research_output_transform.py` — one-off diagnostic: runs the
  config above against real records fetched via `PureAPI`, using uuids
  already sitting in Part 1's changes table.
- `cfgs/HBKU_cfg_transform_activity.py` — transform config
  (`ACTIVITY_TRANSFORM_CONFIG`) covering all 4 Custom Sections subtypes.
  Ported from `Transformer.process_activities`. `persons` (the participant
  list) and organization-name resolution are left for the orchestration
  notebook, same reasoning as `contributors` in the research output config.
  Validated locally against synthetic records (external- and
  internal-organization `memberOf` cases).
- `hbku/test_activity_transform.py` — same diagnostic pattern, against
  Part 1's Custom Sections changes table (only ~2 real events seen so far,
  per Part 1's low-volume finding).

## Still to build

- `cfgs/` config for Grants, replacing `GRANTS_CONFIG` from
  `ip-pure2far-integration` with the unified engine's format (this one
  should be the easiest: `GRANTS_CONFIG` was already declarative, just
  needs adapting to this engine's exact action names).
- `hbku/enrich_changes.py` — the main orchestration notebook: reads Part 1's
  latest changes tables, fetches full records, applies the transform config,
  joins entities (from `sync_*` tables above) + FAR `primary_id`, explodes
  authors (Scholarly Activities and Grants only — Custom Sections has no
  author table), and outputs the enriched batch for Part 3.
