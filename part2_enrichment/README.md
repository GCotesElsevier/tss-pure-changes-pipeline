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

## Still to build

- `hbku/config.py` — secrets/constants for Part 2 (new Pure API key, FAR
  HMAC keys, database).
- `hbku/sync_entities.py` — initial + incremental sync notebook for Person,
  Event, Publisher, InternalOrganization, ExternalOrganization.
- JSON transform configs per scope/subtype in `cfgs/`, replacing
  `process_subtype_data` / `process_activities` / `GRANTS_CONFIG` from
  `ip-pure2far-integration` with the unified engine's format.
- `hbku/enrich_changes.py` — the main orchestration notebook: reads Part 1's
  latest changes tables, fetches full records, applies the transform config,
  joins entities + FAR `primary_id`, explodes authors (Scholarly Activities
  and Grants only — Custom Sections has no author table), and outputs the
  enriched batch for Part 3.
