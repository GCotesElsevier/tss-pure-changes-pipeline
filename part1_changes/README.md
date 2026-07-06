# Part 1 — Fetch Pure Changes

- `changes_client.py` — `PureChangesClient`, a thin wrapper around Pure's
  legacy `/changes/{tokenOrDate}` endpoint. Paginates via `resumptionToken`
  and optionally filters events client-side by `familySystemName` (the
  endpoint has no server-side filter for family or `changeType`). Also
  provides `dedupe_last_event_per_uuid`, which collapses multiple events for
  the same record within a batch into the last one, per Pure's own guidance.
- `sync_state.py` — persists the single **global** `resumptionToken` between
  runs in a one-row control table (`get_last_resumption_token` /
  `save_resumption_token`). The token is global, not per scope, because it
  is a position in Pure's one shared changes stream and a single pipeline
  run processes every scope at once.
- `hbku/config.py` — HBKU-specific secrets and constants (legacy API key,
  base URL, target database, sync-state table name, default start date).
- `cfgs/HBKU_cfg_changes.py` — scope -> Pure family homologation
  (`CHANGES_CONFIG`). A `.py` file, not `.json` — see the root README's
  "Conventions" section for why.
- `hbku/fetch_changes.py` — the orchestration notebook. No `SCOPE` widget:
  every run fetches the changes stream once (filtered to the union of all
  families across scopes), tags each event with its scope via
  `cfgs/HBKU_cfg_changes.py`, de-duplicates by `uuid`, saves one output
  table per scope, and only then persists the new resumption token.
- `hbku/discover_families.py` — one-off diagnostic notebook: pulls the
  **unfiltered** changes stream and reports the distinct `familySystemName`
  values seen, to confirm open design point 1 below against real data.
- `hbku/check_recent_awards.py` — one-off diagnostic notebook: checks Pure's
  regular `awards` endpoint (server-side `modifiedAfter` filter, uuid-only)
  for recent activity, without paging the full changes stream. Used to tell
  whether the absence of `Award` events in `discover_families.py` is due to
  low volume rather than a wrong family name.
- `hbku/reset_sync_state.py` — dev utility: drops the resumptionToken
  control table so the next `fetch_changes.py` run starts over from
  `DEFAULT_SINCE_DATE`. Needed after a run's output got corrupted/lost even
  though the events were already consumed from the stream (see the Arrow
  bug below) — resuming normally would skip those events forever, since
  the changes stream can't replay a range twice. Not for routine use.

## Open design points

1. ~~Family names per scope.~~ **Resolved.** Confirmed against a live,
   unfiltered run of `hbku/discover_families.py` over 2026-07-01 →
   2026-07-06: `ResearchOutput` (Scholarly Activities, 5,752 events),
   `Activity` (Custom Sections, 2 events), and `Project` (Grants, 1 event)
   all appeared exactly as configured. `Award` (Grants) did not appear in
   that window, but `hbku/check_recent_awards.py` (a direct, server-side
   `createdAfter`-filtered call to Pure's `awards` endpoint — much cheaper
   than re-running the unfiltered changes stream, which has no server-side
   family filter) confirmed **0 awards were created in the last ~30 days**,
   which fully explains the absence: low volume, not a wrong family name.
   Caveat: this only checked creations, not updates to existing awards (same
   limitation as `ip-pure2far-integration`'s own grants sync, which also
   hardcodes `createdAfter`) — acceptable for now, revisit if `Award`
   updates turn out to matter in Part 2.
   Also observed: `InternalOrganization`/`Organisation` never appeared in
   the `discover_families.py` window either (only `ExternalOrganisation`
   did) — irrelevant to Part 1's 3 scopes, but worth remembering for Part 2,
   since internal org changes may not surface reliably through this stream.
2. ~~Resumption token persistence.~~ **Resolved:** a single global token is
   persisted in `<DATABASE>.<SYNC_STATE_TABLE>` (see `sync_state.py`) and
   only advanced after every scope's output table for the current run has
   been saved successfully.
3. **Output table name/schema.** `changes_<scope_slug>_<date>` in
   `fetch_changes.py` is a placeholder. The real destination shape should be
   decided together with Part 2, since that is what actually consumes it.

## First run

`DEFAULT_SINCE_DATE` in `hbku/config.py` is set to `2026-07-01`: the day
after the last confirmed fully-current state across all 3 scopes (Scholarly
Activities ran through 2026-06-30; Grants and Custom Sections last ran
2026-06-22 but had no new changes as of 2026-06-30, so they were already
current too). This value is only used before `SYNC_STATE_TABLE` exists —
every run after the first ignores it and resumes from the persisted token.

## Known issue fixed: Arrow silently corrupting small saved tables

A run on 2026-07-06 saved `changes_grants_<date>` and
`changes_custom_sections_<date>` with exactly 1 row each, but every column
was null — even though the underlying pandas data (`changes_df`,
`raw_events`) was confirmed correct by printing it directly in the
notebook. The Arrow-optimized `createDataFrame` path threw `Cannot grow
BufferHolder by size -32` converting one of these small DataFrames and
silently fell back instead of raising, producing the null row. Fixed by
`spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")` in
`fetch_changes.py` (`tss-dedup` already does this proactively in its own
notebooks). If a run's output was affected before this fix landed, use
`hbku/reset_sync_state.py` to reprocess from `DEFAULT_SINCE_DATE` again —
the affected events were already consumed from the stream, so resuming
normally would skip them forever.
