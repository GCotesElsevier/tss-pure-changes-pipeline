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
- `hbku/fetch_changes.py` — the orchestration notebook. No `SCOPE` widget:
  every run fetches the changes stream once (filtered to the union of all
  families across scopes), tags each event with its scope via
  `cfgs/HBKU_cfg_changes.json`, de-duplicates by `uuid`, saves one output
  table per scope, and only then persists the new resumption token.
- `hbku/discover_families.py` — one-off diagnostic notebook: pulls the
  **unfiltered** changes stream and reports the distinct `familySystemName`
  values seen, to confirm open design point 1 below against real data.
- `hbku/check_recent_awards.py` — one-off diagnostic notebook: checks Pure's
  regular `awards` endpoint (server-side `modifiedAfter` filter, uuid-only)
  for recent activity, without paging the full changes stream. Used to tell
  whether the absence of `Award` events in `discover_families.py` is due to
  low volume rather than a wrong family name.

## Open design points

1. **Family names per scope.** Confirmed against a live, unfiltered run of
   `hbku/discover_families.py` over 2026-07-01 → 2026-07-06:
   `ResearchOutput` (Scholarly Activities, 5,752 events), `Activity` (Custom
   Sections, 2 events), and `Project` (Grants, 1 event) all appeared exactly
   as configured. `Award` (Grants) did **not** appear in this window — with
   only 1 `Project` event in 5 days, that is plausibly just low volume rather
   than a wrong family name (it is a real Pure content type, mirrored by the
   separate `awards` endpoint in `ip-pure2far-integration`), but it remains
   unconfirmed. Since Pure's `/changes` endpoint has no server-side family
   filter, re-running `discover_families.py` over a longer window is just as
   expensive as before rather than cheaper. Use `hbku/check_recent_awards.py`
   instead — a single, server-side-filtered call to the regular `awards`
   endpoint — to check whether any award changed recently at all before
   relying on `Award` in Part 2.
   Also observed: `InternalOrganization`/`Organisation` never appeared in
   this window either (only `ExternalOrganisation` did) — irrelevant to
   Part 1's 3 scopes, but worth remembering for Part 2, since internal org
   changes may not surface reliably through this stream.
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
