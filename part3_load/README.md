# Part 3 — Transform & Load

Adapted from `tss-dedup`'s `postprocessing/transformers.py` and
`Step3_Postprocessor.ipynb`: builds Faculty180 (FAR) upload templates from
Part 2's enriched records.

## Why this is simpler than `tss-dedup`'s Step 3

`Step3_Postprocessor.ipynb` joins its primary/author tables against
`unmatched_{type}` (a table produced by `tss-dedup`'s Step2 PURE-vs-FAR
matching stage) to figure out which record+faculty combinations still need
to be pushed, then re-explodes by every internal author via
`explode_by_internal_authors` to backfill any author the matching-filtered
join dropped. **This pipeline has no matching stage** — Part 1's Changes
Endpoint already says new/update/delete directly — so the equivalent of
`df_all_data` is just: Part 2's enriched main table INNER JOINed with its
authors table filtered to internal, resolved-`faculty_id` rows. That join
is already one row per (record, internal author); there's nothing left to
explode. This was confirmed with the user before building, not assumed.

## Files

- `far_templates.py` — ported from `postprocessing/transformers.py`
  (`Pure_Books_Transformer`, `Pure_Chapter_Transformer`, ... one class per
  Scholarly Activities/Grants/Custom Sections output type). Field-mapping
  logic is unchanged from the original. **Renamed throughout:** the
  original used two differently-named columns for the same value —
  `facultyid` (display) and `primary_id` (internal-row filtering), both
  built via a regex-cleaning step in `Step3_Postprocessor.ipynb` itself.
  Part 2 only ever produces one column for this, `faculty_id` — every
  `facultyid` / `primary_id` reference is `faculty_id` now.

  **Bug found and fixed during local testing (not in the original):** the
  original also carried a trailing passthrough column named after the
  faculty id (`primary_id`) alongside the real `Faculty ID` field — those
  don't collide in the original because `normalize_columns` lowercases
  `Faculty ID` to `faculty_id`, which is different from `primary_id`. Once
  renamed to `faculty_id` here, the trailing passthrough column collided
  with the real one under the same name after normalization. It wasn't
  referenced anywhere downstream in `Step3_Postprocessor.ipynb`, so it was
  dropped rather than renamed again — validated locally (synthetic
  `df_all_data` for Journal Article, Grants, and a Custom Sections type)
  before pushing.
- `cfgs/HBKU_cfg_far_templates.py` (`FAR_TEMPLATES_CONFIG`) — ported from
  `cfgs/HBKU_cfg_postprocessor.json`. Drops `source_primary` /
  `source_authors` (pointed at `tss-dedup`'s dated-alias tables; this
  pipeline computes `enriched_<scope>_<CURRENT_DAY>` directly, same as
  Part 1/Part 2). Keeps `subtype_to_type` (Scholarly Activities' 16 Pure
  subtypes -> 7 output types), `types`, `type_slug` (Custom Sections'
  4 subtype strings aren't safe table-name suffixes), and `sftp_folder`
  (now wired up — see `sftp_utils.py` below).
- `spark_utils.py` — same `safe_save_table` as `part2_enrichment/`, copied
  rather than shared across Parts (this repo's per-Part self-contained
  convention). If it ever needs to change, change both copies.
- `sftp_utils.py` — `upload_df_to_sftp`, ported from
  `Step3_Postprocessor.ipynb`'s function of the same name. Same
  connection/archiving mechanics; the only real change (designed together
  with the user) is an extra path segment —
  `{SFTP_BASE}/{scope_folder}/{new,updates,deletes}/{filename}` instead of
  just `{SFTP_BASE}/{scope_folder}/{filename}` — with the `old_files`
  archiving behavior preserved exactly, just scoped to each status
  subfolder individually. Also has `csv_ready` (the
  `.fillna("").astype(str).replace("nan", "")` step the original applied
  inline before every upload).
- `hbku/config.py` — `DATABASE` + `CURRENT_DAY` (same formula as Part
  1/Part 2's own `config.py` files), plus `YEAR`/`MONTH`/`DAY` (for FAR's
  `Faculty180_<type>_<year>-<month>-<day>_01.csv` filename pattern),
  `SFTP_BASE` (same path `tss-dedup` already uses), and
  `SFTP_SECRET_SCOPE` (`"sftp_scope"` — the same secret scope
  `Step3_Postprocessor.ipynb` already uses for this SFTP server, reused
  not recreated).
- `hbku/postprocess_changes.py` — the main orchestration notebook. Per
  scope: reads today's `enriched_*` tables, builds `df_all_data` (see
  above), runs each output type through its `far_templates.py`
  transformer, adds the small scope-specific constant columns
  (`Publication Status` for Scholarly Activities, drops
  `Co-Investigator(s)` for Grants — both ported as-is from Step 3),
  normalizes columns to snake_case, and saves:
  - `far_results_<type>_<date>` + `far_sample_results_<type>_<date>` (all
    3 scopes) as Delta tables
  - `far_collaborators_<type>_<date>` (Scholarly Activities + Grants only
    — lists every contributor, internal and external, of each exported
    record; ported from Step 3's `split_author`. Custom Sections has no
    author data at all, same as in Part 2, so no collaborator file for it)
  - The results and collaborator DataFrames are then each split by
    `changeType` (`CREATE` -> `new/`, `UPDATE` -> `updates/`) and uploaded
    to SFTP under the matching subfolder, same filename pattern as the
    original (`Faculty180_<type>_<date>_01.csv` /
    `Faculty180_<type>_collaborator_<date>_01.csv`).
  - Deletes (Part 2's `enriched_<scope>_deletes_<date>`, bare `uuid` +
    `changeType` + `version` — never enriched, see Part 2's own docs) get
    one CSV per scope (not per type — a deleted record's subtype was never
    fetched) uploaded to `deletes/`.

  **`changeType` had to be threaded through Part 2 first** (commit history
  after Part 2's initial build): `enrich_changes.py`'s
  `process_research_output` / `process_custom_sections` / `process_grants`
  now take the full changes DataFrame (not just a bare uuid list) and merge
  `changeType` into the enriched output, since Part 2 originally discarded
  it right after splitting deletes from non-deletes. `build_far_template`
  attaches it onto each type's `df_template` by `uuid` (not position —
  `.build()` re-filters to internal rows internally too) using
  `uuid_output`, a column Custom Sections' transformers didn't produce
  before (added to `far_templates.py`'s `_build_common_columns` + each
  Custom Sections `col_order`, purely for this uniform merge key — "Record
  ID" already equals the raw uuid there, but a shared column name across
  all scopes avoids scope-specific merge logic in the orchestration
  notebook).
  - `hbku/migrate_sftp_layout.py` — **one-time, manual migration utility,
  not part of the regular pipeline** (same spirit as Part 1's
  `reset_sync_state.py` / `discover_families.py`). Moves whatever
  currently sits directly in each scope's SFTP folder (including its
  existing `old_files/` archive) into a new `new/` subfolder, then creates
  empty `updates/` and `deletes/` subfolders (each with their own
  `old_files/`) alongside it — per the user's explicit request to treat
  today's existing content as `new/`. Run this ONCE before
  `postprocess_changes.py` ever uploads to SFTP for real; not needed again
  after that.

  Validated locally (no spark/dbutils/network needed) end-to-end for all
  3 scopes against synthetic `enriched_*`-shaped DataFrames, including the
  edge case of a record with only external authors (correctly produces
  zero export rows) and the `changeType` new/updates split (verified
  correct row counts, filenames, and folder routing with a mocked
  `upload_df_to_sftp`). **The actual SFTP connection itself is untested
  from here** — no network access to verify against the real server; that
  needs a real Databricks run.

## Still to build / not yet validated

- Run `postprocess_changes.py` in Databricks against real Part 2 output —
  validated once already for the Delta-table-saving half (Scholarly
  Activities + Grants completed successfully against real HBKU data,
  Custom Sections needed two follow-up fixes — see project memory), but
  the SFTP upload half has never run for real.
- Run `hbku/migrate_sftp_layout.py` once, before the first real SFTP
  upload from `postprocess_changes.py`.
- Retrofit Part 1's `fetch_changes.py` to use `safe_save_table` too (it
  currently uses its own simpler `.astype(str)`, which works today but
  isn't this) — a separate, non-urgent follow-up mentioned when
  `spark_utils.py` was first added to Part 2.
- `EditorialWork: Publication Peer-review` (a real Custom Sections subtype
  seen in production) has no FAR template mapped — deliberately out of
  scope for now (flagged via `log_unmapped_subtypes`'s warning log, not
  silently dropped) until confirmed with the business/FAR what it should
  map to.
