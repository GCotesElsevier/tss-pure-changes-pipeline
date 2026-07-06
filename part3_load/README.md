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
  subtypes -> 7 output types), `types`, and `type_slug` (Custom Sections'
  4 subtype strings aren't safe table-name suffixes). `sftp_folder` is
  kept as a placeholder per scope but not wired up yet — see below.
- `spark_utils.py` — same `safe_save_table` as `part2_enrichment/`, copied
  rather than shared across Parts (this repo's per-Part self-contained
  convention). If it ever needs to change, change both copies.
- `hbku/config.py` — `DATABASE` + `CURRENT_DAY` (same formula as Part
  1/Part 2's own `config.py` files).
- `hbku/postprocess_changes.py` — the main orchestration notebook. Per
  scope: reads today's `enriched_*` tables, builds `df_all_data` (see
  above), runs each output type through its `far_templates.py`
  transformer, adds the small scope-specific constant columns
  (`Publication Status` for Scholarly Activities, drops
  `Co-Investigator(s)` for Grants — both ported as-is from Step 3),
  normalizes columns to snake_case, and saves:
  - `far_results_<type>_<date>` + `far_sample_results_<type>_<date>` (all
    3 scopes)
  - `far_collaborators_<type>_<date>` (Scholarly Activities + Grants only
    — lists every contributor, internal and external, of each exported
    record; ported from Step 3's `split_author`. Custom Sections has no
    author data at all, same as in Part 2, so no collaborator file for it)

  Validated locally (no spark/dbutils needed) end-to-end for all 3 scopes
  against synthetic `enriched_*`-shaped DataFrames, including the edge
  case of a record with only external authors (correctly produces zero
  export rows, since Faculty180 only wants internal-faculty rows).

## Still to build

- **SFTP upload.** Deliberately left out of `postprocess_changes.py` for
  now — the folder structure needs to change from `tss-dedup`'s single
  folder + `old_files` archive to separate `new` / `updates` / `deletes`
  subfolders per scope, and that structure is still to be designed
  together with the user before wiring up the upload step.
- Retrofit Part 1's `fetch_changes.py` to use `safe_save_table` too (it
  currently uses its own simpler `.astype(str)`, which works today but
  isn't this) — a separate, non-urgent follow-up mentioned when
  `spark_utils.py` was first added to Part 2.
- Run `postprocess_changes.py` in Databricks against real Part 2 output —
  not yet validated against real data, only locally against synthetic
  records.
