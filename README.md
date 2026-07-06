# Pure Changes Pipeline

Sync pipeline that keeps an external system (FAR / Interfolio) up to date with
scholarly records, grants, and custom sections from Elsevier Pure, using
Pure's Changes Stream endpoint instead of full-record polling.

## Architecture

The pipeline has three parts, each a module in this repo:

1. **`part1_changes/`** — Polls Pure's legacy Changes Stream endpoint
   (`/changes/{tokenOrDate}`) and returns `(uuid, changeType, familySystemName,
   version)` for every record that was created, updated, or deleted since the
   last run, filtered to the Pure families relevant to each scope.
2. **`part2_enrichment/`** — For every `uuid` from Part 1 that is not a
   `DELETE`, fetches the full record from Pure's current API, joins it with
   supporting entities (persons, organizations, publishers, events), and
   resolves each internal author's FAR `faculty_id` via an email lookup
   against FAR's user directory. Validated end-to-end against real HBKU
   data for all 3 scopes.
3. **`part3_load/`** — Builds Faculty180 (FAR) upload templates from Part
   2's enriched records (`far_templates.py`, ported from `tss-dedup`'s
   `postprocessing/transformers.py`). Validated locally against synthetic
   data; not yet run against real Databricks data. SFTP upload (split into
   `new` / `updates` / `deletes` subfolders, replacing `tss-dedup`'s single
   folder + `old_files` archive) is still to be designed and built.

## Scopes

The pipeline covers the same three scopes as the existing dedup pipeline:
Scholarly Activities, Grants, and Custom Sections. Pure's `familySystemName`
values for each scope are homologated in
`part1_changes/cfgs/HBKU_cfg_changes.py`.

## Repository layout

- `part1_changes/`, `part2_enrichment/`, `part3_load/` — one folder per
  pipeline stage. Shared logic (client classes, transform engine) lives at
  the root of each folder; client-specific settings and orchestration
  notebooks live in a per-client subfolder (e.g. `hbku/`).
- **`cfgs/` lives inside each part's own folder** (e.g.
  `part1_changes/cfgs/`, `part2_enrichment/cfgs/`), not shared at the repo
  root — see "Conventions" below for why.

## Conventions

- All code, comments, file and folder names are in English.
- **Institutional config is a plain data file, not a `.json` file.** Every
  config (`cfgs/HBKU_cfg_*.py`) is a `.py` file with a `# Databricks
  notebook source` header holding nothing but a dict/constant assignment,
  loaded via `%run` — no logic, same intent as a JSON config, just a
  different extension. This was forced by a real limitation confirmed
  directly against this workspace's Databricks Repos: plain files (`.json`,
  `.csv`, etc.) are visible in the Repos UI but are **not** reliably
  readable via `open()` / `os.listdir()` / `dbutils.fs.ls()` from a running
  notebook, regardless of which folder they're in — only files Databricks
  recognizes as notebooks (`.py` with this header, `.ipynb`, `.sql`, `.r`)
  resolve reliably, via `%run`. `tss-dedup` never hit this because its
  notebooks read `.json` configs living in the exact same folder as
  themselves at the repo root; nothing in this repo does that.
- The actual pipeline runs are Databricks notebooks in source format (`.py`
  files with a `# Databricks notebook source` header and
  `# COMMAND ----------` cell separators), matching the existing
  `ip-pure2far-integration` repo.
- This repo has no local execution path: secrets, source tables, and the
  target catalog all live in Databricks, so it must be connected via
  Databricks Repos to actually run.

## Status

Parts 1 and 2 are validated end-to-end against real HBKU data. Part 3 is
built and validated locally against synthetic data, but not yet run in
Databricks, and does not upload to SFTP yet. See each part's own README for
open design points.
