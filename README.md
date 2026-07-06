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
2. **`part2_enrichment/`** *(not started yet)* — For every `uuid` from Part 1
   that is not a `DELETE`, fetches the full record from Pure's current API,
   joins it with supporting entities (persons, organizations, publishers,
   events), and resolves the record's FAR `primary_id` via an email lookup
   against FAR's user directory.
3. **`part3_load/`** *(not started yet)* — Transforms enriched records into
   FAR's upload templates and loads them to Databricks tables and SFTP,
   split into `new` / `updates` / `deletes` subfolders.

## Scopes

The pipeline covers the same three scopes as the existing dedup pipeline:
Scholarly Activities, Grants, and Custom Sections. Pure's `familySystemName`
values for each scope are homologated in `cfgs/HBKU_cfg_changes.json`.

## Repository layout

- `cfgs/` — JSON configuration files (per client, per step). Institutional
  logic belongs here, not hardcoded in Python.
- `part1_changes/`, `part2_enrichment/`, `part3_load/` — one folder per
  pipeline stage. Shared logic (client classes, transform engine) lives at
  the root of each folder; client-specific settings and orchestration
  notebooks live in a per-client subfolder (e.g. `hbku/`).

## Conventions

- All code, comments, file and folder names are in English.
- Shared logic and configuration live in `.py` / `.json` files; the actual
  pipeline runs are Databricks notebooks in source format (`.py` files with
  a `# Databricks notebook source` header and `# COMMAND ----------` cell
  separators), matching the existing `ip-pure2far-integration` repo.
- This repo has no local execution path: secrets, source tables, and the
  target catalog all live in Databricks, so it must be connected via
  Databricks Repos to actually run.

## Status

Only Part 1 (`part1_changes/hbku/`) has been scaffolded so far. See its
README for open design points before it is production-ready.
