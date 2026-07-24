# Databricks notebook source
from datetime import datetime

DATABASE = "academicinformationsystems_technicalservices.ajman"

# Same formula as part1_changes/ajman/config.py and part2_enrichment/ajman/config.py.
# postprocess_changes.py runs right after enrich_changes.py in the same
# pipeline execution, same day, so it reads enriched_<scope>_<CURRENT_DAY>
# directly instead of searching for the "latest" table.
CURRENT_DAY = datetime.now().strftime("%Y%m%d")

date_object = datetime.strptime(CURRENT_DAY, "%Y%m%d")
YEAR = date_object.strftime("%Y")
MONTH = date_object.strftime("%m")
DAY = date_object.strftime("%d")

# Confirmed with the user 2026-07-24: same SFTP server/credentials as
# HBKU (same SFTP_SECRET_SCOPE), just a different base path. Only
# `/ajman` existed (empty) at the time this was confirmed — the
# `new`/`updates`/`deletes` subfolders per scope get created automatically
# by sftp_utils.py's `_ensure_remote_dir` on first upload, same as HBKU's
# own folders were originally created.
SFTP_BASE = "/ajman/incoming/pure2far/ajman_prod"
SFTP_SECRET_SCOPE = "sftp_scope"
