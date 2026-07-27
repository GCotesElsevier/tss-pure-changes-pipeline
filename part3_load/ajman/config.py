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

# Corrected 2026-07-27: Ajman does NOT share a server with HBKU, even
# though the credentials (username/private_key) are the same. HBKU is on
# transfer.ops.interfolio.com (SFTP_SECRET_SCOPE "sftp_scope"); Ajman is
# on transfer.eu1.interfolio.com, tracked in its own scope
# "sftp_scope_ajman" (same username/private_key values copied over, only
# `host` differs). The base path was also corrected: it's `ajman_dev`,
# not `ajman_prod`. The `new`/`updates`/`deletes` subfolders per scope
# get created automatically by sftp_utils.py's `_ensure_remote_dir` on
# first upload, same as HBKU's own folders were originally created.
SFTP_BASE = "/ajman/incoming/pure2far/ajman_dev"
SFTP_SECRET_SCOPE = "sftp_scope_ajman"
