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

# TODO(user): confirm Ajman's real SFTP base path and secret scope before
# running anything that imports this file — unlike HBKU (which reused
# tss-dedup's already-existing path/scope for the same server), Ajman is a
# brand-new client with no prior SFTP delivery to inherit from.
SFTP_BASE = "REPLACE_ME_AJMAN_SFTP_BASE_PATH"
SFTP_SECRET_SCOPE = "sftp_scope"
