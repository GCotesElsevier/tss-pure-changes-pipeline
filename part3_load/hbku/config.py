# Databricks notebook source
from datetime import datetime

DATABASE = "academicinformationsystems_technicalservices.hbku"

# Same formula as part1_changes/hbku/config.py and part2_enrichment/hbku/config.py.
# postprocess_changes.py runs right after enrich_changes.py in the same
# pipeline execution, same day, so it reads enriched_<scope>_<CURRENT_DAY>
# directly instead of searching for the "latest" table.
CURRENT_DAY = datetime.now().strftime("%Y%m%d")

date_object = datetime.strptime(CURRENT_DAY, "%Y%m%d")
YEAR = date_object.strftime("%Y")
MONTH = date_object.strftime("%m")
DAY = date_object.strftime("%d")

# Same base path tss-dedup already uses -- not a secret itself (unlike the
# Pure hostname, this path alone reveals nothing actionable without the
# SFTP host/credentials below), kept as a plain constant like DATABASE.
SFTP_BASE = "/hbku/incoming/pure2far/hbku_dev"

# Same secret scope tss-dedup's Step3_Postprocessor.ipynb already uses for
# this same SFTP server (host/username/private_key) -- reused, not
# recreated, same reasoning as reusing Pure/FAR secrets in Part 2.
SFTP_SECRET_SCOPE = "sftp_scope"
