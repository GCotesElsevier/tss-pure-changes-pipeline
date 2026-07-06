# Databricks notebook source
from datetime import datetime

DATABASE = "academicinformationsystems_technicalservices.hbku"

# Same formula as part1_changes/hbku/config.py and part2_enrichment/hbku/config.py.
# postprocess_changes.py runs right after enrich_changes.py in the same
# pipeline execution, same day, so it reads enriched_<scope>_<CURRENT_DAY>
# directly instead of searching for the "latest" table.
CURRENT_DAY = datetime.now().strftime("%Y%m%d")
