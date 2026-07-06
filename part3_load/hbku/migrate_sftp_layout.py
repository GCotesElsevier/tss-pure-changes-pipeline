# Databricks notebook source
# MAGIC %md
# MAGIC # One-time SFTP layout migration
# MAGIC **Not part of the regular pipeline — run this ONCE, manually, then
# MAGIC don't run it again.** Same spirit as `part1_changes/hbku/reset_sync_state.py`
# MAGIC / `discover_families.py`: a utility notebook, not an orchestration step.
# MAGIC
# MAGIC For each scope's SFTP folder (`pure2far_scholarly`, `pure2far_grants`,
# MAGIC `pure2far_custom`), moves everything currently sitting directly in
# MAGIC that folder — including its existing `old_files/` archive — into a
# MAGIC new `new/` subfolder, then creates empty `updates/` and `deletes/`
# MAGIC subfolders (each with their own `old_files/`) alongside it. This
# MAGIC matches what the user asked for: treat whatever is already there
# MAGIC today as `new/` content, since `postprocess_changes.py` now uploads to
# MAGIC `{sftp_folder}/{new,updates,deletes}/` instead of a single folder.
# MAGIC
# MAGIC **Before running:** confirm nothing else is actively writing to these
# MAGIC SFTP folders at the same time (e.g. don't run this concurrently with
# MAGIC `postprocess_changes.py`, and make sure `tss-dedup`'s own Step3 isn't
# MAGIC also uploading to the same folders around the same time).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../sftp_utils

# COMMAND ----------

# MAGIC %run ../cfgs/HBKU_cfg_far_templates

# COMMAND ----------

import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

def migrate_scope_folder(sftp, base_remote_dir: str, scope_folder: str) -> None:
    scope_remote_dir = f"{base_remote_dir}/{scope_folder}"
    new_remote_dir = f"{scope_remote_dir}/new"

    _ensure_remote_dir(sftp, new_remote_dir)

    moved = 0
    for entry in sftp.listdir(scope_remote_dir):
        if entry in ("new", "updates", "deletes"):
            continue  # already migrated, or a sibling created by a previous partial run

        src = f"{scope_remote_dir}/{entry}"
        dst = f"{new_remote_dir}/{entry}"
        try:
            sftp.stat(dst)
            logger.warning("Skipping %s -> %s (destination already exists)", src, dst)
            continue
        except FileNotFoundError:
            pass

        sftp.rename(src, dst)
        moved += 1
        logger.info("Moved %s -> %s", src, dst)

    # If the scope folder never had its own old_files/ (or it just got
    # moved above as part of the loop), make sure new/ ends up with one --
    # future uploads' archiving step expects it to exist.
    _ensure_remote_dir(sftp, f"{new_remote_dir}/old_files")

    for status_folder in ("updates", "deletes"):
        _ensure_remote_dir(sftp, f"{scope_remote_dir}/{status_folder}")
        _ensure_remote_dir(sftp, f"{scope_remote_dir}/{status_folder}/old_files")

    logger.info("[%s] migration done: %d item(s) moved into new/", scope_folder, moved)

# COMMAND ----------

client = _connect_sftp(SFTP_SECRET_SCOPE)
sftp = client.open_sftp()

try:
    for scope_name, scope_cfg in FAR_TEMPLATES_CONFIG.items():
        migrate_scope_folder(sftp, SFTP_BASE, scope_cfg["sftp_folder"])
finally:
    sftp.close()
    client.close()
