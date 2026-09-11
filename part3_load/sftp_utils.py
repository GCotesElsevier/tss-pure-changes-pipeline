# Databricks notebook source
# MAGIC %md
# MAGIC ### SFTP upload
# MAGIC Ported from `tss-dedup`'s `Step3_Postprocessor.ipynb` `upload_df_to_sftp`.
# MAGIC Same connection/archiving mechanics — the only real change is an
# MAGIC extra path segment: `{base_remote_dir}/{scope_folder}/{status_folder}/`
# MAGIC instead of just `{base_remote_dir}/{scope_folder}/`, where
# MAGIC `status_folder` is `new` / `updates` / `deletes`. Confirmed with the
# MAGIC user: keep the exact same file-naming and `old_files` archiving
# MAGIC behavior as the original, just one level deeper per status.
# MAGIC
# MAGIC **Archiving** (changed 2026-09-11, at the user's request): the first
# MAGIC upload into a given `{status_folder}/` this run sweeps *every* existing
# MAGIC file there (not just ones sharing the file being uploaded's prefix) into
# MAGIC `{status_folder}/old_files/`, so that folder only ever holds this run's
# MAGIC fresh output — no leftover file from a type that didn't happen to change
# MAGIC today sitting there indefinitely. Done ONCE per status folder per
# MAGIC notebook execution (tracked in `_archived_status_dirs`, reset every time
# MAGIC this file is `%run` — i.e. once per pipeline run): later uploads to the
# MAGIC SAME folder in the SAME run must not re-archive files this run already
# MAGIC wrote moments earlier.
# MAGIC
# MAGIC **Initial migration note** (not code — a one-time manual/scripted step,
# MAGIC see `hbku/migrate_sftp_layout.py`): the user asked to treat whatever is
# MAGIC already sitting directly in each scope folder today as `new/` content,
# MAGIC moved there once before this code starts writing to the nested layout.

# COMMAND ----------

import os
from io import StringIO

import paramiko

# COMMAND ----------

def _ensure_remote_dir(sftp, remote_dir: str) -> None:
    """mkdir -p equivalent -- paramiko's SFTPClient has no built-in one."""
    current = ""
    for part in remote_dir.strip("/").split("/"):
        current += f"/{part}"
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


# Which status folders have already had their "archive everything" sweep
# done THIS run -- module-level, reset each time this file is %run (once per
# pipeline execution), so it naturally means "once per run", not "once ever".
_archived_status_dirs = set()


def _archive_all_existing(sftp, status_remote_dir: str, logger) -> None:
    """
    Moves every file currently in status_remote_dir (except old_files/
    itself) into status_remote_dir/old_files/ -- the whole folder, not just
    files matching the type about to be uploaded (see module docstring). A
    no-op after the first call for a given status_remote_dir within this run.
    """
    if status_remote_dir in _archived_status_dirs:
        return
    _archived_status_dirs.add(status_remote_dir)

    for existing_file in sftp.listdir(status_remote_dir):
        if existing_file == "old_files":
            continue

        src = f"{status_remote_dir}/{existing_file}"
        dst = f"{status_remote_dir}/old_files/{existing_file}"

        try:
            sftp.stat(dst)
            sftp.remove(dst)
        except IOError:
            pass

        try:
            sftp.rename(src, dst)
            logger.info("Moved %s -> %s/old_files/%s", existing_file, status_remote_dir, existing_file)
        except Exception as exc:
            logger.warning("Could not move %s: %s", existing_file, exc)


def _connect_sftp(secret_scope: str):
    host = dbutils.secrets.get(secret_scope, "host")
    username = dbutils.secrets.get(secret_scope, "username")
    key_str = dbutils.secrets.get(secret_scope, "private_key")
    key = paramiko.RSAKey.from_private_key(StringIO(key_str))

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host, port=22, username=username, pkey=key,
        timeout=15, banner_timeout=15, auth_timeout=15,
    )
    return client


def upload_df_to_sftp(
    df,
    base_remote_dir: str,
    scope_folder: str,
    status_folder: str,
    filename: str,
    logger,
    tmp_dir: str = "/dbfs/tmp",
    secret_scope: str = "sftp_scope",
) -> str:
    """
    Uploads a pandas DataFrame as CSV to:
        {base_remote_dir}/{scope_folder}/{status_folder}/{filename}

    `status_folder` is "new" / "updates" / "deletes". The first upload into
    that status folder this run archives everything already sitting there
    to `old_files/` (see module docstring / `_archive_all_existing`).
    """
    local_path = f"{tmp_dir}/{filename}"
    status_remote_dir = f"{base_remote_dir}/{scope_folder}/{status_folder}"
    remote_path = f"{status_remote_dir}/{filename}"

    df.to_csv(local_path, index=False)

    client = _connect_sftp(secret_scope)
    sftp = client.open_sftp()
    try:
        _ensure_remote_dir(sftp, status_remote_dir)
        _ensure_remote_dir(sftp, f"{status_remote_dir}/old_files")
        _archive_all_existing(sftp, status_remote_dir, logger)
        sftp.put(local_path, remote_path)
    finally:
        sftp.close()
        client.close()
        os.remove(local_path)

    return remote_path


def csv_ready(df):
    """
    FAR's CSV upload wants every cell as a plain string, no NaN/None
    literal text. Ported from Step3_Postprocessor.ipynb's inline
    `.fillna("").astype(str).replace("nan", "")` before each upload.
    """
    return df.fillna("").astype(str).replace("nan", "")
