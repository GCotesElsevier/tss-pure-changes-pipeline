# Databricks notebook source
# MAGIC %md
# MAGIC ### FAR users client
# MAGIC Every Pure record only carries an author's email, not a FAR
# MAGIC (Interfolio Faculty180) faculty ID. This client fetches FAR's user
# MAGIC directory (HMAC-signed auth) so records can be enriched with the
# MAGIC matching FAR `userid` — renamed `primary_id` downstream — by joining
# MAGIC on email.
# MAGIC
# MAGIC Trimmed from `tss-dedup`'s `FAR_API_Client`: only `fetch_all_users` is
# MAGIC needed here. The original's `fetch_user_activities` existed to pull
# MAGIC FAR's existing activities for the PURE-vs-FAR dedup matching step —
# MAGIC this pipeline has no matching step (Part 1 already knows new / update
# MAGIC / delete from Pure's own change log), so that method was dropped.

# COMMAND ----------

import base64
import datetime
import hashlib
import hmac

import requests

# COMMAND ----------

class FARUsersClient:
    """Client for FAR's (Interfolio Faculty180) `/users` endpoint."""

    def __init__(self, public_key: str, private_key: str, database: str):
        self.public_key = public_key
        self.private_key = private_key
        self.database = database
        self.url = "https://faculty180.interfolio.com/api.php"

    def _get_headers(self, path: str) -> dict:
        method = "GET"
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        canonical_string = f"{method}\n\n\n{timestamp}\n{path}"
        signature = base64.b64encode(
            hmac.new(self.private_key.encode(), canonical_string.encode(), hashlib.sha1).digest()
        ).decode()
        return {
            "Authorization": f"INTF {self.public_key}:{signature}",
            "TimeStamp": timestamp,
            "INTF-DatabaseID": self.database,
            "Accept": "application/json",
        }

    def fetch_all_users(self, limit: int = 100) -> list:
        """Returns every FAR user record (includes email and userid)."""
        endpoint = "/users"
        all_users = []
        offset = 1

        while True:
            headers = self._get_headers(endpoint)
            # No timeout meant a single stalled connection could hang
            # forever — same class of bug found and fixed in
            # pure_api_client.py 2026-07-23 (see that file for the full story).
            response = requests.get(
                f"{self.url}{endpoint}?data=detailed&limit={limit}&offset={offset}", headers=headers, timeout=30
            )
            response.raise_for_status()
            data = response.json()

            records = data.get("users", []) if isinstance(data, dict) else data
            if not records:
                break

            all_users.extend(records)
            if len(records) < limit:
                break
            offset += limit

        return all_users
