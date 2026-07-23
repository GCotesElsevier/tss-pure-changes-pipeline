# Databricks notebook source
# MAGIC %md
# MAGIC ### Pure API clients
# MAGIC `PureAPI` reads Pure's current REST API: full-text pagination
# MAGIC (`size`/`offset`) for a full sync, and single-record fetch by `uuid`
# MAGIC for the changed records Part 1 already identified.
# MAGIC
# MAGIC `LegacyPureAPI` reads Pure's legacy XML query API. It is only used
# MAGIC here for the supporting entities Part 2 syncs itself (Person, Event,
# MAGIC Publisher, Organization) — content records (research outputs,
# MAGIC activities, awards, projects) are always fetched individually via
# MAGIC `PureAPI.read_record`, driven by the uuids from Part 1.

# COMMAND ----------

import requests
import urllib3

# Both clients below call `verify=False` (matching Part 1 and every other
# repo talking to this Pure instance) since Elsevier's certificate for
# elmi.hbku.edu.qa is not validated here. Silenced once at import time so
# every notebook using these clients doesn't need to remember to do it.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

class PureAPI:
    """Client for Pure's current REST API."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    def _get(self, end_point: str, params: dict = None) -> dict:
        url = f"{self.base_url}/{end_point}"
        headers = {"accept": "application/json", "api-key": self.api_key}
        # No timeout meant a single stalled connection could hang forever
        # with no way to recover — found 2026-07-23 when Ajman's
        # journal_impact_factor backfill (~5100 individual calls) appeared
        # to freeze near the end. Callers already catch broad exceptions
        # around calls that can legitimately fail (e.g.
        # fetch_journal_impact_factor), so a timeout just turns "hangs
        # forever" into "fails like any other error".
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code} for {url}: {response.text}")
        return response.json()

    def read_record(self, end_point: str, uuid: str) -> dict:
        """Fetch a single full record, e.g. `read_record('research-outputs', uuid)`."""
        return self._get(f"{end_point}/{uuid}")

    def read_all(self, end_point: str, size: int = 500) -> list:
        """Page through every record at an endpoint (used for a full entity sync)."""
        data = []
        offset = 0
        while True:
            results = self._get(end_point, {"size": size, "offset": offset})
            items = results.get("items", [])
            data.extend(items)
            count = results.get("count", 0)
            offset += size
            if offset >= count:
                break
        return data

    def read_related(self, end_point: str) -> list:
        """
        GET a related-data endpoint that isn't a uuid lookup and isn't
        necessarily paginated the same way as `read_all` (e.g.
        `projects/{uuid}/award-clusters`). Handles the response being
        either `{"items": [...]}` or a plain list directly, since it's
        unconfirmed which shape this specific kind of endpoint returns.
        """
        result = self._get(end_point)
        if isinstance(result, dict):
            return result.get("items", [])
        if isinstance(result, list):
            return result
        return []


# COMMAND ----------

class LegacyPureAPI:
    """
    Client for Pure's legacy XML query API.

    Used only to list uuids created since a given point in time, for the
    supporting entities Part 2 keeps in sync. `since_datetime` must be a
    full ISO datetime ("YYYY-MM-DDTHH:MM:SSZ"), not a bare date — Pure
    validates it as an XML Schema `dateTime` and rejects a bare date with
    `cvc-datatype-valid.1.2.1` (confirmed against the real API in
    part1_changes/hbku/check_recent_awards.py).
    """

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    def _build_payload(self, query_field: str, size: int, offset: int, since_datetime: str) -> str:
        # `createdAfter` is hardcoded, not a configurable tag: Pure's legacy
        # XML queries rejected `modifiedAfter` in practice for at least one
        # entity (awards) — see the same check_recent_awards.py fix above.
        return f"""
            <{query_field}>
            <size>{size}</size>
            <offset>{offset}</offset>
            <fields>
                <field>uuid</field>
            </fields>
            <createdAfter>{since_datetime}</createdAfter>
            </{query_field}>
        """

    def read_uuids_since(self, end_point: str, query_field: str, since_datetime: str, size: int = 100) -> list:
        """Page through the legacy XML query, returning uuids created since `since_datetime`."""
        uuids = []
        offset = 0
        headers = {"accept": "application/json", "Content-Type": "application/xml", "api-key": self.api_key}

        while True:
            payload = self._build_payload(query_field, size, offset, since_datetime)
            response = requests.post(f"{self.base_url}/{end_point}", headers=headers, data=payload, verify=False, timeout=30)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")

            results = response.json()
            uuids.extend(item["uuid"] for item in results.get("items", []))
            count = results.get("count", 0)
            offset += size
            if offset >= count:
                break

        return uuids
