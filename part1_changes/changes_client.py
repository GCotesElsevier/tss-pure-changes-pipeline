# Databricks notebook source
# MAGIC %md
# MAGIC ### PureChangesClient
# MAGIC Thin wrapper around Pure's legacy Changes Stream endpoint
# MAGIC (`/changes/{tokenOrDate}`). Pages through `resumptionToken` until
# MAGIC `moreChanges` is `False` and returns every change event, optionally
# MAGIC filtered client-side by `familySystemName` — the endpoint does not
# MAGIC support server-side filtering by family or by `changeType`.
# MAGIC
# MAGIC An event with no `uuid` (seen once for Ajman 2026-09-11 — Pure
# MAGIC returned a "ResearchOutput" event with everything else missing) is
# MAGIC dropped here, not passed through: there is nothing to fetch without a
# MAGIC uuid, and letting it flow downstream ended up writing the literal
# MAGIC string `"NaN"` as a uuid (pandas 3.0 + Spark string conversion of a
# MAGIC missing value), which Part 2 then tried to GET from Pure.

# COMMAND ----------

import logging

import requests

logger = logging.getLogger(__name__)


class PureChangesClient:
    """Client for Pure's legacy `/changes/{tokenOrDate}` endpoint."""

    def __init__(self, base_url: str, api_key: str, verify_ssl: bool = False):
        self.base_url = base_url
        self.api_key = api_key
        self.verify_ssl = verify_ssl

    def _get_page(self, token_or_date: str) -> dict:
        url = f"{self.base_url}/changes/{token_or_date}"
        headers = {"accept": "application/json", "api-key": self.api_key}
        # No timeout meant a single stalled connection could hang forever —
        # same class of bug found and fixed in pure_api_client.py
        # 2026-07-23 (see that file for the full story).
        response = requests.get(url, headers=headers, verify=self.verify_ssl, timeout=30)
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")
        return response.json()

    def fetch_changes(self, start_token_or_date: str, families=None):
        """
        Page through the changes stream starting at `start_token_or_date`
        (an ISO date on the first run, or a previously saved `resumptionToken`
        on later runs) until `moreChanges` is `False`.

        If `families` is provided, only events whose `familySystemName` is in
        that list are kept.

        Returns a tuple `(events, next_token)` so the caller can persist
        `next_token` and resume from there on the next run.
        """
        all_events = []
        token = start_token_or_date
        batch = 0

        while True:
            batch += 1
            response = self._get_page(token)

            items = response.get("items", [])
            more_changes = response.get("moreChanges", False)
            token = response.get("resumptionToken")

            logger.info(
                "Batch %d: %d events | moreChanges: %s", batch, len(items), more_changes
            )

            for change in items:
                if families is not None and change.get("familySystemName") not in families:
                    continue
                uuid = change.get("uuid")
                if not uuid:
                    # A genuinely malformed/incomplete event from Pure (seen
                    # 2026-09-11: familySystemName populated, uuid/changeType/
                    # version all missing) -- there is no record to fetch
                    # without a uuid, so this is dropped here instead of
                    # flowing downstream as a row Part 2 will try to enrich.
                    # Left unfiltered, it used to reach changes_<scope>_<date>
                    # with uuid=None; pandas 3.0's astype(str) doesn't safely
                    # stringify that missing value (leaves a real float NaN
                    # inside a nominally string column), and Spark then wrote
                    # it out as the literal string "NaN" -- which Part 2 then
                    # tried to GET as if it were a real uuid.
                    logger.warning(
                        "Skipping a /changes event with no uuid (familySystemName=%r, changeType=%r) -- "
                        "malformed/incomplete event from Pure, nothing to enrich.",
                        change.get("familySystemName"), change.get("changeType"),
                    )
                    continue
                all_events.append(
                    {
                        "uuid": uuid,
                        "changeType": change.get("changeType"),
                        "familySystemName": change.get("familySystemName"),
                        "version": change.get("version"),
                    }
                )

            if not more_changes:
                break

        return all_events, token


# COMMAND ----------

def dedupe_last_event_per_uuid(events: list) -> list:
    """
    Collapse multiple events for the same `uuid` within a batch into the last
    one received, per Pure's own guidance: a record can appear more than once
    in the same batch (e.g. CREATE followed by UPDATE) and the last event wins.

    Relies on `events` being in the order returned by the stream.
    """
    last_event_by_uuid = {}
    for event in events:
        last_event_by_uuid[event["uuid"]] = event
    return list(last_event_by_uuid.values())
