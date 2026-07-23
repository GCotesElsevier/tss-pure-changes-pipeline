# Databricks notebook source
# MAGIC %md
# MAGIC ### PureChangesClient
# MAGIC Thin wrapper around Pure's legacy Changes Stream endpoint
# MAGIC (`/changes/{tokenOrDate}`). Pages through `resumptionToken` until
# MAGIC `moreChanges` is `False` and returns every change event, optionally
# MAGIC filtered client-side by `familySystemName` — the endpoint does not
# MAGIC support server-side filtering by family or by `changeType`.

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
                if families is None or change.get("familySystemName") in families:
                    all_events.append(
                        {
                            "uuid": change.get("uuid"),
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
