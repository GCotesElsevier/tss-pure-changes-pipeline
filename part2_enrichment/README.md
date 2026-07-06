# Part 2 — Enrichment

Not started yet.

Will take the `(uuid, changeType, familySystemName)` output of Part 1 and, for
every record that is not a `DELETE`, fetch the full record from Pure's
current API, join it with supporting entities (persons, organizations,
publishers, events), and resolve the FAR `primary_id` via an email lookup
against FAR's user directory — replacing the ad hoc date-filter incremental
detection used in `ip-pure2far-integration` with the Part 1 output.
