# Databricks notebook source
# MAGIC %md
# MAGIC # One-time remediation: re-deliver Ajman Grants from 2026-09-10 with the TSSH-1111/1112/1113/1114 + title-bug fixes
# MAGIC **Not part of the regular pipeline — run this ONCE, manually, cell by
# MAGIC cell, then don't run it again.** Same spirit as
# MAGIC `remediate_editorial_to_other.py` / `hbku/migrate_sftp_layout.py`.
# MAGIC
# MAGIC The 2026-09-10 Grants delivery went out BEFORE commits `f1608e2`
# MAGIC (title bug), `00998c6`+`1f9c20e` (TSSH-1111/1112/1113/1114). This
# MAGIC re-delivers the SAME 236 uuids with those fixes applied.
# MAGIC
# MAGIC **Why this can't reuse `enriched_grants_20260910`, unlike the Editorial
# MAGIC remediation:** that table was accidentally deleted. There is no saved
# MAGIC enrichment left for that day — everything here is re-fetched fresh
# MAGIC from the Pure API, using the uuid list read off the dashboard report
# MAGIC (`Ajman_grants_changes_report_2026-09-10.html`)'s own `RECORDS` data,
# MAGIC which is the only surviving authoritative source (confirmed against
# MAGIC the delivered CSV: the CSV alone was missing exactly 1 uuid — a
# MAGIC `new`-event record that went to a separate `new/` SFTP folder CSV
# MAGIC never captured — the dashboard's `outcome == "delivered"` set has all
# MAGIC 236).
# MAGIC
# MAGIC ## What this does
# MAGIC 1. Re-fetches each of the 236 uuids from Pure fresh (trying `projects/`
# MAGIC    first, `awards/` as a fallback, since which family each uuid
# MAGIC    belongs to was never saved either), through the SAME
# MAGIC    `fetch_and_merge_grant` (`grants_merge.py`, already fixed) +
# MAGIC    `GRANTS_TRANSFORM_CONFIG` (already fixed) the regular pipeline uses.
# MAGIC 2. Re-resolves internal participants fresh via `explode_participants` +
# MAGIC    `attach_faculty_id` — the same functions `enrich_changes.py` uses,
# MAGIC    NOT the old collaborator CSV (which also only covered 235/236).
# MAGIC 3. Every uuid is delivered as `changeType = "UPDATE"` regardless of
# MAGIC    its original changeType — confirmed with the user 2026-09-17:
# MAGIC    these records already exist in FAR from the 09-10 delivery, so
# MAGIC    from FAR's perspective this is always a correction, never a new
# MAGIC    record. Everything goes to the `updates/` SFTP subfolder.
# MAGIC 4. Builds the corrected FAR CSVs through the exact same
# MAGIC    `Pure_Grants_Transformer` (`far_templates.py`, untouched) +
# MAGIC    `apply_ajman_grants_alignment` (copied from `postprocess_changes.py`
# MAGIC    below, since that file is a full orchestration notebook and can't
# MAGIC    be `%run` for just one function without re-running today's regular
# MAGIC    pipeline as a side effect).
# MAGIC 5. Builds ONE combined dashboard report: these 236 remediated records
# MAGIC    + the 7 real records from the 2026-09-16 run (`enriched_grants_20260916`,
# MAGIC    already correct, read as-is, not touched) — reusing
# MAGIC    `render_report_html` (`dashboard_report.py`, untouched) with a
# MAGIC    hand-built `records`/`subtypes` list (Grants only ever has one FAR
# MAGIC    type, so this doesn't need `build_dashboard.py`'s full multi-subtype
# MAGIC    machinery — see the docstring above the `records`-building cell).
# MAGIC
# MAGIC **No intermediate Delta tables are saved** (confirmed with the user
# MAGIC 2026-09-17) — everything stays in memory for this one notebook run;
# MAGIC only the final CSVs and the HTML report touch persistent storage
# MAGIC (both via SFTP, nothing written to the Databricks catalog at all).
# MAGIC
# MAGIC ## SFTP note — read before running
# MAGIC Uploads into `pure2far_grants/updates/` (CSVs, prefix `Fixed_`) and
# MAGIC `pure2far_grants/reports/` (the combined HTML, its own distinct
# MAGIC filename). `sftp_utils.py`'s `_archive_all_existing` sweeps every
# MAGIC existing file in `updates/` into `old_files/` on first upload this
# MAGIC run — do not run this concurrently with `postprocess_changes.py`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../far_templates

# COMMAND ----------

# MAGIC %run ../sftp_utils

# COMMAND ----------

# MAGIC %run ../cfgs/AJMAN_cfg_far_templates

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/pure_api_client

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/transform_engine

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/far_users_client

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/grants_merge

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/participant_explode

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/ajman/config

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/ajman/far_users_source

# COMMAND ----------

# MAGIC %run ../../part2_enrichment/cfgs/AJMAN_cfg_transform_grants

# COMMAND ----------

# MAGIC %run ../../part4_dashboard/ajman/config

# COMMAND ----------

# MAGIC %run ../../part4_dashboard/ajman/dashboard_report

# COMMAND ----------

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.propagate = False

# COMMAND ----------

# The 236 "delivered" uuids from Ajman_grants_changes_report_2026-09-10.html
# (outcome == "delivered" in that report's own RECORDS data) -- the only
# surviving authoritative source, since enriched_grants_20260910 was
# accidentally deleted and changes_grants_20260910 no longer exists either.
REMEDIATION_UUIDS = [
    "001c4586-8b57-4dc2-a6e0-b29f5b7498c5", "05cdb2e8-d0a6-46d6-85b2-119831b9f8cc", "060ad938-9e2a-4295-84d3-3bd00e0d6a4b", "06150f28-73d1-45a7-bdf0-879929fe8d93",
    "062e40d9-997d-48f3-a349-a43dabedf5de", "0634485e-fe5e-4d6e-96a9-e76b4e5f90a1", "063f64df-4e2a-4e94-8c6a-1bcf78b6de4d", "0801caec-8b28-4041-b05c-57810e882b34",
    "0924beb8-ad6e-48c3-8bed-eeb5fb5b616c", "0bd6e9e3-2b89-4fc8-9467-d17665fa6bc1", "0c79d890-257d-479c-b26c-a9a88facb43d", "0dc337d3-94b9-42cc-ab65-738086e7314f",
    "0f8227ad-5a50-4361-8020-3ba7bdd63928", "1026b863-a947-4d3c-86fa-d8d6fedc065f", "114eddee-e4e2-444b-b18d-9700b529d43b", "11a3658b-d113-47b5-921b-a7a6ffc6b375",
    "127dd4a2-41df-455a-b78a-5bdf888513f7", "12c7d1a5-af8f-4ca9-ab6a-a5d14d50bfca", "13fffbd2-282c-4c5f-94d2-ff22e536ed4d", "144f7acd-4f1b-4e7c-8d3b-cb8be39e7266",
    "1454d1f3-6310-4f12-bdaf-d603d3bc6aec", "15417f11-ad9d-48b4-8a2e-76e30f1635bf", "155e0e10-385c-418d-972e-f7403a902011", "16c2ea51-fe64-4ff2-9987-679eea9ddf7f",
    "1709f15e-ffd3-476b-ab8a-d3300819de16", "170e1913-e018-418b-a2fe-5845d144043d", "174acee8-9041-4b79-a10d-d1011cbd53dd", "18b3cbe5-78f9-48b0-8f41-c9aafb7db858",
    "1a09d732-1dee-4e0f-a855-3404e07e9204", "1a5f8d5f-09ce-49dd-814a-8863a361b685", "1aae0366-082f-4748-8c71-3f22bb93c8f0", "1b7bd953-8491-4995-b674-1352ef502b72",
    "1c667097-2707-48e0-bce6-42b1c12d83f8", "1c6e3674-d955-446f-8eb4-afdb2c52ca60", "1e7eacbb-4d89-41bb-8565-55652ed378ef", "20053c91-2a87-495a-86b8-0fa3c158a8ca",
    "206b4852-beb6-419f-94f6-ab087ffb5295", "221929c1-675c-4352-aa4c-ae8d6a3d8999", "222c47c9-28ee-4e9d-8e9f-bba1c300157f", "22cb8045-f5cf-472c-9026-64df1fda9025",
    "22d8fb7a-cb5f-4a6f-84fd-fc62e893ed45", "23a1584c-1dd1-4c7f-8c7d-a5a5d8cc60d5", "23b75073-88b0-48ad-ba61-0e278a853306", "24d37b63-567e-4377-a0b5-b1318fa126e8",
    "2561673f-0567-4fc7-832e-99912c6019eb", "27e0ac62-afab-40b4-88d6-5d2e02442f4c", "287667bb-d976-49e6-aa80-42462274836f", "2a9b4e2c-75b8-4c5f-8170-8285725d22ee",
    "2afd95df-82d9-4edb-97c7-94615301d513", "2b3dccd0-d3fd-4fa6-beec-a25b86456991", "2ca59ece-ab8f-4518-8ee9-9cc7220fa843", "2d104c4c-a005-4aec-b798-a7e447d3afea",
    "2d7317ba-83a0-42ef-8e1a-48faeb79f3a6", "2e1bbbb6-45e8-48fb-b1c7-d4611cd8dbdc", "2e3cc768-9553-4329-8c66-b73fecb939f6", "2ebcf3d3-a053-49c5-b27e-73edc777dfbc",
    "305f17ef-f8c2-4175-a5e5-e3274bfd4432", "31075b68-fdf2-4353-b7a3-c3cb13c1e345", "339896a8-ac12-4d2d-893b-0e82297ed8db", "341a33b9-9510-48b7-a5ae-3a329cd4cbad",
    "36695e15-e266-48cd-b946-7df449255bf7", "37f6179e-bcb7-4499-94b0-2dfc076e9032", "38e576e1-f256-4810-a37c-aba2ddc16c4c", "38e8c48c-4991-47ba-8d48-c5789764bf92",
    "395f1662-fa82-4fa1-a7f5-79ff671478e8", "3cec41b4-06e5-4ac8-b50a-386d98ff42a3", "3d192fe2-7c8b-4454-b874-624ed4e6d936", "3e449fe0-0a25-4224-8714-b5a1cac355b6",
    "3f489345-9b0e-4b75-8e37-1df29dcb5483", "3fd40875-9933-4f84-bbb8-3c8f9c848a6e", "404666a1-4534-4925-912e-7db5eabb5389", "4313f89a-d095-4c7a-b5c7-128aff30f724",
    "43666114-0bdc-4ffa-8541-86ea654f041c", "43ac098c-51be-4eb7-bcac-3974c7a5070b", "43f3f1db-f983-4938-af2c-4f73d08750ff", "44f45687-8656-4d3d-8d0b-9ac5129a4717",
    "457d2932-b6a1-4836-88a6-4a92dc1e7fb7", "462e3af3-3034-4f76-b62e-968c21026755", "478270b6-d5d6-4761-85c7-a2c9dd7fb7b2", "48ccb00f-e191-4fef-b83d-d97941a33130",
    "490fd750-4347-4ba5-a2ed-1cc7b955708a", "4a96369a-ea1e-40e1-a9e5-de1421ccc215", "4c493cf4-c06b-4cda-aa57-75770b907911", "4c9ebdff-a92e-4b95-be8f-f49c2f83e018",
    "4ddcce29-f69b-4e79-a19d-ca35e9a5df47", "4e8e228b-ce4b-4c6b-9ea4-e673e3fca581", "4f5074f0-b0d2-433e-a115-fd431d3b5dc0", "4f98bf31-261b-4f71-baf3-539c641faf55",
    "50349bf9-9ff4-4287-8251-063ea564ef6e", "51104771-01da-41aa-bb7b-76c6d5a082b3", "52323c95-89c3-47e7-a54d-36c4984722c5", "54659b12-c655-4fdb-8ad2-f4582f2929b7",
    "546a218a-f457-47b5-b941-a0aacd50881c", "54d433d9-f9f2-4fea-bbfe-297042bdc6ac", "553f9b1e-980f-4600-9dd2-e6c9935d2116", "567045e4-a6b9-4889-9a76-44eef282b57f",
    "57d66b34-16f2-45fe-bcf4-c4a2766e01b6", "589bad78-d946-4bac-bea1-30660b2437ba", "5a217daf-fbc5-4730-b37f-995ad48a35d2", "5b574e7e-aac8-4e15-ad69-6d35ca84a2b3",
    "5b8b3f3a-ef4d-407b-8eed-dcd05e2a49fb", "5c449f62-4dba-46db-a664-1e0a2c0ae7ad", "5cee1009-658b-49d9-bec7-23d6b18ff36f", "5e8503ad-de11-4347-b8c6-e08a7d624baf",
    "608110c1-2f33-44a0-ae8d-a5edd138f9c1", "6288abc7-77ce-492d-920a-e28b9eb6ed64", "63ba786a-dade-4cb9-beac-893342195a80", "64969d78-c5d5-4bda-9198-b28a8eacb8e1",
    "657cc136-94d8-49f0-a40a-6fd84926d4e7", "680f6164-6f92-41b5-9109-08b2f87e2d01", "6abb52d0-bc43-4706-89fe-2be00070495c", "6ce5afcd-b46b-4aac-acf3-f8c06bec5163",
    "6d99e23c-f3f7-4f4b-9347-bb165966ffcb", "6ee0523d-ee66-4dd5-a2b2-f989c29893e2", "6f7643ab-3c38-40ca-ba20-ffc7addc7365", "6fff24cd-7246-4078-baaf-bf3eb8e7b320",
    "70608a2c-1474-4aca-ab99-18c69c50b6be", "70856649-fc20-4733-bac2-f5c53903660e", "70fe43bf-9012-4b27-9aa8-5ca13057f10f", "71237b90-00b5-4a72-8b35-afaf1b9af136",
    "712890bc-0a49-483d-8fb9-fc709ba12ea1", "7332b7c0-8c3b-4c5d-a4e5-9b021973d5cc", "74c0be63-0bd0-45ed-a344-ac19e72e438e", "763ee295-9e49-4d9b-9c66-77dd6ac4005f",
    "765e298c-8b3a-42a9-bcd7-038d40a72428", "7aef7d39-600e-4bf4-9a80-c5948db27697", "7b539ee2-4665-44f8-bae2-dddc02e35927", "7c895974-30ee-4f8f-a556-3753926cfb33",
    "7d13411f-0b64-4d17-9a81-2580d4c1386c", "7dc7c82c-a0c5-4e0b-b320-30068d253058", "7e740e5c-d046-4ccd-a8d2-6788f9c6dc22", "7f029495-cb82-4c99-b1e5-4f663401aa79",
    "7f31ad31-8f07-442b-bb94-cfb798c57706", "7fe0d543-0b99-48da-93b0-9e1810c957ed", "80149eda-af5e-415c-b573-305f59623917", "823a844c-aa58-48b4-a045-0274a622966e",
    "826b7acb-41d6-4eee-ad7e-e2b64ef55f5d", "83918728-609a-410a-bf09-01b3ab3c6791", "84750712-32c8-4892-be33-3f1db5328489", "857f4a10-ae75-4c46-92b2-b8d6aed70919",
    "8a056d6a-ba8b-4cab-bd9b-5fbcd337b012", "8a0ba1ff-edc7-4cb7-9701-c554c5cca421", "8add8e87-0860-49a1-832c-ba323510b5e8", "8b407746-e0cb-44b2-b4b1-801174439223",
    "8b601fd0-1668-46ab-88ed-b246338de25c", "8becb869-1b14-4970-a936-da15628d5f7a", "8e59d87d-2968-4241-a596-aa0ab9f5b812", "912c0822-3727-4359-acf3-30e936006d53",
    "925e8097-1cfa-4b8e-8c0c-920e3bacfd23", "96423655-33b5-47a8-acf1-94f2c9314d08", "981cf488-f92a-47d7-9d3e-e37c24c63114", "98d1aefc-d9d9-41eb-8754-9c09e3b3bed7",
    "9ab2e02a-a84c-4a81-8213-36ee6b9a4f33", "9c73ce38-dc0c-45e3-8e23-44ad3e701be5", "9cdbde5e-d17d-4098-b3dc-4325e0883e27", "9d8b5fd1-55e8-4685-9848-783fc0a747d4",
    "9d912021-a17c-41a9-bfc3-5fb6b6e6b980", "9d9c2d31-c4b6-4f4e-adf7-30ca311510b9", "9ddc6e0b-66a2-4e20-b427-684d40eb3640", "9e647be3-c53c-4fb0-bec5-8468b4e7d363",
    "9f19f24f-2d5e-413d-8fa3-1363b0b9b745", "9f1e4e8a-d82e-48fe-99f4-ac69acccb4ad", "9fa3faf4-1d85-4b35-9a78-521305a19702", "a05ab99a-a6bb-4cee-8ff2-dfa2338d0dc6",
    "a2083766-65b0-4753-a191-ca5a520716bc", "a29c4d58-bebe-40b6-b89f-a4d84a3edfd4", "a2b3aa20-ddea-4bef-adfd-7bab230e8ff4", "a3bcb8b2-c032-4cfd-809f-e1f4c4df2ff7",
    "a9e13894-6722-4874-a95e-df8e936f5958", "b1f8406b-572f-45d0-9c32-18bc7a35a844", "b222ccd5-da86-4c86-a0e7-d0c62aa53c1a", "b2323200-cbce-4719-8c44-8f19abfb2db7",
    "b30b4012-494a-48af-8d25-3134291f9284", "b364549e-8428-429a-a815-f6af5d7b13e7", "b7bfb7b0-8d39-4361-aca0-722c43943931", "b93e18df-f006-4c0a-ba67-0ad0ba3075ed",
    "b9499f13-52ff-4882-b3b3-4e5d92b85dd8", "b9f13873-ffdb-4b83-87f5-ab46b506d6a2", "ba4c539e-51bc-41bb-a1fd-b5c912ff0d00", "ba5cb81a-a026-417d-be57-1b9b09db7235",
    "bb947991-3191-4ff2-b081-d3244db1fd5c", "bc204360-0185-4a45-9160-9e2700a4390b", "be104d2c-f255-4786-b06f-4285a8c50753", "be24ba5f-d3cd-4065-82f9-0eb478349bf1",
    "bfa6f857-b0f5-47e6-a9f6-ff93856d6585", "c0245e94-7855-4dc5-bf22-b2e570f4db17", "c0bcc22f-6c4d-4933-813d-fbd306d2b09f", "c1a02ccd-e2df-4dd6-b0df-a9efd3074902",
    "c4f4fd70-f14a-48dc-aa74-d08ddc5472ea", "cadb232a-c4f3-4b03-a94b-c6071e0b2bf5", "cbfc5d1e-3bdf-4339-954d-872da89f854f", "cc7a7373-5949-499b-b2ad-3074ecfdd906",
    "cd1d6669-2389-4e56-8773-12d11bc5cf53", "ceebd37c-3eed-41d2-a1f2-310ae1d7c862", "cf146436-fb16-4780-80af-675589f8cb7b", "cf471a15-6cb4-4010-908b-6ce6746129e6",
    "cfa0b954-8dc1-4fd2-8f8b-8a9b8c9a229d", "cfd14dd9-dd18-4c98-8bab-3bad0385d805", "d1123e01-d087-4e2b-bb3e-cd4aae5f02e2", "d14504bd-417d-48e6-84e3-1386b6fe5686",
    "d1b5e4e9-1999-42e8-9c1c-76eda4d9110a", "d59c301e-8c35-497d-934f-2f78f93b1adc", "d5fc2ef8-7636-4844-bb9e-d6ec8c1c5625", "d87115f5-5780-491c-a9d7-a4d25c1c75ed",
    "d9c389d1-4ed8-4e62-a6a9-3b8ecb1fb5e6", "da28696c-b419-45e8-af0f-2fc78cf6f964", "dafea263-2e18-419f-b1bc-ff4f303a4e54", "dbf717ba-a787-4620-84ac-f5ddee90eae1",
    "dc43d51b-800c-4de0-ac70-5968d01db439", "dd93c858-8d3e-4ec3-bc87-590268a5d97a", "ddd22e26-0c4a-4c9c-8e89-3d9dbb9c4b02", "de1b71bf-025d-403a-8f95-4e6630fed5cd",
    "dfd1142d-60b5-459e-b087-d8d3232cc24e", "e1c919aa-79e4-4eea-9b34-b021ae8de238", "e4e80fed-3355-42e9-89dd-5623e4a519c0", "e6b0928d-0a5b-4f39-84f4-d1c2f49ebaa5",
    "e71ec91e-8ada-4dfb-b94b-307ef1aecf9b", "e7601c89-4ac9-488a-a6af-108e41b40e0a", "e77f346a-80b1-4bfb-8d0e-08f5993eb772", "e8253d77-0738-4497-89c1-19bd614994c9",
    "e938f5b5-31db-409b-91d2-0828b373fb76", "e9681f04-f17a-4c75-a046-f9eeb8cecb73", "eb459c65-1cae-4845-a466-6eaddb28016a", "ec50dd3d-c4c2-408b-b279-be9ed6153c24",
    "ed021a89-0207-4c5d-8489-2d15365fce80", "ee08b309-dff4-4dce-ad22-bbd39458d639", "eeb2b867-664a-4120-ae5e-f94fa9d67884", "f1ecfa22-b83a-48e9-8130-7c05ebcef48e",
    "f2196f4f-71ef-454c-a249-40b265a880eb", "f261f96d-5c8a-41d3-a52b-06cc540664c0", "f2964fd8-4e54-443b-b8d7-1f95d21c9f10", "f3045f75-4ea2-49a8-9e57-27cfb04e116e",
    "f55bf342-6517-4b9b-8dd5-a36385d28b4f", "f569acde-da0b-42c1-a9ae-5e4266eb433d", "f707216c-edcb-4664-8b88-21b6d368b7e3", "fb07d9d3-ec13-4f43-9d03-947ab3791a76",
]

assert len(set(REMEDIATION_UUIDS)) == len(REMEDIATION_UUIDS), "Duplicate uuid(s) in REMEDIATION_UUIDS"
logger.info("Remediation target: %d uuid(s)", len(REMEDIATION_UUIDS))

# COMMAND ----------

pure_api = PureAPI(base_url=API_URL, api_key=API_KEY)
persons_df = spark.table(f"{DATABASE}.{PERSON_TABLE}").toPandas()
external_orgs_df = spark.table(f"{DATABASE}.{EXTERNAL_ORG_TABLE}").toPandas()
email_to_faculty_id = get_email_to_faculty_id(spark, logger)
logger.info("Loaded %d FAR users for email -> faculty_id lookup", len(email_to_faculty_id))

# COMMAND ----------

# Same retry wrapper enrich_changes.py uses (copied, not imported -- it's
# defined inline there, not in a separate %run-able module).
FETCH_MAX_WORKERS = 8
FETCH_RETRY_ATTEMPTS = 3
FETCH_RETRY_BACKOFF_SECONDS = 5


def _fetch_with_retries(fetch_fn, item):
    last_error = None
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            return fetch_fn(item)
        except Exception as e:
            last_error = e
            if attempt < FETCH_RETRY_ATTEMPTS:
                logger.warning(
                    "Fetch failed (attempt %d/%d) for %r: %s -- retrying in %ds",
                    attempt, FETCH_RETRY_ATTEMPTS, item, e, FETCH_RETRY_BACKOFF_SECONDS,
                )
                time.sleep(FETCH_RETRY_BACKOFF_SECONDS)
    logger.error(
        "Fetch permanently failed for %r after %d attempts: %s -- excluded from this remediation.",
        item, FETCH_RETRY_ATTEMPTS, last_error,
    )
    return None


def fetch_records_parallel(fetch_fn, items: list, label: str, max_workers: int = FETCH_MAX_WORKERS,
                            log_every: int = 50) -> list:
    total = len(items)
    if total == 0:
        return []
    logger.info("[%s] fetching %d records (%d concurrent workers)...", label, total, max_workers)
    results = [None] * total
    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {executor.submit(_fetch_with_retries, fetch_fn, item): i for i, item in enumerate(items)}
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            results[index] = future.result()
            completed += 1
            if completed % log_every == 0 or completed == total:
                logger.info("[%s] fetched %d/%d records", label, completed, total)
    return results


def _resolve_family_and_merge(uuid: str) -> dict:
    """
    familySystemName was never saved for this batch -- tries Project first
    (the overwhelming majority of Ajman's real Grants uuids, per this
    session's earlier dashboard-title investigation), falls back to Award.
    Raises (caught by _fetch_with_retries) only if the uuid resolves as
    neither -- e.g. genuinely deleted from Pure since 2026-09-10.
    """
    try:
        return fetch_and_merge_grant(pure_api, uuid, "Project")
    except Exception as project_error:
        try:
            return fetch_and_merge_grant(pure_api, uuid, "Award")
        except Exception as award_error:
            raise Exception(
                f"uuid {uuid} not found as Project ({project_error}) or Award ({award_error})"
            )

# COMMAND ----------

merged_records = fetch_records_parallel(_resolve_family_and_merge, REMEDIATION_UUIDS, label="grants-remediation")
fetched_uuids = {u for u, r in zip(REMEDIATION_UUIDS, merged_records) if r is not None}
missing_uuids = [u for u in REMEDIATION_UUIDS if u not in fetched_uuids]
if missing_uuids:
    logger.warning(
        "%d/%d uuid(s) could not be re-fetched from Pure -- excluded from this remediation, "
        "investigate before treating the remediation as complete: %s",
        len(missing_uuids), len(REMEDIATION_UUIDS), missing_uuids,
    )

merged_records = [r for r in merged_records if r is not None]
flat = flatten_dataframe(merged_records)
result = apply_transforms(flat, GRANTS_TRANSFORM_CONFIG, context={"external_organizations": external_orgs_df})

# Confirmed with the user: every remediated record is delivered as UPDATE,
# regardless of its original changeType -- FAR already has these records.
result["changeType"] = "UPDATE"

authors = explode_participants(result, id_column="uuid", list_column="participants", language=LANGUAGE)
authors = attach_faculty_id(authors, persons_df, email_to_faculty_id)
result = result.drop(columns=["participants"])

logger.info(
    "Re-enriched %d/%d uuid(s); %d participant row(s) exploded",
    len(fetched_uuids), len(REMEDIATION_UUIDS), len(authors),
)

# COMMAND ----------

# Copied from part3_load/ajman/postprocess_changes.py -- that file is a full
# orchestration notebook (reads today's real changes_grants_<CURRENT_DAY>
# etc. at module level), so it can't be %run here without re-triggering
# today's regular pipeline as a side effect.

def filter_to_internal_faculty(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["internal_num"] = pd.to_numeric(out["internal"], errors="coerce").fillna(0).astype(int)
    mask = (out["internal_num"] == 1) & out["faculty_id"].notna() & (out["faculty_id"].astype(str).str.strip() != "")
    return out[mask].drop(columns=["internal_num"])


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[ ,;{}()\n\t=]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


def apply_ajman_grants_alignment(df_template: pd.DataFrame, df_all_data: pd.DataFrame, type_name: str) -> pd.DataFrame:
    if type_name != "Award" or df_template.empty:
        return df_template
    df = df_template.copy()
    by_uuid = df_all_data.drop_duplicates(subset="uuid").set_index("uuid")

    def from_source(col):
        if col not in by_uuid.columns:
            return pd.Series([""] * len(df), index=df.index)
        return df["uuid_output"].map(by_uuid[col]).fillna("")

    df["Classification"] = from_source("classification")
    df["Funded Status"] = from_source("fundedStatus")
    return df


def build_collaborators(authors_df: pd.DataFrame, results_df: pd.DataFrame) -> pd.DataFrame:
    if authors_df.empty or results_df.empty:
        return pd.DataFrame()
    uuid_to_record = results_df[["uuid_output", "record_id"]].drop_duplicates().rename(columns={"uuid_output": "uuid"})
    out_df = authors_df.merge(uuid_to_record, on="uuid", how="inner")
    out_df["pure_id"] = out_df["record_id"]
    out_df["middle_initial"] = None
    out_df["percent_effort"] = None
    out_df["custom_coauthor_classifications"] = None
    out_df = out_df.drop(columns=["internal"], errors="ignore")
    cols = [
        "record_id", "faculty_id", "first_name", "middle_initial", "last_name",
        "role", "percent_effort", "sort_order", "custom_coauthor_classifications",
        "pure_id", "uuid",
    ]
    return out_df[cols].drop_duplicates()

# COMMAND ----------

internal_authors = filter_to_internal_faculty(authors)
df_all_data = result.merge(internal_authors, on="uuid", how="inner")

df_template = Pure_Grants_Transformer().build(df_all_data)
df_template = apply_ajman_grants_alignment(df_template, df_all_data, "Award")
df_template = df_template.drop(columns=["Co-Investigator(s)"], errors="ignore")
df_template["Review"] = "To be Reviewed"
df_template = normalize_columns(df_template).drop_duplicates()

collaborators_df = build_collaborators(authors, df_template)

delivered_uuids = set(df_template["uuid_output"].unique()) if not df_template.empty else set()
no_internal_author_uuids = set(fetched_uuids) - delivered_uuids
if no_internal_author_uuids:
    logger.warning(
        "%d fetched uuid(s) produced no FAR row (no internal author with a resolved "
        "faculty_id): %s",
        len(no_internal_author_uuids), sorted(no_internal_author_uuids),
    )

print(f"Target uuids: {len(REMEDIATION_UUIDS)}")
print(f"Re-fetched from Pure: {len(fetched_uuids)}")
print(f"Delivered (has internal author + faculty_id): {len(delivered_uuids)}")
print(f"Collaborator rows: {len(collaborators_df)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload corrected CSVs to `updates/` (prefix `Fixed_`)
# MAGIC Run this cell only once you're satisfied with the counts above.

# COMMAND ----------

CSV_PREFIX = "Fixed_"
grants_cfg = FAR_TEMPLATES_CONFIG["Grants"]

if not df_template.empty:
    filename = f"{CSV_PREFIX}Faculty180_award_{YEAR}-{MONTH}-{DAY}_01.csv"
    remote_path = upload_df_to_sftp(
        csv_ready(df_template), SFTP_BASE, grants_cfg["sftp_folder"], "updates", filename, logger,
        secret_scope=SFTP_SECRET_SCOPE,
    )
    logger.info("Uploaded %d rows to %s", len(df_template), remote_path)

if not collaborators_df.empty:
    filename = f"{CSV_PREFIX}Faculty180_award_collaborator_{YEAR}-{MONTH}-{DAY}_01.csv"
    remote_path = upload_df_to_sftp(
        csv_ready(collaborators_df), SFTP_BASE, grants_cfg["sftp_folder"], "updates", filename, logger,
        secret_scope=SFTP_SECRET_SCOPE,
    )
    logger.info("Uploaded %d rows to %s", len(collaborators_df), remote_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combined dashboard: these 236 remediated + the 7 real 2026-09-16 records
# MAGIC Grants only ever has ONE FAR type ("Award") -- unlike
# MAGIC `build_dashboard.py`'s general multi-subtype machinery (built for
# MAGIC Research Output's many types), so `records`/`subtypes` are built by
# MAGIC hand here rather than porting that generality for a one-off script.
# MAGIC Reuses `has_real_title`/`render_report_html` from `dashboard_report.py`
# MAGIC unchanged. No `enriched_grants_<today>` table is written or read --
# MAGIC this never touches the name the regular pipeline uses, so it can't
# MAGIC collide with a real run on any date.

# COMMAND ----------

EVENT_LABEL = {"CREATE": "New", "UPDATE": "Updated", "DELETE": "Deleted"}
EVENT_SLUG = {"CREATE": "new", "UPDATE": "updated", "DELETE": "deleted"}


def upload_html_to_sftp(html_content: str, remote_folder: str, filename: str) -> str:
    """
    Copied from part4_dashboard/ajman/build_dashboard.py -- that file is a
    full orchestration notebook, can't be %run here without re-running
    today's regular pipeline as a side effect. Reuses _connect_sftp/
    _ensure_remote_dir from sftp_utils.py (already %run above) unchanged.
    """
    remote_dir = f"{SFTP_BASE}/{remote_folder}/reports"
    remote_path = f"{remote_dir}/{filename}"
    client = _connect_sftp(SFTP_SECRET_SCOPE)
    sftp = client.open_sftp()
    try:
        _ensure_remote_dir(sftp, remote_dir)
        with sftp.open(remote_path, "w") as remote_file:
            remote_file.write(html_content)
    finally:
        sftp.close()
        client.close()
    return remote_path


def _build_records_for_remediation(result_df: pd.DataFrame, authors_df: pd.DataFrame) -> list:
    """Same record shape build_dashboard.py's build_records produces, for
    this remediation's already-known-delivered set."""
    if result_df.empty:
        return []
    a = authors_df.copy()
    a["_int"] = pd.to_numeric(a.get("internal", 0), errors="coerce").fillna(0).astype(int)
    a_int = a[a["_int"] == 1][["uuid", "first_name", "last_name", "faculty_id"]]

    merged = result_df[["uuid", "changeType", "title", "typeDisc"]].merge(a_int, on="uuid", how="inner")
    merged = merged[merged["faculty_id"].notna() & (merged["faculty_id"].astype(str).str.strip() != "")]

    records = []
    for _, r in merged.iterrows():
        title = r.get("title")
        title_ok = has_real_title(title)
        records.append({
            "name": f"{r.get('first_name') or ''} {r.get('last_name') or ''}".strip(),
            "fid": str(r["faculty_id"]),
            "title": str(title) if title_ok else "",
            "titleMissing": not title_ok,
            "uuid": str(r["uuid"]),
            "subtypePure": str(r.get("typeDisc") or "Award"),
            "subtypeFar": "Award",
            "event": EVENT_SLUG.get(r["changeType"], "updated"),
            "outcome": "delivered",
        })
    return records


remediation_records = _build_records_for_remediation(result, authors)

# The 7 real 2026-09-16 records -- read as-is, not modified.
enriched_916 = spark.table(f"{DATABASE}.enriched_grants_20260916").toPandas()
authors_916 = spark.table(f"{DATABASE}.enriched_grants_authors_20260916").toPandas()
recent_records = _build_records_for_remediation(enriched_916, authors_916)

all_records = remediation_records + recent_records
logger.info(
    "Combined dashboard records: %d remediated (2026-09-10) + %d recent (2026-09-16) = %d",
    len(remediation_records), len(recent_records), len(all_records),
)

# COMMAND ----------

from collections import Counter

subtype_counts = Counter((r["subtypePure"], r["event"]) for r in all_records)
subtypes_list = []
for pure_subtype in sorted({k[0] for k in subtype_counts}):
    row = {
        "pure": pure_subtype,
        "far_type": "Award",
        "far": FAR_TYPE_DISPLAY.get("Award", "Award"),
        "far_slug": "award",
        "color_var": FAR_TYPE_COLOR_VAR.get("Award", "--cat-award"),
        "new": subtype_counts.get((pure_subtype, "new"), 0),
        "updated": subtype_counts.get((pure_subtype, "updated"), 0),
    }
    subtypes_list.append(row)

faculty_affected = len({r["fid"] for r in all_records if r["fid"]})
received_new = sum(1 for r in all_records if r["event"] == "new")
received_updated = sum(1 for r in all_records if r["event"] == "updated")

context = {
    "client_name": "Ajman",
    "scope_label": "Grants",
    "eyebrow": "Grants (remediation) · Pure → FAR",
    "delivery_date": "2026-09-17",
    "coverage_start": "2026-09-10",
    "coverage_end": "2026-09-17",
    "faculty_affected": faculty_affected,
    "received": {"CREATE": received_new, "UPDATE": received_updated, "DELETE": 0},
    "dropped": {"CREATE": 0, "UPDATE": 0},
    "deletes_delivered": 0,
    "match_rate": 1.0,
    "subtypes": subtypes_list,
    "records": all_records,
    "malformed_count": 0,
}

report_html = render_report_html(context, "grants")
report_filename = "Ajman_grants_remediation_report_2026-09-17.html"
remote_path = upload_html_to_sftp(report_html, grants_cfg["sftp_folder"], report_filename)
logger.info("Uploaded combined remediation dashboard to %s", remote_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final summary

# COMMAND ----------

print(f"Remediated (2026-09-10, forced UPDATE): {len(delivered_uuids)} uuid(s), {len(collaborators_df)} collaborator row(s)")
print(f"Recent (2026-09-16, untouched): {enriched_916['uuid'].nunique() if not enriched_916.empty else 0} uuid(s)")
print(f"Combined dashboard record rows: {len(all_records)}")
if missing_uuids or no_internal_author_uuids:
    print(f"NOT remediated -- investigate: fetch failed={len(missing_uuids)}, no internal author={len(no_internal_author_uuids)}")
else:
    print("All 236 target uuids accounted for.")
