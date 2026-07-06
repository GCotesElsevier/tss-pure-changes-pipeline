# Part 3 — Transform & Load

Not started yet.

Will adapt `postprocessing/transformers.py` and the orchestration logic from
`tss-dedup`'s `Step3_Postprocessor.ipynb` to build FAR upload templates from
Part 2's enriched records, then load them to Databricks tables and SFTP,
split into `new` / `updates` / `deletes` subfolders instead of the single
folder + `old_files` archive pattern used by the dedup pipeline.
