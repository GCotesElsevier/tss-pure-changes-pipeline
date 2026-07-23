# Databricks notebook source
# MAGIC %md
# MAGIC ### FAR upload templates
# MAGIC Ported from `tss-dedup`'s `postprocessing/transformers.py` (one
# MAGIC `Pure_*_Transformer` class per Scholarly Activities/Grants/Custom
# MAGIC Sections type, each mapping a wide, per-record-per-author DataFrame
# MAGIC to Faculty180's upload column names). Field mapping logic is
# MAGIC unchanged from the original — only the column names that carry a
# MAGIC person's FAR id were unified.
# MAGIC
# MAGIC **Renamed throughout:** the original used TWO differently-named
# MAGIC columns for the same value — `facultyid` (display, built in
# MAGIC `Step3_Postprocessor.ipynb` via a regex-cleaned copy of `primary_id`)
# MAGIC and `primary_id` (used for internal-row filtering). Part 2 of this
# MAGIC pipeline only ever produces one column for this, `faculty_id`
# MAGIC (already a clean value from FAR's `/users` endpoint, not a
# MAGIC regex-cleaned copy of anything) — every `facultyid` / `primary_id`
# MAGIC reference below is `faculty_id` instead.
# MAGIC
# MAGIC **This module expects "df_all_data" already shaped as one row per
# MAGIC (record, internal author)** — the join that produces that shape
# MAGIC lives in `hbku/postprocess_changes.py`, not here (see that notebook's
# MAGIC docstring for why this pipeline doesn't need `tss-dedup`'s
# MAGIC `unmatched_full_{type}` / `explode_by_internal_authors` step at all).

# COMMAND ----------

import pandas as pd
from datetime import datetime

# COMMAND ----------

# =========================================================
# BASE CLASS
# =========================================================

class PureBaseTransformer:
    """
    Common utilities for transforming a Pure record + its included authors
    (df_all_data, one row per record-author pair) into FAR upload columns.
    """

    def __init__(self, role_map=None):
        self.ROLE_MAP = role_map or {
            "AUTHOR": "Author",
            "EDITOR": "Editor",
            "GUEST_EDITOR": "Editor",
            "ILLUSTRATOR": "Author",
            "TRANSLATOR": "Author",
            "PUBLISHER": "Author",
        }

    # --------------------------
    # FORMATTERS
    # --------------------------

    @staticmethod
    def _fmt_status_date(y, m, d):
        def clean_int(v, default):
            if pd.isna(v):
                return default
            s = str(v).strip().lower()
            if s in ("", "none", "nan"):
                return default
            try:
                return int(float(s))
            except Exception:
                return default

        if pd.isna(y) or str(y).strip().lower() in ("", "none", "nan"):
            return pd.NA

        try:
            return datetime(
                clean_int(y, 1),
                clean_int(m, 1),
                clean_int(d, 1)
            ).strftime("%m/%d/%Y")
        except Exception:
            return pd.NA

    @staticmethod
    def _fmt_isbn(electronic, print_):
        parts = []
        if pd.notna(electronic) and str(electronic).strip() and str(electronic).strip().lower() != "nan":
            parts.append(str(electronic).strip())
        if pd.notna(print_) and str(print_).strip() and str(print_).strip().lower() != "nan":
            parts.append(str(print_).strip())
        return "; ".join(parts) if parts else pd.NA

    @staticmethod
    def _fmt_doi(doi):
        """
        Normalizes a DOI to its canonical form: `10.xxx` -> `https://doi.org/10.xxx`.
        """
        if pd.isna(doi):
            return pd.NA

        s = str(doi).strip()
        if not s:
            return pd.NA

        lower_s = s.lower()
        if lower_s.startswith("https://doi.org/") or lower_s.startswith("http://doi.org/"):
            return s

        if s.startswith("10."):
            return f"https://doi.org/{s}"

        return pd.NA

    @staticmethod
    def _fmt_pub_display(v):
        if pd.isna(v):
            return "No"
        s = str(v).upper()
        if "FREE" in s:
            return "Yes"
        if "RESTRICTION" in s:
            return "No"
        return "No"

    @staticmethod
    def _fmt_name(first, middle, last):
        parts = [str(last).strip()] if pd.notna(last) and str(last).strip() else []
        first_middle = " ".join(
            p for p in [
                (str(first).strip() if pd.notna(first) and str(first).strip() else None),
                (str(middle).strip() if pd.notna(middle) and str(middle).strip() else None)
            ] if p
        )
        if first_middle:
            parts.append(first_middle)
        return ", ".join(parts) if parts else pd.NA

    @staticmethod
    def _fmt_peer_review(value):
        if pd.isna(value):
            return pd.NA
        v = str(value).strip().lower()
        if v in ("true", "1", "yes", "y"):
            return "Blind Peer Reviewed"
        if v in ("false", "0", "no", "n"):
            return "Not Peer-reviewed"
        return pd.NA

    # --------------------------
    # FACULTY ID
    # --------------------------

    @staticmethod
    def _normalize_faculty_id(value):
        if pd.isna(value):
            return pd.NA
        s = str(value).strip()
        if not s:
            return pd.NA
        try:
            f = float(s)
            i = int(f)
            if f == i:
                return str(i)
        except Exception:
            pass
        return s

    # --------------------------
    # AUTHORS
    # --------------------------

    def _build_author_string(self, df_authors_for_uuid: pd.DataFrame) -> str:
        if df_authors_for_uuid.empty:
            return pd.NA

        tmp = df_authors_for_uuid.copy()
        tmp["sort_order"] = pd.to_numeric(tmp.get("sort_order"), errors="coerce")
        tmp["sort_order_null"] = tmp["sort_order"].isna()

        tmp = tmp.sort_values(
            by=["sort_order_null", "sort_order", "last_name", "first_name"],
            ascending=[True, True, True, True]
        )

        items, seen = [], set()
        for _, r in tmp.iterrows():
            role_raw = str(r.get("role") or "").upper()
            role = self.ROLE_MAP.get(role_raw, "Author")
            name = self._fmt_name(r.get("first_name"), r.get("middle"), r.get("last_name"))
            if pd.isna(name):
                continue
            key = (name, role)
            if key in seen:
                continue
            seen.add(key)
            items.append(f"{name} |{role}|")

        return "; ".join(items) if items else pd.NA

    def _ensure_author_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        need = ["uuid", "internal", "sort_order", "role", "first_name", "last_name", "faculty_id", "middle"]
        for c in need:
            if c not in df.columns:
                df[c] = pd.NA
        return df

    def _build_authors_map(self, df: pd.DataFrame):
        df = self._ensure_author_cols(df)
        return {u: self._build_author_string(g) for u, g in df.groupby("uuid", dropna=False)}

    def _get_row_faculty_id(self, row):
        internal_val = pd.to_numeric(row.get("internal", 0), errors="coerce")
        if pd.isna(internal_val) or int(internal_val) != 1:
            return pd.NA
        return self._normalize_faculty_id(row.get("faculty_id"))

    def _filter_internal_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._ensure_author_cols(df).copy()
        df["internal_num"] = pd.to_numeric(df["internal"], errors="coerce").fillna(0).astype(int)
        mask = (
            (df["internal_num"] == 1) &
            pd.notna(df["faculty_id"]) &
            (df["faculty_id"].astype(str).str.strip() != "")
        )
        return df[mask].copy()

    # --------------------------
    # RECORD ID
    # --------------------------

    def _add_record_id_pure_faculty(self, out: pd.DataFrame) -> pd.DataFrame:
        def make(row):
            fid = self._normalize_faculty_id(row.get("Faculty ID"))
            if pd.isna(fid) or not str(fid).strip():
                return row["Record ID"]
            return f"{row['Record ID']}_{fid}"

        out["Record ID (Pure_Faculty)"] = out.apply(make, axis=1)
        return out

    # --------------------------
    # HELPERS
    # --------------------------

    @staticmethod
    def _compose_larger_work(row):
        ht = row.get("host_title")
        hs = row.get("host_subtitle")
        ht = ht if pd.notna(ht) and str(ht).strip() else None
        hs = hs if pd.notna(hs) and str(hs).strip() else None
        if ht and hs:
            return f"{ht}: {hs}"
        return hs or ht

    @staticmethod
    def _col(df: pd.DataFrame, name: str) -> pd.Series:
        """Return the named column as a Series, or an all-None Series if absent."""
        if name in df.columns:
            return df[name]
        return pd.Series([None] * len(df), index=df.index)

    @staticmethod
    def _get_col(df, col_name):
        return df.get(
            col_name,
            pd.Series([""] * len(df), index=df.index)
        ).fillna("")

    @staticmethod
    def _compose_event_location(city, country):
        parts = []
        if pd.notna(city) and str(city).strip() and str(city).strip().lower() != "nan":
            parts.append(str(city).strip())
        if pd.notna(country) and str(country).strip() and str(country).strip().lower() != "nan":
            parts.append(str(country).strip())
        return ", ".join(parts) if parts else pd.NA


# COMMAND ----------

# =========================================================
# 1) BOOKS
# =========================================================

class Pure_Books_Transformer(PureBaseTransformer):

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "pureId"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "status"),
            "Status Date": [
                self._fmt_status_date(y, m, d)
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Title": self._get_col(df, "title"),
            "Series Title": self._get_col(df, "publicationSeries"),
            "Year": self._get_col(df, "statusYear"),
            "Date Publication": empty,
            "Publisher": self._get_col(df, "publisher_name"),
            "Publisher City and State": self._get_col(df, "publisher_country"),
            "Volume": self._get_col(df, "volume"),
            "Edition": self._get_col(df, "edition"),
            "Number of Pages": self._get_col(df, "numberOfPages"),
            "ISBN": [
                self._fmt_isbn(e, p)
                for e, p in zip(self._col(df, "isbn_electronic"), self._col(df, "isbn_print"))
            ],
            "DOI": self._get_col(df, "doi").apply(self._fmt_doi) if "doi" in df.columns else empty,
            "CoAuthor": df["uuid"].map(authors_map),
            "CoEditor": self._get_col(df, "hostPublicationEditors"),
            "URL": self._get_col(df, "url"),
            "Description": self._get_col(df, "abstract"),
            "<Activity Classification Name>": self._get_col(df, "subtype"),
            "uuid_output": self._get_col(df, "uuid")
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 2) CHAPTERS
# =========================================================

class Pure_Chapter_Transformer(PureBaseTransformer):

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "pureId"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "status"),
            "Status Date": [
                self._fmt_status_date(y, m, d)
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Chapter Title": self._get_col(df, "title"),
            "Book Title": self._get_col(df, "host_title"),
            "Series Title": self._get_col(df, "publicationSeries"),
            "Year": self._get_col(df, "statusYear"),
            "Date Published": empty,
            "Publisher": self._get_col(df, "publisher_name"),
            "Publisher City and State": self._get_col(df, "publisher_country"),
            "Edition": self._get_col(df, "edition"),
            "Page Number": self._get_col(df, "pages"),
            "ISSN": self._get_col(df, "issn"),
            "DOI": self._get_col(df, "doi").apply(self._fmt_doi) if "doi" in df.columns else empty,
            "CoAuthor": df["uuid"].map(authors_map),
            "CoEditor": self._get_col(df, "hostPublicationEditors"),
            "URL": self._get_col(df, "url"),
            "Description": self._get_col(df, "abstract"),
            "<Activity Classification Name>": self._get_col(df, "subtype"),
            "uuid_output": self._get_col(df, "uuid")
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 3) JOURNAL ARTICLES
# =========================================================

class Pure_Journal_Article_Transformer(PureBaseTransformer):

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "pureId"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "status"),
            "Status Date": [
                self._fmt_status_date(y, m, d)
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Semester": [
                "Summer" if (m == 5 and d >= 1) or (m == 7 and d <= 31) else
                "Fall" if (m == 8 and d >= 1) or (m == 12 and d <= 31) else
                "Spring" if (m == 1 and d >= 1) or (m == 4 and d <= 30) else
                ""
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Title": self._get_col(df, "title"),
            "Journal Title": self._get_col(df, "journal_title"),
            "Series Title": self._get_col(df, "publicationSeries"),
            "Month / Season": self._get_col(df, "statusMonth"),
            "Year": self._get_col(df, "statusYear"),
            "Date Published": empty,
            "Publisher": self._get_col(df, "publisher_name"),
            "Publisher City and State": self._get_col(df, "publisher_country"),
            "Volume": self._get_col(df, "volume"),
            "Issue Number / Edition": self._get_col(df, "journalNumber"),
            "Page Number": self._get_col(df, "pages"),
            "ISSN": self._get_col(df, "issn"),
            "DOI": self._get_col(df, "doi").apply(self._fmt_doi) if "doi" in df.columns else empty,
            "CoAuthor": df["uuid"].map(authors_map),
            "URL": self._get_col(df, "url"),
            "Description": self._get_col(df, "abstract"),
            "Impact Factor": self._get_col(df, "journal_impact_factor"),
            "uuid_output": self._get_col(df, "uuid")
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 4) CONFERENCE PROCEEDINGS
# =========================================================

class Pure_Conference_Transformer(PureBaseTransformer):

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "pureId"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "status"),
            "Status Date": [
                self._fmt_status_date(y, m, d)
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Title of Paper": self._get_col(df, "title"),
            "Title of Published Proceedings": self._get_col(df, "host_title"),
            "Title of Conference": self._get_col(df, "event_title"),
            "Conference Location": [
                self._compose_event_location(c, co)
                for c, co in zip(self._col(df, "event_city"), self._col(df, "event_country"))
            ],
            "Month / Season": self._get_col(df, "statusMonth"),
            "Year": self._get_col(df, "statusYear"),
            "Publisher": self._get_col(df, "publisher_name"),
            "Publisher City and State": self._get_col(df, "publisher_country"),
            "Volume": self._get_col(df, "volume"),
            "Issue Number / Edition": self._get_col(df, "journalNumber"),
            "Page Number": self._get_col(df, "pages"),
            "DOI": self._get_col(df, "doi").apply(self._fmt_doi) if "doi" in df.columns else empty,
            "CoAuthor": df["uuid"].map(authors_map),
            "CoEditor": self._get_col(df, "hostPublicationEditors"),
            "URL": self._get_col(df, "url"),
            "Description": self._get_col(df, "abstract"),
            "<Activity Classification Name>": self._get_col(df, "subtype"),
            "uuid_output": self._get_col(df, "uuid")
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 5) OTHER SCHOLARLY WORK
# =========================================================

class Pure_Other_Transformer(PureBaseTransformer):

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "pureId"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "status"),
            "Status Date": [
                self._fmt_status_date(y, m, d)
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Title": self._get_col(df, "title"),
            "Journal Title": empty,
            "Series Title": self._get_col(df, "publicationSeries"),
            "Month": self._get_col(df, "statusMonth"),
            "Year": self._get_col(df, "statusYear"),
            "Publisher": self._get_col(df, "publisher_name"),
            "Publisher City and State": self._get_col(df, "publisher_country"),
            "Volume": self._get_col(df, "volume"),
            "Issue Number / Edition": self._get_col(df, "journalNumber"),
            "Page Number": self._get_col(df, "pages"),
            "ISSN": self._get_col(df, "issn"),
            "Co-Contributor": df["uuid"].map(authors_map),
            "URL": self._get_col(df, "url"),
            "Description": self._get_col(df, "abstract"),
            "<Activity Classification Name>": self._get_col(df, "subtype"),
            "uuid_output": self._get_col(df, "uuid")
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 6) PATENT
# =========================================================

class Pure_Patent_Transformer(PureBaseTransformer):

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "pureId"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "status"),
            "Status Date": [
                self._fmt_status_date(y, m, d)
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Title": self._get_col(df, "title"),
            "Year": self._get_col(df, "statusYear"),
            "Patent ID": self._get_col(df, "patent_number"),
            "Patent Type": empty,
            "Patent Nationality": self._get_col(df, "country"),
            "If Patent Cooperation Treaty, List Nations": empty,
            "Co-Contributor": df["uuid"].map(authors_map),
            "URL": self._get_col(df, "url"),
            "Description": self._get_col(df, "abstract"),
            "<Activity Classification Name>": self._get_col(df, "subtype"),
            "uuid_output": self._get_col(df, "uuid")
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 7) EDITORIAL WORK
# =========================================================

class Pure_Editorial_Transformer(PureBaseTransformer):

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "pureId"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "status"),
            "Status Date": [
                self._fmt_status_date(y, m, d)
                for y, m, d in zip(self._col(df, "statusYear"), self._col(df, "statusMonth"), self._col(df, "statusDay"))
            ],
            "Title": self._get_col(df, "title"),
            "Year": self._get_col(df, "statusYear"),
            "Journal": self._get_col(df, "journal_title"),
            "Page Numbers": self._get_col(df, "pages"),
            "Publisher": self._get_col(df, "publisher_name"),
            "URL": self._get_col(df, "url"),
            "Description": self._get_col(df, "abstract"),
            "Impact Factor": self._get_col(df, "journal_impact_factor"),
            "CoAuthor": df["uuid"].map(authors_map),
            "uuid_output": self._get_col(df, "uuid")
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 8) GRANTS (Award)
# =========================================================

class Pure_Grants_Transformer(PureBaseTransformer):

    def __init__(self):
        super().__init__(role_map={
            "PI": "PI",
            "COPI": "CoPI",
            "COINVESTIGATOR": "COINVESTIGATOR",
            "OTHER": "OTHER",
        })

    def build(self, df_all_data: pd.DataFrame) -> pd.DataFrame:

        authors_map = self._build_authors_map(df_all_data)
        df = self._filter_internal_rows(df_all_data)

        empty = pd.Series([""] * len(df), index=df.index)

        out = pd.DataFrame({
            "Record ID": self._get_col(df, "uuid"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Faculty Percent Contribution": empty,
            "Faculty Role": self._get_col(df, "role"),
            "Status": self._get_col(df, "awardStatus"),
            "Status Date": self._get_col(df, "awardStatusDate"),
            "Title": self._get_col(df, "title"),
            "Sponsor": self._get_col(df, "sponsor"),
            "Grant ID / Contract ID": self._get_col(df, "contractid"),
            "Award Date": self._get_col(df, "awardDate"),
            "Start Date": self._get_col(df, "startDate"),
            "End Date": self._get_col(df, "endDate"),
            "Type of Funding": self._get_col(df, "fundingType"),
            "Total Funding": self._get_col(df, "totalAwardAmount"),
            "Currency": self._get_col(df, "currency"),
            "Abstract": self._get_col(df, "abstract"),
            "Description": self._get_col(df, "description"),
            "URL": self._get_col(df, "url"),
            "Co-Investigator(s)": df["uuid"].map(authors_map),
            # GRANTS_TRANSFORM_CONFIG (both HBKU's and Ajman's) renames the
            # grant-type field to "grantType", never "type" -- this read
            # "type" (a column that never existed here) since this
            # transformer was first ported, so "Type of Grant" always came
            # out empty. Found 2026-07-23 while reconciling Ajman's initial
            # load against far_templates.py's actual column reads.
            "Type of Grant": self._get_col(df, "grantType"),
            "uuid_output": self._get_col(df, "uuid"),
        })

        return self._add_record_id_pure_faculty(out)


# COMMAND ----------

# =========================================================
# 9) CUSTOM SECTIONS — BASE
# =========================================================

class PureCustomBaseTransformer(PureBaseTransformer):
    """Shared utilities for all Custom Section transformers."""

    MONTH_NAMES = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    @classmethod
    def _fmt_ym_to_month_year(cls, year, month) -> str:
        """Convert separate year/month values to 'Month YYYY' (FAR date format)."""
        if pd.isna(year) or pd.isna(month):
            return ""
        try:
            y, m = int(float(str(year))), int(float(str(month)))
            if not (1 <= m <= 12):
                return ""
            return f"{cls.MONTH_NAMES[m]} {y}"
        except (ValueError, TypeError):
            return ""

    @staticmethod
    def _derive_term_from_ym(year, month) -> str:
        """Convert separate year/month values to academic term 'Fall/Spring/Summer YYYY'."""
        if pd.isna(year) or pd.isna(month):
            return ""
        try:
            y, m = int(float(str(year))), int(float(str(month)))
        except (ValueError, TypeError):
            return ""
        if 1 <= m <= 5:
            term = "Spring"
        elif 6 <= m <= 7:
            term = "Summer"
        else:
            term = "Fall"
        return f"{term} {y}"

    @classmethod
    def _fmt_ymd(cls, year, month, day="01") -> str:
        """Convert separate year/month/day values to 'YYYY-MM-DD'."""

        if pd.isna(year):
            return ""

        try:
            y = int(float(str(year)))

            if pd.isna(month) or str(month).strip() == "" or str(month).lower() == "nan":
                m = 1
            else:
                m = int(float(str(month)))

            if pd.isna(day) or str(day).strip() == "" or str(day).lower() == "nan":
                d = 1
            else:
                d = int(float(str(day)))

            if not (1 <= m <= 12 and 1 <= d <= 31):
                return ""

            return f"{y:04d}-{m:02d}-{d:02d}"

        except (ValueError, TypeError):
            return ""

    def _build_common_columns(self, df: pd.DataFrame) -> dict:
        """Return columns shared by all custom section subtypes.

        The input (enriched_custom_sections_<date>) already stores raw Pure
        field names: title, descriptions, managing_organization,
        member_of_name, start_date_year/month/day, end_date_year/month/day.
        """
        return {
            "Record ID": self._get_col(df, "uuid"),
            "Faculty ID": self._get_col(df, "faculty_id"),
            "Subtype": self._get_col(df, "subtype"),
            "Work Title": self._get_col(df, "title"),
            "Start Date": [
                self._fmt_ymd(y, m, d)
                for y, m, d in zip(self._col(df, "start_date_year"), self._col(df, "start_date_month"), self._col(df, "start_date_day"))
            ],
            "End Date": [
                self._fmt_ymd(y, m, d)
                for y, m, d in zip(self._col(df, "end_date_year"), self._col(df, "end_date_month"), self._col(df, "end_date_day"))
            ],
            "Status": self._get_col(df, "status"),
            # Not a real FAR field -- carried through so postprocess_changes.py
            # can attach changeType (new/updates SFTP split) by uuid, same as
            # Scholarly Activities/Grants' "uuid_output" (which "Record ID"
            # already IS for Custom Sections, but kept under this name too
            # for a uniform merge key across all scopes).
            "uuid_output": self._get_col(df, "uuid"),
        }


# COMMAND ----------

# =========================================================
# 10) SERVICE: PROFESSIONAL
# =========================================================

class Pure_Custom_SP_Transformer(PureCustomBaseTransformer):

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        empty = pd.Series([""] * len(df), index=df.index)
        cols = self._build_common_columns(df)
        cols.update({
            "Description": self._get_col(df, "descriptions"),
            "Organization": self._get_col(df, "managing_organization"),
            "Num Participants": empty,
            "Benefits to University": empty,
        })
        col_order = [
            "Record ID", "Faculty ID", "Subtype", "Work Title",
            "Description", "Start Date", "End Date",
            "Organization", "Num Participants", "Benefits to University",
            "Status", "uuid_output",
        ]
        return pd.DataFrame(cols)[col_order]


# COMMAND ----------

# =========================================================
# 11) SERVICE: UNIVERSITY
# =========================================================

class Pure_Custom_SU_Transformer(PureCustomBaseTransformer):

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        empty = pd.Series([""] * len(df), index=df.index)
        cols = self._build_common_columns(df)
        cols.update({
            "Description": self._get_col(df, "descriptions"),
            "Organization": self._get_col(df, "managing_organization"),
            "Num Participants": empty,
            "Benefits to University": empty,
        })
        col_order = [
            "Record ID", "Faculty ID", "Subtype", "Work Title",
            "Description", "Start Date", "End Date",
            "Organization", "Num Participants", "Benefits to University",
            "Status", "uuid_output",
        ]
        return pd.DataFrame(cols)[col_order]


# COMMAND ----------

# =========================================================
# 12) OTHER: PROFESSIONAL MEMBERSHIP
# =========================================================

class Pure_Custom_OPM_Transformer(PureCustomBaseTransformer):

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        empty = pd.Series([""] * len(df), index=df.index)
        cols = self._build_common_columns(df)
        cols.update({
            "Description": self._get_col(df, "descriptions"),
            "Organization": self._get_col(df, "managing_organization"),
            "Num Participants": empty,
            "Benefits to University": empty,
        })
        col_order = [
            "Record ID", "Faculty ID", "Subtype", "Work Title",
            "Description", "Start Date", "End Date",
            "Organization", "Num Participants", "Benefits to University",
            "Status", "uuid_output",
        ]
        return pd.DataFrame(cols)[col_order]


# COMMAND ----------

# =========================================================
# 13) OTHER: CONSULTING
# =========================================================

class Pure_Custom_Consulting_Transformer(PureCustomBaseTransformer):

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        empty = pd.Series([""] * len(df), index=df.index)
        cols = self._build_common_columns(df)

        raw_client = self._get_col(df, "member_of_name")
        managing_org = self._get_col(df, "managing_organization")

        def clean_val(v):
            if pd.isna(v):
                return ""
            v_str = str(v).strip()
            if v_str.lower() in ("", "nan", "null", "none", "<na>"):
                return ""
            return v_str

        primary_client = raw_client.apply(clean_val)
        fallback_org = managing_org.apply(clean_val)
        client_name = primary_client.where(primary_client != "", fallback_org)

        cols.update({
            "Description of Engagement": self._get_col(df, "descriptions"),
            "Client Name": client_name,
            "Estimated Hours": empty,
        })
        col_order = [
            "Record ID", "Faculty ID", "Subtype", "Work Title",
            "Description of Engagement", "Start Date", "End Date",
            "Client Name", "Estimated Hours",
            "Status", "uuid_output",
        ]
        return pd.DataFrame(cols)[col_order]
