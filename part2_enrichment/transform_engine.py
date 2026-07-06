# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform engine
# MAGIC Generic, config-driven engine for turning a raw Pure JSON record into
# MAGIC our normalized schema. Used for all 3 scopes (Scholarly Activities,
# MAGIC Grants, Custom Sections) — every subtype gets its own JSON config in
# MAGIC `cfgs/`, instead of a dedicated Python class per subtype.
# MAGIC
# MAGIC Adapted from the declarative engine originally written for Grants
# MAGIC only (`ip-pure2far-integration`, `grants_integration_upd` branch,
# MAGIC `Transformer.flatten_dataframe` / `apply_transforms`), generalized
# MAGIC here to every scope per the project's decision to standardize on one
# MAGIC transform style instead of a hybrid of config + hardcoded classes.
# MAGIC
# MAGIC Supported actions (all executed in the order listed under each field):
# MAGIC - `add` — creates the column if it does not exist yet (null-filled)
# MAGIC - `drop` — removes the column
# MAGIC - `lowercase` / `strip` — string cleanup
# MAGIC - `fill_from` — copies another column's value where the field is null
# MAGIC - `cast` — `to_type` one of `datetime`, `date`, `string`, `int`, `float`
# MAGIC - `fill_null` — replaces null (and empty string) with a constant
# MAGIC - `extract_from_list` — pulls a value out of a list of dicts, optionally
# MAGIC   matching on a nested key first (`match.path` / `match.equals`)
# MAGIC - `lookup_from_dataframe` — enriches by joining against another
# MAGIC   DataFrame passed in `context`
# MAGIC - `map_values` — dictionary-based value mapping, with an optional
# MAGIC   default (`"__SELF__"` keeps the original value when unmapped)
# MAGIC - `if_null_else` — sets one of two constants depending on nullness
# MAGIC - `rename` — renames the column (should be the last action for a field)

# COMMAND ----------

import numpy as np
import pandas as pd

# COMMAND ----------

def get_by_path(obj, path):
    """Safely get a nested value from a dict using a list of keys."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _flatten_value(value, parent_key="", sep="."):
    """
    Recursively flattens a single value. Dicts are flattened into
    `parent.child` keys; lists and scalars are preserved as-is (a list is
    kept whole so `extract_from_list` can operate on it later).
    """
    if isinstance(value, dict):
        items = {}
        for key, sub_value in value.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.update(_flatten_value(sub_value, new_key, sep))
        return items
    return {parent_key: value}


def flatten_dataframe(df: pd.DataFrame, sep: str = ".") -> pd.DataFrame:
    """
    Flattens every dict-valued column in a DataFrame (row cardinality is
    preserved — this does not explode lists, only flattens nested dicts).
    """
    flattened_rows = []
    for row in df.to_dict(orient="records"):
        flat_row = {}
        for col, value in row.items():
            flat_row.update(_flatten_value(value, col, sep))
        flattened_rows.append(flat_row)
    return pd.DataFrame(flattened_rows)


# COMMAND ----------

def apply_transforms(df: pd.DataFrame, config: dict, context: dict = None) -> pd.DataFrame:
    """
    Applies a field -> actions config (see module docstring above) to a
    flattened DataFrame.

    `context` holds any extra DataFrames needed by `lookup_from_dataframe`,
    keyed by the name used in the config's `reference`.

    Example:
        df = apply_transforms(df, SOME_CONFIG, context={"persons": persons_df})
    """
    df = df.copy()

    for field, rule in config.items():
        for action in rule.get("actions", []):
            action_type = action["type"]

            if field not in df.columns and action_type != "add":
                raise ValueError(f"Field '{field}' does not exist and must be created with 'add' first")

            if action_type == "add":
                if field not in df.columns:
                    df[field] = pd.NA

            elif action_type == "drop":
                df = df.drop(columns=[field])
                break

            elif action_type == "lowercase":
                df[field] = df[field].str.lower()

            elif action_type == "strip":
                df[field] = df[field].str.strip()

            elif action_type == "fill_from":
                source = action["source"]
                if source in df.columns:
                    df[field] = df[field].where(df[field].notna(), df[source].where(df[source].notna()))

            elif action_type == "cast":
                to_type = action["to_type"]
                target = action.get("to", field)
                if to_type == "datetime":
                    df[field] = pd.to_datetime(df[field], errors="coerce")
                elif to_type == "date":
                    df[target] = pd.to_datetime(df[field], errors="coerce").dt.date
                elif to_type in ("string", "str"):
                    df[target] = df[field].map(lambda x: str(x) if pd.notna(x) else None).astype("string")
                elif to_type == "int":
                    df[target] = pd.to_numeric(df[field], errors="coerce").astype("Int64")
                elif to_type == "float":
                    df[target] = pd.to_numeric(df[field], errors="coerce")
                else:
                    raise ValueError(f"Unsupported cast type: {to_type}")

            elif action_type == "fill_null":
                value = action.get("value")
                df[field] = df[field].replace("", pd.NA).fillna(value)

            elif action_type == "extract_from_list":
                match = action.get("match")
                list_path = action.get("list_path")
                value_path = action["value_path"]
                target = action.get("to", field)
                default = action.get("default")

                def extract(cell, match=match, list_path=list_path, value_path=value_path, default=default):
                    if not isinstance(cell, list) or not cell:
                        return default

                    if match:
                        match_path = match["path"]
                        match_value = match["equals"]
                        for item in cell:
                            if not isinstance(item, dict):
                                continue
                            if get_by_path(item, match_path) == match_value:
                                value_obj = item
                                if list_path:
                                    value_obj = get_by_path(item, list_path)
                                    if not isinstance(value_obj, list) or not value_obj:
                                        return default
                                    value_obj = value_obj[0]
                                value = get_by_path(value_obj, value_path)
                                return value if value is not None else default
                        return default

                    item = cell[0]
                    if not isinstance(item, dict):
                        return default
                    value = get_by_path(item, value_path)
                    return value if value is not None else default

                df[target] = df[field].apply(extract)

            elif action_type == "lookup_from_dataframe":
                reference = action["reference"]
                lookup_key = action["lookup_key"]
                value_column = action["value_column"]
                target = action.get("to", field)
                default = action.get("default")

                if context is None or reference not in context:
                    raise ValueError(f"lookup_from_dataframe: reference '{reference}' not found in context")

                ref_df = context[reference]
                if lookup_key not in ref_df.columns or value_column not in ref_df.columns:
                    raise ValueError(f"lookup_from_dataframe: '{lookup_key}'/'{value_column}' not in '{reference}'")

                lookup_series = ref_df.set_index(lookup_key)[value_column]
                df[target] = df[field].map(lookup_series)
                if default is not None:
                    df[target] = df[target].fillna(default)

            elif action_type == "map_values":
                mapping = action["mapping"]
                default = action.get("default")
                mapped = df[field].map(mapping)
                if default == "__SELF__":
                    df[field] = mapped.where(mapped.notna(), df[field])
                elif default is not None:
                    df[field] = mapped.fillna(default)
                else:
                    df[field] = mapped

            elif action_type == "if_null_else":
                null_value = action["null_value"]
                else_value = action["else_value"]
                target = action.get("to", field)
                treat_empty = action.get("treat_empty_string_as_null", False)
                series = df[field].replace("", np.nan) if treat_empty else df[field]
                df[target] = np.where(series.isna(), null_value, else_value)

            elif action_type == "rename":
                new_name = action["to"]
                df = df.rename(columns={field: new_name})
                field = new_name

            else:
                raise ValueError(f"Unknown transform action type: {action_type}")

    return df
