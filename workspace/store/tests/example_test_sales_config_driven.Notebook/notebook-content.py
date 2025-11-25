# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "39744a72-13a7-4928-92c7-16a2a86c96c7",
# META       "default_lakehouse_name": "testLH1",
# META       "default_lakehouse_workspace_id": "412e9e96-5a6e-48f9-afeb-a02917a58965",
# META       "known_lakehouses": [
# META         {
# META           "id": "39744a72-13a7-4928-92c7-16a2a86c96c7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run helpers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from notebookutils import mssparkutils

# 1) Load config
config_path = "Files/tests/data_tests.json"
config_text = mssparkutils.fs.head(config_path, 1024 * 1024)
cfg = json.loads(config_text)

env = cfg["environment"]
print(f"Running data tests for environment: {env}")

# 2) Iterate tables from config
for t in cfg["tables"]:
    table_name = t["name"]
    context = table_name
    print(f"Testing table: {table_name}")

    df = spark.read.table(table_name)

    # Row count
    if "min_rows" in t:
        assert_min_rows(df, t["min_rows"], context)

    # Null checks
    if "no_null_columns" in t:
        assert_no_nulls(df, t["no_null_columns"], context)

    # Allowed values
    for col, allowed in t.get("allowed_values", {}).items():
        assert_values_in_set(df, col, allowed, context=f"{context}.{col}")

    # Numeric ranges
    for col, bounds in t.get("numeric_ranges", {}).items():
        assert_numeric_range(
            df,
            col,
            min_value=bounds.get("min"),
            max_value=bounds.get("max"),
            context=f"{context}.{col}"
        )

    # Group coverage
    for col, min_groups in t.get("group_coverage", {}).items():
        assert_group_coverage(
            df,
            group_col=col,
            min_groups=min_groups,
            context=f"{context}.{col} distribution"
        )

    # Foreign keys
    for fk_def in t.get("foreign_keys", []):
        fact_df = df
        dim_df = spark.read.table(fk_def["dim_table"])
        assert_fk_integrity(
            fact_df,
            dim_df,
            fk_col=fk_def["fk_col"],
            dim_key_col=fk_def["dim_key"],
            context=f"{table_name}.{fk_def['fk_col']} → {fk_def['dim_table']}.{fk_def['dim_key']}"
        )

print("Config-driven data tests passed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
