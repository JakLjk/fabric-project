# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# helpers notebook
from typing import Iterable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def assert_no_nulls(df: DataFrame, cols: Iterable[str], context: str = ""):
    for c in cols:
        nulls = df.filter(F.col(c).isNull()).count()
        assert nulls == 0, f"[{context}] Column '{c}' has {nulls} NULLs"

def assert_min_rows(df: DataFrame, min_rows: int, context: str = ""):
    cnt = df.count()
    assert cnt >= min_rows, f"[{context}] Row count {cnt} < {min_rows}"

def assert_values_in_set(df: DataFrame, col: str, allowed: Iterable, context: str = ""):
    bad_count = (
        df
        .select(col)
        .distinct()
        .filter(~F.col(col).isin(list(allowed)))
        .count()
    )
    assert bad_count == 0, f"[{context}] Column '{col}' has {bad_count} values not in allowed set"

def assert_unique(df: DataFrame, cols: Iterable[str], context: str = ""):
    dup_count = df.groupBy(*cols).count().filter("count > 1").count()
    assert dup_count == 0, f"[{context}] Duplicate values found for {cols}"


def assert_numeric_range(
    df: DataFrame, col: str,
    min_value=None, max_value=None,
    context: str = ""
):
    if min_value is not None:
        below = df.filter(F.col(col) < min_value).count()
        assert below == 0, f"[{context}] Column '{col}' has {below} values < {min_value}"
    if max_value is not None:
        above = df.filter(F.col(col) > max_value).count()
        assert above == 0, f"[{context}] Column '{col}' has {above} values > {max_value}"


def assert_group_coverage(df: DataFrame, group_col: str, min_groups: int, context: str = ""):
    groups = df.select(group_col).distinct().count()
    assert groups >= min_groups, f"[{context}] Only {groups} distinct '{group_col}' values (min {min_groups})"


def assert_fk_integrity(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fk_col: str,
    dim_key_col: str,
    context: str = ""
):
    missing = (
        fact_df.select(fk_col).distinct()
               .join(dim_df.select(dim_key_col).distinct(),
                     fact_df[fk_col] == dim_df[dim_key_col],
                     "left_anti")
               .count()
    )
    assert missing == 0, f"[{context}] {missing} FK values in fact not found in dimension"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
