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

# PARAMETERS CELL ********************

ENV = "DEV"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run helpers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

print(f"Running data tests for ENV={ENV}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========= LOAD TABLES =========
dimension_city_df      = spark.read.table("testLH1.dimension_city")
dimension_customer_df  = spark.read.table("testLH1.dimension_customer")
fact_sale_df           = spark.read.table("testLH1.fact_sale")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========= BASIC CHECKS =========
assert_min_rows(dimension_city_df,     1, "dimension_city")
assert_min_rows(dimension_customer_df, 1, "dimension_customer")
assert_min_rows(fact_sale_df,          1, "fact_sale")


assert_no_nulls(dimension_city_df,     ["CityKey"],     "dimension_city")
assert_no_nulls(dimension_customer_df, ["CustomerKey"], "dimension_customer")
assert_no_nulls(fact_sale_df,          ["SaleKey"],     "fact_sale")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========= FK INTEGRITY =========
missing_city_keys = (
    fact_sale_df.select("CityKey").distinct()
    .join(dimension_city_df.select("CityKey").distinct(),
          on="CityKey", how="left_anti")
    .count()
)
assert missing_city_keys == 0, f"{missing_city_keys} CityKey in fact_sale not in dimension_city"

missing_customer_keys = (
    fact_sale_df.select("CustomerKey").distinct()
    .join(dimension_customer_df.select("CustomerKey").distinct(),
          on="CustomerKey", how="left_anti")
    .count()
)
assert missing_customer_keys == 0, f"{missing_customer_keys} CustomerKey in fact_sale not in dimension_customer"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========= ADVANCED CHECKS (EXAMPLE) =========
# Amount in proper range
assert_numeric_range(
    fact_sale_df, "Quantity",
    min_value=0,
    max_value=1_000_000,
    context="fact_sale.Quantity"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enough dates
assert_group_coverage(
    fact_sale_df,
    group_col="DeliveryDateKey",
    min_groups=1,
    context="fact_sale.SaleDate distribution"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Allowed Countries in dimension_city
allowed_countries = ["United States", "N/A"]
assert_values_in_set(
    dimension_city_df,
    col="Country",
    allowed=allowed_countries,
    context="dimension_city.Country"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Data tests for ENV={ENV} passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
