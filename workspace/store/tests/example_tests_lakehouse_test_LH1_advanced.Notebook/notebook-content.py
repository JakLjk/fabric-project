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


from pyspark.sql import functions as F

dimension_city_df      = spark.read.table("testLH1.dimension_city")
dimension_customer_df  = spark.read.table("testLH1.dimension_customer")
fact_sale_df           = spark.read.table("testLH1.fact_sale")

# 1) Basic checks (can keep or import from basic notebook)
assert_min_rows(dimension_city_df,     1, "dimension_city")
assert_min_rows(dimension_customer_df, 1, "dimension_customer")
assert_min_rows(fact_sale_df,          1, "fact_sale")

assert_no_nulls(dimension_city_df,     ["CityKey"],     "dimension_city")
assert_no_nulls(dimension_customer_df, ["CustomerKey"], "dimension_customer")
assert_no_nulls(fact_sale_df,          ["SaleKey"],     "fact_sale")

# 2) Key uniqueness
assert_unique(dimension_city_df,     ["CityKey"],     "dimension_city PK")
assert_unique(dimension_customer_df, ["CustomerKey"], "dimension_customer PK")
assert_unique(fact_sale_df,          ["SaleKey"],     "fact_sale PK")

# 3) Numeric range checks for fact_sale.Amount
assert_numeric_range(fact_sale_df, "Quantity", min_value=0, max_value=1_000_000,
                     context="fact_sale.Quantity")

# 4) Date distribution: at least 10 distinct SaleDate values
assert_group_coverage(fact_sale_df, "DeliveryDateKey", min_groups=10,
                      context="fact_sale.DeliveryDateKey distribution")

# 5) City coverage: enough cities in dimension_city and fact_sale
assert_group_coverage(dimension_city_df, "CityKey", min_groups=1,
                      context="dimension_city.CityKey distribution")
assert_group_coverage(fact_sale_df,      "CityKey", min_groups=1,
                      context="fact_sale.CityKey distribution")

# 6) Domain checks (allowed country codes)
allowed_countries = ["PL", "DE", "US", "UK", "FR"]

assert_values_in_set(
    dimension_city_df,
    col="Country",
    allowed=allowed_countries,
    context="dimension_city.Country"
)

print("Advanced data tests passed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
