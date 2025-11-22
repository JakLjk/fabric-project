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

# Imports
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales = spark.read.table("testLH1.fact_sale")
df_city = spark.read.table("testLH1.dimension_city")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales = df_sales.withColumn("delivery_month", F.month("DeliveryDateKey"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales = df_sales.repartition(50, F.col("CityKey"))
df_city = df_city.repartition(50, F.col("CityKey"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_city)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales_city = df_sales.alias("sales")\
    .join(df_city.alias("city"), "CityKey")\
    .selectExpr(
        "sales.*", 
        "city.City as tCity_City", 
        "city.StateProvince as tcity_StateProvince")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales_city.write.format("delta").mode("overwrite").saveAsTable("testLH1.transformed_sales_city")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
