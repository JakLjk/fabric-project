# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

param_base_abfs_mainLH1 = "abfss://mainSources@onelake.dfs.fabric.microsoft.com/mainLH1.Lakehouse"
param_base_abfs_testLH1 = "abfss://devStore@onelake.dfs.fabric.microsoft.com/testLH1.Lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# imports
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = os.path.join(param_base_abfs_mainLH1, "Tables/fact_sale")
df = spark.read.format("delta").load(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = os.path.join(param_base_abfs_testLH1, "Tables/transformed_fact_sale")
df = df.write.format("delta").mode("overwrite").save(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
