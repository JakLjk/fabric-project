# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

abfs_testLH1 = "abfss://devStore@onelake.dfs.fabric.microsoft.com/testLH1.Lakehouse"
abfs_testLH2 = "abfss://devStore@onelake.dfs.fabric.microsoft.com/testLH2.Lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load(os.path.join(abfs_testLH1, "Tables/transformed_sales_city"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").save(os.path.join(abfs_testLH2, "Tables/sales_city_table_imported_from_LH1"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
