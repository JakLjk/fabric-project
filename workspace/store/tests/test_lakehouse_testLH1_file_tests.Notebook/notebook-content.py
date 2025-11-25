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

ENV = "DEV"  # overridden by pipeline

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

%run test_lakehouse_testLH1_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from notebookutils import mssparkutils
import datetime as dt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_cfg = FILE_TEST_CONFIG[ENV]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# -------------------------------------------------------------------
# 2. FILE / LAYOUT TESTS (using FILE_TEST_CONFIG + mssparkutils)
# -------------------------------------------------------------------


print(f"[FILE TEST] Using FILE_TEST_CONFIG for ENV={ENV}")

# 2.1 raw_paths existence and min file counts
for raw in file_cfg.get("raw_paths", []):
    path = raw["path"]
    min_files = raw.get("min_files", 1)
    print(f"[FILE TEST] Checking path {path} (min_files={min_files})")

    files = mssparkutils.fs.ls(path)
    file_count = len(files)
    assert file_count >= min_files, (
        f"[{ENV}:file_tests] Only {file_count} files in {path}, expected >= {min_files}"
    )


print(f"=== ALL TESTS PASSED for ENV={ENV} ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
