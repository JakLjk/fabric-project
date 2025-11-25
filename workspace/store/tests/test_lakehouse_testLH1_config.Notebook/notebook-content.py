# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# test_config.Notebook

# Per-environment configuration for TABLE tests and FILE tests.
# This lives in source control, is deployed by fabric-cicd,
# and is selected at runtime via ENV parameter in the test notebook.

DATA_TEST_CONFIG = {
    "DEV": {
        "tables": {
            "testLH1.dimension_city": {
                "min_rows": 10,
                "no_null_columns": ["CityKey"]
            },
            "testLH1.dimension_customer": {
                "min_rows": 50,
                "no_null_columns": ["CustomerKey"]
            },
            "testLH1.fact_sale": {
                "min_rows": 500,
                "no_null_columns": ["SaleKey"],
                "numeric_ranges": {
                    "Amount": {"min": 0, "max": 1_000_000}
                },
                "group_coverage": {
                    "SaleDate": 5   # at least 5 distinct dates
                },
                "foreign_keys": [
                    {
                        "fk_col": "CityKey",
                        "dim_table": "testLH1.dimension_city",
                        "dim_key": "CityKey"
                    },
                    {
                        "fk_col": "CustomerKey",
                        "dim_table": "testLH1.dimension_customer",
                        "dim_key": "CustomerKey"
                    }
                ]
            }
        }
    },

    "TEST": {
        "tables": {
            "testLH1.dimension_city": {
                "min_rows": 20,
                "no_null_columns": ["CityKey"]
            },
            "testLH1.dimension_customer": {
                "min_rows": 200,
                "no_null_columns": ["CustomerKey"]
            },
            "testLH1.fact_sale": {
                "min_rows": 5_000,
                "no_null_columns": ["SaleKey"],
                "numeric_ranges": {
                    "Amount": {"min": 0, "max": 5_000_000}
                },
                "group_coverage": {
                    "SaleDate": 10
                },
                "foreign_keys": [
                    {
                        "fk_col": "CityKey",
                        "dim_table": "testLH1.dimension_city",
                        "dim_key": "CityKey"
                    },
                    {
                        "fk_col": "CustomerKey",
                        "dim_table": "testLH1.dimension_customer",
                        "dim_key": "CustomerKey"
                    }
                ]
            }
        }
    },

    "PROD": {
        "tables": {
            "testLH1.dimension_city": {
                "min_rows": 100,
                "no_null_columns": ["CityKey"]
            },
            "testLH1.dimension_customer": {
                "min_rows": 1_000,
                "no_null_columns": ["CustomerKey"]
            },
            "testLH1.fact_sale": {
                "min_rows": 50_000,
                "no_null_columns": ["SaleKey"],
                "numeric_ranges": {
                    "Amount": {"min": 0, "max": 10_000_000}
                },
                "group_coverage": {
                    "SaleDate": 30
                },
                "foreign_keys": [
                    {
                        "fk_col": "CityKey",
                        "dim_table": "testLH1.dimension_city",
                        "dim_key": "CityKey"
                    },
                    {
                        "fk_col": "CustomerKey",
                        "dim_table": "testLH1.dimension_customer",
                        "dim_key": "CustomerKey"
                    }
                ]
            }
        }
    }
}

FILE_TEST_CONFIG = {
    "DEV": {
        "raw_paths": [
            { "path": "Files", "min_files": 1 },               # root of Files
            # or e.g. { "path": "Files/raw", "min_files": 1 }
        ],
        "partition_checks": [
            {
                "base_path": "lakehouse/default/Tables/fact_sale",
                "column": "SaleDate",
                "days_back": 3
            }
        ]
    },
    "TEST": {
        "raw_paths": [
            { "path": "Files", "min_files": 1 },               # root of Files
            # or e.g. { "path": "Files/raw", "min_files": 1 }
        ],
        "partition_checks": [
            {
                "base_path": "lakehouse/default/Tables/fact_sale",
                "column": "SaleDate",
                "days_back": 7
            }
        ]
    },
    "PROD": {
        "raw_paths": [
            { "path": "Files", "min_files": 1 },               # root of Files
            # or e.g. { "path": "Files/raw", "min_files": 1 }
        ],
        "partition_checks": [
            {
                "base_path": "lakehouse/default/Tables/fact_sale",
                "column": "SaleDate",
                "days_back": 30
            }
        ]
    }
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
