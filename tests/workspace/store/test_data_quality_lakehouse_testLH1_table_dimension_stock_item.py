import os
import pytest
import pyodbc

TABLE = "dbo.dimension_stock_item"

@pytest.fixture(scope="session")
def env():
    return os.environ["FABRIC_ENVIRONMENT"].upper()

@pytest.fixture(scope="session")
def conn(env):
    endpoint = os.environ["FABRIC_SQL_ENDPOINT_LH1"]
    database = "testLH1"

    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    driver = "{ODBC Driver 17 for SQL Server}"
 
    connection_string = (
        f"Driver={driver};"
        f"Server={endpoint},1433;"
        f"Database={database};"
        f"Uid={client_id};"
        f"Pwd={client_secret};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Authentication=ActiveDirectoryServicePrincipal;"
    )

    return pyodbc.connect(connection_string)

def test_dimension_stock_item_not_empty(conn, env):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    (cnt,) = cur.fetchone()

    # Different thresholds per env
    if env == "DEV":
        assert cnt >= 1, f"[DEV] {TABLE} empty: {cnt}"
    elif env == "TEST":
        assert cnt >= 100, f"[TEST] {TABLE} too small: {cnt}"
    elif env == "PROD":
        assert cnt >= 1000, f"[PROD] {TABLE} too small: {cnt}"

def test_dimension_stock_item_key_unique(conn):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    (total,) = cur.fetchone()

    cur.execute(f"SELECT COUNT(DISTINCT StockItemKey) FROM {TABLE}")
    (distinct_orders,) = cur.fetchone()

    assert total == distinct_orders, "StockItemKey not unique in dimension_stock_item"

def test_dimension_stock_item_no_negative_amounts(conn):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE QuantityPerOuter < 0")
    (neg_cnt,) = cur.fetchone()
    assert neg_cnt == 0, f"Found {neg_cnt} negative QuantityPerOuter values"

def test_dimension_stock_item_tax_rate_below_0(conn):
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE TaxRate < 0.0")
    (cnt,) = cur.fetchone()
    assert cnt == 0, f"Found {cnt}  TaxRate values below 0.0 threshhold"

