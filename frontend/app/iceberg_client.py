import os
from functools import lru_cache

from pyiceberg.catalog.sql import SqlCatalog

ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "sqlite:///./warehouse/catalog.db")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "./warehouse")
ICEBERG_NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "alpaca")
ICEBERG_TABLE = os.environ.get("ICEBERG_TABLE", "bars")


@lru_cache(maxsize=1)
def get_table():
    if ICEBERG_WAREHOUSE.startswith(("./", "/")) and not ICEBERG_WAREHOUSE.startswith("gs://"):
        os.makedirs(ICEBERG_WAREHOUSE, exist_ok=True)
    catalog = SqlCatalog("alpaca_catalog", uri=ICEBERG_CATALOG_URI, warehouse=ICEBERG_WAREHOUSE)
    return catalog.load_table(f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE}")
