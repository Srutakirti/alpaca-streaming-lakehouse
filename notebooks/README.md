# Iceberg catalog exploration

`iceberg_hadoop_catalog_explore.ipynb` is a read-only PySpark client for the
same HadoopCatalog layout used by the Java loader. It is intended for schema,
snapshot, and bounded-query exploration; it must not be used to write or
maintain tables.

Create persistent local data first:

```bash
uv run python scripts/run_local_stack.py --source synthetic --runtime-dir .local-notebook
ICEBERG_WAREHOUSE="$PWD/.local-notebook/warehouse" uv run jupyter lab notebooks/
```

The **Notebook configuration** cell exposes the catalog, warehouse, GCS mode,
connector JAR, namespace, table, UTC bounds, and limit. Namespace/table/query
changes take effect by rerunning that cell and the later query cells. Changing
warehouse, catalog, GCS mode, or connector JAR requires a kernel restart before
recreating Spark.

The notebook finds the checkout when its kernel starts in the repository or a
subdirectory. If your Jupyter server uses another working directory, set its
explicit source location before starting Jupyter:

```bash
GCE_HADOOP_CATALOG_REPOSITORY="$PWD" uv run jupyter lab notebooks/
```

GCS is intentionally disabled unless both settings are supplied:

```bash
NOTEBOOK_ENABLE_GCS=true \
ICEBERG_WAREHOUSE=gs://YOUR_NEW_BUCKET/warehouse \
GCS_CONNECTOR_JAR=/tmp/gcs-connector-hadoop3-2.2.30-shaded.jar \
uv run jupyter lab notebooks/
```

The GCS connector path follows the proven legacy setup. Authentication uses
Application Default Credentials; no credentials are stored in the notebook.
