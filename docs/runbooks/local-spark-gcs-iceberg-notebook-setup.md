# Local Spark GCS Iceberg Notebook Setup

This note captures the setup used to run local PySpark notebooks against the GCS Iceberg warehouse, with a Hadoop Iceberg catalog enabled by default and the Cloud SQL/JDBC catalog enabled on demand.

## GCP Resources

- Project: `project-66783f65-9c3e-4880-9a3`
- Region: `us-east1`
- GCS warehouse: `gs://project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse`
- Cloud SQL instance: `alpaca-iceberg-catalog`
- Cloud SQL connection name: `project-66783f65-9c3e-4880-9a3:us-east1:alpaca-iceberg-catalog`
- Spark service account: `alpaca-spark-gcs-reader@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com`

## IAM And Org Policy Changes

Service account key creation was blocked by the org policy constraint `iam.disableServiceAccountKeyCreation`. To create the local Spark key, these changes were applied:

1. Granted org-level Organization Policy Administrator to the active user:

```bash
gcloud organizations add-iam-policy-binding 437119136348 \
  --member=user:cloudsrutakirti@proton.me \
  --role=roles/orgpolicy.policyAdmin
```

2. Disabled service account key creation enforcement at the project and organization levels:

```bash
gcloud resource-manager org-policies disable-enforce \
  iam.disableServiceAccountKeyCreation \
  --project=project-66783f65-9c3e-4880-9a3 \
  --quiet

gcloud resource-manager org-policies disable-enforce \
  iam.disableServiceAccountKeyCreation \
  --organization=437119136348 \
  --quiet
```

3. Enabled the Organization Policy API so the effective policy could be inspected:

```bash
gcloud services enable orgpolicy.googleapis.com \
  --project=project-66783f65-9c3e-4880-9a3
```

4. Verified the effective policy:

```bash
gcloud org-policies describe constraints/iam.disableServiceAccountKeyCreation \
  --project=project-66783f65-9c3e-4880-9a3 \
  --effective \
  --format=yaml
```

Expected effective rule:

```yaml
spec:
  rules:
  - enforce: false
```

## Spark Service Account

The local Spark service account was created for notebook access to the GCS warehouse:

```bash
gcloud iam service-accounts create alpaca-spark-gcs-reader \
  --project=project-66783f65-9c3e-4880-9a3 \
  --display-name="Local Spark GCS Iceberg Reader Writer"
```

It needs object read/write/delete access and bucket metadata read access:

```bash
gcloud storage buckets add-iam-policy-binding \
  gs://project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse \
  --member=serviceAccount:alpaca-spark-gcs-reader@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin \
  --project=project-66783f65-9c3e-4880-9a3

gcloud storage buckets add-iam-policy-binding \
  gs://project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse \
  --member=serviceAccount:alpaca-spark-gcs-reader@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com \
  --role=roles/storage.legacyBucketReader \
  --project=project-66783f65-9c3e-4880-9a3
```

`roles/storage.objectAdmin` allows reading and writing warehouse objects. `roles/storage.legacyBucketReader` supplies `storage.buckets.get`, which HadoopCatalog needs when it checks bucket paths.

Create the local key:

```bash
gcloud iam service-accounts keys create /tmp/alpaca-spark-gcs-reader.json \
  --project=project-66783f65-9c3e-4880-9a3 \
  --iam-account=alpaca-spark-gcs-reader@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

Treat `/tmp/alpaca-spark-gcs-reader.json` like a password. Do not commit it.

For day-to-day notebook setup, use the repo helper. It checks both local files and recreates only the missing pieces:

```bash
./scripts/setup_notebook_gcs_prereqs.sh
```

Use this when `/tmp` has been cleaned and the notebook reports:

```text
GCS key exists   : False
GCS jar exists   : False
```

To force recreation:

```bash
./scripts/setup_notebook_gcs_prereqs.sh --force-key --force-jar
```

## Spark GCS Connector Jars

The notebooks use a shaded GCS connector jar from `/tmp`.

The working default for the notebooks is the Hadoop 3 connector:

```bash
curl -s -L \
  https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.30/gcs-connector-hadoop3-2.2.30-shaded.jar \
  -o /tmp/gcs-connector-hadoop3-2.2.30-shaded.jar
```

The newer `4.0.4` shaded connector worked for Parquet reads but failed for Spark CSV text reads in this local Spark 3.5 environment with a missing Hadoop class:

```text
org/apache/hadoop/fs/Options$OpenFileOptions
```

So the notebooks default to:

```text
/tmp/gcs-connector-hadoop3-2.2.30-shaded.jar
```

## Notebook Roles

- `notebooks/hadoop_catalog_with_on_demand_cloudsql.ipynb`
  - Starts Spark with GCS Hadoop Iceberg catalog enabled by default as `gcs_iceberg`.
  - Creates and queries exploratory Iceberg tables without Cloud SQL.
  - Provides `enable_cloudsql_catalog()` to register the production JDBC catalog later in the same Spark session.

- `notebooks/gcs_hadoop_catalog_explore.ipynb`
  - Broader exploration notebook for HadoopCatalog Iceberg tables and native Spark file formats: Parquet, CSV, JSON, ORC.
  - Also has an optional Cloud SQL catalog path.

## Start Cloud SQL Only When Needed

For the on-demand Cloud SQL path, first start the Cloud SQL instance:

```bash
gcloud sql instances patch alpaca-iceberg-catalog \
  --project=project-66783f65-9c3e-4880-9a3 \
  --activation-policy=ALWAYS \
  --quiet
```

Wait until it is runnable:

```bash
gcloud sql instances list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --format='table(name,state,settings.activationPolicy)'
```

Start Cloud SQL Auth Proxy in a separate terminal:

```bash
cloud-sql-proxy project-66783f65-9c3e-4880-9a3:us-east1:alpaca-iceberg-catalog --port 5432
```

Then start the notebook:

```bash
UV_CACHE_DIR=/tmp/uv-cache SPARK_LOCAL_IP=127.0.0.1 \
uv run jupyter lab notebooks/hadoop_catalog_with_on_demand_cloudsql.ipynb
```

Inside the notebook, enable the production catalog on demand:

```python
enable_cloudsql_catalog()
show_cloudsql_catalog()
```

Production table identifier:

```text
alpaca_catalog.alpaca.bars
```

Hadoop exploration table identifiers use:

```text
gcs_iceberg.<namespace>.<table>
```

## Stop Cloud SQL After Exploration

When finished, stop the proxy with `Ctrl-C`, then stop Cloud SQL:

```bash
gcloud sql instances patch alpaca-iceberg-catalog \
  --project=project-66783f65-9c3e-4880-9a3 \
  --activation-policy=NEVER \
  --quiet
```

Verify:

```bash
gcloud sql instances list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --format='table(name,state,settings.activationPolicy)'
```

Expected idle state:

```text
alpaca-iceberg-catalog  STOPPED  NEVER
```

## Important Catalog Behavior

The production table was created through the JDBC catalog and is keyed under the catalog name `alpaca_catalog`. The Hadoop catalog can see the GCS directory layout, but it may not load the production `alpaca.bars` table because JDBC-created tables do not necessarily contain HadoopCatalog's `metadata/version-hint.text`.

Use:

```text
alpaca_catalog.alpaca.bars
```

for production Iceberg semantics, and use:

```text
gcs_iceberg.<namespace>.<table>
```

for exploration tables created by the Hadoop catalog.
