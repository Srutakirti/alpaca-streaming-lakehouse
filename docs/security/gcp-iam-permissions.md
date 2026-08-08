# GCP IAM, Service Accounts, and Access Controls

This document records how identities and permissions are configured for the Alpaca
pipeline. It combines Terraform intent with a read-only inspection of the live GCP
project performed on 2026-08-08.

## Scope

```text
project: project-66783f65-9c3e-4880-9a3
region:  us-east1
zone:    us-east1-b
```

This is an operational snapshot, not a replacement for the live IAM policy. Re-run
the audit commands near the end of this document after changing IAM, service
accounts, Cloud Run, Scheduler, buckets, or organization policies.

## Identity Flow

```text
Cloud Scheduler
    -> OAuth token for alpaca-scheduler
    -> Cloud Run Jobs API

alpaca-extractor Cloud Run Job
    -> alpaca-extractor service account
    -> Secret Manager and Cloud Logging
    -> Tansu Kafka endpoint

alpaca-loader Cloud Run Service
    -> alpaca-loader service account
    -> Cloud SQL, GCS warehouse, and Cloud Logging

alpaca-frontend Cloud Run Service
    -> alpaca-frontend service account
    -> Cloud SQL, read-only GCS warehouse, and Cloud Logging reads

Local Spark notebooks
    -> alpaca-spark-gcs-reader service-account key
    -> GCS warehouse
```

Each production workload has a dedicated service account. The retired probe reuses
the extractor account. Infrastructure scheduling and infrastructure mutation
currently share the `alpaca-scheduler` account.

## Workload Service Accounts

### Extractor

```text
alpaca-extractor@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

Used by:

- `alpaca-extractor` Cloud Run Job.
- `alpaca-probe` Cloud Run Job, which is retained but paused.

Project roles:

| Role | Purpose |
| --- | --- |
| `roles/logging.logWriter` | Writes extractor and probe logs. |
| `roles/secretmanager.secretAccessor` | Reads Alpaca credentials. This is currently project-wide. |

The extractor receives `ALPACA_KEY` and `ALPACA_SECRET` as environment variables
whose values are sourced from Secret Manager. The Rust process does not use a GCP
SDK directly.

Terraform source:

```text
terraform/modules/extractor-job/main.tf
```

### Loader

```text
alpaca-loader@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

Used by `alpaca-loader`, a Cloud Run Service.

| Scope | Role | Purpose |
| --- | --- | --- |
| Project | `roles/cloudsql.client` | Opens the Cloud SQL connection through the mounted Cloud SQL socket. |
| Project | `roles/logging.logWriter` | Writes loader logs and metrics. |
| Warehouse bucket | `roles/storage.objectAdmin` | Reads, creates, updates, and deletes Iceberg data and metadata objects. |

`roles/cloudsql.client` permits the connection but does not replace PostgreSQL
authentication. The loader also authenticates as the `iceberg` PostgreSQL user.

Terraform source:

```text
terraform/modules/loader-service/main.tf
```

### Frontend

```text
alpaca-frontend@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

Used by `alpaca-frontend`, a Cloud Run Service.

| Scope | Role | Purpose |
| --- | --- | --- |
| Project | `roles/cloudsql.client` | Connects to the production Iceberg JDBC catalog. |
| Project | `roles/logging.logWriter` | Writes application logs. |
| Project | `roles/logging.viewer` | Reads extractor and loader logs for monitoring. |
| Warehouse bucket | `roles/storage.objectViewer` | Reads Iceberg data and metadata without write access. |

Terraform source:

```text
terraform/modules/frontend-service/main.tf
```

### Scheduler and Infrastructure Controller

```text
alpaca-scheduler@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

Used by:

- Cloud Scheduler OAuth targets.
- `alpaca-infra-start` Cloud Run Job.
- `alpaca-infra-stop` Cloud Run Job.

Live project roles:

| Role | Purpose |
| --- | --- |
| `roles/run.invoker` | Invokes Cloud Run jobs. |
| `roles/run.admin` | Changes loader scaling and controls Cloud Run resources. |
| `roles/cloudsql.admin` | Starts and stops the Cloud SQL instance. |
| `roles/compute.instanceAdmin.v1` | Starts and stops the Tansu VM. |
| `roles/artifactregistry.reader` | Pulls the control-job container image. |

Terraform currently creates this account and grants `roles/run.invoker`. The
additional control roles and the infra start/stop jobs were configured directly
with `gcloud` and are not yet fully represented in Terraform.

The account has a broad blast radius because it is both the scheduler caller and
the execution identity for the infrastructure control jobs. A future hardening
change should separate these responsibilities.

Terraform source:

```text
terraform/modules/scheduler/main.tf
```

## Local Spark Service Account

```text
alpaca-spark-gcs-reader@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

Despite the `reader` name, this is a reader/writer identity for exploratory Spark
workloads.

| Scope | Role | Purpose |
| --- | --- | --- |
| Warehouse bucket | `roles/storage.objectAdmin` | Reads and writes Iceberg and native Spark files. |
| Warehouse bucket | `roles/storage.legacyBucketReader` | Supplies bucket metadata access such as `storage.buckets.get`. |

The live account has two user-managed keys. Keys are long-lived credentials and
must not be stored in the repository. The notebook helper expects a temporary key
at:

```text
/tmp/alpaca-spark-gcs-reader.json
```

Detailed setup is in:

```text
docs/runbooks/local-spark-gcs-iceberg-notebook-setup.md
```

## Default and Unused Accounts

### Compute Engine Default Service Account

```text
343048392486-compute@developer.gserviceaccount.com
```

The live project policy contains several broad or legacy grants for this account,
including Logging Writer, Pub/Sub Publisher, Pub/Sub Subscriber, Secret Manager
Secret Accessor, Storage Admin, and Storage Object Creator. Terraform also grants
it Artifact Registry writer access at the `alpaca-datalake` repository.

The current Tansu VM has no service account attached, so these permissions are not
available from that VM. The account may still be used by Cloud Build or other
default-compute workloads and should be audited before removing any roles.

### Test Account

```text
test-sa1@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

No pipeline workload or relevant project role was found for this account. Remove
it after confirming it is not used by an external experiment.

### Google-Managed Service Agents

The project also contains Google-managed service agents for Cloud Run, Cloud
Scheduler, Cloud Build, Artifact Registry, Compute Engine, Logging, Pub/Sub, and
other enabled APIs. These agents let the corresponding Google service operate and
should not be treated like application service accounts.

## Human Administration

The primary administrative user is:

```text
cloudsrutakirti@proton.me
```

Live project-level roles include Owner and Organization Administrator. Live
organization-level roles include Billing Admin, Billing Creator, Organization
Policy Administrator, Organization Administrator, Project Creator, Project Mover,
Service Usage Admin, and Workforce Pool Admin.

These permissions are appropriate only for an organization administrator. Routine
pipeline operation should use narrower service identities instead of personal
owner credentials.

## Secret and Database Authentication

Secret Manager contains:

| Secret | Consumer | Current access path |
| --- | --- | --- |
| `ALPACA_KEY` | Extractor | Secret Manager reference injected into an environment variable. |
| `ALPACA_SECRET` | Extractor | Secret Manager reference injected into an environment variable. |
| `ICEBERG_DB_PASSWORD` | Operational record | Terraform creates it, but runtime services currently receive the password inside `ICEBERG_CATALOG_URI`. |

The Cloud SQL catalog URI includes the PostgreSQL password and is stored in
Terraform state and the Cloud Run service configuration. Marking a Terraform
output as sensitive only hides normal CLI display; it does not remove the value
from state.

A future change should construct the database URI at startup from a Secret Manager
reference, or move to Cloud SQL IAM database authentication where practical.

## Cloud Run Invocation and Ingress

Live state for both `alpaca-loader` and `alpaca-frontend`:

```text
ingress: internal
allUsers roles/run.invoker binding: absent
explicit service-level IAM bindings: absent
```

The services therefore are not anonymously reachable from the public internet.
The generated `run.app` URL does not imply public access. A caller still needs an
allowed network path and applicable IAM authorization.

Terraform defaults match this state:

- Frontend `allow_unauthenticated` defaults to `false`.
- Frontend ingress defaults to `INGRESS_TRAFFIC_INTERNAL_ONLY`.
- Loader ingress defaults to `INGRESS_TRAFFIC_INTERNAL_ONLY`.

## Scheduler State

All live Scheduler jobs were `PAUSED` during the 2026-08-08 inspection, including:

- `alpaca-infra-start`
- `alpaca-extractor-start`
- `alpaca-infra-stop`
- Retired probe schedules

This is intentional cost control. Keep the jobs paused until the next planned
pipeline run. Pausing does not remove Scheduler IAM permissions or delete the
target Cloud Run jobs.

When schedules are resumed, the intended weekday flow is:

```text
09:05 America/New_York -> alpaca-infra-start
09:30 America/New_York -> alpaca-extractor-start
17:20 America/New_York -> alpaca-infra-stop
```

## Tansu VM Identity and Network Controls

The `tansu-broker` VM:

- Has no GCP service account attached.
- Uses the default VPC and internal IP `10.142.0.4`.
- Has static external IP `34.138.155.73`.
- Is selected by the `tansu-server` network tag.

Live ingress firewall rules currently permit:

| Port | Purpose | Source |
| --- | --- | --- |
| `22/tcp` | SSH | `0.0.0.0/0` |
| `9092/tcp` | Kafka | `0.0.0.0/0` |
| `9100/tcp` | Prometheus metrics | `0.0.0.0/0` |

The VM cannot call GCP APIs using an attached workload identity, but the open
firewall rules remain the largest network exposure in the current architecture.
Kafka and metrics should eventually use private networking or restricted source
ranges. SSH should be restricted to a trusted source or replaced with IAP-based
access.

## Organization Policy and Service-Account Keys

Effective policy during the live inspection:

```yaml
iam.disableServiceAccountKeyCreation:
  enforce: false
```

Key creation was enabled to support the local Spark notebook. This allows key
creation project-wide and should not remain the long-term default. After replacing
the notebook key with Application Default Credentials, service-account
impersonation, or Workload Identity Federation, restore key-creation enforcement.

## Recommended Hardening Backlog

Prioritized recommendations:

1. Restrict Tansu SSH, Kafka, and Prometheus firewall source ranges.
2. Split `alpaca-scheduler` into a narrow Scheduler invoker identity and a separate infrastructure controller identity.
3. Replace broad predefined control roles with resource-level bindings or a narrowly scoped custom role.
4. Restrict extractor secret access to only `ALPACA_KEY` and `ALPACA_SECRET`.
5. Remove redundant Spark user-managed keys and rotate the retained credential.
6. Replace the Spark JSON key with ADC, service-account impersonation, or Workload Identity Federation.
7. Re-enable `iam.disableServiceAccountKeyCreation` after keyless notebook authentication is available.
8. Move the Cloud SQL password out of `ICEBERG_CATALOG_URI` and inject it from Secret Manager.
9. Audit and reduce the default Compute service account's legacy project roles.
10. Remove `test-sa1` if it is confirmed unused.
11. Codify the infra start/stop jobs and their IAM bindings in Terraform.

## Audit Commands

List application service accounts:

```bash
gcloud iam service-accounts list \
  --project=project-66783f65-9c3e-4880-9a3
```

Map service accounts to project roles:

```bash
gcloud projects get-iam-policy project-66783f65-9c3e-4880-9a3 \
  --flatten='bindings[].members' \
  --filter='bindings.members:serviceAccount' \
  --format='table(bindings.members,bindings.role)'
```

Inspect warehouse bucket IAM:

```bash
gcloud storage buckets get-iam-policy \
  gs://project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse
```

Inspect Cloud Run runtime identities and ingress:

```bash
gcloud run services describe alpaca-loader \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1

gcloud run services describe alpaca-frontend \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1
```

Inspect scheduler state and OAuth identities:

```bash
gcloud scheduler jobs list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --location=us-east1 \
  --format='table(name,httpTarget.oauthToken.serviceAccountEmail,state)'
```

Inspect user-managed Spark keys without displaying private key material:

```bash
gcloud iam service-accounts keys list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --iam-account=alpaca-spark-gcs-reader@project-66783f65-9c3e-4880-9a3.iam.gserviceaccount.com
```

Inspect the effective key-creation policy:

```bash
gcloud org-policies describe \
  constraints/iam.disableServiceAccountKeyCreation \
  --project=project-66783f65-9c3e-4880-9a3 \
  --effective \
  --format=yaml
```
