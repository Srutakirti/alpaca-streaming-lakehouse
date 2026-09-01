# Phase 2 infrastructure

This Terraform root creates only the isolated GCE foundation:

- one e2-micro Ubuntu VM, reachable only through IAP SSH;
- a versioned GCS warehouse bucket for the HadoopCatalog;
- a versioned GCS release bucket for native runtime archives;
- a dedicated VM service account with write access only to the warehouse, read access only to releases, and permission to export observability data.

It creates no Cloud SQL, Cloud Run, UI, scheduler, or change to existing resources. The VM bootstrap installs operating-system runtime dependencies only: Java 17, OpenSSL/zlib, `flock`, CA certificates, and curl. It does not install Docker, Maven, Cargo, UV, or Spark.

## Plan

Use unique bucket names and explicitly inspect the plan before applying it:

```bash
terraform -chdir=terraform init
terraform -chdir=terraform plan \
  -var='project_id=YOUR_PROJECT_ID' \
  -var='resource_prefix=YOUR_EXISTING_OR_NEW_PREFIX' \
  -var='warehouse_bucket_name=YOUR_UNIQUE_WAREHOUSE_BUCKET' \
  -var='release_bucket_name=YOUR_UNIQUE_RELEASE_BUCKET'
```

The resulting `warehouse_uri` is written to `/etc/gce-hadoop-catalog/runtime.env` by the VM startup script. Native release installation, Ops Agent configuration, and service enablement are deliberately separate acceptance-test actions.

## Public dashboard identity

This Terraform root also creates the read-only identity for the public GitHub
Pages dashboard. It creates a dedicated service account with only
`roles/logging.viewer`, a GitHub Actions Workload Identity Pool/provider, and
an impersonation binding. The provider accepts only OIDC tokens from:

- `Srutakirti/alpaca-streaming-lakehouse`;
- the `architecture/gce-hadoop-catalog` default branch; and
- `.github/workflows/dashboard-metrics.yml` on that branch.

No service-account key is created. The workflow exchanges its GitHub OIDC token
for short-lived, read-only GCP credentials. The variables can be overridden if
the repository, default branch, or workflow filename changes.

The dashboard identity also receives one conditional `roles/storage.objectViewer`
binding on the warehouse bucket. The condition permits reads only below
`warehouse/alpaca_candidate/bars_direct/metadata/`, so the exporter can read
`version-hint.text` and the current metadata JSON but cannot read table data
files, arbitrary manifests, or write any object.

For an existing deployment, use its original `resource_prefix` exactly. A
different prefix describes a different VM, network, service account, and
buckets; Terraform will plan replacement resources rather than update the
existing stack.
