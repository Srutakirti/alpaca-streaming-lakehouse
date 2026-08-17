# Phase 2 infrastructure

This Terraform root creates only the isolated GCE foundation:

- one e2-micro Ubuntu VM, reachable only through IAP SSH;
- a versioned GCS warehouse bucket for the HadoopCatalog;
- a versioned GCS release bucket for native runtime archives;
- a dedicated VM service account with write access only to the warehouse and read access only to releases.

It creates no Cloud SQL, Cloud Run, UI, scheduler, or change to existing resources. The VM bootstrap installs operating-system runtime dependencies only: Java 17, OpenSSL/zlib, `flock`, CA certificates, and curl. It does not install Docker, Maven, Cargo, UV, or Spark.

## Plan

Use unique bucket names and explicitly inspect the plan before applying it:

```bash
terraform -chdir=terraform init
terraform -chdir=terraform plan \
  -var='project_id=YOUR_PROJECT_ID' \
  -var='warehouse_bucket_name=YOUR_UNIQUE_WAREHOUSE_BUCKET' \
  -var='release_bucket_name=YOUR_UNIQUE_RELEASE_BUCKET'
```

The resulting `warehouse_uri` is written to `/etc/gce-hadoop-catalog/runtime.env` by the VM startup script. Native release installation and service enablement are deliberately separate acceptance-test actions.
