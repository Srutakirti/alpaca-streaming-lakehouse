# GitHub Actions to GCP authentication flow

The public dashboard uses GitHub OpenID Connect (OIDC) and Google Cloud
Workload Identity Federation (WIF). It does not store a GCP service-account
key in GitHub.

```mermaid
sequenceDiagram
    participant GH as GitHub Actions workflow
    participant OIDC as GitHub OIDC issuer
    participant WIP as GCP Workload Identity Provider
    participant IAM as GCP IAM
    participant SA as Dashboard service account
    participant GCP as Cloud Logging / GCS

    GH->>OIDC: Request short-lived OIDC token
    OIDC-->>GH: Signed token<br/>repository, branch, workflow claims

    GH->>WIP: Present token
    WIP->>WIP: Validate GitHub signature<br/>and allowed repo, branch, workflow
    WIP->>WIP: Map claims to attributes<br/>attribute.repository, attribute.ref

    WIP->>IAM: Is derived principal in principalSet?
    IAM->>IAM: Check roles/iam.workloadIdentityUser<br/>on dashboard service account
    IAM-->>GH: Allow service-account impersonation

    GH->>SA: Request temporary GCP credentials
    SA-->>GH: Short-lived credentials

    GH->>GCP: Query pipeline logs and Iceberg metadata
    GCP->>GCP: Enforce service-account permissions:<br/>Logging Viewer + conditional GCS Object Viewer
    GCP-->>GH: Allowed dashboard metrics only
```

## What each piece does

1. **GitHub OIDC token**: a signed, short-lived statement that identifies the
   running workflow, including its repository, branch, and workflow file.
2. **Workload Identity Provider**: validates that token and maps claims into
   GCP attributes such as `attribute.repository`.
3. **Federated principal**: a temporary GCP identity derived from those mapped
   attributes. It is not a stored user or service account.
4. **Principal set**: an IAM selector for matching temporary principals. For
   example, this selects tokens from this repository:

   ```text
   principalSet://iam.googleapis.com/POOL_NAME/attribute.repository/Srutakirti/alpaca-streaming-lakehouse
   ```

5. **`roles/iam.workloadIdentityUser`**: granted on the dashboard service
   account to that principal set. It allows matching GitHub workflow identities
   to impersonate the service account.
6. **Service-account roles**: determine the resulting credentials' real data
   access. The dashboard account has `roles/logging.viewer` and a conditional
   `roles/storage.objectViewer` binding limited to the configured Iceberg
   metadata directory.

In short:

```text
GitHub token claims → mapped GCP attributes → temporary principal
→ workloadIdentityUser permission → dashboard service account
→ narrowly scoped log and metadata reads
```
