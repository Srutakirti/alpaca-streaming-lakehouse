# End-to-End Architecture

The production path has one writer to the raw Iceberg table. Market frames flow through the
VM, while a separate read-only GitHub Actions path publishes sanitized operational metrics.

```mermaid
flowchart LR
    Alpaca[Alpaca market-data<br/>WebSocket]
    Browser[Dashboard visitor]

    subgraph GCP[Google Cloud]
        subgraph VM[GCE e2-micro VM]
            Timer[systemd market timer]
            Extractor[Rust WebSocket<br/>extractor]
            Synthetic[Java synthetic<br/>publisher]
            Tansu[Tansu Kafka API<br/>SQLite storage]
            Loader[Java Iceberg loader<br/>bounded batches]
            Agent[Google Cloud<br/>Ops Agent]
        end

        Warehouse[(GCS warehouse<br/>Parquet + HadoopCatalog metadata)]
        Logs[(Cloud Logging)]
        Costs[(BigQuery cost snapshot)]
        VMIdentity[VM service account<br/>warehouse writer]
        DashboardIdentity[Dashboard service account<br/>read only]
        WIF[Workload Identity<br/>Federation]
    end

    subgraph GitHub[GitHub]
        Trigger[Schedule or<br/>manual dispatch]
        Workflow[Dashboard metrics<br/>Actions workflow]
        Exporter[Python metrics<br/>exporter]
        Metrics[Sanitized<br/>metrics.json]
        Vite[Vite static build]
        Pages[GitHub Pages]
        OIDC[Short-lived<br/>OIDC token]
    end

    Terraform[Terraform]

    Timer --> Extractor
    Alpaca -->|JSON bar frames| Extractor
    Extractor -->|Kafka messages| Tansu
    Synthetic -.->|acceptance data| Tansu
    Tansu -->|manual-offset consumer| Loader
    Loader -->|commit Parquet and metadata| Warehouse
    Loader -->|commit offsets after Iceberg| Tansu
    VMIdentity -.->|authorizes writes| Loader
    Extractor --> Agent
    Loader --> Agent
    Tansu --> Agent
    Agent --> Logs

    Trigger --> Workflow
    Workflow --> OIDC --> WIF --> DashboardIdentity
    Workflow --> Exporter
    DashboardIdentity -.->|read logs| Logs
    DashboardIdentity -.->|read selected metadata| Warehouse
    DashboardIdentity -.->|read aggregate costs| Costs
    Logs --> Exporter
    Warehouse --> Exporter
    Costs --> Exporter
    Exporter --> Metrics --> Vite --> Pages
    Browser -->|HTTPS| Pages
    Browser -->|fetch static JSON| Metrics

    Terraform -.-> VM
    Terraform -.-> Warehouse
    Terraform -.-> VMIdentity
    Terraform -.-> WIF
    Terraform -.-> DashboardIdentity
```

## Runtime Guarantees and Boundaries

- The extractor preserves each Alpaca WebSocket frame as one Kafka message.
- The loader writes bounded batches directly with Iceberg Java APIs; Spark is not in the write
  path. Kafka offsets advance only after the associated Iceberg commit succeeds.
- HadoopCatalog has one writer on the VM. `flock` and systemd prevent concurrent loader
  processes; `payload_hash` makes at-least-once replays identifiable downstream.
- The VM service account can write the warehouse. The dashboard identity is short-lived and
  can read only Cloud Logging, selected Iceberg metadata, and aggregate cost data.
- GitHub Pages is static. Browsers receive the Vite bundle and sanitized `metrics.json`; they
  never connect to GCP or receive cloud credentials.

For operating details, see the [VM services](runbooks/vm-services.md), [public dashboard](runbooks/public-dashboard.md), and [WIF authentication](github-actions-wif-auth-flow.md) guides.
