# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tansu Kafka - Infrastructure-as-code setup for deploying Tansu (a Kafka-compatible broker) on cloud VMs using Terraform, with a Python test suite for validation.

## Commands

### Infrastructure Setup (GCP)

```bash
# Prerequisites: gcloud auth application-default login

cd terraform/gcp
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project_id

terraform init
terraform apply

# Save SSH key for VM access
terraform output -raw ssh_private_key > ~/.ssh/tansu_vm.pem
chmod 600 ~/.ssh/tansu_vm.pem

# Get broker URL
terraform output broker_url
```

### Test Kafka

```bash
cd python
uv sync
uv run tansu-test --broker <VM_IP>:9092

# Or using terraform output directly
uv run tansu-test --broker $(cd ../terraform/gcp && terraform output -raw broker_url)
```

### SSH to VM

```bash
cd terraform/gcp
$(terraform output -raw ssh_command)
```

### Teardown

```bash
cd terraform/gcp
terraform destroy
```

## Architecture

```
terraform/
├── modules/
│   ├── gcp-vm/           # GCP-specific VM provisioning (e2-micro, firewall, SSH key)
│   └── tansu-install/    # Cloud-agnostic Tansu installation via SSH
└── gcp/                  # GCP environment wiring both modules

python/                   # uv-managed Kafka test suite
```

### Multi-Cloud Design

The `tansu-install` module is cloud-agnostic - it only needs:
- `host`: VM IP address
- `private_key`: SSH private key
- `ssh_user`: SSH username

To add a new cloud provider (e.g., AWS), create a new VM module at `terraform/modules/aws-vm/` that outputs these same values, then create `terraform/aws/` environment to wire them together.

### Key Ports

- 9092: Kafka API
- 9100: Prometheus metrics
- 22: SSH

### Tansu Configuration

Storage engine defaults to `memory://` for testing. Configure via `storage_engine` variable in `tansu-install` module for other backends (postgres://, s3://).

### Notes

- **Python client**: Uses `confluent-kafka` (not `kafka-python`) for better Tansu compatibility
- **Ubuntu version**: Requires Ubuntu 24.04 LTS for glibc 2.39 (Tansu binary requirement)
- **CLI flags**: Tansu configured via CLI flags in systemd service (environment variables not supported)
