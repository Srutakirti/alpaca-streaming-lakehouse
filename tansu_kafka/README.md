# Tansu Kafka on GCP

Infrastructure-as-code setup for deploying [Tansu](https://github.com/tansu-io/tansu) (a Kafka-compatible broker written in Rust) on cloud VMs using Terraform.

## Quick Start

```bash
# 1. Authenticate with GCP
gcloud auth application-default login

# 2. Configure and deploy
cd terraform/gcp
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project_id

terraform init
terraform apply

# 3. Save SSH key
terraform output -raw ssh_private_key > ~/.ssh/tansu_vm.pem
chmod 600 ~/.ssh/tansu_vm.pem

# 4. Test the broker
cd ../../python
uv sync
uv run tansu-test --broker $(cd ../terraform/gcp && terraform output -raw broker_url)

# 5. Teardown when done
cd ../terraform/gcp
terraform destroy
```

## Repository Structure

```
tansu_kafka/
├── terraform/
│   ├── modules/
│   │   ├── gcp-vm/              # GCP-specific VM provisioning
│   │   │   ├── main.tf          # VM, firewall rules, SSH key generation
│   │   │   ├── variables.tf     # project_id, region, zone, machine_type
│   │   │   └── outputs.tf       # public_ip, private_key, ssh_user
│   │   │
│   │   └── tansu-install/       # Cloud-agnostic Tansu installation
│   │       ├── main.tf          # Downloads binary, creates systemd service
│   │       ├── variables.tf     # host, private_key, ssh_user, tansu_version
│   │       └── outputs.tf       # broker_url, prometheus_url
│   │
│   └── gcp/                     # GCP environment
│       ├── main.tf              # Wires both modules together
│       ├── variables.tf         # project_id, region, zone
│       ├── outputs.tf           # broker_url, ssh_command, etc.
│       └── terraform.tfvars.example
│
├── python/                      # Test suite
│   ├── pyproject.toml           # uv project with confluent-kafka
│   └── src/tansu_test/
│       ├── __init__.py
│       └── main.py              # Produce/consume test
│
├── CLAUDE.md                    # Claude Code guidance
└── README.md                    # This file
```

## Architecture

### Multi-Cloud Design

The setup is modular to support multiple cloud providers:

```
┌─────────────────┐     ┌──────────────────┐
│   gcp-vm        │     │  tansu-install   │
│   module        │────▶│  module          │
│                 │     │  (cloud-agnostic)│
│ Outputs:        │     │                  │
│ - public_ip     │     │ Inputs:          │
│ - private_key   │     │ - host           │
│ - ssh_user      │     │ - private_key    │
└─────────────────┘     │ - ssh_user       │
                        └──────────────────┘
```

To add AWS/Azure support:
1. Create `terraform/modules/aws-vm/` with same outputs interface
2. Create `terraform/aws/` environment wiring the modules

### Ports

| Port | Service | Description |
|------|---------|-------------|
| 22   | SSH     | Remote access |
| 9092 | Kafka   | Broker API |
| 9100 | Prometheus | Metrics |

## Key Configuration

### Terraform Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `project_id` | - | GCP project ID (required) |
| `region` | us-central1 | GCP region |
| `zone` | us-central1-a | GCP zone |
| `machine_type` | e2-micro | VM type (cheapest) |
| `tansu_version` | v0.6.0 | Tansu release version |
| `storage_engine` | memory:// | Storage backend |

### Tansu Storage Engines

- `memory://` - In-memory (default, for testing)
- `postgres://user:pass@host/db` - PostgreSQL
- `s3://bucket/` - S3-compatible storage

## Important Notes

### Ubuntu Version Requirement

**Tansu requires Ubuntu 24.04 LTS** (Noble Numbat) because the binary needs glibc 2.39+. Ubuntu 22.04 LTS only has glibc 2.35 and will fail with:

```
/usr/local/bin/tansu: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found
```

### CLI Flags vs Environment Variables

Tansu does **not** read configuration from environment variables. The systemd service must use CLI flags:

```ini
# Correct
ExecStart=/usr/local/bin/tansu broker --storage-engine memory:// --kafka-advertised-listener-url tcp://1.2.3.4:9092

# Wrong (won't work)
Environment=STORAGE_ENGINE=memory://
ExecStart=/usr/local/bin/tansu broker
```

### Python Client Compatibility

Use **confluent-kafka** (not kafka-python). The kafka-python library has compatibility issues with Tansu's Kafka protocol implementation:

```python
# Works
from confluent_kafka import Producer, Consumer

# Doesn't work reliably with Tansu
from kafka import KafkaProducer, KafkaConsumer
```

## Common Operations

### SSH to VM

```bash
cd terraform/gcp
$(terraform output -raw ssh_command)
# or
ssh -i ~/.ssh/tansu_vm.pem tansu@$(terraform output -raw vm_ip)
```

### Check Tansu Status

```bash
ssh -i ~/.ssh/tansu_vm.pem tansu@<VM_IP> "sudo systemctl status tansu"
```

### View Tansu Logs

```bash
ssh -i ~/.ssh/tansu_vm.pem tansu@<VM_IP> "sudo journalctl -u tansu -f"
```

### List Topics

```bash
ssh -i ~/.ssh/tansu_vm.pem tansu@<VM_IP> "/usr/local/bin/tansu topic list"
```

### Test with kcat (on VM)

```bash
# Produce
echo "Hello" | kcat -P -b localhost:9092 -t my-topic

# Consume
kcat -C -b localhost:9092 -t my-topic -e -o beginning
```

## Costs

- **e2-micro**: ~$0.0076/hour (~$5.50/month)
- **Free tier**: 1 e2-micro instance per month in us-central1
- **Storage**: 10GB standard persistent disk included

## Troubleshooting

### "Broker not available" from local machine

1. Check firewall rule exists: `gcloud compute firewall-rules list | grep tansu`
2. Verify VM is running: `gcloud compute instances list`
3. Check Tansu service: `ssh ... "sudo systemctl status tansu"`

### "Failed to update metadata" with kafka-python

Switch to confluent-kafka (see Python Client Compatibility above).

### Tansu crashes on startup

Check logs for configuration issues:
```bash
ssh ... "sudo journalctl -u tansu -n 50"
```

Common causes:
- Invalid storage engine URL
- Port already in use
- Missing CLI flags
