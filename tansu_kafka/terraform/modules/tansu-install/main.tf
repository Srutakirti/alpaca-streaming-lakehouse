terraform {
  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

resource "null_resource" "tansu_install" {
  triggers = {
    host          = var.host
    tansu_version = var.tansu_version
  }

  connection {
    type        = "ssh"
    user        = var.ssh_user
    private_key = var.private_key
    host        = var.host
    timeout     = "5m"
  }

  # Install Tansu and create systemd service
  provisioner "remote-exec" {
    inline = [
      "echo 'Waiting for cloud-init to complete...'",
      "cloud-init status --wait || true",

      "echo 'Downloading Tansu ${var.tansu_version}...'",
      "curl -LO https://github.com/tansu-io/tansu/releases/download/${var.tansu_version}/tansu-x86_64-unknown-linux-gnu.tar.gz",

      "echo 'Extracting Tansu...'",
      "tar -xzf tansu-x86_64-unknown-linux-gnu.tar.gz",

      "echo 'Installing Tansu binary...'",
      "sudo mv ./bin/tansu /usr/local/bin/tansu",
      "sudo chmod +x /usr/local/bin/tansu",

      "echo 'Cleaning up...'",
      "rm -rf tansu-x86_64-unknown-linux-gnu.tar.gz bin etc LICENSE README.md THIRD-PARTY-LICENSE.html compose.yaml example.env",

      "echo 'Verifying installation...'",
      "/usr/local/bin/tansu --version",

      "echo 'Creating systemd service file...'",
      "echo '[Unit]' | sudo tee /etc/systemd/system/tansu.service",
      "echo 'Description=Tansu Kafka-compatible Broker' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo 'After=network.target' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo '' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo '[Service]' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo 'Type=simple' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo 'User=root' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo 'ExecStart=/usr/local/bin/tansu broker --storage-engine ${var.storage_engine} --kafka-listener-url ${var.kafka_listener_url} --kafka-advertised-listener-url tcp://${var.host}:9092' | sudo tee -a /etc/systemd/system/tansu.service",
      var.s3_access_key_id != "" ? "echo 'Environment=AWS_ACCESS_KEY_ID=${var.s3_access_key_id}' | sudo tee -a /etc/systemd/system/tansu.service" : "true",
      var.s3_secret_access_key != "" ? "echo 'Environment=AWS_SECRET_ACCESS_KEY=${var.s3_secret_access_key}' | sudo tee -a /etc/systemd/system/tansu.service" : "true",
      var.s3_access_key_id != "" ? "echo 'Environment=AWS_ENDPOINT_URL=${var.s3_endpoint_url}' | sudo tee -a /etc/systemd/system/tansu.service" : "true",
      "echo 'Restart=always' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo 'RestartSec=5' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo '' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo '[Install]' | sudo tee -a /etc/systemd/system/tansu.service",
      "echo 'WantedBy=multi-user.target' | sudo tee -a /etc/systemd/system/tansu.service",

      "echo 'Enabling and starting Tansu service...'",
      "sudo systemctl daemon-reload",
      "sudo systemctl enable tansu",
      "sudo systemctl start tansu",

      "echo 'Waiting for Tansu to start...'",
      "sleep 5",

      "echo 'Checking Tansu status...'",
      "sudo systemctl status tansu --no-pager || true",
    ]
  }
}
