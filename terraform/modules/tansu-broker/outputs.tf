output "broker_ip" {
  value = module.vm.public_ip
}

output "broker_url" {
  value = "${module.vm.public_ip}:9092"
}
