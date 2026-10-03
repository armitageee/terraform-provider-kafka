provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
  sasl_mechanism    = "scram-sha256"
  sasl_username     = "terraform"
  sasl_password     = var.kafka_password
  tls_enabled       = true
}
