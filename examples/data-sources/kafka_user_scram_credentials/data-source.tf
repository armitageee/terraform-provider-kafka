data "kafka_user_scram_credentials" "all" {}

# Users that still use SCRAM-SHA-256
output "sha256_users" {
  value = [
    for c in data.kafka_user_scram_credentials.all.credentials :
    c.username if c.scram_mechanism == "SCRAM-SHA-256"
  ]
}
