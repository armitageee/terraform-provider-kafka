resource "kafka_user_scram_credential" "producer" {
  username        = "producer-service"
  scram_mechanism = "SCRAM-SHA-256"
  password        = var.producer_password
}
