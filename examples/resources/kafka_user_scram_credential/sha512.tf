resource "kafka_user_scram_credential" "admin" {
  username         = "kafka-admin"
  scram_mechanism  = "SCRAM-SHA-512"
  scram_iterations = 8192
  password         = var.admin_password
}
