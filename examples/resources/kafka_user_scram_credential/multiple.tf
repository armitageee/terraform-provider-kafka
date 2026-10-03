# Generate random passwords for better security
resource "random_password" "service_passwords" {
  for_each = toset(["order-service", "payment-service", "shipping-service"])

  length  = 32
  special = true
}

# Create SCRAM credentials for each service
resource "kafka_user_scram_credential" "services" {
  for_each = random_password.service_passwords

  username        = each.key
  scram_mechanism = "SCRAM-SHA-256"
  password        = each.value.result
}
