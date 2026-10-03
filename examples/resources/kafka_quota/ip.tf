# Rate limit connections from a specific IP
resource "kafka_quota" "external_ip" {
  entity_name = "203.0.113.0"
  entity_type = "ip"

  config = {
    "connection_creation_rate" = "10" # Max 10 connections per second
  }
}
