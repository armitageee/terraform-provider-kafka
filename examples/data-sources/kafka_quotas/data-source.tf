data "kafka_quotas" "users" {
  entity_type = "user"
}

# producer_byte_rate per user, "default" for the default user quota
output "producer_byte_rates" {
  value = {
    for q in data.kafka_quotas.users.quotas :
    (q.default ? "default" : q.entity_name) => lookup(q.config, "producer_byte_rate", null)
  }
}
