data "kafka_broker_config" "broker_1" {
  broker_id = 1
}

# Settings changed at runtime (not in server.properties)
output "dynamic_settings" {
  value = {
    for c in data.kafka_broker_config.broker_1.configs :
    c.name => c.value if startswith(c.source, "dynamic_")
  }
}
