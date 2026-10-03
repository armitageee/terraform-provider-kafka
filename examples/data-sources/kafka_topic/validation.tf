data "kafka_topic" "production_topic" {
  name = "orders"
}

# Validate topic meets production standards
locals {
  topic_validation = {
    has_enough_replicas       = data.kafka_topic.production_topic.replication_factor >= 3
    has_min_insync_replicas   = lookup(data.kafka_topic.production_topic.config, "min.insync.replicas", "1") >= "2"
    has_appropriate_retention = lookup(data.kafka_topic.production_topic.config, "retention.ms", "0") >= "604800000" # 7 days
  }
}

output "topic_compliance" {
  value = alltrue(values(local.topic_validation)) ? "COMPLIANT" : "NON-COMPLIANT"
}
