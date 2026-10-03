data "kafka_topic" "existing" {
  name = "application-events"
}

output "topic_partitions" {
  value = data.kafka_topic.existing.partitions
}

output "topic_replication" {
  value = data.kafka_topic.existing.replication_factor
}
