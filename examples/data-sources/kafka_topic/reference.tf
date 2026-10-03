data "kafka_topic" "logs" {
  name = "system-logs"
}

# Use the configuration in other resources
resource "kafka_topic" "logs_backup" {
  name               = "${data.kafka_topic.logs.name}-backup"
  partitions         = data.kafka_topic.logs.partitions
  replication_factor = data.kafka_topic.logs.replication_factor

  # Copy configuration from existing topic
  config = data.kafka_topic.logs.config
}
