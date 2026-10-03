# Get information about source topic
data "kafka_topic" "source" {
  name = var.source_topic_name
}

# Create mirror topic with same configuration
resource "kafka_topic" "mirror" {
  name               = "${var.source_topic_name}-mirror"
  partitions         = data.kafka_topic.source.partitions
  replication_factor = data.kafka_topic.source.replication_factor
  config             = data.kafka_topic.source.config
}

# Create consumer group ACLs based on topic
resource "kafka_acl" "consumer_group" {
  resource_name       = "${data.kafka_topic.source.name}-consumers"
  resource_type       = "Group"
  acl_principal       = "User:consumer-service"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}
