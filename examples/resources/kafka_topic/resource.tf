resource "kafka_topic" "example" {
  name               = "example-topic"
  replication_factor = 3
  partitions         = 10
}
