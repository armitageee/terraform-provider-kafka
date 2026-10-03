resource "kafka_topic" "confluent" {
  name               = "confluent"
  replication_factor = -1
  partitions         = 10

  config = {
    "confluent.placement.constraints" = "{\"version\": 1, \"replicas\":[{\"count\": 1, \"constraints\": {\"rack\": \"rack-1\"}}], \"observers\":[{\"count\": 1, \"constraints\": {\"rack\": \"rack-2\"}}]}"
  }
}
