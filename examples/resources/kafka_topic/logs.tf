resource "kafka_topic" "logs" {
  name               = "application-logs"
  replication_factor = 3
  partitions         = 50

  config = {
    "retention.ms"     = "604800000" # 7 days
    "segment.ms"       = "86400000"  # 1 day
    "cleanup.policy"   = "delete"
    "compression.type" = "gzip"
  }
}
