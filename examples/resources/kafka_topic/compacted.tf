resource "kafka_topic" "events" {
  name               = "user-events"
  replication_factor = 3
  partitions         = 100

  config = {
    "cleanup.policy"        = "compact"
    "retention.ms"          = "-1"       # Keep forever
    "min.compaction.lag.ms" = "3600000"  # 1 hour
    "delete.retention.ms"   = "86400000" # 1 day tombstone retention
    "compression.type"      = "lz4"
    "segment.bytes"         = "1073741824" # 1GB segments
  }
}
