resource "kafka_topic" "metrics" {
  name               = "system-metrics"
  replication_factor = 2
  partitions         = 200

  config = {
    "retention.ms"                   = "86400000" # 1 day
    "segment.ms"                     = "3600000"  # 1 hour
    "compression.type"               = "lz4"
    "max.message.bytes"              = "1048576" # 1MB
    "min.insync.replicas"            = "2"
    "unclean.leader.election.enable" = "false"
  }
}
