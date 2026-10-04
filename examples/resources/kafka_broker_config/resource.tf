# Cluster-wide dynamic defaults (no broker_id): apply to every broker
# without a restart and override server.properties.
resource "kafka_broker_config" "cluster" {
  config = {
    "log.retention.ms"    = "604800000" # 7 days
    "message.max.bytes"   = "2097152"
    "min.insync.replicas" = "1"
  }
}

# Settings of one broker; they win over the cluster-wide defaults.
resource "kafka_broker_config" "broker_1" {
  broker_id = 1
  config = {
    "log.cleaner.threads" = "2"
  }
}
