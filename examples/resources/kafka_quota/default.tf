# Set default quotas for all users (when entity_name is omitted)
resource "kafka_quota" "default_user" {
  entity_type = "user"

  config = {
    "consumer_byte_rate" = "2000000" # 2 MB/s default consumer bandwidth
    "producer_byte_rate" = "1000000" # 1 MB/s default producer bandwidth
    "request_percentage" = "100"     # 100% of a single CPU
  }
}
