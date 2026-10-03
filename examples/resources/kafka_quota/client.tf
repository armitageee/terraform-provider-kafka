# Limit a specific client's bandwidth
resource "kafka_quota" "mobile_app" {
  entity_name = "mobile-app-v1"
  entity_type = "client-id"

  config = {
    "consumer_byte_rate" = "5000000" # 5 MB/s consumer bandwidth
    "producer_byte_rate" = "2500000" # 2.5 MB/s producer bandwidth
    "request_percentage" = "200"     # 200% of a single CPU
  }
}
