# Set quotas for a specific user
resource "kafka_quota" "service_account" {
  entity_name = "payment-service"
  entity_type = "user"

  config = {
    "consumer_byte_rate" = "10000000" # 10 MB/s consumer bandwidth
    "producer_byte_rate" = "10000000" # 10 MB/s producer bandwidth
    "request_percentage" = "400"      # 400% of a single CPU
  }
}
