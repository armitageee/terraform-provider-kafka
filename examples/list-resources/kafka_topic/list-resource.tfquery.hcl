# Every non-internal topic in the cluster
list "kafka_topic" "all" {
  provider = kafka
}

# Only the orders domain, with current settings in the output
list "kafka_topic" "orders" {
  provider         = kafka
  include_resource = true

  config {
    name_prefix = "orders."
  }
}
