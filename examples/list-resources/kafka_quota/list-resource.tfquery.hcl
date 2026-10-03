# Every single-entity quota, including default ones
list "kafka_quota" "all" {
  provider = kafka
}

# User quotas of the orders team
list "kafka_quota" "orders_users" {
  provider = kafka

  config {
    entity_type        = "user"
    entity_name_prefix = "orders-"
  }
}
