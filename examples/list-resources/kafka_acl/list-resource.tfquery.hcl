# Every ACL in the cluster
list "kafka_acl" "all" {
  provider = kafka
}

# Topic ACLs of one service
list "kafka_acl" "orders_service" {
  provider = kafka

  config {
    acl_principal        = "User:orders-service"
    resource_type        = "Topic"
    resource_name_prefix = "orders."
  }
}
