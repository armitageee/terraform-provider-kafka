# Allow read access to topic
resource "kafka_acl" "consumer_read" {
  resource_name       = "orders"
  resource_type       = "Topic"
  acl_principal       = "User:consumer-service"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

# Allow access to consumer group
resource "kafka_acl" "consumer_group" {
  resource_name       = "order-processors"
  resource_type       = "Group"
  acl_principal       = "User:consumer-service"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}
