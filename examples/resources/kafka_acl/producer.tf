resource "kafka_acl" "producer" {
  resource_name       = "orders"
  resource_type       = "Topic"
  acl_principal       = "User:producer-service"
  acl_host            = "*"
  acl_operation       = "Write"
  acl_permission_type = "Allow"
}

# Also grant describe permission for producers
resource "kafka_acl" "producer_describe" {
  resource_name       = "orders"
  resource_type       = "Topic"
  acl_principal       = "User:producer-service"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}
