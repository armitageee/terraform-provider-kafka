# Grant cluster-level admin access
resource "kafka_acl" "admin_cluster" {
  resource_name       = "kafka-cluster"
  resource_type       = "Cluster"
  acl_principal       = "User:admin"
  acl_host            = "*"
  acl_operation       = "All"
  acl_permission_type = "Allow"
}
