# Grant access to all topics with prefix "logs-"
resource "kafka_acl" "logs_access" {
  resource_name                = "logs-"
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"
  acl_principal                = "User:log-aggregator"
  acl_host                     = "*"
  acl_operation                = "Read"
  acl_permission_type          = "Allow"
}
