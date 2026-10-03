# Create user credentials
resource "kafka_user_scram_credential" "analytics" {
  username        = "analytics-processor"
  scram_mechanism = "SCRAM-SHA-256"
  password        = var.analytics_password
}

# Grant permissions to the user
resource "kafka_acl" "analytics_read" {
  resource_name                = "events-*"
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"
  acl_principal                = "User:${kafka_user_scram_credential.analytics.username}"
  acl_host                     = "*"
  acl_operation                = "Read"
  acl_permission_type          = "Allow"
}
