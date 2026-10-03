# For named entities
terraform import kafka_quota.example client-id:my-client

# For default quotas (no entity name)
terraform import kafka_quota.default_user user:
