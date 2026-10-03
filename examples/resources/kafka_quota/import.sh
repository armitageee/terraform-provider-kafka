# Named entity: entity_name|entity_type (the resource ID) or entity_type:entity_name
terraform import kafka_quota.example 'my-client|client-id'
terraform import kafka_quota.example client-id:my-client

# Default quota of a type (no entity name)
terraform import kafka_quota.default_user 'entity-default|user'
terraform import kafka_quota.default_user user:
