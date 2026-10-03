# Format: ${acl_principal}|${acl_host}|${acl_operation}|${acl_permission_type}|${resource_type}|${resource_name}|${resource_pattern_type_filter}
terraform import kafka_acl.example 'User:producer|*|Write|Allow|Topic|orders|Literal'
