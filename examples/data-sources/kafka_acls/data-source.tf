# All ACLs of one principal
data "kafka_acls" "orders_service" {
  acl_principal = "User:orders-service"
}

# Resource IDs of existing ACLs, for import blocks
output "acl_import_ids" {
  value = [for a in data.kafka_acls.orders_service.acls : a.id]
}
