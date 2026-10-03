---
page_title: "Import existing topics and ACLs with terraform query"
subcategory: ""
description: |-
  Discover topics and ACLs that already exist in a cluster with the kafka_topic and kafka_acl list resources and generate import blocks and configuration for them.
---

# Import existing topics and ACLs with `terraform query`

Clusters usually have topics and ACLs that were created by hand, by applications
or by another tool. The `kafka_topic` and `kafka_acl` **list resources** find them
and let Terraform write the `import` blocks and resource configuration for you.

Requires **Terraform 1.14 or later** (`terraform query`) and provider `0.15.0`
or later (`kafka_acl`: `0.16.0` or later).
OpenTofu does not implement `query` yet.

## 1. Describe what to find

Create a file ending in `.tfquery.hcl` next to your configuration:

```terraform
# topics.tfquery.hcl

# Every non-internal topic in the cluster
list "kafka_topic" "all" {
  provider = kafka
}

# Only the orders domain, with current settings in the output
list "kafka_topic" "orders" {
  provider         = kafka
  include_resource = true

  config {
    name_prefix = "orders."
  }
}
```

### Filter arguments (`config` block)

- `name_prefix` (String) Only topics whose name starts with this prefix.
- `name_regex` (String) Only topics whose name matches this RE2 regular expression.
- `include_internal` (Boolean) Include internal topics whose name starts with `__` (for example `__consumer_offsets`). Default `false`.

Filters combine with AND. Results are sorted by name, so generated configuration is stable between runs.

## 2. List

```shell
terraform query
```

```
list.kafka_topic.all      name=orders.v1   orders.v1
list.kafka_topic.all      name=orders.v2   orders.v2
list.kafka_topic.all      name=payments    payments

list.kafka_topic.orders   name=orders.v1   orders.v1
list.kafka_topic.orders   name=orders.v2   orders.v2
```

## 3. Generate configuration and import

```shell
terraform query -generate-config-out=generated.tf
```

For every topic Terraform writes a `kafka_topic` resource with its real
partitions, replication factor and non-default configuration, plus an
`import` block addressed by the topic's identity:

```terraform
resource "kafka_topic" "all_2" {
  provider = kafka
  config = {
    "cleanup.policy" = "compact"
  }
  name               = "payments"
  partitions         = 3
  replication_factor = 3
}

import {
  to       = kafka_topic.all_2
  provider = kafka
  identity = {
    name = "payments"
  }
}
```

Review the file (rename resources, drop topics you do not want to manage; if
two `list` blocks match the same topic, keep one), then:

```shell
terraform plan    # Plan: 3 to import, 0 to add, 0 to change, 0 to destroy.
terraform apply
terraform plan    # No changes.
```

## ACLs

`list "kafka_acl"` returns one result per ACL entry, i.e. per `kafka_acl`
resource. The workflow is the same as for topics:

```terraform
# acls.tfquery.hcl
list "kafka_acl" "orders_service" {
  provider = kafka

  config {
    acl_principal        = "User:orders-service"
    resource_type        = "Topic"
    resource_name_prefix = "orders."
  }
}
```

Filter arguments (all optional, combined with AND):

- `acl_principal` (String) Only ACLs for this principal, e.g. `User:alice` (exact match).
- `resource_type` (String) Only ACLs on this resource type: `Topic`, `Group`, `Cluster`, `TransactionalID` or `DelegationToken`.
- `resource_name_prefix` (String) Only ACLs whose `resource_name` starts with this prefix.

```shell
terraform query
```

```
list.kafka_acl.orders_service   acl_host=*,acl_operation=Read,...   Allow User:orders-service Read Topic orders.v1 (Literal) from *
list.kafka_acl.orders_service   acl_host=*,acl_operation=Write,...  Allow User:orders-service Write Topic orders.v1 (Literal) from *
```

`terraform query -generate-config-out=generated.tf` then writes a `kafka_acl`
resource and an `import` block per ACL:

```terraform
resource "kafka_acl" "orders_service_0" {
  provider                     = kafka
  acl_host                     = "*"
  acl_operation                = "Read"
  acl_permission_type          = "Allow"
  acl_principal                = "User:orders-service"
  resource_name                = "orders.v1"
  resource_pattern_type_filter = "Literal"
  resource_type                = "Topic"
}

import {
  to       = kafka_acl.orders_service_0
  provider = kafka
  identity = {
    acl_host                     = "*"
    acl_operation                = "Read"
    acl_permission_type          = "Allow"
    acl_principal                = "User:orders-service"
    resource_name                = "orders.v1"
    resource_pattern_type_filter = "Literal"
    resource_type                = "Topic"
  }
}
```

## Resource identity

`kafka_topic` has a resource identity `{ name }`, `kafka_acl` the seven fields
that make an ACL unique (the same ones as in its pipe-delimited ID). Besides
`terraform query` they can be used in hand-written import blocks:

```terraform
import {
  to       = kafka_topic.payments
  identity = { name = "payments" }
}
```

Importing by ID (`terraform import kafka_topic.payments payments`,
`terraform import kafka_acl.x 'User:alice|*|Read|Allow|Topic|orders.v1|Literal'`) keeps working.
