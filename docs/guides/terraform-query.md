---
page_title: "Import existing resources with terraform query"
subcategory: ""
description: |-
  Discover topics, ACLs, quotas and SCRAM credentials that already exist in a cluster with list resources and generate import blocks and configuration for them.
---

# Import existing resources with `terraform query`

Clusters usually have topics, ACLs, quotas and users that were created by hand,
by applications or by another tool. Every resource of this provider has a
**list resource** that finds them and lets Terraform write the `import` blocks
and resource configuration for you.

| List resource | Since | Filters |
|---|---|---|
| `kafka_topic` | 0.15.0 | `name_prefix`, `name_regex`, `include_internal` |
| `kafka_acl` | 0.16.0 | `acl_principal`, `resource_type`, `resource_name_prefix` |
| `kafka_quota` | 0.17.0 | `entity_type`, `entity_name_prefix` |
| `kafka_user_scram_credential` | 0.17.0 | `scram_mechanism`, `username_prefix` |

Requires **Terraform 1.14 or later** (`terraform query`).
OpenTofu does not implement `query` yet; see [Auditing with OpenTofu](#auditing-with-opentofu).

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

## Quotas

`list "kafka_quota"` returns every quota on a single entity, including default
quotas (no `entity_name`). Quotas on combined entities (for example user +
client-id) are skipped: `kafka_quota` manages one entity.

```terraform
list "kafka_quota" "users" {
  provider = kafka
  config {
    entity_type = "user"
  }
}
```

## SCRAM credentials

`list "kafka_user_scram_credential"` returns one result per user and mechanism.
Kafka never returns passwords, so the generated configuration has none and
`terraform plan` fails until you add one. Put the user's **current** password
into `password_wo` and leave `password_wo_version` unset: then the import
changes nothing. Setting `password_wo_version` makes the next apply re-set the
password.

```terraform
resource "kafka_user_scram_credential" "services_0" {
  provider         = kafka
  username         = "svc-orders"
  scram_mechanism  = "SCRAM-SHA-512"
  scram_iterations = 4096
  password_wo      = var.svc_orders_password # add by hand
}
```

## Auditing with OpenTofu

OpenTofu (and Terraform before 1.14) cannot run `terraform query`, but the
same information is available through data sources with the same filters:

| Data source | Returns |
|---|---|
| `kafka_cluster` | cluster ID, active controller, brokers |
| `kafka_topics` | all topics with partitions, replication factor, config |
| `kafka_acls` | ACLs (`acl_principal`, `resource_type`, `resource_name_prefix` filters) |
| `kafka_quotas` | single-entity quotas, including defaults (`entity_type`, `entity_name_prefix`) |
| `kafka_user_scram_credentials` | users and mechanisms, never passwords (`scram_mechanism`, `username_prefix`) |

Every entry has an `id` that is the resource ID, so the data sources also give
you what you need for `import` blocks:

```terraform
data "kafka_acls" "orders_service" {
  acl_principal = "User:orders-service"
}

output "acl_import_ids" {
  value = [for a in data.kafka_acls.orders_service.acls : a.id]
}
```

```shell
tofu apply -refresh-only   # or: tofu plan, then read the output
tofu output acl_import_ids
```

## Resource identity

Every resource has a resource identity: `kafka_topic` `{ name }`, `kafka_acl`
the seven fields that make an ACL unique (the same ones as in its
pipe-delimited ID), `kafka_quota` `{ entity_type, entity_name }`,
`kafka_user_scram_credential` `{ username, scram_mechanism }`. Besides
`terraform query` they can be used in hand-written import blocks:

```terraform
import {
  to       = kafka_topic.payments
  identity = { name = "payments" }
}
```

Importing by ID (`terraform import kafka_topic.payments payments`,
`terraform import kafka_acl.x 'User:alice|*|Read|Allow|Topic|orders.v1|Literal'`) keeps working.
