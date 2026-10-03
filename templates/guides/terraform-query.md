---
page_title: "Import existing topics with terraform query"
subcategory: ""
description: |-
  Discover topics that already exist in a cluster with the kafka_topic list resource and generate import blocks and configuration for them.
---

# Import existing topics with `terraform query`

Clusters usually have topics that were created by hand, by applications or by
another tool. The `kafka_topic` **list resource** finds them and lets
Terraform write the `import` blocks and resource configuration for you.

Requires **Terraform 1.14 or later** (`terraform query`) and provider `0.15.0` or later.
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

## Resource identity

`kafka_topic` has a resource identity `{ name }`. Besides `terraform query` it
can be used in hand-written import blocks:

```terraform
import {
  to       = kafka_topic.payments
  identity = { name = "payments" }
}
```

Importing by ID (`terraform import kafka_topic.payments payments`) keeps working.
