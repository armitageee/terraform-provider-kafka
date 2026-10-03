data "kafka_cluster" "this" {}

output "kafka_cluster_id" {
  value = data.kafka_cluster.this.cluster_id
}

# Fail the plan when the provider points at the wrong cluster
check "expected_cluster" {
  assert {
    condition     = data.kafka_cluster.this.cluster_id == "MkU3OEVBNTcwNTJENDM2Qk"
    error_message = "Connected to an unexpected Kafka cluster."
  }
}
