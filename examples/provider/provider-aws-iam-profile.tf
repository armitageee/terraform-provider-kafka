provider "kafka" {
  bootstrap_servers = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled       = true
  sasl_mechanism    = "aws-iam"
  sasl_aws_region   = "us-east-1"
  sasl_aws_profile  = "production"
}
