# Example using HashiCorp Vault to retrieve temporary AWS credentials
provider "vault" {
  auth_login_jwt {
    role = "jwt-role-name"
  }
}

data "vault_aws_access_credentials" "creds" {
  backend = "aws"
  type    = "sts"
  role    = "kafka-access-role"
}

provider "kafka" {
  bootstrap_servers   = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled         = true
  sasl_mechanism      = "aws-iam"
  sasl_aws_region     = "us-east-1"
  sasl_aws_access_key = data.vault_aws_access_credentials.creds.access_key
  sasl_aws_secret_key = data.vault_aws_access_credentials.creds.secret_key
  sasl_aws_token      = data.vault_aws_access_credentials.creds.security_token
}
