---
page_title: "Authentication Guide"
subcategory: ""
description: |-
  Comprehensive guide for configuring authentication with the Kafka provider.
---

# Authentication Guide

This guide covers all supported authentication methods for connecting to Apache Kafka clusters using the Terraform Kafka provider.

## Table of Contents

- [TLS/SSL Authentication](#tlsssl-authentication)
- [SASL/PLAIN Authentication](#saslplain-authentication)
- [SASL/SCRAM Authentication](#saslscram-authentication)
- [AWS IAM Authentication](#aws-iam-authentication)
- [OAuth2/OIDC Authentication](#oauth2oidc-authentication)
- [Kerberos (SASL/GSSAPI) Authentication](#kerberos-saslgssapi-authentication)
- [Troubleshooting](#troubleshooting)

## TLS/SSL Authentication

TLS provides encryption and optional mutual authentication using certificates.

### Basic TLS (Encryption Only)

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  ca_cert          = file("ca-cert.pem")
}
```

### Mutual TLS (mTLS) Authentication

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  ca_cert          = file("ca-cert.pem")
  client_cert      = file("client-cert.pem")
  client_key       = file("client-key.pem")
}
```

### TLS with Encrypted Private Key

```hcl
provider "kafka" {
  bootstrap_servers    = ["broker1.example.com:9093"]
  tls_enabled         = true
  ca_cert             = file("ca-cert.pem")
  client_cert         = file("client-cert.pem")
  client_key          = file("client-key-encrypted.pem")
  client_key_passphrase = var.key_passphrase
}
```

### Skip TLS Verification (Development Only)

```hcl
provider "kafka" {
  bootstrap_servers = ["localhost:9093"]
  tls_enabled      = true
  skip_tls_verify  = true  # INSECURE - Development only
}
```

## SASL/PLAIN Authentication

SASL/PLAIN sends credentials in plain text and should always be used with TLS.

### Basic Configuration

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "plain"
  sasl_username    = "terraform-user"
  sasl_password    = var.kafka_password
}
```

### With Certificate Validation

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  ca_cert          = file("ca-cert.pem")
  sasl_mechanism   = "plain"
  sasl_username    = "terraform-user"
  sasl_password    = var.kafka_password
}
```

## SASL/SCRAM Authentication

SCRAM provides secure authentication without transmitting passwords in plain text.

### SCRAM-SHA-256

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "scram-sha256"
  sasl_username    = "terraform-user"
  sasl_password    = var.kafka_password
}

# Create corresponding user credentials
resource "kafka_user_scram_credential" "terraform" {
  username        = "terraform-user"
  scram_mechanism = "SCRAM-SHA-256"
  password        = var.kafka_password
}
```

### SCRAM-SHA-512

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "scram-sha512"
  sasl_username    = "terraform-user"
  sasl_password    = var.kafka_password
}

# Create corresponding user credentials
resource "kafka_user_scram_credential" "terraform" {
  username         = "terraform-user"
  scram_mechanism  = "SCRAM-SHA-512"
  scram_iterations = 8192
  password         = var.kafka_password
}
```

## AWS IAM Authentication

AWS IAM authentication is commonly used with Amazon MSK.

### Using IAM Role (AssumeRole)

```hcl
provider "kafka" {
  bootstrap_servers = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled      = true
  sasl_mechanism   = "aws-iam"
  sasl_aws_region  = "us-east-1"
  sasl_aws_role_arn = "arn:aws:iam::123456789012:role/kafka-terraform-role"
}
```

### Using AWS Profile

```hcl
provider "kafka" {
  bootstrap_servers = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled      = true
  sasl_mechanism   = "aws-iam"
  sasl_aws_region  = "us-east-1"
  sasl_aws_profile = "production"
}
```

### Using Custom Config File

```hcl
provider "kafka" {
  bootstrap_servers            = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled                 = true
  sasl_mechanism              = "aws-iam"
  sasl_aws_region             = "us-east-1"
  sasl_aws_profile            = "custom-profile"
  sasl_aws_shared_config_files = ["/path/to/custom/config"]
}
```

### Using Static Credentials

```hcl
provider "kafka" {
  bootstrap_servers   = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled        = true
  sasl_mechanism     = "aws-iam"
  sasl_aws_region    = "us-east-1"
  sasl_aws_access_key = var.aws_access_key
  sasl_aws_secret_key = var.aws_secret_key
  sasl_aws_token     = var.aws_session_token  # Optional for temporary credentials
}
```

### Using Container Credentials (ECS/EKS)

```hcl
provider "kafka" {
  bootstrap_servers = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled      = true
  sasl_mechanism   = "aws-iam"
  sasl_aws_region  = "us-east-1"
  # Container credentials are automatically detected
}
```

### Using Pod Identity (EKS)

```hcl
provider "kafka" {
  bootstrap_servers = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled      = true
  sasl_mechanism   = "aws-iam"
  sasl_aws_region  = "us-east-1"
  sasl_aws_container_authorization_token_file = "/var/run/secrets/eks.amazonaws.com/serviceaccount/token"
  sasl_aws_container_credentials_full_uri     = "http://169.254.170.23/v1/credentials"
}
```

### Debugging AWS Authentication

```hcl
provider "kafka" {
  bootstrap_servers  = ["b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098"]
  tls_enabled       = true
  sasl_mechanism    = "aws-iam"
  sasl_aws_region   = "us-east-1"
  sasl_aws_creds_debug = true  # Enable debug logging
}
```

## OAuth2/OIDC Authentication

OAuth2 authentication using the OAUTHBEARER mechanism.

### Basic OAuth2

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "oauthbearer"
  sasl_token_url   = "https://auth.example.com/oauth2/token"
}
```

### OAuth2 with Scopes

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "oauthbearer"
  sasl_token_url   = "https://auth.example.com/oauth2/token"
  sasl_oauth_scopes = ["kafka:read", "kafka:write", "kafka:admin"]
}
```

## Kerberos (SASL/GSSAPI) Authentication

Use `sasl_mechanism = "gssapi"` for clusters secured with Kerberos (MIT KDC, Active Directory, Cloudera/CDP). The credential source is picked in this order:

1. **Keytab** — `sasl_gssapi_keytab_path` is set (recommended for CI).
2. **Password** — `sasl_gssapi_password` is set.
3. **Credentials cache** — otherwise; e.g. after `kinit`. Path: `sasl_gssapi_ccache_path`, then `$KRB5CCNAME`, then `/tmp/krb5cc_<uid>`. Only `FILE:` caches are supported.

The realm and user name come from `sasl_gssapi_principal` (`primary[/instance]@REALM`). With a credentials cache the principal can be omitted — it is read from the cache. If the principal has no realm, `default_realm` from krb5.conf is used.

### Keytab (CI/CD)

```terraform
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled       = true
  ca_cert           = file("ca.pem")

  sasl_mechanism                   = "gssapi"
  sasl_gssapi_service_name         = "kafka" # primary of the broker principal kafka/<host>@REALM
  sasl_gssapi_principal            = "terraform/ci@EXAMPLE.COM"
  sasl_gssapi_keytab_path          = "/run/secrets/terraform.keytab"
  sasl_gssapi_kerberos_config_path = "/etc/krb5.conf"
}
```

### Credentials cache (developer laptop)

```shell
kinit alice@EXAMPLE.COM
export KAFKA_SASL_MECHANISM=gssapi
terraform plan
```

```terraform
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  sasl_mechanism    = "gssapi"
  # principal and cache are taken from the kinit session ($KRB5CCNAME)
}
```

On macOS the default cache is `API:`; use a file cache: `KRB5CCNAME=FILE:/tmp/krb5cc_$(id -u) kinit alice@EXAMPLE.COM`.

### Password

```terraform
provider "kafka" {
  bootstrap_servers     = ["broker1.example.com:9093"]
  sasl_mechanism        = "gssapi"
  sasl_gssapi_principal = "alice@EXAMPLE.COM"
  sasl_gssapi_password  = var.kerberos_password
}
```

Every `sasl_gssapi_*` argument can also be set with the matching `KAFKA_SASL_GSSAPI_*` environment variable (for example `KAFKA_SASL_GSSAPI_KEYTAB_PATH`).

### Kerberos troubleshooting

- `KDC_ERR_S_PRINCIPAL_UNKNOWN` — `sasl_gssapi_service_name` does not match the broker principal; the broker host name used in `bootstrap_servers` must match the `<host>` part of `kafka/<host>@REALM` (no IP addresses).
- `KDC_ERR_PREAUTH_FAILED` — wrong password or a keytab with a stale kvno; re-export the keytab.
- Active Directory — keep `sasl_gssapi_disable_pafxfast = true` (default).
- `realm unknown` — set `sasl_gssapi_realm`, use a principal with `@REALM`, or configure `default_realm` in krb5.conf.

## Security Best Practices

### 1. Always Use TLS in Production

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true  # Always enable TLS
  ca_cert          = file("ca-cert.pem")
  # ... other settings
}
```

### 2. Use Environment Variables for Secrets

```hcl
# Set environment variables:
# export TF_VAR_kafka_password="secure-password"
# export TF_VAR_client_key_passphrase="key-passphrase"

variable "kafka_password" {
  type      = string
  sensitive = true
}

variable "client_key_passphrase" {
  type      = string
  sensitive = true
}

provider "kafka" {
  bootstrap_servers    = ["broker1.example.com:9093"]
  tls_enabled         = true
  sasl_mechanism      = "scram-sha512"
  sasl_username       = "terraform-user"
  sasl_password       = var.kafka_password
  client_key_passphrase = var.client_key_passphrase
}
```

### 3. Use HashiCorp Vault for Credentials

```hcl
data "vault_generic_secret" "kafka" {
  path = "secret/kafka/terraform"
}

provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "scram-sha512"
  sasl_username    = data.vault_generic_secret.kafka.data["username"]
  sasl_password    = data.vault_generic_secret.kafka.data["password"]
}
```

### 4. Rotate Credentials Regularly

```hcl
# Use random password generation with rotation
resource "random_password" "kafka" {
  length  = 32
  special = true
  
  keepers = {
    rotation = formatdate("YYYY-MM", timestamp())  # Rotate monthly
  }
}

resource "kafka_user_scram_credential" "service" {
  username        = "service-account"
  scram_mechanism = "SCRAM-SHA-512"
  password        = random_password.kafka.result
}
```

## Troubleshooting

### Connection Timeout

```hcl
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  timeout          = 120  # Increase timeout to 120 seconds
  # ... other settings
}
```

### TLS Certificate Issues

1. Verify certificate validity:
```bash
openssl x509 -in client-cert.pem -text -noout
```

2. Check certificate chain:
```bash
openssl verify -CAfile ca-cert.pem client-cert.pem
```

3. Test connection:
```bash
openssl s_client -connect broker1.example.com:9093 -CAfile ca-cert.pem
```

### SASL Authentication Failures

1. Enable debug logging:
```hcl
provider "kafka" {
  # ... other settings
  sasl_aws_creds_debug = true  # For AWS IAM
}
```

2. Verify credentials are correct
3. Check ACLs for the user
4. Ensure SCRAM credentials exist in Kafka

### AWS IAM Specific Issues

1. Verify IAM role permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:AlterCluster",
        "kafka-cluster:DescribeCluster"
      ],
      "Resource": [
        "arn:aws:kafka:region:account-id:cluster/cluster-name/*"
      ]
    }
  ]
}
```

2. Check AWS credentials:
```bash
aws sts get-caller-identity
```

3. Verify MSK cluster authentication is set to IAM

## Migration Between Authentication Methods

### From SASL/PLAIN to SASL/SCRAM

```hcl
# Step 1: Create SCRAM credentials while still using PLAIN
resource "kafka_user_scram_credential" "migration" {
  username        = "existing-user"
  scram_mechanism = "SCRAM-SHA-256"
  password        = var.existing_password
}

# Step 2: Update provider configuration
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "scram-sha256"  # Changed from "plain"
  sasl_username    = "existing-user"
  sasl_password    = var.existing_password
}
```

### From mTLS to SASL/SCRAM

```hcl
# Step 1: Create SCRAM user matching certificate CN
resource "kafka_user_scram_credential" "mtls_migration" {
  username        = "CN=service-account"
  scram_mechanism = "SCRAM-SHA-512"
  password        = var.new_password
}

# Step 2: Update ACLs if needed
resource "kafka_acl" "migration" {
  resource_name       = "my-topic"
  resource_type       = "Topic"
  acl_principal       = "User:CN=service-account"
  acl_host            = "*"
  acl_operation       = "All"
  acl_permission_type = "Allow"
}

# Step 3: Switch provider configuration
provider "kafka" {
  bootstrap_servers = ["broker1.example.com:9093"]
  tls_enabled      = true
  sasl_mechanism   = "scram-sha512"
  sasl_username    = "CN=service-account"
  sasl_password    = var.new_password
}
```