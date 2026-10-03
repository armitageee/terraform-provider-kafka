terraform {
  required_providers {
    kafka = {
      source  = "armitageee/kafka"
      version = "~> 0.14"
    }
  }
}

provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
  ca_cert           = file("../secrets/ca.crt")
  client_cert       = file("../secrets/client-cert.pem")
  client_key        = file("../secrets/client-key.pem")
  tls_enabled       = true
}
