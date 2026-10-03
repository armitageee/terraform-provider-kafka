provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
  sasl_mechanism    = "oauthbearer"
  sasl_token_url    = "https://oauth.example.com/oauth2/token"
  sasl_oauth_scopes = ["kafka:read", "kafka:write"]
  tls_enabled       = true
}
