# Every SCRAM credential (one result per user and mechanism)
list "kafka_user_scram_credential" "all" {
  provider = kafka
}

# SCRAM-SHA-512 credentials of service accounts
list "kafka_user_scram_credential" "services" {
  provider = kafka

  config {
    scram_mechanism = "SCRAM-SHA-512"
    username_prefix = "svc-"
  }
}
