# username|scram_mechanism; then set password_wo in the configuration
terraform import kafka_user_scram_credential.example 'my-user|SCRAM-SHA-256'

# Legacy form with the password (stored in state as `password`)
terraform import kafka_user_scram_credential.example 'my-user|SCRAM-SHA-256|my-password'
