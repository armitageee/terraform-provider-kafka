# Cluster-wide defaults: every dynamic setting at that level becomes managed
terraform import kafka_broker_config.cluster default

# One broker
terraform import kafka_broker_config.broker_1 1
