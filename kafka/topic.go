package kafka

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

type Topic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]*string
}

func (t *Topic) Equal(other Topic) bool {
	mape := MapEq(other.Config, t.Config)

	// -1 (Confluent placement constraints) accepts whatever Kafka reports.
	rfEqual := t.ReplicationFactor == -1 || other.ReplicationFactor == t.ReplicationFactor
	if mape == nil && (other.Name == t.Name) && (other.Partitions == t.Partitions) && rfEqual {
		return true
	}
	return false
}

// ReplicaCount returns the replication_factor for a partition
// Returns an error if it cannot determine the count, or if the number of
// replicas is different across partitions
func ReplicaCount(c sarama.Client, topic string, partitions []int32) (int, error) {
	count := -1

	for _, p := range partitions {
		replicas, err := c.Replicas(topic, p)
		if err != nil {
			return -1, errors.New("could not get replicas for partition")
		}
		if count == -1 {
			count = len(replicas)
		}
		if count != len(replicas) {
			return count, fmt.Errorf("the replica count isn't the same across partitions %d != %d", count, len(replicas))
		}
	}
	return count, nil

}

func configToResources(topic Topic, c *Config) []*sarama.AlterConfigsResource {
	configEntries := topic.Config

	// AWS MSK Serverless does not support updating cleanup.policy
	// Create a copy of the config map to avoid mutating the caller's map
	if topic.Config["cleanup.policy"] != nil && c.isAWSMSKServerless() {
		configEntries = make(map[string]*string, len(topic.Config))
		for k, v := range topic.Config {
			if k != "cleanup.policy" {
				configEntries[k] = v
			}
		}
	}

	return []*sarama.AlterConfigsResource{
		{
			Type:          sarama.TopicResource,
			Name:          topic.Name,
			ConfigEntries: configEntries,
		},
	}
}

// isDefault reports whether a topic config value is inherited (Kafka default,
// server.properties, or a dynamic broker setting at cluster or broker level)
// rather than set on the topic. A per-broker dynamic setting (e.g. from
// kafka_broker_config) is inherited too; treating it as the topic's own made
// every topic on that broker drift.
func isDefault(tc *sarama.ConfigEntry, version int) bool {
	if version == 0 {
		return tc.Default
	}
	return tc.Source == sarama.SourceDefault ||
		tc.Source == sarama.SourceStaticBroker ||
		tc.Source == sarama.SourceDynamicDefaultBroker ||
		tc.Source == sarama.SourceDynamicBroker
}
