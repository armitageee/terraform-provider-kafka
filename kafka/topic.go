package kafka

import (
	"errors"
	"fmt"
	"slices"
	"strings"

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

// csvConfigKeys are topic configs whose value is a comma-separated list where
// element order carries no meaning; compared order-insensitively.
var csvConfigKeys = map[string]bool{"cleanup.policy": true}

func normalizeCSV(v string) string {
	parts := strings.Split(v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	slices.Sort(parts)
	return strings.Join(parts, ",")
}

// configAdoptable checks whether an existing topic (actual, from a raw
// describe) can be adopted for the planned config (planned): every key pinned
// in the planned config must be present on the topic with an equal value.
// Extra keys on the topic (broker defaults, out-of-band changes) are ignored —
// they are left to ordinary drift detection. Returns a multi-line error
// listing every mismatch, or nil when the topic is adoptable.
func configAdoptable(actual, planned map[string]*string) error {
	var problems []string
	for k, plannedV := range planned {
		actualV, ok := actual[k]
		switch {
		case !ok || actualV == nil:
			problems = append(problems, fmt.Sprintf("config[%q]: not set on the existing topic (config pins %q)", k, *plannedV))
		case plannedV == nil:
			problems = append(problems, fmt.Sprintf("config[%q]: existing topic has %q, config wants null", k, *actualV))
		default:
			a, p := *actualV, *plannedV
			if csvConfigKeys[k] {
				a, p = normalizeCSV(a), normalizeCSV(p)
			}
			if a != p {
				problems = append(problems, fmt.Sprintf("config[%q]: existing topic has %q, config wants %q", k, *actualV, *plannedV))
			}
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
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
