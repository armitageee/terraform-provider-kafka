package kafka

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

// Dynamic broker configuration (on top of server.properties, no restart).
// The level is one broker (its node ID) or the cluster default ("").

// BrokerConfigEntry is one effective broker setting with where it comes from.
type BrokerConfigEntry struct {
	Name      string
	Value     string
	Source    string // dynamic_broker, dynamic_default, static, default, ...
	ReadOnly  bool
	Sensitive bool
}

func brokerResourceName(brokerID *int64) string {
	if brokerID == nil {
		return ""
	}
	return strconv.FormatInt(*brokerID, 10)
}

func configSourceName(s sarama.ConfigSource) string {
	switch s {
	case sarama.SourceDynamicBroker:
		return "dynamic_broker"
	case sarama.SourceDynamicDefaultBroker:
		return "dynamic_default"
	case sarama.SourceStaticBroker:
		return "static"
	case sarama.SourceDefault:
		return "default"
	case sarama.SourceTopic:
		return "topic"
	default:
		return "unknown"
	}
}

func (c *Client) describeBroker(name string) ([]sarama.ConfigEntry, error) {
	admin, err := sarama.NewClusterAdminFromClient(c.client)
	if err != nil {
		return nil, err
	}
	results, err := admin.DescribeConfigs([]*sarama.ConfigResource{{Type: sarama.BrokerResource, Name: name}}, sarama.DescribeConfigsOptions{})
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("describing broker config %q: got %d results", name, len(results))
	}
	if res := results[0]; res.ErrorCode != sarama.ErrNoError {
		if res.ErrorMsg != "" {
			return nil, errors.New(res.ErrorMsg)
		}
		return nil, res.ErrorCode
	}
	return results[0].Configs, nil
}

// DynamicBrokerConfigs returns the dynamic settings set at exactly this level:
// per broker (source dynamic broker) or cluster default (dynamic default).
func (c *Client) DynamicBrokerConfigs(brokerID *int64) (map[string]string, error) {
	entries, err := c.describeBroker(brokerResourceName(brokerID))
	if err != nil {
		return nil, err
	}
	want := sarama.SourceDynamicDefaultBroker
	if brokerID != nil {
		want = sarama.SourceDynamicBroker
	}
	out := map[string]string{}
	for _, e := range entries {
		if e.Source == want {
			out[e.Name] = e.Value
		}
	}
	return out, nil
}

// AlterBrokerConfigs sets and deletes dynamic settings at one level. With
// validateOnly the brokers check the request (read-only keys, per-broker-only
// keys at cluster level, invalid values) without applying it.
func (c *Client) AlterBrokerConfigs(brokerID *int64, set map[string]string, del []string, validateOnly bool) error {
	if len(set) == 0 && len(del) == 0 {
		return nil
	}
	admin, err := sarama.NewClusterAdminFromClient(c.client)
	if err != nil {
		return err
	}
	entries := make(map[string]sarama.IncrementalAlterConfigsEntry, len(set)+len(del))
	for k, v := range set {
		entries[k] = sarama.IncrementalAlterConfigsEntry{Operation: sarama.IncrementalAlterConfigsOperationSet, Value: &v}
	}
	for _, k := range del {
		entries[k] = sarama.IncrementalAlterConfigsEntry{Operation: sarama.IncrementalAlterConfigsOperationDelete}
	}
	return brokerError(admin.IncrementalAlterConfig(sarama.BrokerResource, brokerResourceName(brokerID), entries, validateOnly))
}

// brokerError keeps Kafka's own reason (e.g. "Cannot update these configs
// dynamically: [log.dirs]") and drops sarama's generic text for the error
// code, which only says the request was invalid.
func brokerError(err error) error {
	var kerr sarama.KError
	if err == nil || !errors.As(err, &kerr) {
		return err
	}
	if reason, ok := strings.CutPrefix(err.Error(), kerr.Error()+": "); ok {
		return errors.New(reason)
	}
	return err
}

// SensitiveBrokerConfigs lists the sensitive setting names, read from any
// broker (the cluster level does not describe unset keys).
func (c *Client) SensitiveBrokerConfigs() (map[string]bool, error) {
	brokers := c.client.Brokers()
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers known")
	}
	entries, err := c.describeBroker(strconv.Itoa(int(brokers[0].ID())))
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, e := range entries {
		if e.Sensitive {
			out[e.Name] = true
		}
	}
	return out, nil
}

// BrokerConfigEntries returns the effective configuration of one broker,
// sorted by name.
func (c *Client) BrokerConfigEntries(brokerID int64) ([]BrokerConfigEntry, error) {
	entries, err := c.describeBroker(strconv.FormatInt(brokerID, 10))
	if err != nil {
		return nil, err
	}
	out := make([]BrokerConfigEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, BrokerConfigEntry{
			Name: e.Name, Value: e.Value, Source: configSourceName(e.Source), ReadOnly: e.ReadOnly, Sensitive: e.Sensitive,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
