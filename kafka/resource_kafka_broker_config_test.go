package kafka

import (
	"fmt"
	"regexp"
	"testing"

	r "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"
)

// Broker 2 of docker-compose.yaml; no other test touches its dynamic config.
// Keys without a topic-level counterpart only: the compat tests run 0.17.0,
// which counted per-broker dynamic settings (e.g. message.max.bytes →
// max.message.bytes) as topic config and would see drift.
var testBrokerID = int64(2)

func brokerConfigHCL(brokerID *int64, config map[string]string) string {
	body := ""
	for k, v := range config {
		body += fmt.Sprintf("    %q = %q\n", k, v)
	}
	broker := ""
	if brokerID != nil {
		broker = fmt.Sprintf("  broker_id = %d\n", *brokerID)
	}
	return fmt.Sprintf(`
resource "kafka_broker_config" "test" {
%s  config = {
%s  }
}
`, broker, body)
}

// testAccCheckBrokerConfigGone: none of the keys is set at that level.
func testAccCheckBrokerConfigGone(brokerID *int64, keys ...string) func(*terraform.State) error {
	return func(*terraform.State) error {
		have, err := testProvider.client.DynamicBrokerConfigs(brokerID)
		if err != nil {
			return err
		}
		for _, k := range keys {
			if v, ok := have[k]; ok {
				return fmt.Errorf("%s still set to %q at %s", k, v, levelName(brokerID))
			}
		}
		return nil
	}
}

func TestAcc_BrokerConfigPerBroker(t *testing.T) {
	t.Parallel()
	bs := testBootstrapServers[0]
	id := &testBrokerID

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckBrokerConfigGone(id, "log.cleaner.threads", "num.replica.fetchers"),
		Steps: []r.TestStep{
			{
				Config: cfg(t, bs, brokerConfigHCL(id, map[string]string{"log.cleaner.threads": "2"})),
				ConfigStateChecks: []statecheck.StateCheck{
					statecheck.ExpectKnownValue("kafka_broker_config.test", tfjsonpath.New("id"), knownvalue.StringExact("2")),
				},
				Check: func(*terraform.State) error {
					have, err := testProvider.client.DynamicBrokerConfigs(id)
					if err != nil {
						return err
					}
					if have["log.cleaner.threads"] != "2" {
						return fmt.Errorf("log.cleaner.threads = %q at broker 2", have["log.cleaner.threads"])
					}
					return nil
				},
			},
			{
				Config: cfg(t, bs, brokerConfigHCL(id, map[string]string{"log.cleaner.threads": "3", "num.replica.fetchers": "2"})),
			},
			{
				// Dropping a key reverts it (DELETE), the other one stays.
				Config: cfg(t, bs, brokerConfigHCL(id, map[string]string{"num.replica.fetchers": "2"})),
				Check:  testAccCheckBrokerConfigGone(id, "log.cleaner.threads"),
			},
			{
				ResourceName:      "kafka_broker_config.test",
				ImportState:       true,
				ImportStateId:     "2",
				ImportStateVerify: true,
				Config:            cfg(t, bs, brokerConfigHCL(id, map[string]string{"num.replica.fetchers": "2"})),
			},
		},
	})
}

// Not parallel: the only test on the cluster-default level.
func TestAcc_BrokerConfigClusterDefaultDrift(t *testing.T) {
	bs := testBootstrapServers[0]
	config := cfg(t, bs, brokerConfigHCL(nil, map[string]string{"log.retention.ms": "604800000"}))

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckBrokerConfigGone(nil, "log.retention.ms"),
		Steps: []r.TestStep{
			{
				Config: config,
				ConfigStateChecks: []statecheck.StateCheck{
					statecheck.ExpectKnownValue("kafka_broker_config.test", tfjsonpath.New("id"), knownvalue.StringExact("default")),
				},
			},
			{
				// Changed outside the code: the plan must want to restore it.
				PreConfig: func() {
					if err := testProvider.client.AlterBrokerConfigs(nil, map[string]string{"log.retention.ms": "3600000"}, nil, false); err != nil {
						t.Fatal(err)
					}
				},
				Config:             config,
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				Config: config,
				Check: func(*terraform.State) error {
					have, err := testProvider.client.DynamicBrokerConfigs(nil)
					if err != nil {
						return err
					}
					if have["log.retention.ms"] != "604800000" {
						return fmt.Errorf("log.retention.ms = %q after apply", have["log.retention.ms"])
					}
					return nil
				},
			},
		},
	})
}

func TestAcc_BrokerConfigRejectedAtPlan(t *testing.T) {
	t.Parallel()
	bs := testBootstrapServers[0]
	id := &testBrokerID
	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		Steps: []r.TestStep{
			{
				// Read-only: only server.properties + restart.
				Config:      cfg(t, bs, brokerConfigHCL(id, map[string]string{"auto.create.topics.enable": "false"})),
				PlanOnly:    true,
				ExpectError: regexp.MustCompile(`(?s)Kafka rejected the broker 2 settings.*Cannot update these configs\s+dynamically`),
			},
			{
				Config:      cfg(t, bs, brokerConfigHCL(id, map[string]string{"ssl.keystore.password": "secret"})),
				PlanOnly:    true,
				ExpectError: regexp.MustCompile(`Sensitive broker settings are not supported`),
			},
		},
	})
}

func TestAcc_BrokerConfigDataSource(t *testing.T) {
	t.Parallel()
	bs := testBootstrapServers[0]
	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		Steps: []r.TestStep{{
			Config: cfg(t, bs, `
data "kafka_broker_config" "b1" {
  broker_id = 1
}
`),
			Check: func(s *terraform.State) error {
				attrs := s.RootModule().Resources["data.kafka_broker_config.b1"].Primary.Attributes
				for i := 0; attrs[fmt.Sprintf("configs.%d.name", i)] != ""; i++ {
					if attrs[fmt.Sprintf("configs.%d.name", i)] != "node.id" {
						continue
					}
					if src := attrs[fmt.Sprintf("configs.%d.source", i)]; src != "static" {
						return fmt.Errorf("node.id source = %q, want static", src)
					}
					if ro := attrs[fmt.Sprintf("configs.%d.read_only", i)]; ro != "true" {
						return fmt.Errorf("node.id read_only = %q", ro)
					}
					return nil
				}
				return fmt.Errorf("node.id not listed")
			},
		}},
	})
}
