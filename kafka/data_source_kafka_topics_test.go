package kafka

import (
	"fmt"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"strconv"
	"testing"

	r "github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAcc_Topics(t *testing.T) {
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	topicName := fmt.Sprintf("syslog-%s", u)

	bs := testBootstrapServers[0]
	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		Steps: []r.TestStep{
			{
				Config: cfg(t, bs, fmt.Sprintf(testDataSourceKafkaTopics, topicName)),
				Check: r.ComposeTestCheckFunc(
					testDatasourceTopics(topicName),
				),
			},
		},
	})
}

const testDataSourceKafkaTopics = `
resource "kafka_topic" "test" {
  name               = "%[1]s"
  replication_factor = 1
  partitions         = 1
  config = {
    "retention.ms" = "22222"
  }
}
data "kafka_topics" "test" {
 depends_on = [kafka_topic.test]
}
`

// testDatasourceTopics finds the topic created by the test in the list (other
// tests create topics in parallel, so its position is unknown) and compares it
// with what Kafka reports.
func testDatasourceTopics(topicName string) r.TestCheckFunc {
	return func(s *terraform.State) error {
		resourceState := s.Modules[0].Resources["data.kafka_topics.test"]
		if resourceState == nil {
			return fmt.Errorf("resource not found in state")
		}
		attrs := resourceState.Primary.Attributes
		count, err := strconv.Atoi(attrs["list.#"])
		if err != nil {
			return fmt.Errorf("list.# = %q: %w", attrs["list.#"], err)
		}
		for i := range count {
			if attrs[fmt.Sprintf("list.%d.topic_name", i)] != topicName {
				continue
			}
			expected, err := testProvider.Meta().(*LazyClient).ReadTopic(topicName, true)
			if err != nil {
				return fmt.Errorf("failed to read topic %s: %w", topicName, err)
			}
			if got := attrs[fmt.Sprintf("list.%d.partitions", i)]; got != fmt.Sprint(expected.Partitions) {
				return fmt.Errorf("expected %d partitions for %s, got %s", expected.Partitions, topicName, got)
			}
			if got := attrs[fmt.Sprintf("list.%d.replication_factor", i)]; got != fmt.Sprint(expected.ReplicationFactor) {
				return fmt.Errorf("expected replication factor %d for %s, got %s", expected.ReplicationFactor, topicName, got)
			}
			if got := attrs[fmt.Sprintf("list.%d.config.retention.ms", i)]; got != "22222" {
				return fmt.Errorf("expected retention.ms 22222 for %s, got %q", topicName, got)
			}
			return nil
		}
		return fmt.Errorf("topic %s not found in data source list (%d topics)", topicName, count)
	}
}
