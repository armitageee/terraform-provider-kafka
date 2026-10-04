package kafka

import (
	"fmt"
	"testing"

	uuid "github.com/hashicorp/go-uuid"
	r "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"
)

const testAuditDataSources = `
resource "kafka_acl" "test" {
  resource_name       = "%[1]s"
  resource_type       = "Topic"
  acl_principal       = "User:Alice"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

resource "kafka_quota" "test" {
  entity_name = "%[1]s"
  entity_type = "client-id"
  config = {
    "consumer_byte_rate" = "4000000"
  }
}

resource "kafka_user_scram_credential" "test" {
  username        = "%[1]s"
  scram_mechanism = "SCRAM-SHA-512"
  password_wo     = "write-only-test"
}

data "kafka_cluster" "this" {}

data "kafka_acls" "test" {
  resource_name_prefix = "%[1]s"
  depends_on           = [kafka_acl.test]
}

data "kafka_quotas" "test" {
  entity_type        = "client-id"
  entity_name_prefix = "%[1]s"
  depends_on         = [kafka_quota.test]
}

data "kafka_user_scram_credentials" "test" {
  username_prefix = "%[1]s"
  depends_on      = [kafka_user_scram_credential.test]
}
`

func TestAcc_AuditDataSources(t *testing.T) {
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("audit-%s", u)
	bs := testBootstrapServers[0]
	at := func(root string, idx int, keys ...string) tfjsonpath.Path {
		p := tfjsonpath.New(root).AtSliceIndex(idx)
		for _, k := range keys {
			p = p.AtMapKey(k)
		}
		return p
	}

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		Steps: []r.TestStep{
			{
				Config: cfg(t, bs, fmt.Sprintf(testAuditDataSources, name)),
				ConfigStateChecks: []statecheck.StateCheck{
					// docker-compose.yaml: CLUSTER_ID, 3 brokers, kafka1 is the only controller
					statecheck.ExpectKnownValue("data.kafka_cluster.this", tfjsonpath.New("cluster_id"), knownvalue.StringExact("MkU3OEVBNTcwNTJENDM2Qk")),
					statecheck.ExpectKnownValue("data.kafka_cluster.this", tfjsonpath.New("brokers"), knownvalue.ListSizeExact(3)),
					statecheck.ExpectKnownValue("data.kafka_cluster.this", at("brokers", 0, "id"), knownvalue.Int64Exact(1)),

					statecheck.ExpectKnownValue("data.kafka_acls.test", tfjsonpath.New("acls"), knownvalue.ListSizeExact(1)),
					statecheck.ExpectKnownValue("data.kafka_acls.test", at("acls", 0, "id"),
						knownvalue.StringExact(fmt.Sprintf("User:Alice|*|Read|Allow|Topic|%s|Literal", name))),

					statecheck.ExpectKnownValue("data.kafka_quotas.test", tfjsonpath.New("quotas"), knownvalue.ListSizeExact(1)),
					statecheck.ExpectKnownValue("data.kafka_quotas.test", at("quotas", 0, "id"), knownvalue.StringExact(name+"|client-id")),
					statecheck.ExpectKnownValue("data.kafka_quotas.test", at("quotas", 0, "config", "consumer_byte_rate"), knownvalue.Float64Exact(4000000)),
					statecheck.ExpectKnownValue("data.kafka_quotas.test", at("quotas", 0, "default"), knownvalue.Bool(false)),

					statecheck.ExpectKnownValue("data.kafka_user_scram_credentials.test", tfjsonpath.New("credentials"), knownvalue.ListSizeExact(1)),
					statecheck.ExpectKnownValue("data.kafka_user_scram_credentials.test", at("credentials", 0, "scram_mechanism"), knownvalue.StringExact("SCRAM-SHA-512")),
					statecheck.ExpectKnownValue("data.kafka_user_scram_credentials.test", at("credentials", 0, "scram_iterations"), knownvalue.Int64Exact(4096)),
				},
			},
		},
	})
}
