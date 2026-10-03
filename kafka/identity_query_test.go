package kafka

import (
	"fmt"
	"testing"

	uuid "github.com/hashicorp/go-uuid"
	r "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/querycheck"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/hashicorp/terraform-plugin-testing/tfversion"
)

// Identity, import by identity and `terraform query` need Terraform >= 1.14
// (list resources); older binaries skip these tests.
var requireQuery = []tfversion.TerraformVersionCheck{tfversion.SkipBelow(tfversion.Version1_14_0)}

func TestAcc_TopicIdentityImportAndQuery(t *testing.T) {
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("identity-%s", u)
	bs := testBootstrapServers[0]
	identity := map[string]knownvalue.Check{"name": knownvalue.StringExact(name)}

	r.Test(t, r.TestCase{
		ProtoV5ProviderFactories: protoV5ProviderFactories(),
		TerraformVersionChecks:   requireQuery,
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckTopicDestroy,
		Steps: []r.TestStep{
			{
				Config:            cfg(t, bs, fmt.Sprintf(testResourceTopic_initialConfig, name)),
				ConfigStateChecks: []statecheck.StateCheck{statecheck.ExpectIdentity("kafka_topic.test", identity)},
			},
			{
				ResourceName:    "kafka_topic.test",
				ImportState:     true,
				ImportStateKind: r.ImportBlockWithResourceIdentity,
				Config:          cfg(t, bs, fmt.Sprintf(testResourceTopic_initialConfig, name)),
			},
			{
				// Query mode adds this config as .tfquery.hcl next to the previous
				// step's .tf, which already has the provider block.
				Query: true,
				Config: fmt.Sprintf(`
list "kafka_topic" "test" {
  provider = kafka
  config {
    name_prefix = %q
  }
}
`, name),
				QueryResultChecks: []querycheck.QueryResultCheck{
					querycheck.ExpectLength("kafka_topic.test", 1),
					querycheck.ExpectIdentity("kafka_topic.test", identity),
				},
			},
		},
	})
}

func TestAcc_ACLIdentityImportAndQuery(t *testing.T) {
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("identity-%s", u)
	bs := testBootstrapServers[0]
	// Matches testResourceACL_initialConfig.
	identity := map[string]knownvalue.Check{
		"acl_principal":                knownvalue.StringExact("User:Alice"),
		"acl_host":                     knownvalue.StringExact("*"),
		"acl_operation":                knownvalue.StringExact("Write"),
		"acl_permission_type":          knownvalue.StringExact("Allow"),
		"resource_type":                knownvalue.StringExact("Topic"),
		"resource_name":                knownvalue.StringExact(name),
		"resource_pattern_type_filter": knownvalue.StringExact("Literal"),
	}

	r.Test(t, r.TestCase{
		ProtoV5ProviderFactories: protoV5ProviderFactories(),
		TerraformVersionChecks:   requireQuery,
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             func(*terraform.State) error { return testAccCheckAclDestroy(name) },
		Steps: []r.TestStep{
			{
				Config:            cfg(t, bs, fmt.Sprintf(testResourceACL_initialConfig, name)),
				ConfigStateChecks: []statecheck.StateCheck{statecheck.ExpectIdentity("kafka_acl.test", identity)},
			},
			{
				ResourceName:    "kafka_acl.test",
				ImportState:     true,
				ImportStateKind: r.ImportBlockWithResourceIdentity,
				Config:          cfg(t, bs, fmt.Sprintf(testResourceACL_initialConfig, name)),
			},
			{
				// Query mode adds this config as .tfquery.hcl next to the previous
				// step's .tf, which already has the provider block.
				Query: true,
				Config: fmt.Sprintf(`
list "kafka_acl" "test" {
  provider = kafka
  config {
    acl_principal        = "User:Alice"
    resource_type        = "Topic"
    resource_name_prefix = %q
  }
}
`, name),
				QueryResultChecks: []querycheck.QueryResultCheck{
					querycheck.ExpectLength("kafka_acl.test", 1),
					querycheck.ExpectIdentity("kafka_acl.test", identity),
				},
			},
		},
	})
}
