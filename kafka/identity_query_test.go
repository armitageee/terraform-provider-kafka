package kafka

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
// (list resources); older binaries skip these tests. OpenTofu has no `query`
// (and its version numbers are not Terraform's), so it is skipped by name.
var requireQuery = []tfversion.TerraformVersionCheck{tfversion.SkipBelow(tfversion.Version1_14_0)}

// skipOnOpenTofu skips a test when the acceptance tests run on OpenTofu
// (TF_ACC_TERRAFORM_PATH points to a tofu binary).
func skipOnOpenTofu(t *testing.T, why string) {
	t.Helper()
	if strings.Contains(filepath.Base(os.Getenv("TF_ACC_TERRAFORM_PATH")), "tofu") {
		t.Skipf("OpenTofu: %s", why)
	}
}

func TestAcc_TopicIdentityImportAndQuery(t *testing.T) {
	skipOnOpenTofu(t, "no terraform query / import by identity")
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("identity-%s", u)
	bs := testBootstrapServers[0]
	identity := map[string]knownvalue.Check{"name": knownvalue.StringExact(name)}

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
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
	skipOnOpenTofu(t, "no terraform query / import by identity")
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
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
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

func TestAcc_QuotaIdentityImportAndQuery(t *testing.T) {
	skipOnOpenTofu(t, "no terraform query / import by identity")
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("identity-%s", u)
	bs := testBootstrapServers[0]
	identity := map[string]knownvalue.Check{
		"entity_type": knownvalue.StringExact("client-id"),
		"entity_name": knownvalue.StringExact(name),
	}
	config := cfg(t, bs, fmt.Sprintf(testResourceQuota1, name, "4000000"))

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		TerraformVersionChecks:   requireQuery,
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckQuotaDestroy,
		Steps: []r.TestStep{
			{
				Config:            config,
				ConfigStateChecks: []statecheck.StateCheck{statecheck.ExpectIdentity("kafka_quota.test1", identity)},
			},
			{
				// Import by ID was not wired up before 0.17 although documented.
				ResourceName:      "kafka_quota.test1",
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateId:     "client-id:" + name,
				Config:            config,
			},
			{
				ResourceName:    "kafka_quota.test1",
				ImportState:     true,
				ImportStateKind: r.ImportBlockWithResourceIdentity,
				Config:          config,
			},
			{
				Query: true,
				Config: fmt.Sprintf(`
list "kafka_quota" "test" {
  provider = kafka
  config {
    entity_type        = "client-id"
    entity_name_prefix = %q
  }
}
`, name),
				QueryResultChecks: []querycheck.QueryResultCheck{
					querycheck.ExpectLength("kafka_quota.test", 1),
					querycheck.ExpectIdentity("kafka_quota.test", identity),
				},
			},
		},
	})
}

// Not parallel: the default client-id quota is shared with the other default
// quota tests.
func TestAcc_DefaultQuotaImport(t *testing.T) {
	skipOnOpenTofu(t, "no terraform query / import by identity")
	bs := testBootstrapServers[0]
	config := cfg(t, bs, fmt.Sprintf(testResourceQuotaDefault, "4000000"))

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		TerraformVersionChecks:   requireQuery,
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckQuotaDestroy,
		Steps: []r.TestStep{
			{
				Config: config,
				ConfigStateChecks: []statecheck.StateCheck{statecheck.ExpectIdentity("kafka_quota.test1", map[string]knownvalue.Check{
					"entity_type": knownvalue.StringExact("client-id"),
					"entity_name": knownvalue.StringExact(""),
				})},
			},
			{
				ResourceName:      "kafka_quota.test1",
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateId:     "entity-default|client-id",
				Config:            config,
			},
			{
				ResourceName:    "kafka_quota.test1",
				ImportState:     true,
				ImportStateKind: r.ImportBlockWithResourceIdentity,
				Config:          config,
			},
		},
	})
}

// No password_wo_version: after import the state has none either, so the
// import plan is a no-op. The password itself is write-only and not compared.
const testResourceUserScramCredential_ImportWriteOnly = `
resource "kafka_user_scram_credential" "test" {
  username        = "%s"
  scram_mechanism = "SCRAM-SHA-512"
  password_wo     = "write-only-test"
}
`

func TestAcc_UserScramCredentialIdentityImportAndQuery(t *testing.T) {
	skipOnOpenTofu(t, "no terraform query / import by identity")
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("identity-%s", u)
	bs := testBootstrapServers[0]
	identity := map[string]knownvalue.Check{
		"username":        knownvalue.StringExact(name),
		"scram_mechanism": knownvalue.StringExact("SCRAM-SHA-512"),
	}
	config := cfg(t, bs, fmt.Sprintf(testResourceUserScramCredential_ImportWriteOnly, name))

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		TerraformVersionChecks:   requireQuery,
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckUserScramCredentialDestroy,
		Steps: []r.TestStep{
			{
				Config:            config,
				ConfigStateChecks: []statecheck.StateCheck{statecheck.ExpectIdentity("kafka_user_scram_credential.test", identity)},
			},
			{
				ResourceName:    "kafka_user_scram_credential.test",
				ImportState:     true,
				ImportStateKind: r.ImportBlockWithResourceIdentity,
				Config:          config,
			},
			{
				Query: true,
				Config: fmt.Sprintf(`
list "kafka_user_scram_credential" "test" {
  provider = kafka
  config {
    scram_mechanism = "SCRAM-SHA-512"
    username_prefix = %q
  }
}
`, name),
				QueryResultChecks: []querycheck.QueryResultCheck{
					querycheck.ExpectLength("kafka_user_scram_credential.test", 1),
					querycheck.ExpectIdentity("kafka_user_scram_credential.test", identity),
				},
			},
		},
	})
}
