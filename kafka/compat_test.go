package kafka

import (
	"fmt"
	"testing"

	uuid "github.com/hashicorp/go-uuid"
	r "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// State written by the last SDKv2 release must plan clean with the
// terraform-plugin-framework provider: step 1 applies with armitageee/kafka
// 0.17.0 from the Registry, step 2 plans the same config with this code.
// Needs network access to registry.terraform.io (and no network_mirror).
//
// One resource per config: releases up to 0.18.0 race when one provider
// process reads several resources at once ("kafka: broker not connected",
// fixed in 0.18.1), and step 1 runs such a release.
const lastSDKv2Release = "0.17.0"

func compatSteps(t *testing.T, config string) []r.TestStep {
	return []r.TestStep{
		{
			ExternalProviders: map[string]r.ExternalProvider{
				"kafka": {Source: "registry.terraform.io/armitageee/kafka", VersionConstraint: lastSDKv2Release},
			},
			Config: config,
		},
		{
			ProtoV6ProviderFactories: protoV6ProviderFactories(),
			Config:                   config,
			ConfigPlanChecks: r.ConfigPlanChecks{
				PreApply: []plancheck.PlanCheck{plancheck.ExpectEmptyPlan()},
			},
		},
	}
}

func compatName(t *testing.T) string {
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	return "compat-" + u
}

func TestAcc_CompatTopic(t *testing.T) {
	t.Parallel()
	name := compatName(t)
	r.Test(t, r.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: testAccCheckTopicDestroy,
		Steps:        compatSteps(t, providerOnlyCfg(t, testBootstrapServers[0], fmt.Sprintf(testResourceTopic_initialConfig, name))),
	})
}

// A topic without config: SDKv2 stored config = null, the framework must too.
func TestAcc_CompatTopicWithoutConfig(t *testing.T) {
	t.Parallel()
	name := compatName(t)
	r.Test(t, r.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: compatSteps(t, providerOnlyCfg(t, testBootstrapServers[0], fmt.Sprintf(`
resource "kafka_topic" "noconfig" {
  name               = "%s-noconfig"
  replication_factor = 1
  partitions         = 2
}
`, name))),
	})
}

func TestAcc_CompatACL(t *testing.T) {
	t.Parallel()
	name := compatName(t)
	r.Test(t, r.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: func(*terraform.State) error { return testAccCheckAclDestroy(name) },
		Steps:        compatSteps(t, providerOnlyCfg(t, testBootstrapServers[0], fmt.Sprintf(testResourceACL_initialConfig, name))),
	})
}

func TestAcc_CompatQuota(t *testing.T) {
	t.Parallel()
	name := compatName(t)
	r.Test(t, r.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: testAccCheckQuotaDestroy,
		Steps:        compatSteps(t, providerOnlyCfg(t, testBootstrapServers[0], fmt.Sprintf(testResourceQuota1, name, "4000000"))),
	})
}

// Not parallel: the default client-id quota is shared with other tests.
func TestAcc_CompatDefaultQuota(t *testing.T) {
	r.Test(t, r.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: testAccCheckQuotaDestroy,
		Steps:        compatSteps(t, providerOnlyCfg(t, testBootstrapServers[0], fmt.Sprintf(testResourceQuotaDefault, "4000000"))),
	})
}

// Legacy password: stored in state by SDKv2.
func TestAcc_CompatUserScramCredential(t *testing.T) {
	t.Parallel()
	name := compatName(t)
	r.Test(t, r.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: testAccCheckUserScramCredentialDestroy,
		Steps:        compatSteps(t, providerOnlyCfg(t, testBootstrapServers[0], fmt.Sprintf(testResourceUserScramCredential_SHA256, name))),
	})
}

// Write-only password with a version and non-default iterations.
func TestAcc_CompatUserScramCredentialWriteOnly(t *testing.T) {
	t.Parallel()
	name := compatName(t)
	r.Test(t, r.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: compatSteps(t, providerOnlyCfg(t, testBootstrapServers[0], fmt.Sprintf(`
resource "kafka_user_scram_credential" "wo" {
  username            = "%s-wo"
  scram_mechanism     = "SCRAM-SHA-512"
  scram_iterations    = 8192
  password_wo         = "write-only-test"
  password_wo_version = "1"
}
`, name))),
	})
}
