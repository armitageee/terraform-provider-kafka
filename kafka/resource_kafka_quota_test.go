package kafka

import (
	"fmt"
	"testing"

	uuid "github.com/hashicorp/go-uuid"
	r "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAcc_BasicQuota(t *testing.T) {
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	quotaEntityName := fmt.Sprintf("quota1-%s", u)
	bs := testBootstrapServers[0]

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckQuotaDestroy,
		Steps: []r.TestStep{
			{
				Config: cfgs(t, bs, fmt.Sprintf(testResourceQuota1, quotaEntityName, "4000000")),
				Check:  testResourceQuota_initialCheck,
			},
		},
	})
}

func TestAcc_QuotaConfigUpdate(t *testing.T) {
	t.Parallel()
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	quotaEntityName := fmt.Sprintf("quota1-%s", u)
	bs := testBootstrapServers[0]

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckQuotaDestroy,
		Steps: []r.TestStep{
			{
				Config: cfg(t, bs, fmt.Sprintf(testResourceQuota1, quotaEntityName, "4000000")),
				Check:  testResourceQuota_initialCheck,
			},
			{
				Config: cfg(t, bs, fmt.Sprintf(testResourceQuota1, quotaEntityName, "3000000")),
				Check:  testResourceQuota_updateCheck,
				// A value change is an in-place update, not delete + create.
				ConfigPlanChecks: r.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("kafka_quota.test1", plancheck.ResourceActionUpdate)},
				},
			},
			{
				// Dropping a key removes just that value.
				Config: cfg(t, bs, fmt.Sprintf(`
resource "kafka_quota" "test1" {
  entity_name = "%s"
  entity_type = "client-id"
  config = {
    "consumer_byte_rate" = "3000000"
  }
}
`, quotaEntityName)),
				ConfigPlanChecks: r.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction("kafka_quota.test1", plancheck.ResourceActionUpdate)},
				},
				Check: func(*terraform.State) error {
					q, err := testProvider.client.DescribeQuota("client-id", quotaEntityName)
					if err != nil {
						return err
					}
					if len(q.Ops) != 1 || q.Ops[0].Key != "consumer_byte_rate" || q.Ops[0].Value != 3000000 {
						return fmt.Errorf("quota values = %+v, want only consumer_byte_rate=3000000", q.Ops)
					}
					return nil
				},
			},
		},
	})
}

func TestAcc_DefaultEntityBasicQuota(t *testing.T) {
	bs := testBootstrapServers[0]

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckQuotaDestroy,
		Steps: []r.TestStep{
			{
				Config: cfgs(t, bs, fmt.Sprintf(testResourceQuotaDefault, "4000000")),
				Check:  testResourceQuota_initialCheck,
			},
		},
	})
}

func TestAcc_DefaultEntityQuotaConfigUpdate(t *testing.T) {
	bs := testBootstrapServers[0]

	r.Test(t, r.TestCase{
		ProtoV6ProviderFactories: protoV6ProviderFactories(),
		PreCheck:                 func() { testAccPreCheck(t) },
		CheckDestroy:             testAccCheckQuotaDestroy,
		Steps: []r.TestStep{
			{
				Config: cfg(t, bs, fmt.Sprintf(testResourceQuotaDefault, "4000000")),
				Check:  testResourceQuota_initialCheck,
			},
			{
				Config: cfg(t, bs, fmt.Sprintf(testResourceQuotaDefault, "3000000")),
				Check:  testResourceQuota_updateCheck,
			},
		},
	})
}

func testResourceQuota_initialCheck(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["kafka_quota.test1"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	entityType := instanceState.Attributes["entity_type"]
	entityName := instanceState.Attributes["entity_name"]

	client := testProvider.Meta().(*LazyClient)
	quota, err := client.DescribeQuota(entityType, entityName)
	if err != nil {
		return err
	}

	id := instanceState.ID
	qID := fmt.Sprintf("%s|%s", entityName, entityType)
	if entityName == "" {
		qID = fmt.Sprintf("%s|%s", entityDefault, entityType)
	}

	if id != qID {
		return fmt.Errorf("id doesn't match for %s, got %s", id, qID)
	}

	if len(quota.Ops) != 2 {
		return fmt.Errorf("expected configs for %s, got %v", quota.EntityName, quota.Ops)
	}

	for _, q := range quota.Ops {
		if q.Key != "consumer_byte_rate" && q.Key != "producer_byte_rate" {
			return fmt.Errorf("consumer_byte_rate and producer_byte_rate is missing got: %v", q)
		}
		if q.Key == "consumer_byte_rate" && (q.Value != 4000000 || q.Remove) {
			return fmt.Errorf("consumer_byte_rate did not get set, expected 4000000 got: %v", q)
		}
		if q.Key == "producer_byte_rate" && (q.Value != 2500000 || q.Remove) {
			return fmt.Errorf("producer_byte_rate did not get set, expected 2500000 got: %v", q)
		}
	}

	return nil
}

func testResourceQuota_updateCheck(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["kafka_quota.test1"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	entityType := instanceState.Attributes["entity_type"]
	entityName := instanceState.Attributes["entity_name"]

	client := testProvider.Meta().(*LazyClient)
	quota, err := client.DescribeQuota(entityType, entityName)
	if err != nil {
		return err
	}

	id := instanceState.ID
	qID := fmt.Sprintf("%s|%s", entityName, entityType)
	if entityName == "" {
		qID = fmt.Sprintf("%s|%s", entityDefault, entityType)
	}

	if id != qID {
		return fmt.Errorf("id doesn't match for %s, got %s", id, qID)
	}

	if len(quota.Ops) != 2 {
		return fmt.Errorf("expected configs for %s, got %v", quota.EntityName, quota.Ops)
	}

	for _, q := range quota.Ops {
		if q.Key != "consumer_byte_rate" && q.Key != "producer_byte_rate" {
			return fmt.Errorf("consumer_byte_rate and producer_byte_rate is missing got: %v", q)
		}
		if q.Key == "consumer_byte_rate" && (q.Value != 3000000 || q.Remove) {
			return fmt.Errorf("consumer_byte_rate did not get set, expected 3000000 got: %v", q)
		}
		if q.Key == "producer_byte_rate" && (q.Value != 2500000 || q.Remove) {
			return fmt.Errorf("producer_byte_rate did not get set, expected 2500000 got: %v", q)
		}
	}

	return nil
}

func testAccCheckQuotaDestroy(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["kafka_quota.test1"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	entityType := instanceState.Attributes["entity_type"]
	entityName := instanceState.Attributes["entity_name"]

	meta := testProvider.Meta()
	if meta == nil {
		return fmt.Errorf("provider Meta() returned nil")
	}

	client := meta.(*LazyClient)
	_, err := client.DescribeQuota(entityType, entityName)

	if err == nil {
		return fmt.Errorf("quota was found")
	}

	if _, ok := err.(QuotaMissingError); !ok {
		return fmt.Errorf("quota was found %v", err.Error())
	}

	return nil
}

// lintignore:AT004
func cfgs(t *testing.T, bs string, extraCfg string) string {
	return cfg(t, bs, extraCfg)
}

const testResourceQuota1 = `
resource "kafka_quota" "test1" {
  entity_name               = "%s"
  entity_type               = "client-id"
  config = {
    "consumer_byte_rate" = "%s"
	"producer_byte_rate" = "2500000"
  }
}
`

const testResourceQuotaDefault = `
resource "kafka_quota" "test1" {
  entity_type               = "client-id"
  config = {
    "consumer_byte_rate" = "%s"
	"producer_byte_rate" = "2500000"
  }
}
`
