package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func kafkaQuotaResource() *schema.Resource {
	//lintignore:R011
	return &schema.Resource{
		CreateContext: quotaCreate,
		ReadContext:   quotaRead,
		DeleteContext: quotaDelete,
		Importer: &schema.ResourceImporter{
			StateContext: importQuota,
		},
		// Identity: entity type and name (empty name = the default quota of
		// that type). Used by `terraform query` and import blocks.
		Identity: &schema.ResourceIdentity{
			SchemaFunc: func() map[string]*schema.Schema {
				return map[string]*schema.Schema{
					"entity_type": {Type: schema.TypeString, RequiredForImport: true, Description: "client-id, user or ip."},
					"entity_name": {Type: schema.TypeString, OptionalForImport: true, Description: "Empty or omitted for the default quota of the type."},
				}
			},
		},
		Schema: map[string]*schema.Schema{
			"entity_name": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The name of the entity (if entity_name is not provided, it will create entity-default Kafka quota)",
			},
			"entity_type": {
				Type:             schema.TypeString,
				Required:         true,
				ForceNew:         true,
				ValidateDiagFunc: validateDiagFunc(validation.StringInSlice([]string{"client-id", "user", "ip"}, false)),
				Description:      "The type of the entity (client-id, user, ip)",
			},
			"config": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    true,
				Description: "A map of string k/v properties.",
				Elem:        schema.TypeFloat,
			},
		},
	}
}

func quotaCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*LazyClient)
	quota := newQuota(d, false)
	log.Printf("[INFO] Creating Quota %s", quota)

	err := c.AlterQuota(quota)
	if err != nil {
		log.Println("[ERROR] Failed to create Quota")
		return diag.FromErr(err)
	}

	stateConf := &retry.StateChangeConf{
		Pending:      []string{"Pending"},
		Target:       []string{"Created"},
		Refresh:      quotaCreatedFunc(c, quota),
		Timeout:      time.Duration(c.Config.Timeout) * time.Second,
		Delay:        1 * time.Second,
		PollInterval: 2 * time.Second,
	}

	if _, err := stateConf.WaitForStateContext(ctx); err != nil {
		return diag.FromErr(fmt.Errorf("error waiting for quota (%s) to be created: %s", quota.ID(), err))
	}

	d.SetId(quota.ID())

	return diag.FromErr(setQuotaIdentity(d, quota.EntityType, quota.EntityName))
}

func quotaCreatedFunc(client *LazyClient, q Quota) retry.StateRefreshFunc {
	return func() (result interface{}, s string, err error) {
		fq, err := client.DescribeQuota(q.EntityType, q.EntityName)
		switch e := err.(type) {
		case QuotaMissingError:
			return fq, "Pending", nil
		case nil:
			return fq, "Created", nil
		default:
			return fq, "Error", e
		}
	}
}

func quotaDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*LazyClient)
	quota := newQuota(d, true)
	log.Printf("[INFO] Deleting quota %s", quota)

	err := c.AlterQuota(quota)
	if err != nil {
		log.Println("[ERROR] Failed to delete Quota")
		return diag.FromErr(err)
	}

	// Like create: AlterClientQuotas returns before every broker has applied
	// the change, and a Describe on a lagging broker still sees the quota.
	stateConf := &retry.StateChangeConf{
		Pending:      []string{"Present"},
		Target:       []string{"Deleted"},
		Refresh:      quotaDeletedFunc(c, quota),
		Timeout:      time.Duration(c.Config.Timeout) * time.Second,
		Delay:        1 * time.Second,
		PollInterval: 2 * time.Second,
	}
	if _, err := stateConf.WaitForStateContext(ctx); err != nil {
		return diag.FromErr(fmt.Errorf("error waiting for quota (%s) to be deleted: %w", quota.ID(), err))
	}

	return nil
}

func quotaDeletedFunc(client *LazyClient, q Quota) retry.StateRefreshFunc {
	return func() (interface{}, string, error) {
		_, err := client.DescribeQuota(q.EntityType, q.EntityName)
		switch err.(type) {
		case QuotaMissingError:
			return q, "Deleted", nil
		case nil:
			return q, "Present", nil
		default:
			return nil, "Error", err
		}
	}
}

func quotaRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	log.Println("[INFO] Reading Quota")
	c := meta.(*LazyClient)

	entityType := d.Get("entity_type").(string)
	entityName := d.Get("entity_name").(string)
	log.Printf("[INFO] Reading Quota %s", entityName)

	foundQuota, err := c.DescribeQuota(entityType, entityName)
	if err != nil {
		log.Printf("[ERROR] Error getting quota %s from Kafka", err)
		_, ok := err.(QuotaMissingError)
		if ok {
			d.SetId("")
			return nil
		}

		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] Setting the state from Kafka %v", foundQuota)
	configs := map[string]float64{}
	for _, op := range foundQuota.Ops {
		configs[op.Key] = op.Value
	}

	errSet := errSetter{d: d}
	if foundQuota.EntityName != "" {
		// A default quota keeps entity_name unset; "" would differ from the
		// state of a config without entity_name (and show up after import).
		errSet.Set("entity_name", foundQuota.EntityName)
	}
	errSet.Set("entity_type", foundQuota.EntityType)
	errSet.Set("config", configs)
	if errSet.err != nil {
		return diag.FromErr(errSet.err)
	}

	log.Printf("[INFO] Found Quota %s %+v.", foundQuota.ID(), foundQuota.Ops)
	return diag.FromErr(setQuotaIdentity(d, foundQuota.EntityType, foundQuota.EntityName))
}

// importQuota accepts the resource ID (`name|type`, `entity-default|type`),
// the `type:name` / `type:` form from the docs, or an identity.
func importQuota(ctx context.Context, d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	var entityType, entityName string
	id := d.Id()
	switch {
	case id == "":
		identity, err := d.Identity()
		if err != nil {
			return nil, err
		}
		entityType, _ = identity.Get("entity_type").(string)
		entityName, _ = identity.Get("entity_name").(string)
	case strings.Count(id, "|") == 1:
		parts := strings.SplitN(id, "|", 2)
		entityName, entityType = parts[0], parts[1]
		if entityName == entityDefault {
			entityName = ""
		}
	case strings.Count(id, ":") >= 1:
		parts := strings.SplitN(id, ":", 2)
		entityType, entityName = parts[0], parts[1]
	default:
		return nil, fmt.Errorf("failed importing quota %q; expected entity_name|entity_type (entity-default|entity_type for a default quota) or entity_type:entity_name", id)
	}
	switch entityType {
	case "client-id", "user", "ip":
	default:
		return nil, fmt.Errorf("failed importing quota %q: entity type %q is not one of client-id, user, ip", id, entityType)
	}

	errSet := errSetter{d: d}
	errSet.Set("entity_type", entityType)
	if entityName != "" {
		// Unset (not "") for a default quota, like a config without entity_name.
		errSet.Set("entity_name", entityName)
	}
	if errSet.err != nil {
		return nil, errSet.err
	}
	d.SetId(Quota{EntityType: entityType, EntityName: entityName}.ID())
	if err := setQuotaIdentity(d, entityType, entityName); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func setQuotaIdentity(d *schema.ResourceData, entityType, entityName string) error {
	identity, err := d.Identity()
	if err != nil {
		return err
	}
	if err := identity.Set("entity_type", entityType); err != nil {
		return err
	}
	return identity.Set("entity_name", entityName)
}

func newQuota(d *schema.ResourceData, removeAll bool) Quota {
	config := d.Get("config").(map[string]interface{})
	ops := []QuotaOp{}
	for key, value := range config {
		switch value := value.(type) {
		case float64:
			ops = append(ops, QuotaOp{
				Key:    key,
				Value:  value,
				Remove: removeAll,
			})
		}
	}

	return Quota{
		EntityType: d.Get("entity_type").(string),
		EntityName: d.Get("entity_name").(string),
		Ops:        ops,
	}
}
