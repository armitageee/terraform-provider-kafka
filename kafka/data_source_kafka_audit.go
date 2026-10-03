package kafka

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

// Read-only views of what exists in the cluster. They mirror the list
// resources (same filters) for tools without `terraform query`, e.g. OpenTofu:
// audits, outputs, checks and preconditions.

func computedString(desc string) *schema.Schema {
	return &schema.Schema{Type: schema.TypeString, Computed: true, Description: desc}
}

func optionalFilter(desc string) *schema.Schema {
	return &schema.Schema{Type: schema.TypeString, Optional: true, Description: desc}
}

func kafkaClusterDataSource() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceClusterRead,
		Description: "Cluster ID, active controller and brokers of the Kafka cluster the provider is connected to.",
		Schema: map[string]*schema.Schema{
			"cluster_id":    computedString("The cluster ID."),
			"controller_id": {Type: schema.TypeInt, Computed: true, Description: "Node ID of the active controller."},
			"brokers": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "Brokers as advertised to clients, sorted by ID.",
				Elem: &schema.Resource{Schema: map[string]*schema.Schema{
					"id":   {Type: schema.TypeInt, Computed: true, Description: "Broker node ID."},
					"host": computedString("Advertised host."),
					"port": {Type: schema.TypeInt, Computed: true, Description: "Advertised port."},
					"rack": computedString("Rack (`broker.rack`), empty if not set."),
				}},
			},
		},
	}
}

func dataSourceClusterRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	info, err := meta.(*LazyClient).DescribeCluster()
	if err != nil {
		return diag.FromErr(err)
	}
	brokers := make([]any, 0, len(info.Brokers))
	for _, b := range info.Brokers {
		brokers = append(brokers, map[string]any{"id": int(b.ID), "host": b.Host, "port": b.Port, "rack": b.Rack})
	}
	errSet := errSetter{d: d}
	errSet.Set("cluster_id", info.ClusterID)
	errSet.Set("controller_id", int(info.ControllerID))
	errSet.Set("brokers", brokers)
	if errSet.err != nil {
		return diag.FromErr(errSet.err)
	}
	d.SetId(info.ClusterID)
	return nil
}

func kafkaACLsDataSource() *schema.Resource {
	elem := map[string]*schema.Schema{
		"acl_principal":                computedString("Principal, e.g. `User:alice`."),
		"acl_host":                     computedString("Host the principal may connect from, `*` for any."),
		"acl_operation":                computedString("Operation, e.g. `Read`, `Write`, `All`."),
		"acl_permission_type":          computedString("`Allow` or `Deny`."),
		"resource_type":                computedString("`Topic`, `Group`, `Cluster`, `TransactionalID` or `DelegationToken`."),
		"resource_name":                computedString("Resource name or prefix."),
		"resource_pattern_type_filter": computedString("`Literal` or `Prefixed`."),
	}
	elem["id"] = computedString("The `kafka_acl` resource ID (pipe-delimited), usable with `terraform import`.")
	return &schema.Resource{
		ReadContext: dataSourceACLsRead,
		Description: "Lists existing ACLs, one entry per ACL (the same shape as the `kafka_acl` resource). Filters combine with AND.",
		Schema: map[string]*schema.Schema{
			"acl_principal":        optionalFilter("Only ACLs for this principal, e.g. `User:alice` (exact match)."),
			"resource_type":        optionalFilter("Only ACLs on this resource type: `Topic`, `Group`, `Cluster`, `TransactionalID` or `DelegationToken`."),
			"resource_name_prefix": optionalFilter("Only ACLs whose `resource_name` starts with this prefix."),
			"acls": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "Matching ACLs, sorted by ID.",
				Elem:        &schema.Resource{Schema: elem},
			},
		},
	}
}

func dataSourceACLsRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*LazyClient)
	if err := c.InvalidateACLCache(); err != nil {
		return diag.FromErr(err)
	}
	res, err := c.ListACLs()
	if err != nil {
		return diag.FromErr(err)
	}
	f := aclFilter{
		principal:    d.Get("acl_principal").(string),
		resourceType: d.Get("resource_type").(string),
		namePrefix:   d.Get("resource_name_prefix").(string),
	}
	acls := filterACLs(flattenACLs(res), f)
	out := make([]any, 0, len(acls))
	for _, a := range acls {
		m := map[string]any{"id": a.String()}
		for k, v := range aclAttributes(a) {
			m[k] = v
		}
		out = append(out, m)
	}
	if err := d.Set("acls", out); err != nil {
		return diag.FromErr(err)
	}
	d.SetId(filterID("acls", f.principal, f.resourceType, f.namePrefix))
	return nil
}

func kafkaQuotasDataSource() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceQuotasRead,
		Description: "Lists existing client quotas on a single entity, including default quotas. Quotas on combined entities (e.g. user + client-id) are not included.",
		Schema: map[string]*schema.Schema{
			"entity_type": {
				Type:             schema.TypeString,
				Optional:         true,
				ValidateDiagFunc: validateDiagFunc(validation.StringInSlice([]string{"client-id", "user", "ip"}, false)),
				Description:      "Only quotas of this entity type: `user`, `client-id` or `ip`.",
			},
			"entity_name_prefix": optionalFilter("Only quotas whose entity name starts with this prefix (excludes default quotas)."),
			"quotas": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "Matching quotas, sorted by ID.",
				Elem: &schema.Resource{Schema: map[string]*schema.Schema{
					"id":          computedString("The `kafka_quota` resource ID, usable with `terraform import`."),
					"entity_type": computedString("`user`, `client-id` or `ip`."),
					"entity_name": computedString("Entity name; empty for a default quota."),
					"default":     {Type: schema.TypeBool, Computed: true, Description: "Whether this is the default quota of the entity type."},
					"config":      {Type: schema.TypeMap, Computed: true, Elem: &schema.Schema{Type: schema.TypeFloat}, Description: "Quota values, e.g. `producer_byte_rate`."},
				}},
			},
		},
	}
}

func dataSourceQuotasRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	all, err := meta.(*LazyClient).ListQuotas()
	if err != nil {
		return diag.FromErr(err)
	}
	entityType, prefix := d.Get("entity_type").(string), d.Get("entity_name_prefix").(string)
	quotas := filterQuotas(all, entityType, prefix)
	out := make([]any, 0, len(quotas))
	for _, q := range quotas {
		config := map[string]any{}
		for _, op := range q.Ops {
			config[op.Key] = op.Value
		}
		out = append(out, map[string]any{
			"id": q.ID(), "entity_type": q.EntityType, "entity_name": q.EntityName,
			"default": q.EntityName == "", "config": config,
		})
	}
	if err := d.Set("quotas", out); err != nil {
		return diag.FromErr(err)
	}
	d.SetId(filterID("quotas", entityType, prefix))
	return nil
}

func kafkaUserScramCredentialsDataSource() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceUserScramCredentialsRead,
		Description: "Lists existing SCRAM credentials, one entry per user and mechanism. Passwords are never returned by Kafka.",
		Schema: map[string]*schema.Schema{
			"scram_mechanism": {
				Type:             schema.TypeString,
				Optional:         true,
				ValidateDiagFunc: validateDiagFunc(validation.StringInSlice([]string{"SCRAM-SHA-256", "SCRAM-SHA-512"}, false)),
				Description:      "Only credentials of this mechanism: `SCRAM-SHA-256` or `SCRAM-SHA-512`.",
			},
			"username_prefix": optionalFilter("Only users whose name starts with this prefix."),
			"credentials": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "Matching credentials, sorted by ID.",
				Elem: &schema.Resource{Schema: map[string]*schema.Schema{
					"id":               computedString("The `kafka_user_scram_credential` resource ID, usable with `terraform import`."),
					"username":         computedString("User name."),
					"scram_mechanism":  computedString("`SCRAM-SHA-256` or `SCRAM-SHA-512`."),
					"scram_iterations": {Type: schema.TypeInt, Computed: true, Description: "SCRAM iterations."},
				}},
			},
		},
	}
}

func dataSourceUserScramCredentialsRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	all, err := meta.(*LazyClient).ListUserScramCredentials()
	if err != nil {
		return diag.FromErr(err)
	}
	mechanism, prefix := d.Get("scram_mechanism").(string), d.Get("username_prefix").(string)
	creds := filterSCRAM(all, mechanism, prefix)
	out := make([]any, 0, len(creds))
	for _, c := range creds {
		out = append(out, map[string]any{
			"id": c.ID(), "username": c.Name, "scram_mechanism": c.Mechanism.String(), "scram_iterations": int(c.Iterations),
		})
	}
	if err := d.Set("credentials", out); err != nil {
		return diag.FromErr(err)
	}
	d.SetId(filterID("scram", mechanism, prefix))
	return nil
}

// filterID gives a list data source a stable ID derived from its filters.
func filterID(kind string, filters ...string) string {
	return kind + "|" + strings.Join(filters, "|")
}
