package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Read-only views of the cluster. The audit ones (cluster, acls, quotas,
// user_scram_credentials) mirror the list resources (same filters) for tools
// without `terraform query`, e.g. OpenTofu.

type baseDataSource struct {
	client *LazyClient
}

func (d *baseDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = clientFrom(req.ProviderData, &resp.Diagnostics)
}

func computedString(desc string) schema.StringAttribute {
	return schema.StringAttribute{Computed: true, Description: desc}
}

func optionalFilter(desc string) schema.StringAttribute {
	return schema.StringAttribute{Optional: true, Description: desc}
}

// filterID gives a list data source a stable ID derived from its filters.
func filterID(kind string, filters ...string) string {
	return kind + "|" + strings.Join(filters, "|")
}

// --- kafka_topic -------------------------------------------------------------

type topicDataSource struct{ baseDataSource }

func newTopicDataSource() datasource.DataSource { return &topicDataSource{} }

type topicDataModel struct {
	ID                types.String `tfsdk:"id"`
	Name              types.String `tfsdk:"name"`
	Partitions        types.Int64  `tfsdk:"partitions"`
	ReplicationFactor types.Int64  `tfsdk:"replication_factor"`
	Config            types.Map    `tfsdk:"config"`
}

func (d *topicDataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "kafka_topic"
}

func (d *topicDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Retrieves information about an existing Kafka topic including its configuration, partition count, and replication factor.",
		Attributes: map[string]schema.Attribute{
			"id":                 computedString("The topic name."),
			"name":               schema.StringAttribute{Required: true, Description: "The name of the topic."},
			"partitions":         schema.Int64Attribute{Computed: true, Description: "Number of partitions."},
			"replication_factor": schema.Int64Attribute{Computed: true, Description: "Number of replicas."},
			"config":             schema.MapAttribute{Computed: true, ElementType: types.StringType, Description: "A map of string k/v attributes."},
		},
	}
}

func (d *topicDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m topicDataModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	name := m.Name.ValueString()
	topic, err := d.client.ReadTopic(name, true)
	var missing TopicMissingError
	if errors.As(err, &missing) {
		resp.Diagnostics.AddError("Topic not found", fmt.Sprintf("could not find topic '%s'", name))
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading topic "+name, err.Error())
		return
	}
	config, diags := types.MapValueFrom(ctx, types.StringType, strPtrMapToStrMap(topic.Config))
	resp.Diagnostics.Append(diags...)
	m.ID = types.StringValue(name)
	m.Partitions = types.Int64Value(int64(topic.Partitions))
	m.ReplicationFactor = types.Int64Value(int64(topic.ReplicationFactor))
	m.Config = config
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// --- kafka_topics ------------------------------------------------------------

type topicsDataSource struct{ baseDataSource }

func newTopicsDataSource() datasource.DataSource { return &topicsDataSource{} }

var topicsElem = map[string]attr.Type{
	"topic_name":         types.StringType,
	"partitions":         types.Int64Type,
	"replication_factor": types.Int64Type,
	"config":             types.MapType{ElemType: types.StringType},
}

func (d *topicsDataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "kafka_topics"
}

func (d *topicsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Provides a list of all Kafka topics in the cluster.",
		Attributes: map[string]schema.Attribute{
			"id": computedString("The number of topics."),
			"list": schema.ListNestedAttribute{
				Computed:    true,
				Description: "A list containing all the topics.",
				NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
					"topic_name":         computedString("The name of the topic."),
					"partitions":         schema.Int64Attribute{Computed: true, Description: "Number of partitions."},
					"replication_factor": schema.Int64Attribute{Computed: true, Description: "Number of replicas."},
					"config":             schema.MapAttribute{Computed: true, ElementType: types.StringType, Description: "A map of string k/v attributes."},
				}},
			},
		},
	}
}

func (d *topicsDataSource) Read(ctx context.Context, _ datasource.ReadRequest, resp *datasource.ReadResponse) {
	topics, err := d.client.GetKafkaTopics()
	if err != nil {
		resp.Diagnostics.AddError("Listing topics", err.Error())
		return
	}
	items := make([]map[string]attr.Value, 0, len(topics))
	for _, t := range topics {
		config, diags := types.MapValueFrom(ctx, types.StringType, strPtrMapToStrMap(t.Config))
		resp.Diagnostics.Append(diags...)
		items = append(items, map[string]attr.Value{
			"topic_name":         types.StringValue(t.Name),
			"partitions":         types.Int64Value(int64(t.Partitions)),
			"replication_factor": types.Int64Value(int64(t.ReplicationFactor)),
			"config":             config,
		})
	}
	list := objectList(topicsElem, items, resp)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, pathRoot("list"), list)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, pathRoot("id"), fmt.Sprint(len(topics)))...)
}

// objectList builds a list of objects for a ListNestedAttribute.
func objectList(elem map[string]attr.Type, items []map[string]attr.Value, resp *datasource.ReadResponse) types.List {
	objType := types.ObjectType{AttrTypes: elem}
	values := make([]attr.Value, 0, len(items))
	for _, it := range items {
		o, diags := types.ObjectValue(elem, it)
		resp.Diagnostics.Append(diags...)
		values = append(values, o)
	}
	l, diags := types.ListValue(objType, values)
	resp.Diagnostics.Append(diags...)
	return l
}

// --- kafka_cluster -----------------------------------------------------------

type clusterDataSource struct{ baseDataSource }

func newClusterDataSource() datasource.DataSource { return &clusterDataSource{} }

var brokerElem = map[string]attr.Type{
	"id": types.Int64Type, "host": types.StringType, "port": types.Int64Type, "rack": types.StringType,
}

func (d *clusterDataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "kafka_cluster"
}

func (d *clusterDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Cluster ID, active controller and brokers of the Kafka cluster the provider is connected to.",
		Attributes: map[string]schema.Attribute{
			"id":            computedString("The cluster ID."),
			"cluster_id":    computedString("The cluster ID."),
			"controller_id": schema.Int64Attribute{Computed: true, Description: "Node ID of the active controller."},
			"brokers": schema.ListNestedAttribute{
				Computed:    true,
				Description: "Brokers as advertised to clients, sorted by ID.",
				NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
					"id":   schema.Int64Attribute{Computed: true, Description: "Broker node ID."},
					"host": computedString("Advertised host."),
					"port": schema.Int64Attribute{Computed: true, Description: "Advertised port."},
					"rack": computedString("Rack (`broker.rack`), empty if not set."),
				}},
			},
		},
	}
}

func (d *clusterDataSource) Read(ctx context.Context, _ datasource.ReadRequest, resp *datasource.ReadResponse) {
	info, err := d.client.DescribeCluster()
	if err != nil {
		resp.Diagnostics.AddError("Describing the cluster", err.Error())
		return
	}
	items := make([]map[string]attr.Value, 0, len(info.Brokers))
	for _, b := range info.Brokers {
		items = append(items, map[string]attr.Value{
			"id": types.Int64Value(int64(b.ID)), "host": types.StringValue(b.Host),
			"port": types.Int64Value(int64(b.Port)), "rack": types.StringValue(b.Rack),
		})
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, pathRoot("id"), info.ClusterID)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, pathRoot("cluster_id"), info.ClusterID)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, pathRoot("controller_id"), int64(info.ControllerID))...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, pathRoot("brokers"), objectList(brokerElem, items, resp))...)
}

// --- kafka_acls --------------------------------------------------------------

type aclsDataSource struct{ baseDataSource }

func newACLsDataSource() datasource.DataSource { return &aclsDataSource{} }

var aclElem = map[string]attr.Type{
	"id": types.StringType, "acl_principal": types.StringType, "acl_host": types.StringType,
	"acl_operation": types.StringType, "acl_permission_type": types.StringType, "resource_type": types.StringType,
	"resource_name": types.StringType, "resource_pattern_type_filter": types.StringType,
}

type aclsDataModel struct {
	ID                 types.String `tfsdk:"id"`
	ACLPrincipal       types.String `tfsdk:"acl_principal"`
	ResourceType       types.String `tfsdk:"resource_type"`
	ResourceNamePrefix types.String `tfsdk:"resource_name_prefix"`
	ACLs               types.List   `tfsdk:"acls"`
}

func (d *aclsDataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "kafka_acls"
}

func (d *aclsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists existing ACLs, one entry per ACL (the same shape as the `kafka_acl` resource). Filters combine with AND.",
		Attributes: map[string]schema.Attribute{
			"id":                   computedString("Derived from the filters."),
			"acl_principal":        optionalFilter("Only ACLs for this principal, e.g. `User:alice` (exact match)."),
			"resource_type":        optionalFilter("Only ACLs on this resource type: `Topic`, `Group`, `Cluster`, `TransactionalID` or `DelegationToken`."),
			"resource_name_prefix": optionalFilter("Only ACLs whose `resource_name` starts with this prefix."),
			"acls": schema.ListNestedAttribute{
				Computed:    true,
				Description: "Matching ACLs, sorted by ID.",
				NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
					"id":                           computedString("The `kafka_acl` resource ID (pipe-delimited), usable with `terraform import`."),
					"acl_principal":                computedString("Principal, e.g. `User:alice`."),
					"acl_host":                     computedString("Host the principal may connect from, `*` for any."),
					"acl_operation":                computedString("Operation, e.g. `Read`, `Write`, `All`."),
					"acl_permission_type":          computedString("`Allow` or `Deny`."),
					"resource_type":                computedString("`Topic`, `Group`, `Cluster`, `TransactionalID` or `DelegationToken`."),
					"resource_name":                computedString("Resource name or prefix."),
					"resource_pattern_type_filter": computedString("`Literal` or `Prefixed`."),
				}},
			},
		},
	}
}

func (d *aclsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m aclsDataModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := d.client.InvalidateACLCache(); err != nil {
		resp.Diagnostics.AddError("Listing ACLs", err.Error())
		return
	}
	res, err := d.client.ListACLs()
	if err != nil {
		resp.Diagnostics.AddError("Listing ACLs", err.Error())
		return
	}
	f := aclFilter{principal: m.ACLPrincipal.ValueString(), resourceType: m.ResourceType.ValueString(), namePrefix: m.ResourceNamePrefix.ValueString()}
	items := []map[string]attr.Value{}
	for _, a := range filterACLs(flattenACLs(res), f) {
		it := map[string]attr.Value{"id": types.StringValue(a.String())}
		for k, v := range aclAttributes(a) {
			it[k] = types.StringValue(v)
		}
		items = append(items, it)
	}
	m.ACLs = objectList(aclElem, items, resp)
	m.ID = types.StringValue(filterID("acls", f.principal, f.resourceType, f.namePrefix))
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// --- kafka_quotas ------------------------------------------------------------

type quotasDataSource struct{ baseDataSource }

func newQuotasDataSource() datasource.DataSource { return &quotasDataSource{} }

var quotaElem = map[string]attr.Type{
	"id": types.StringType, "entity_type": types.StringType, "entity_name": types.StringType,
	"default": types.BoolType, "config": types.MapType{ElemType: types.Float64Type},
}

type quotasDataModel struct {
	ID               types.String `tfsdk:"id"`
	EntityType       types.String `tfsdk:"entity_type"`
	EntityNamePrefix types.String `tfsdk:"entity_name_prefix"`
	Quotas           types.List   `tfsdk:"quotas"`
}

func (d *quotasDataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "kafka_quotas"
}

func (d *quotasDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists existing client quotas on a single entity, including default quotas. Quotas on combined entities (e.g. user + client-id) are not included.",
		Attributes: map[string]schema.Attribute{
			"id": computedString("Derived from the filters."),
			"entity_type": schema.StringAttribute{
				Optional:    true,
				Description: "Only quotas of this entity type: `user`, `client-id` or `ip`.",
				Validators:  []validator.String{stringvalidator.OneOf("client-id", "user", "ip")},
			},
			"entity_name_prefix": optionalFilter("Only quotas whose entity name starts with this prefix (excludes default quotas)."),
			"quotas": schema.ListNestedAttribute{
				Computed:    true,
				Description: "Matching quotas, sorted by ID.",
				NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
					"id":          computedString("The `kafka_quota` resource ID, usable with `terraform import`."),
					"entity_type": computedString("`user`, `client-id` or `ip`."),
					"entity_name": computedString("Entity name; empty for a default quota."),
					"default":     schema.BoolAttribute{Computed: true, Description: "Whether this is the default quota of the entity type."},
					"config":      schema.MapAttribute{Computed: true, ElementType: types.Float64Type, Description: "Quota values, e.g. `producer_byte_rate`."},
				}},
			},
		},
	}
}

func (d *quotasDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m quotasDataModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	all, err := d.client.ListQuotas()
	if err != nil {
		resp.Diagnostics.AddError("Listing quotas", err.Error())
		return
	}
	entityType, prefix := m.EntityType.ValueString(), m.EntityNamePrefix.ValueString()
	items := []map[string]attr.Value{}
	for _, q := range filterQuotas(all, entityType, prefix) {
		config := map[string]float64{}
		for _, op := range q.Ops {
			config[op.Key] = op.Value
		}
		cv, diags := types.MapValueFrom(ctx, types.Float64Type, config)
		resp.Diagnostics.Append(diags...)
		items = append(items, map[string]attr.Value{
			"id": types.StringValue(q.ID()), "entity_type": types.StringValue(q.EntityType),
			"entity_name": types.StringValue(q.EntityName), "default": types.BoolValue(q.EntityName == ""), "config": cv,
		})
	}
	m.Quotas = objectList(quotaElem, items, resp)
	m.ID = types.StringValue(filterID("quotas", entityType, prefix))
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// --- kafka_user_scram_credentials --------------------------------------------

type userScramCredentialsDataSource struct{ baseDataSource }

func newUserScramCredentialsDataSource() datasource.DataSource {
	return &userScramCredentialsDataSource{}
}

var scramElem = map[string]attr.Type{
	"id": types.StringType, "username": types.StringType, "scram_mechanism": types.StringType, "scram_iterations": types.Int64Type,
}

type scramsDataModel struct {
	ID             types.String `tfsdk:"id"`
	ScramMechanism types.String `tfsdk:"scram_mechanism"`
	UsernamePrefix types.String `tfsdk:"username_prefix"`
	Credentials    types.List   `tfsdk:"credentials"`
}

func (d *userScramCredentialsDataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "kafka_user_scram_credentials"
}

func (d *userScramCredentialsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Lists existing SCRAM credentials, one entry per user and mechanism. Passwords are never returned by Kafka.",
		Attributes: map[string]schema.Attribute{
			"id": computedString("Derived from the filters."),
			"scram_mechanism": schema.StringAttribute{
				Optional:    true,
				Description: "Only credentials of this mechanism: `SCRAM-SHA-256` or `SCRAM-SHA-512`.",
				Validators:  []validator.String{stringvalidator.OneOf("SCRAM-SHA-256", "SCRAM-SHA-512")},
			},
			"username_prefix": optionalFilter("Only users whose name starts with this prefix."),
			"credentials": schema.ListNestedAttribute{
				Computed:    true,
				Description: "Matching credentials, sorted by ID.",
				NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
					"id":               computedString("The `kafka_user_scram_credential` resource ID, usable with `terraform import`."),
					"username":         computedString("User name."),
					"scram_mechanism":  computedString("`SCRAM-SHA-256` or `SCRAM-SHA-512`."),
					"scram_iterations": schema.Int64Attribute{Computed: true, Description: "SCRAM iterations."},
				}},
			},
		},
	}
}

func (d *userScramCredentialsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m scramsDataModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	all, err := d.client.ListUserScramCredentials()
	if err != nil {
		resp.Diagnostics.AddError("Listing SCRAM credentials", err.Error())
		return
	}
	mechanism, prefix := m.ScramMechanism.ValueString(), m.UsernamePrefix.ValueString()
	items := []map[string]attr.Value{}
	for _, c := range filterSCRAM(all, mechanism, prefix) {
		items = append(items, map[string]attr.Value{
			"id": types.StringValue(c.ID()), "username": types.StringValue(c.Name),
			"scram_mechanism": types.StringValue(c.Mechanism.String()), "scram_iterations": types.Int64Value(int64(c.Iterations)),
		})
	}
	m.Credentials = objectList(scramElem, items, resp)
	m.ID = types.StringValue(filterID("scram", mechanism, prefix))
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// --- kafka_broker_config -----------------------------------------------------

type brokerConfigDataSource struct{ baseDataSource }

func newBrokerConfigDataSource() datasource.DataSource { return &brokerConfigDataSource{} }

var brokerConfigElem = map[string]attr.Type{
	"name": types.StringType, "value": types.StringType, "source": types.StringType,
	"read_only": types.BoolType, "sensitive": types.BoolType,
}

type brokerConfigDataModel struct {
	ID              types.String `tfsdk:"id"`
	BrokerID        types.Int64  `tfsdk:"broker_id"`
	IncludeDefaults types.Bool   `tfsdk:"include_defaults"`
	Configs         types.List   `tfsdk:"configs"`
}

func (d *brokerConfigDataSource) Metadata(_ context.Context, _ datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = "kafka_broker_config"
}

func (d *brokerConfigDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Effective configuration of one broker and where each value comes from: `dynamic_broker` (set for this broker), `dynamic_default` (cluster-wide dynamic default), `static` (`server.properties`) or `default` (Kafka default).",
		Attributes: map[string]schema.Attribute{
			"id":               computedString("The broker ID."),
			"broker_id":        schema.Int64Attribute{Required: true, Description: "Node ID of the broker."},
			"include_defaults": schema.BoolAttribute{Optional: true, Description: "Also list settings at their Kafka default. Default `false`."},
			"configs": schema.ListNestedAttribute{
				Computed:    true,
				Description: "Settings sorted by name. Sensitive values are empty (Kafka never returns them).",
				NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
					"name":      computedString("Setting name."),
					"value":     computedString("Effective value."),
					"source":    computedString("`dynamic_broker`, `dynamic_default`, `static` or `default`."),
					"read_only": schema.BoolAttribute{Computed: true, Description: "Only changeable in `server.properties` (restart)."},
					"sensitive": schema.BoolAttribute{Computed: true, Description: "Value is secret and not returned."},
				}},
			},
		},
	}
}

func (d *brokerConfigDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m brokerConfigDataModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	entries, err := d.client.BrokerConfigEntries(m.BrokerID.ValueInt64())
	if err != nil {
		resp.Diagnostics.AddError(fmt.Sprintf("Describing broker %d", m.BrokerID.ValueInt64()), err.Error())
		return
	}
	items := []map[string]attr.Value{}
	for _, e := range entries {
		if e.Source == "default" && !m.IncludeDefaults.ValueBool() {
			continue
		}
		items = append(items, map[string]attr.Value{
			"name": types.StringValue(e.Name), "value": types.StringValue(e.Value), "source": types.StringValue(e.Source),
			"read_only": types.BoolValue(e.ReadOnly), "sensitive": types.BoolValue(e.Sensitive),
		})
	}
	m.Configs = objectList(brokerConfigElem, items, resp)
	m.ID = types.StringValue(fmt.Sprint(m.BrokerID.ValueInt64()))
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}
