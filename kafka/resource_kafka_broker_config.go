package kafka

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// kafka_broker_config manages dynamic broker settings (applied without a
// restart, stored in the cluster metadata, overriding server.properties) at
// one level: one broker, or the cluster default. Only the keys in `config` are
// managed; other dynamic settings at that level are left alone.
type brokerConfigResource struct {
	client *LazyClient
}

var (
	_ resource.ResourceWithConfigure   = (*brokerConfigResource)(nil)
	_ resource.ResourceWithImportState = (*brokerConfigResource)(nil)
	_ resource.ResourceWithIdentity    = (*brokerConfigResource)(nil)
	_ resource.ResourceWithModifyPlan  = (*brokerConfigResource)(nil)
)

func newBrokerConfigResource() resource.Resource { return &brokerConfigResource{} }

const clusterDefaultID = "default"

type brokerConfigModel struct {
	ID       types.String `tfsdk:"id"`
	BrokerID types.Int64  `tfsdk:"broker_id"`
	Config   types.Map    `tfsdk:"config"`
}

type brokerConfigIdentityModel struct {
	Broker types.String `tfsdk:"broker"`
}

func (m brokerConfigModel) brokerID() *int64 {
	if m.BrokerID.IsNull() || m.BrokerID.IsUnknown() {
		return nil
	}
	v := m.BrokerID.ValueInt64()
	return &v
}

func brokerLevelID(brokerID *int64) string {
	if brokerID == nil {
		return clusterDefaultID
	}
	return strconv.FormatInt(*brokerID, 10)
}

func (r *brokerConfigResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_broker_config"
}

func (r *brokerConfigResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Dynamic broker settings: applied without a restart, stored in the cluster metadata and taking precedence over `server.properties`. " +
			"Without `broker_id` they are the cluster-wide defaults; with it, settings of that broker (which win over the defaults). " +
			"Only the keys in `config` are managed; removing a key reverts it to `server.properties` or the Kafka default. " +
			"Read-only settings (e.g. `log.dirs`, `node.id`) can only change in `server.properties` and are rejected at plan time.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "`default` for the cluster level, otherwise the broker ID.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"broker_id": schema.Int64Attribute{
				Optional:      true,
				Description:   "Node ID of the broker. Omit for the cluster-wide defaults.",
				PlanModifiers: []planmodifier.Int64{int64planmodifier.RequiresReplace()},
			},
			"config": schema.MapAttribute{
				Required:    true,
				ElementType: types.StringType,
				Description: "Dynamic settings, e.g. `log.retention.ms`, `message.max.bytes`, `min.insync.replicas`.",
			},
		},
	}
}

func (r *brokerConfigResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	resp.IdentitySchema = identityschema.Schema{
		Attributes: map[string]identityschema.Attribute{
			"broker": identityschema.StringAttribute{RequiredForImport: true, Description: "`default` for the cluster level, otherwise the broker ID."},
		},
	}
}

func (r *brokerConfigResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = clientFrom(req.ProviderData, &resp.Diagnostics)
}

func levelName(brokerID *int64) string {
	if brokerID == nil {
		return "cluster default"
	}
	return fmt.Sprintf("broker %d", *brokerID)
}

// ModifyPlan asks the brokers to validate the planned change (validate-only
// IncrementalAlterConfigs): read-only keys, keys that only exist per broker,
// and invalid values fail the plan instead of the apply.
func (r *brokerConfigResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.Plan.Raw.IsNull() || r.client == nil {
		return
	}
	var plan brokerConfigModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() || plan.Config.IsUnknown() || plan.BrokerID.IsUnknown() {
		return
	}
	set := stringMap(ctx, plan.Config, &resp.Diagnostics)
	var del []string
	if !req.State.Raw.IsNull() {
		var state brokerConfigModel
		resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
		if state.BrokerID.Equal(plan.BrokerID) {
			for k := range stringMap(ctx, state.Config, &resp.Diagnostics) {
				if _, keep := set[k]; !keep {
					del = append(del, k)
				}
			}
		}
	}

	sensitive, err := r.client.SensitiveBrokerConfigs()
	if err != nil {
		resp.Diagnostics.AddError("Reading broker config metadata", err.Error())
		return
	}
	var secret []string
	for k := range set {
		if sensitive[k] {
			secret = append(secret, k)
		}
	}
	if len(secret) > 0 {
		sort.Strings(secret)
		resp.Diagnostics.AddAttributeError(path.Root("config"), "Sensitive broker settings are not supported",
			fmt.Sprintf("%s: Kafka never returns these values, so they cannot be tracked in state. Set them in server.properties.", strings.Join(secret, ", ")))
		return
	}
	if err := r.client.AlterBrokerConfigs(plan.brokerID(), set, del, true); err != nil {
		resp.Diagnostics.AddAttributeError(path.Root("config"), "Kafka rejected the "+levelName(plan.brokerID())+" settings", err.Error())
	}
}

// waitApplied waits until the level shows the wanted values and none of the
// removed keys (the metadata change reaches every broker asynchronously).
func (r *brokerConfigResource) waitApplied(ctx context.Context, brokerID *int64, want map[string]string, gone []string) error {
	return waitFor(ctx, levelName(brokerID)+" settings to apply", r.client.timeout(), 0, time.Second, func() (bool, error) {
		have, err := r.client.DynamicBrokerConfigs(brokerID)
		if err != nil {
			return false, err
		}
		for k, v := range want {
			if have[k] != v {
				return false, nil
			}
		}
		for _, k := range gone {
			if _, ok := have[k]; ok {
				return false, nil
			}
		}
		return true, nil
	})
}

func (r *brokerConfigResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan brokerConfigModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	brokerID := plan.brokerID()
	set := stringMap(ctx, plan.Config, &resp.Diagnostics)
	if err := r.client.AlterBrokerConfigs(brokerID, set, nil, false); err != nil {
		resp.Diagnostics.AddError("Setting "+levelName(brokerID)+" settings", err.Error())
		return
	}
	if err := r.waitApplied(ctx, brokerID, set, nil); err != nil {
		resp.Diagnostics.AddError("Setting "+levelName(brokerID)+" settings", err.Error())
		return
	}
	plan.ID = types.StringValue(brokerLevelID(brokerID))
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, brokerConfigIdentityModel{Broker: plan.ID})...)
}

func (r *brokerConfigResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state brokerConfigModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	brokerID := state.brokerID()
	have, err := r.client.DynamicBrokerConfigs(brokerID)
	if err != nil {
		resp.Diagnostics.AddError("Reading "+levelName(brokerID)+" settings", err.Error())
		return
	}
	// Only managed keys; one reset outside the code drops out and shows as drift.
	current := map[string]string{}
	for k := range stringMap(ctx, state.Config, &resp.Diagnostics) {
		if v, ok := have[k]; ok {
			current[k] = v
		}
	}
	m, d := types.MapValueFrom(ctx, types.StringType, current)
	resp.Diagnostics.Append(d...)
	state.Config = m
	state.ID = types.StringValue(brokerLevelID(brokerID))
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, brokerConfigIdentityModel{Broker: state.ID})...)
}

func (r *brokerConfigResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state brokerConfigModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	brokerID := plan.brokerID()
	want := stringMap(ctx, plan.Config, &resp.Diagnostics)
	had := stringMap(ctx, state.Config, &resp.Diagnostics)
	set := map[string]string{}
	for k, v := range want {
		if old, ok := had[k]; !ok || old != v {
			set[k] = v
		}
	}
	var del []string
	for k := range had {
		if _, ok := want[k]; !ok {
			del = append(del, k)
		}
	}
	if err := r.client.AlterBrokerConfigs(brokerID, set, del, false); err != nil {
		resp.Diagnostics.AddError("Updating "+levelName(brokerID)+" settings", err.Error())
		return
	}
	if err := r.waitApplied(ctx, brokerID, want, del); err != nil {
		resp.Diagnostics.AddError("Updating "+levelName(brokerID)+" settings", err.Error())
		return
	}
	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, brokerConfigIdentityModel{Broker: plan.ID})...)
}

func (r *brokerConfigResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state brokerConfigModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	brokerID := state.brokerID()
	var del []string
	for k := range stringMap(ctx, state.Config, &resp.Diagnostics) {
		del = append(del, k)
	}
	if err := r.client.AlterBrokerConfigs(brokerID, nil, del, false); err != nil {
		resp.Diagnostics.AddError("Removing "+levelName(brokerID)+" settings", err.Error())
		return
	}
	if err := r.waitApplied(ctx, brokerID, nil, del); err != nil {
		resp.Diagnostics.AddError("Removing "+levelName(brokerID)+" settings", err.Error())
	}
}

// ImportState: `default` or a broker ID (or the identity). Every dynamic
// setting of that level becomes managed.
func (r *brokerConfigResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id := req.ID
	if id == "" {
		var ident brokerConfigIdentityModel
		resp.Diagnostics.Append(req.Identity.Get(ctx, &ident)...)
		if resp.Diagnostics.HasError() {
			return
		}
		id = ident.Broker.ValueString()
	}
	m := brokerConfigModel{ID: types.StringValue(id), BrokerID: types.Int64Null()}
	if id != clusterDefaultID {
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			resp.Diagnostics.AddError("Invalid import ID", fmt.Sprintf("expected %q or a broker ID, got %q", clusterDefaultID, id))
			return
		}
		m.BrokerID = types.Int64Value(n)
	}
	have, err := r.client.DynamicBrokerConfigs(m.brokerID())
	if err != nil {
		resp.Diagnostics.AddError("Reading "+levelName(m.brokerID())+" settings", err.Error())
		return
	}
	cfg, d := types.MapValueFrom(ctx, types.StringType, have)
	resp.Diagnostics.Append(d...)
	m.Config = cfg
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, brokerConfigIdentityModel{Broker: m.ID})...)
}
