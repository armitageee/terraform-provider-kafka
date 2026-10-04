package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type topicResource struct {
	client *LazyClient
}

var (
	_ resource.ResourceWithConfigure   = (*topicResource)(nil)
	_ resource.ResourceWithImportState = (*topicResource)(nil)
	_ resource.ResourceWithIdentity    = (*topicResource)(nil)
	_ resource.ResourceWithModifyPlan  = (*topicResource)(nil)
)

func newTopicResource() resource.Resource { return &topicResource{} }

type topicModel struct {
	ID                types.String `tfsdk:"id"`
	Name              types.String `tfsdk:"name"`
	Partitions        types.Int64  `tfsdk:"partitions"`
	ReplicationFactor types.Int64  `tfsdk:"replication_factor"`
	Config            types.Map    `tfsdk:"config"`
}

type topicIdentityModel struct {
	Name types.String `tfsdk:"name"`
}

func (r *topicResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_topic"
}

func (r *topicResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A resource for managing Kafka topics. Supports creating topics with custom configurations and increasing partition counts without recreation.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "The topic name.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"name": schema.StringAttribute{
				Required:      true,
				Description:   "The name of the topic.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"partitions": schema.Int64Attribute{
				Required:    true,
				Description: "Number of partitions.",
				Validators:  []validator.Int64{int64validator.AtLeast(1)},
				// Kafka can only add partitions; fewer means a new topic.
				PlanModifiers: []planmodifier.Int64{int64planmodifier.RequiresReplaceIf(
					func(_ context.Context, req planmodifier.Int64Request, resp *int64planmodifier.RequiresReplaceIfFuncResponse) {
						resp.RequiresReplace = req.PlanValue.ValueInt64() < req.StateValue.ValueInt64()
					},
					"Decreasing partitions recreates the topic.",
					"Decreasing partitions recreates the topic.",
				)},
			},
			"replication_factor": schema.Int64Attribute{
				Required:    true,
				Description: "Number of replicas. If using Confluent Kafka and setting placement constraints, set this to `-1`.",
				Validators:  []validator.Int64{replicationFactorValidator{}},
			},
			"config": schema.MapAttribute{
				Optional:    true,
				ElementType: types.StringType,
				Description: "A map of string k/v attributes.",
			},
		},
	}
}

func (r *topicResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	resp.IdentitySchema = identityschema.Schema{
		Attributes: map[string]identityschema.Attribute{
			"name": identityschema.StringAttribute{RequiredForImport: true, Description: "The name of the topic."},
		},
	}
}

func (r *topicResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = clientFrom(req.ProviderData, &resp.Diagnostics)
}

func modelToTopic(ctx context.Context, m topicModel) (Topic, error) {
	var diags diag.Diagnostics
	config := stringMap(ctx, m.Config, &diags)
	if diags.HasError() {
		return Topic{}, errors.New("invalid config map")
	}
	conf := make(map[string]*string, len(config))
	for k, v := range config {
		conf[k] = &v
	}
	return Topic{
		Name:              m.Name.ValueString(),
		Partitions:        int32(m.Partitions.ValueInt64()),
		ReplicationFactor: int16(m.ReplicationFactor.ValueInt64()),
		Config:            conf,
	}, nil
}

func (r *topicResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan topicModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	t, err := modelToTopic(ctx, plan)
	if err != nil {
		resp.Diagnostics.AddError("Invalid topic", err.Error())
		return
	}
	if err := r.client.CreateTopic(t); err != nil {
		resp.Diagnostics.AddError("Creating topic "+t.Name, err.Error())
		return
	}
	err = waitFor(ctx, "topic "+t.Name+" to be created", r.client.timeout(), time.Second, 2*time.Second, func() (bool, error) {
		_, err := r.client.ReadTopic(t.Name, true)
		var missing TopicMissingError
		if errors.As(err, &missing) {
			return false, nil
		}
		return err == nil, err
	})
	if err != nil {
		resp.Diagnostics.AddError("Creating topic "+t.Name, err.Error())
		return
	}
	plan.ID = types.StringValue(t.Name)
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, topicIdentityModel{Name: plan.Name})...)
}

func (r *topicResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state topicModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	name := state.ID.ValueString()
	topic, err := r.client.ReadTopic(name, false)
	var missing TopicMissingError
	if errors.As(err, &missing) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading topic "+name, err.Error())
		return
	}

	state.ID = types.StringValue(topic.Name)
	state.Name = types.StringValue(topic.Name)
	state.Partitions = types.Int64Value(int64(topic.Partitions))
	// -1 (Confluent placement constraints) stays as configured: Kafka
	// reports the real replica count, which the config does not pin.
	if state.ReplicationFactor.ValueInt64() != -1 || topic.ReplicationFactor <= 1 {
		state.ReplicationFactor = types.Int64Value(int64(topic.ReplicationFactor))
	}
	conf := strPtrMapToStrMap(topic.Config)
	if len(conf) == 0 && (state.Config.IsNull() || len(state.Config.Elements()) == 0) {
		// Keep null (no config attribute) or {} as the state had it.
	} else {
		m, d := types.MapValueFrom(ctx, types.StringType, conf)
		resp.Diagnostics.Append(d...)
		state.Config = m
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, topicIdentityModel{Name: state.Name})...)
}

// ModifyPlan: replication_factor changes in place only when the brokers can
// reassign partitions (Kafka >= 2.4); otherwise the topic is recreated.
func (r *topicResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.State.Raw.IsNull() || req.Plan.Raw.IsNull() || r.client == nil {
		return
	}
	var plan, state topicModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() || plan.ReplicationFactor.IsUnknown() {
		return
	}
	newRF := plan.ReplicationFactor.ValueInt64()
	if newRF == state.ReplicationFactor.ValueInt64() || newRF == -1 {
		return
	}
	canAlter, err := r.client.CanAlterReplicationFactor()
	if err != nil {
		resp.Diagnostics.AddError("Checking whether replication_factor can change in place", err.Error())
		return
	}
	if !canAlter {
		log.Println("[INFO] Need Kafka >= 2.4.0 to update replication_factor in-place")
		resp.RequiresReplace = append(resp.RequiresReplace, path.Root("replication_factor"))
	}
}

func (r *topicResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state topicModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	t, err := modelToTopic(ctx, plan)
	if err != nil {
		resp.Diagnostics.AddError("Invalid topic", err.Error())
		return
	}
	if err := r.client.UpdateTopic(t); err != nil {
		resp.Diagnostics.AddError("Updating topic "+t.Name, err.Error())
		return
	}

	// Replicas of existing partitions first, then new partitions.
	newRF := plan.ReplicationFactor.ValueInt64()
	if newRF != state.ReplicationFactor.ValueInt64() && newRF != -1 {
		log.Printf("[INFO] Updating replication_factor from %d to %d", state.ReplicationFactor.ValueInt64(), newRF)
		if err := r.client.AlterReplicationFactor(t); err != nil {
			resp.Diagnostics.AddError("Updating replication_factor of "+t.Name, err.Error())
			return
		}
		err := waitFor(ctx, "replication_factor of "+t.Name, r.client.timeout(), time.Second, 2*time.Second, func() (bool, error) {
			updating, err := r.client.IsReplicationFactorUpdating(t.Name)
			return !updating, err
		})
		if err != nil {
			resp.Diagnostics.AddError("Updating replication_factor of "+t.Name, err.Error())
			return
		}
	}
	if plan.Partitions.ValueInt64() != state.Partitions.ValueInt64() {
		log.Printf("[INFO] Updating partitions from %d to %d", state.Partitions.ValueInt64(), plan.Partitions.ValueInt64())
		if err := r.client.AddPartitions(t); err != nil {
			resp.Diagnostics.AddError("Adding partitions to "+t.Name, err.Error())
			return
		}
	}

	var last string
	err = waitFor(ctx, "topic "+t.Name+" to be updated", r.client.timeout(), time.Second, 2*time.Second, func() (bool, error) {
		actual, err := r.client.ReadTopic(t.Name, true)
		if err != nil {
			return false, err
		}
		last = fmt.Sprintf("%v != %v", strPtrMapToStrMap(actual.Config), strPtrMapToStrMap(t.Config))
		return t.Equal(actual), nil
	})
	if err != nil {
		resp.Diagnostics.AddError("Updating topic "+t.Name, fmt.Sprintf("%s (last seen: %s)", err, last))
		return
	}
	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, topicIdentityModel{Name: plan.Name})...)
}

func (r *topicResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state topicModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	name := state.Name.ValueString()
	if err := r.client.DeleteTopic(name); err != nil {
		resp.Diagnostics.AddError("Deleting topic "+name, err.Error())
		return
	}
	err := waitFor(ctx, "topic "+name+" to be deleted", 300*time.Second, 3*time.Second, 2*time.Second, func() (bool, error) {
		_, err := r.client.ReadTopic(name, true)
		var missing TopicMissingError
		if errors.As(err, &missing) {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		resp.Diagnostics.AddError("Deleting topic "+name, err.Error())
	}
}

// ImportState: by ID (the topic name) or by identity { name }.
func (r *topicResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughWithIdentity(ctx, path.Root("id"), path.Root("name"), req, resp)
	if req.ID != "" {
		resp.Diagnostics.Append(resp.Identity.Set(ctx, topicIdentityModel{Name: types.StringValue(req.ID)})...)
	}
}
