package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type quotaResource struct {
	client *LazyClient
}

var (
	_ resource.ResourceWithConfigure   = (*quotaResource)(nil)
	_ resource.ResourceWithImportState = (*quotaResource)(nil)
	_ resource.ResourceWithIdentity    = (*quotaResource)(nil)
)

func newQuotaResource() resource.Resource { return &quotaResource{} }

type quotaModel struct {
	ID         types.String `tfsdk:"id"`
	EntityName types.String `tfsdk:"entity_name"`
	EntityType types.String `tfsdk:"entity_type"`
	Config     types.Map    `tfsdk:"config"`
}

// Identity entity_name is "" for a default quota (as written since 0.17).
type quotaIdentityModel struct {
	EntityType types.String `tfsdk:"entity_type"`
	EntityName types.String `tfsdk:"entity_name"`
}

func (r *quotaResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_quota"
}

func (r *quotaResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A resource for managing Kafka quotas to control resource usage by clients, users, or IP addresses.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "`entity_name|entity_type`, `entity-default|entity_type` for a default quota.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"entity_name": schema.StringAttribute{
				Optional:      true,
				Description:   "The name of the entity (if entity_name is not provided, it will create entity-default Kafka quota)",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"entity_type": schema.StringAttribute{
				Required:      true,
				Description:   "The type of the entity (client-id, user, ip)",
				Validators:    []validator.String{stringvalidator.OneOf("client-id", "user", "ip")},
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"config": schema.MapAttribute{
				Optional:      true,
				ElementType:   types.Float64Type,
				Description:   "A map of string k/v properties.",
				PlanModifiers: []planmodifier.Map{mapplanmodifier.RequiresReplace()},
			},
		},
	}
}

func (r *quotaResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	resp.IdentitySchema = identityschema.Schema{
		Attributes: map[string]identityschema.Attribute{
			"entity_type": identityschema.StringAttribute{RequiredForImport: true, Description: "client-id, user or ip."},
			"entity_name": identityschema.StringAttribute{OptionalForImport: true, Description: "Empty or omitted for the default quota of the type."},
		},
	}
}

func (r *quotaResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = clientFrom(req.ProviderData, &resp.Diagnostics)
}

func quotaFromModel(ctx context.Context, m quotaModel, remove bool, diags *diag.Diagnostics) Quota {
	q := Quota{EntityType: m.EntityType.ValueString(), EntityName: m.EntityName.ValueString()}
	for k, v := range float64Map(ctx, m.Config, diags) {
		q.Ops = append(q.Ops, QuotaOp{Key: k, Value: v, Remove: remove})
	}
	return q
}

// waitQuota waits until the quota is (present=true) or is not visible.
func (r *quotaResource) waitQuota(ctx context.Context, q Quota, present bool) error {
	what := "quota " + q.ID()
	if present {
		what += " to be created"
	} else {
		what += " to be deleted"
	}
	return waitFor(ctx, what, r.client.timeout(), time.Second, 2*time.Second, func() (bool, error) {
		_, err := r.client.DescribeQuota(q.EntityType, q.EntityName)
		var missing QuotaMissingError
		if errors.As(err, &missing) {
			return !present, nil
		}
		if err != nil {
			return false, err
		}
		return present, nil
	})
}

func (r *quotaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan quotaModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	q := quotaFromModel(ctx, plan, false, &resp.Diagnostics)
	log.Printf("[INFO] Creating Quota %s", q)
	if err := r.client.AlterQuota(q); err != nil {
		resp.Diagnostics.AddError("Creating quota "+q.ID(), err.Error())
		return
	}
	// AlterClientQuotas returns before every broker applied the change.
	if err := r.waitQuota(ctx, q, true); err != nil {
		resp.Diagnostics.AddError("Creating quota "+q.ID(), err.Error())
		return
	}
	plan.ID = types.StringValue(q.ID())
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, quotaIdentityModel{
		EntityType: types.StringValue(q.EntityType), EntityName: types.StringValue(q.EntityName),
	})...)
}

func (r *quotaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state quotaModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	found, err := r.client.DescribeQuota(state.EntityType.ValueString(), state.EntityName.ValueString())
	var missing QuotaMissingError
	if errors.As(err, &missing) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading quota "+state.ID.ValueString(), err.Error())
		return
	}
	config := map[string]float64{}
	for _, op := range found.Ops {
		config[op.Key] = op.Value
	}
	if len(config) > 0 || !state.Config.IsNull() {
		m, d := types.MapValueFrom(ctx, types.Float64Type, config)
		resp.Diagnostics.Append(d...)
		state.Config = m
	}
	state.EntityType = types.StringValue(found.EntityType)
	// A default quota keeps entity_name unset (null), like a config without it.
	if found.EntityName != "" {
		state.EntityName = types.StringValue(found.EntityName)
	} else {
		state.EntityName = types.StringNull()
	}
	state.ID = types.StringValue(found.ID())
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, quotaIdentityModel{
		EntityType: types.StringValue(found.EntityType), EntityName: types.StringValue(found.EntityName),
	})...)
}

// Update is never called with changes: every attribute requires replacement.
func (r *quotaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan quotaModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func (r *quotaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state quotaModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	q := quotaFromModel(ctx, state, true, &resp.Diagnostics)
	log.Printf("[INFO] Deleting quota %s", q)
	if err := r.client.AlterQuota(q); err != nil {
		resp.Diagnostics.AddError("Deleting quota "+q.ID(), err.Error())
		return
	}
	if err := r.waitQuota(ctx, q, false); err != nil {
		resp.Diagnostics.AddError("Deleting quota "+q.ID(), err.Error())
	}
}

// parseQuotaImportID accepts the resource ID (`name|type`,
// `entity-default|type`) and the documented `type:name` / `type:` form.
func parseQuotaImportID(id string) (entityType, entityName string, err error) {
	switch {
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
		return "", "", fmt.Errorf("expected entity_name|entity_type (entity-default|entity_type for a default quota) or entity_type:entity_name, got %q", id)
	}
	switch entityType {
	case "client-id", "user", "ip":
		return entityType, entityName, nil
	}
	return "", "", fmt.Errorf("entity type %q in %q is not one of client-id, user, ip", entityType, id)
}

func (r *quotaResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	var entityType, entityName string
	if req.ID != "" {
		var err error
		entityType, entityName, err = parseQuotaImportID(req.ID)
		if err != nil {
			resp.Diagnostics.AddError("Invalid import ID", err.Error())
			return
		}
	} else {
		var id quotaIdentityModel
		resp.Diagnostics.Append(req.Identity.Get(ctx, &id)...)
		if resp.Diagnostics.HasError() {
			return
		}
		entityType, entityName = id.EntityType.ValueString(), id.EntityName.ValueString()
	}
	m := quotaModel{
		ID:         types.StringValue(Quota{EntityType: entityType, EntityName: entityName}.ID()),
		EntityType: types.StringValue(entityType),
		EntityName: types.StringNull(),
		Config:     types.MapNull(types.Float64Type),
	}
	if entityName != "" {
		m.EntityName = types.StringValue(entityName)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, quotaIdentityModel{
		EntityType: types.StringValue(entityType), EntityName: types.StringValue(entityName),
	})...)
}
