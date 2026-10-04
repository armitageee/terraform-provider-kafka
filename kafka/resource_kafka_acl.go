package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

type aclResource struct {
	client *LazyClient
}

var (
	_ resource.ResourceWithConfigure    = (*aclResource)(nil)
	_ resource.ResourceWithImportState  = (*aclResource)(nil)
	_ resource.ResourceWithIdentity     = (*aclResource)(nil)
	_ resource.ResourceWithUpgradeState = (*aclResource)(nil)
)

func newACLResource() resource.Resource { return &aclResource{} }

type aclModel struct {
	ID                        types.String `tfsdk:"id"`
	ResourceName              types.String `tfsdk:"resource_name"`
	ResourceType              types.String `tfsdk:"resource_type"`
	ResourcePatternTypeFilter types.String `tfsdk:"resource_pattern_type_filter"`
	ACLPrincipal              types.String `tfsdk:"acl_principal"`
	ACLHost                   types.String `tfsdk:"acl_host"`
	ACLOperation              types.String `tfsdk:"acl_operation"`
	ACLPermissionType         types.String `tfsdk:"acl_permission_type"`
}

type aclIdentityModel struct {
	ACLPrincipal              types.String `tfsdk:"acl_principal"`
	ACLHost                   types.String `tfsdk:"acl_host"`
	ACLOperation              types.String `tfsdk:"acl_operation"`
	ACLPermissionType         types.String `tfsdk:"acl_permission_type"`
	ResourceType              types.String `tfsdk:"resource_type"`
	ResourceName              types.String `tfsdk:"resource_name"`
	ResourcePatternTypeFilter types.String `tfsdk:"resource_pattern_type_filter"`
}

// aclIdentityAttributes are in ID order (see StringlyTypedACL.String).
var aclIdentityAttributes = []string{
	"acl_principal",
	"acl_host",
	"acl_operation",
	"acl_permission_type",
	"resource_type",
	"resource_name",
	"resource_pattern_type_filter",
}

func aclAttributes(a StringlyTypedACL) map[string]string {
	return map[string]string{
		"acl_principal":                a.ACL.Principal,
		"acl_host":                     a.ACL.Host,
		"acl_operation":                a.ACL.Operation,
		"acl_permission_type":          a.ACL.PermissionType,
		"resource_type":                a.Type,
		"resource_name":                a.Name,
		"resource_pattern_type_filter": a.PatternTypeFilter,
	}
}

func (m aclModel) acl() StringlyTypedACL {
	return StringlyTypedACL{
		ACL: ACL{
			Principal:      m.ACLPrincipal.ValueString(),
			Host:           m.ACLHost.ValueString(),
			Operation:      m.ACLOperation.ValueString(),
			PermissionType: m.ACLPermissionType.ValueString(),
		},
		Resource: Resource{
			Type:              m.ResourceType.ValueString(),
			Name:              m.ResourceName.ValueString(),
			PatternTypeFilter: m.ResourcePatternTypeFilter.ValueString(),
		},
	}
}

func aclToModel(a StringlyTypedACL) aclModel {
	return aclModel{
		ID:                        types.StringValue(a.String()),
		ResourceName:              types.StringValue(a.Name),
		ResourceType:              types.StringValue(a.Type),
		ResourcePatternTypeFilter: types.StringValue(a.PatternTypeFilter),
		ACLPrincipal:              types.StringValue(a.ACL.Principal),
		ACLHost:                   types.StringValue(a.ACL.Host),
		ACLOperation:              types.StringValue(a.ACL.Operation),
		ACLPermissionType:         types.StringValue(a.ACL.PermissionType),
	}
}

func aclIdentity(a StringlyTypedACL) aclIdentityModel {
	return aclIdentityModel{
		ACLPrincipal:              types.StringValue(a.ACL.Principal),
		ACLHost:                   types.StringValue(a.ACL.Host),
		ACLOperation:              types.StringValue(a.ACL.Operation),
		ACLPermissionType:         types.StringValue(a.ACL.PermissionType),
		ResourceType:              types.StringValue(a.Type),
		ResourceName:              types.StringValue(a.Name),
		ResourcePatternTypeFilter: types.StringValue(a.PatternTypeFilter),
	}
}

func (r *aclResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_acl"
}

func (r *aclResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Version:     1,
		Description: "A resource for managing Kafka Access Control Lists (ACLs) to control permissions for users and applications.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "The ACL as `acl_principal|acl_host|acl_operation|acl_permission_type|resource_type|resource_name|resource_pattern_type_filter`.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"resource_name": schema.StringAttribute{Required: true, Description: "The name of the resource", PlanModifiers: replace},
			"resource_type": schema.StringAttribute{Required: true, PlanModifiers: replace},
			"resource_pattern_type_filter": schema.StringAttribute{
				Optional:      true,
				Computed:      true,
				Default:       stringdefault.StaticString("Literal"),
				Description:   "How to match the resource name. Valid values: Literal (exact match) or Prefixed (match resources with the given prefix).",
				Validators:    []validator.String{stringvalidator.OneOf("Literal", "Prefixed")},
				PlanModifiers: replace,
			},
			"acl_principal":       schema.StringAttribute{Required: true, PlanModifiers: replace},
			"acl_host":            schema.StringAttribute{Required: true, PlanModifiers: replace},
			"acl_operation":       schema.StringAttribute{Required: true, PlanModifiers: replace},
			"acl_permission_type": schema.StringAttribute{Required: true, PlanModifiers: replace},
		},
	}
}

func (r *aclResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	attrs := map[string]identityschema.Attribute{}
	for _, k := range aclIdentityAttributes {
		attrs[k] = identityschema.StringAttribute{RequiredForImport: true}
	}
	resp.IdentitySchema = identityschema.Schema{Attributes: attrs}
}

func (r *aclResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = clientFrom(req.ProviderData, &resp.Diagnostics)
}

// aclVisible reports whether exactly this ACL is listed by the cluster.
func (r *aclResource) aclVisible(a StringlyTypedACL) (bool, error) {
	if err := r.client.InvalidateACLCache(); err != nil {
		return false, err
	}
	res, err := r.client.ListACLs()
	if err != nil {
		return false, err
	}
	for _, found := range flattenACLs(res) {
		if found.String() == a.String() {
			return true, nil
		}
	}
	return false, nil
}

func (r *aclResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan aclModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	a := plan.acl()
	log.Printf("[INFO] Creating ACL %s", a)
	if err := r.client.CreateACL(a); err != nil {
		resp.Diagnostics.AddError("Creating ACL "+a.String(), err.Error())
		return
	}
	err := waitFor(ctx, "ACL "+a.String()+" to be visible", 2*time.Second, 0, 200*time.Millisecond, func() (bool, error) {
		return r.aclVisible(a)
	})
	if err != nil {
		resp.Diagnostics.AddError("Creating ACL "+a.String(), err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, aclToModel(a))...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, aclIdentity(a))...)
}

func (r *aclResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state aclModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	a := state.acl()
	res, err := r.client.ListACLs()
	if err != nil {
		resp.Diagnostics.AddError("Reading ACLs", err.Error())
		return
	}
	for _, found := range flattenACLs(res) {
		if found.String() == a.String() {
			resp.Diagnostics.Append(resp.State.Set(ctx, aclToModel(found))...)
			resp.Diagnostics.Append(resp.Identity.Set(ctx, aclIdentity(found))...)
			return
		}
	}
	log.Printf("[INFO] Did not find ACL %s", a)
	resp.State.RemoveResource(ctx)
}

// Update is never called with changes: every attribute requires replacement.
func (r *aclResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan aclModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func (r *aclResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state aclModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	a := state.acl()
	log.Printf("[INFO] Deleting ACL %s", a)
	if err := r.client.DeleteACL(a); err != nil {
		resp.Diagnostics.AddError("Deleting ACL "+a.String(), err.Error())
		return
	}
	err := waitFor(ctx, "ACL "+a.String()+" to be removed", 2*time.Second, 0, 200*time.Millisecond, func() (bool, error) {
		visible, err := r.aclVisible(a)
		return !visible, err
	})
	if err != nil {
		resp.Diagnostics.AddError("Deleting ACL "+a.String(), err.Error())
	}
}

// ImportState: the pipe-delimited ID or the identity (same seven fields).
func (r *aclResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	var a StringlyTypedACL
	if req.ID != "" {
		parts := strings.Split(req.ID, "|")
		if len(parts) != len(aclIdentityAttributes) {
			resp.Diagnostics.AddError("Invalid import ID", fmt.Sprintf(
				"expected format is acl_principal|acl_host|acl_operation|acl_permission_type|resource_type|resource_name|resource_pattern_type_filter - got %v segments instead of 7", len(parts)))
			return
		}
		a = StringlyTypedACL{
			ACL:      ACL{Principal: parts[0], Host: parts[1], Operation: parts[2], PermissionType: parts[3]},
			Resource: Resource{Type: parts[4], Name: parts[5], PatternTypeFilter: parts[6]},
		}
	} else {
		var id aclIdentityModel
		resp.Diagnostics.Append(req.Identity.Get(ctx, &id)...)
		if resp.Diagnostics.HasError() {
			return
		}
		a = StringlyTypedACL{
			ACL: ACL{Principal: id.ACLPrincipal.ValueString(), Host: id.ACLHost.ValueString(),
				Operation: id.ACLOperation.ValueString(), PermissionType: id.ACLPermissionType.ValueString()},
			Resource: Resource{Type: id.ResourceType.ValueString(), Name: id.ResourceName.ValueString(),
				PatternTypeFilter: id.ResourcePatternTypeFilter.ValueString()},
		}
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, aclToModel(a))...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, aclIdentity(a))...)
}

// UpgradeState: schema version 0 (provider < 0.2) had no
// resource_pattern_type_filter; those ACLs are Literal.
func (r *aclResource) UpgradeState(context.Context) map[int64]resource.StateUpgrader {
	return map[int64]resource.StateUpgrader{
		0: {StateUpgrader: upgradeACLStateV0},
	}
}

func upgradeACLStateV0(_ context.Context, req resource.UpgradeStateRequest, resp *resource.UpgradeStateResponse) {
	attrs := map[string]any{}
	switch {
	case req.RawState == nil:
		resp.Diagnostics.AddError("Upgrading kafka_acl state", "no prior state")
		return
	case req.RawState.JSON != nil:
		if err := json.Unmarshal(req.RawState.JSON, &attrs); err != nil {
			resp.Diagnostics.AddError("Upgrading kafka_acl state", err.Error())
			return
		}
	default:
		for k, v := range req.RawState.Flatmap {
			attrs[k] = v
		}
	}
	if v, ok := attrs["resource_pattern_type_filter"]; !ok || v == nil || v == "" {
		attrs["resource_pattern_type_filter"] = "Literal"
	}
	out := map[string]any{}
	for _, k := range append([]string{"id"}, aclIdentityAttributes...) {
		out[k] = attrs[k]
	}
	raw, err := json.Marshal(out)
	if err != nil {
		resp.Diagnostics.AddError("Upgrading kafka_acl state", err.Error())
		return
	}
	resp.DynamicValue = &tfprotov6.DynamicValue{JSON: raw}
}
