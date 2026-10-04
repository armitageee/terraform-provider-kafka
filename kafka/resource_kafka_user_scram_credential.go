package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

const defaultIterations int32 = 4096

type userScramCredentialResource struct {
	client *LazyClient
}

var (
	_ resource.ResourceWithConfigure      = (*userScramCredentialResource)(nil)
	_ resource.ResourceWithImportState    = (*userScramCredentialResource)(nil)
	_ resource.ResourceWithIdentity       = (*userScramCredentialResource)(nil)
	_ resource.ResourceWithValidateConfig = (*userScramCredentialResource)(nil)
)

func newUserScramCredentialResource() resource.Resource { return &userScramCredentialResource{} }

type scramModel struct {
	ID                types.String `tfsdk:"id"`
	Username          types.String `tfsdk:"username"`
	ScramMechanism    types.String `tfsdk:"scram_mechanism"`
	ScramIterations   types.Int64  `tfsdk:"scram_iterations"`
	Password          types.String `tfsdk:"password"`
	PasswordWo        types.String `tfsdk:"password_wo"`
	PasswordWoVersion types.String `tfsdk:"password_wo_version"`
}

type scramIdentityModel struct {
	Username       types.String `tfsdk:"username"`
	ScramMechanism types.String `tfsdk:"scram_mechanism"`
}

func (r *userScramCredentialResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_user_scram_credential"
}

func (r *userScramCredentialResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	notBlank := stringvalidator.RegexMatches(nonBlank, "must not be empty or whitespace")
	resp.Schema = schema.Schema{
		Description: "A resource for managing Kafka SCRAM user credentials for SASL authentication.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "`username|scram_mechanism`.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"username": schema.StringAttribute{
				Required:      true,
				Description:   "The name of the credential",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"scram_mechanism": schema.StringAttribute{
				Required:      true,
				Description:   "The SCRAM mechanism used to generate the credential (SCRAM-SHA-256, SCRAM-SHA-512)",
				Validators:    []validator.String{stringvalidator.OneOf(sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512)},
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"scram_iterations": schema.Int64Attribute{
				Optional:    true,
				Computed:    true,
				Default:     int64default.StaticInt64(int64(defaultIterations)),
				Description: "The number of SCRAM iterations used when generating the credential",
				Validators:  []validator.Int64{int64validator.AtLeast(4096)},
			},
			"password": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "The password of the credential (deprecated, use password_wo instead)",
				Validators: []validator.String{
					notBlank,
					stringvalidator.ConflictsWith(path.MatchRoot("password_wo")),
				},
			},
			"password_wo": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				WriteOnly:   true,
				Description: "The write-only password of the credential",
				Validators: []validator.String{
					notBlank,
					stringvalidator.ConflictsWith(path.MatchRoot("password")),
				},
			},
			"password_wo_version": schema.StringAttribute{
				Optional:    true,
				Description: "Version identifier for the write-only password to track changes",
			},
		},
	}
}

func (r *userScramCredentialResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	resp.IdentitySchema = identityschema.Schema{
		Attributes: map[string]identityschema.Attribute{
			"username":        identityschema.StringAttribute{RequiredForImport: true, Description: "The name of the credential."},
			"scram_mechanism": identityschema.StringAttribute{RequiredForImport: true, Description: "SCRAM-SHA-256 or SCRAM-SHA-512."},
		},
	}
}

func (r *userScramCredentialResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = clientFrom(req.ProviderData, &resp.Diagnostics)
}

// ValidateConfig: one of password / password_wo is required (the plan cannot
// create a credential without one, and an imported credential needs it too).
func (r *userScramCredentialResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	var m scramModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() || m.Password.IsUnknown() || m.PasswordWo.IsUnknown() {
		return
	}
	if m.Password.ValueString() == "" && m.PasswordWo.ValueString() == "" {
		resp.Diagnostics.AddError("Missing password",
			"password validation failed: either 'password' or 'password_wo' must be provided with a non-empty value")
	}
}

func convertedScramMechanism(s string) sarama.ScramMechanismType {
	switch s {
	case sarama.SCRAM_MECHANISM_SHA_256.String():
		return sarama.SCRAM_MECHANISM_SHA_256
	case sarama.SCRAM_MECHANISM_SHA_512.String():
		return sarama.SCRAM_MECHANISM_SHA_512
	default:
		return sarama.SCRAM_MECHANISM_UNKNOWN
	}
}

// credential builds the upsert from the plan; the write-only password exists
// only in the configuration.
func credential(ctx context.Context, plan scramModel, config tfsdk.Config, diags *diag.Diagnostics) UserScramCredential {
	password := plan.Password.ValueString()
	if password == "" {
		var wo types.String
		diags.Append(config.GetAttribute(ctx, path.Root("password_wo"), &wo)...)
		password = wo.ValueString()
	}
	if password == "" {
		diags.AddError("Missing password", "either 'password' or 'password_wo' must be provided with a non-empty value")
	}
	return UserScramCredential{
		Name:       plan.Username.ValueString(),
		Mechanism:  convertedScramMechanism(plan.ScramMechanism.ValueString()),
		Iterations: int32(plan.ScramIterations.ValueInt64()),
		Password:   []byte(password),
	}
}

func scramIdentity(user, mechanism string) scramIdentityModel {
	return scramIdentityModel{Username: types.StringValue(user), ScramMechanism: types.StringValue(mechanism)}
}

func (r *userScramCredentialResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan scramModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	usc := credential(ctx, plan, req.Config, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := r.client.UpsertUserScramCredential(usc); err != nil {
		resp.Diagnostics.AddError("Creating user scram credential "+usc.ID(), err.Error())
		return
	}
	// AlterUserScramCredentials returns before every broker has applied the
	// change; a Describe on a lagging broker would answer RESOURCE_NOT_FOUND.
	err := waitFor(ctx, "user scram credential "+usc.ID()+" to be created", r.client.timeout(), time.Second, 2*time.Second, func() (bool, error) {
		_, err := r.client.DescribeUserScramCredential(usc.Name, usc.Mechanism.String())
		var missing UserScramCredentialMissingError
		if errors.As(err, &missing) {
			return false, nil
		}
		return err == nil, err
	})
	if err != nil {
		resp.Diagnostics.AddError("Creating user scram credential "+usc.ID(), err.Error())
		return
	}
	plan.ID = types.StringValue(usc.ID())
	plan.PasswordWo = types.StringNull()
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, scramIdentity(usc.Name, usc.Mechanism.String()))...)
}

func (r *userScramCredentialResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state scramModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	found, err := r.client.DescribeUserScramCredential(state.Username.ValueString(), state.ScramMechanism.ValueString())
	var missing UserScramCredentialMissingError
	if errors.As(err, &missing) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading user scram credential "+state.ID.ValueString(), err.Error())
		return
	}
	state.Username = types.StringValue(found.Name)
	state.ScramMechanism = types.StringValue(found.Mechanism.String())
	state.ScramIterations = types.Int64Value(int64(found.Iterations))
	state.ID = types.StringValue(found.ID())
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, scramIdentity(found.Name, found.Mechanism.String()))...)
}

func (r *userScramCredentialResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state scramModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	// password_wo itself never diffs (write-only); a change is signalled by
	// password_wo_version.
	if !plan.Password.Equal(state.Password) || !plan.PasswordWoVersion.Equal(state.PasswordWoVersion) ||
		!plan.ScramIterations.Equal(state.ScramIterations) {
		usc := credential(ctx, plan, req.Config, &resp.Diagnostics)
		if resp.Diagnostics.HasError() {
			return
		}
		if err := r.client.UpsertUserScramCredential(usc); err != nil {
			resp.Diagnostics.AddError("Updating user scram credential "+usc.ID(), err.Error())
			return
		}
	}
	plan.ID = state.ID
	plan.PasswordWo = types.StringNull()
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func (r *userScramCredentialResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state scramModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	usc := UserScramCredential{
		Name:      state.Username.ValueString(),
		Mechanism: convertedScramMechanism(state.ScramMechanism.ValueString()),
	}
	if err := r.client.DeleteUserScramCredential(usc); err != nil {
		resp.Diagnostics.AddError("Deleting user scram credential "+usc.ID(), err.Error())
		return
	}
	// Like create: wait until no broker still describes the credential.
	err := waitFor(ctx, "user scram credential "+usc.ID()+" to be deleted", r.client.timeout(), time.Second, 2*time.Second, func() (bool, error) {
		_, err := r.client.DescribeUserScramCredential(usc.Name, usc.Mechanism.String())
		var missing UserScramCredentialMissingError
		if errors.As(err, &missing) {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		resp.Diagnostics.AddError("Deleting user scram credential "+usc.ID(), err.Error())
	}
}

// ImportState: `username|scram_mechanism`, the legacy
// `username|scram_mechanism|password`, or an identity. Kafka never returns
// passwords, so the configuration must provide one (password_wo).
func (r *userScramCredentialResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	var user, mechanism, password string
	if req.ID != "" {
		parts := strings.Split(req.ID, "|")
		switch len(parts) {
		case 2:
			user, mechanism = parts[0], parts[1]
		case 3:
			user, mechanism, password = parts[0], parts[1], parts[2]
		default:
			resp.Diagnostics.AddError("Invalid import ID", fmt.Sprintf(
				"expected format is username|scram_mechanism (for write-only passwords) or username|scram_mechanism|password (legacy) - got %v segments instead of 2 or 3", len(parts)))
			return
		}
	} else {
		var id scramIdentityModel
		resp.Diagnostics.Append(req.Identity.Get(ctx, &id)...)
		if resp.Diagnostics.HasError() {
			return
		}
		user, mechanism = id.Username.ValueString(), id.ScramMechanism.ValueString()
	}
	m := scramModel{
		ID:                types.StringValue(user + "|" + mechanism),
		Username:          types.StringValue(user),
		ScramMechanism:    types.StringValue(mechanism),
		ScramIterations:   types.Int64Value(int64(defaultIterations)),
		Password:          types.StringNull(),
		PasswordWo:        types.StringNull(),
		PasswordWoVersion: types.StringNull(),
	}
	if password != "" {
		m.Password = types.StringValue(password)
	}
	log.Printf("[INFO] Importing user scram credential %s", m.ID.ValueString())
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, scramIdentity(user, mechanism))...)
}
