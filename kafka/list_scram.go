package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/list"
	lschema "github.com/hashicorp/terraform-plugin-framework/list/schema"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// scramListResource implements `list "kafka_user_scram_credential"`. Kafka
// never returns passwords, so generated configuration needs password_wo
// added before apply (see the terraform query guide).
type scramListResource struct {
	sdk *sdkSchemas
}

var _ list.ListResourceWithRawV5Schemas = (*scramListResource)(nil)

type scramListConfig struct {
	ScramMechanism types.String `tfsdk:"scram_mechanism"`
	UsernamePrefix types.String `tfsdk:"username_prefix"`
}

func (r *scramListResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_user_scram_credential"
}

func (r *scramListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = lschema.Schema{
		MarkdownDescription: "Lists existing SCRAM credentials (one result per user and mechanism), e.g. to generate `import` blocks with `terraform query`. " +
			"Passwords cannot be read from Kafka: add `password_wo` to the generated configuration before applying.",
		Attributes: map[string]lschema.Attribute{
			"scram_mechanism": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only credentials of this mechanism: `SCRAM-SHA-256` or `SCRAM-SHA-512`.",
			},
			"username_prefix": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only users whose name starts with this prefix.",
			},
		},
	}
}

func (r *scramListResource) RawV5Schemas(ctx context.Context, _ list.RawV5SchemaRequest, resp *list.RawV5SchemaResponse) {
	rs, is, err := r.sdk.resource(ctx, "kafka_user_scram_credential")
	if err != nil {
		return
	}
	resp.ProtoV5Schema = rs
	resp.ProtoV5IdentitySchema = is
}

func (r *scramListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	var cfg scramListConfig
	if diags := req.Config.Get(ctx, &cfg); diags.HasError() {
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}
	client := r.sdk.client()
	if client == nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Provider not configured", "The kafka provider block was not configured before listing."))
		return
	}
	all, err := client.ListUserScramCredentials()
	if err != nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Listing SCRAM credentials", err.Error()))
		return
	}
	creds := filterSCRAM(all, cfg.ScramMechanism.ValueString(), cfg.UsernamePrefix.ValueString())

	stream.Results = func(push func(list.ListResult) bool) {
		for i, c := range creds {
			if req.Limit > 0 && int64(i) >= req.Limit {
				return
			}
			mech := c.Mechanism.String()
			result := req.NewListResult(ctx)
			result.DisplayName = fmt.Sprintf("%s (%s, %d iterations)", c.Name, mech, c.Iterations)
			result.Diagnostics.Append(result.Identity.SetAttribute(ctx, path.Root("username"), c.Name)...)
			result.Diagnostics.Append(result.Identity.SetAttribute(ctx, path.Root("scram_mechanism"), mech)...)
			if req.IncludeResource {
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("id"), c.ID())...)
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("username"), c.Name)...)
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("scram_mechanism"), mech)...)
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("scram_iterations"), int64(c.Iterations))...)
			}
			if !push(result) {
				return
			}
		}
	}
}

// filterSCRAM applies the list block filters and sorts by ID for stable output.
func filterSCRAM(in []UserScramCredential, mechanism, usernamePrefix string) []UserScramCredential {
	out := make([]UserScramCredential, 0, len(in))
	for _, c := range in {
		if mechanism != "" && c.Mechanism.String() != mechanism {
			continue
		}
		if usernamePrefix != "" && !strings.HasPrefix(c.Name, usernamePrefix) {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}
