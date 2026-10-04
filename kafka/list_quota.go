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

// quotaListResource implements `list "kafka_quota"` for `terraform query`.
type quotaListResource struct {
	client *LazyClient
}

var (
	_ list.ListResource              = (*quotaListResource)(nil)
	_ list.ListResourceWithConfigure = (*quotaListResource)(nil)
)

type quotaListConfig struct {
	EntityType       types.String `tfsdk:"entity_type"`
	EntityNamePrefix types.String `tfsdk:"entity_name_prefix"`
}

func (r *quotaListResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_quota"
}

func (r *quotaListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = lschema.Schema{
		MarkdownDescription: "Lists existing client quotas, e.g. to generate `import` blocks and configuration with `terraform query`. " +
			"Default quotas (no entity name) are included; quotas on combined entities (e.g. user + client-id) are skipped because `kafka_quota` manages one entity.",
		Attributes: map[string]lschema.Attribute{
			"entity_type": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only quotas of this entity type: `user`, `client-id` or `ip`.",
			},
			"entity_name_prefix": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only quotas whose entity name starts with this prefix (excludes default quotas).",
			},
		},
	}
}

func (r *quotaListResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = clientFrom(req.ProviderData, &resp.Diagnostics)
}

func (r *quotaListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	var cfg quotaListConfig
	if diags := req.Config.Get(ctx, &cfg); diags.HasError() {
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}
	client := r.client
	if client == nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Provider not configured", "The kafka provider block was not configured before listing."))
		return
	}
	all, err := client.ListQuotas()
	if err != nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Listing quotas", err.Error()))
		return
	}
	quotas := filterQuotas(all, cfg.EntityType.ValueString(), cfg.EntityNamePrefix.ValueString())

	stream.Results = func(push func(list.ListResult) bool) {
		for i, q := range quotas {
			if req.Limit > 0 && int64(i) >= req.Limit {
				return
			}
			result := req.NewListResult(ctx)
			result.DisplayName = quotaDisplayName(q)
			result.Diagnostics.Append(result.Identity.SetAttribute(ctx, path.Root("entity_type"), q.EntityType)...)
			result.Diagnostics.Append(result.Identity.SetAttribute(ctx, path.Root("entity_name"), q.EntityName)...)
			if req.IncludeResource {
				config := map[string]float64{}
				for _, op := range q.Ops {
					config[op.Key] = op.Value
				}
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("id"), q.ID())...)
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("entity_type"), q.EntityType)...)
				// Default quota: entity_name unset in the resource (identity keeps "").
				if q.EntityName != "" {
					result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("entity_name"), q.EntityName)...)
				}
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("config"), config)...)
			}
			if !push(result) {
				return
			}
		}
	}
}

// filterQuotas applies the list block filters and sorts by ID for stable output.
func filterQuotas(in []Quota, entityType, namePrefix string) []Quota {
	out := make([]Quota, 0, len(in))
	for _, q := range in {
		if entityType != "" && q.EntityType != entityType {
			continue
		}
		if namePrefix != "" && (q.EntityName == "" || !strings.HasPrefix(q.EntityName, namePrefix)) {
			continue
		}
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

// quotaDisplayName: "user alice: producer_byte_rate=1048576", "default user: ...".
func quotaDisplayName(q Quota) string {
	who := q.EntityType + " " + q.EntityName
	if q.EntityName == "" {
		who = "default " + q.EntityType
	}
	vals := make([]string, 0, len(q.Ops))
	for _, op := range q.Ops {
		vals = append(vals, fmt.Sprintf("%s=%g", op.Key, op.Value))
	}
	sort.Strings(vals)
	return who + ": " + strings.Join(vals, ", ")
}
