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

// aclListResource implements `list "kafka_acl"` for `terraform query`. Like
// the topic list, the managed resource is SDKv2 and its schemas come in via
// RawV5Schemas.
type aclListResource struct {
	sdk *sdkSchemas
}

var _ list.ListResourceWithRawV5Schemas = (*aclListResource)(nil)

type aclListConfig struct {
	Principal          types.String `tfsdk:"acl_principal"`
	ResourceType       types.String `tfsdk:"resource_type"`
	ResourceNamePrefix types.String `tfsdk:"resource_name_prefix"`
}

type aclFilter struct {
	principal, resourceType, namePrefix string
}

func (r *aclListResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_acl"
}

func (r *aclListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = lschema.Schema{
		MarkdownDescription: "Lists existing Kafka ACLs, e.g. to generate `import` blocks and configuration with `terraform query`. Each result is one ACL entry (one `kafka_acl` resource).",
		Attributes: map[string]lschema.Attribute{
			"acl_principal": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only ACLs for this principal, e.g. `User:alice` (exact match).",
			},
			"resource_type": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only ACLs on this resource type: `Topic`, `Group`, `Cluster`, `TransactionalID` or `DelegationToken`.",
			},
			"resource_name_prefix": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only ACLs whose `resource_name` starts with this prefix.",
			},
		},
	}
}

func (r *aclListResource) RawV5Schemas(ctx context.Context, _ list.RawV5SchemaRequest, resp *list.RawV5SchemaResponse) {
	rs, is, err := r.sdk.resource(ctx, "kafka_acl")
	if err != nil {
		return
	}
	resp.ProtoV5Schema = rs
	resp.ProtoV5IdentitySchema = is
}

func (r *aclListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	var cfg aclListConfig
	if diags := req.Config.Get(ctx, &cfg); diags.HasError() {
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	client := r.sdk.client()
	if client == nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Provider not configured", "The kafka provider block was not configured before listing."))
		return
	}

	// The ACL cache may hold a listing from before ACLs were created outside
	// this process; a query should always see the cluster as it is now.
	if err := client.InvalidateACLCache(); err != nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Listing ACLs", err.Error()))
		return
	}
	res, err := client.ListACLs()
	if err != nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Listing ACLs", err.Error()))
		return
	}

	acls := filterACLs(flattenACLs(res), aclFilter{
		principal:    cfg.Principal.ValueString(),
		resourceType: cfg.ResourceType.ValueString(),
		namePrefix:   cfg.ResourceNamePrefix.ValueString(),
	})

	stream.Results = func(push func(list.ListResult) bool) {
		for i, a := range acls {
			if req.Limit > 0 && int64(i) >= req.Limit {
				return
			}
			result := req.NewListResult(ctx)
			result.DisplayName = aclDisplayName(a)
			attrs := aclAttributes(a)
			for _, k := range aclIdentityAttributes {
				result.Diagnostics.Append(result.Identity.SetAttribute(ctx, path.Root(k), attrs[k])...)
			}
			if req.IncludeResource {
				result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("id"), a.String())...)
				for _, k := range aclIdentityAttributes {
					result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root(k), attrs[k])...)
				}
			}
			if !push(result) {
				return
			}
		}
	}
}

// filterACLs applies the list block filters and returns ACLs sorted by their
// ID, so `terraform query` output and generated config are stable between runs.
func filterACLs(in []StringlyTypedACL, f aclFilter) []StringlyTypedACL {
	out := make([]StringlyTypedACL, 0, len(in))
	for _, a := range in {
		if f.principal != "" && a.ACL.Principal != f.principal {
			continue
		}
		if f.resourceType != "" && !strings.EqualFold(a.Type, f.resourceType) {
			continue
		}
		if f.namePrefix != "" && !strings.HasPrefix(a.Name, f.namePrefix) {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// aclDisplayName reads like the ACL itself:
// "Allow User:alice Read Topic orders (Literal) from *".
func aclDisplayName(a StringlyTypedACL) string {
	return fmt.Sprintf("%s %s %s %s %s (%s) from %s",
		a.ACL.PermissionType, a.ACL.Principal, a.ACL.Operation, a.Type, a.Name, a.PatternTypeFilter, a.ACL.Host)
}
