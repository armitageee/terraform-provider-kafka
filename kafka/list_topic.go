package kafka

import (
	"context"
	"regexp"
	"sort"
	"strings"

	fwdiag "github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/list"
	lschema "github.com/hashicorp/terraform-plugin-framework/list/schema"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// topicListResource implements `list "kafka_topic"` for `terraform query`:
// it finds existing topics and returns their identity (and, on request, the
// full resource) so Terraform can generate import blocks and configuration.
// The managed resource itself is SDKv2; its schemas come in via RawV5Schemas.
type topicListResource struct {
	sdk *sdkSchemas
}

var _ list.ListResourceWithRawV5Schemas = (*topicListResource)(nil)

type topicListConfig struct {
	NamePrefix      types.String `tfsdk:"name_prefix"`
	NameRegex       types.String `tfsdk:"name_regex"`
	IncludeInternal types.Bool   `tfsdk:"include_internal"`
}

func (r *topicListResource) Metadata(_ context.Context, _ resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = "kafka_topic"
}

func (r *topicListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = lschema.Schema{
		MarkdownDescription: "Lists existing Kafka topics, e.g. to generate `import` blocks and configuration with `terraform query`.",
		Attributes: map[string]lschema.Attribute{
			"name_prefix": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only topics whose name starts with this prefix.",
			},
			"name_regex": lschema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Only topics whose name matches this RE2 regular expression.",
			},
			"include_internal": lschema.BoolAttribute{
				Optional:            true,
				MarkdownDescription: "Include internal topics whose name starts with `__` (e.g. `__consumer_offsets`). Default `false`.",
			},
		},
	}
}

func (r *topicListResource) RawV5Schemas(ctx context.Context, _ list.RawV5SchemaRequest, resp *list.RawV5SchemaResponse) {
	rs, is, err := r.sdk.resource(ctx, "kafka_topic")
	if err != nil {
		// No diagnostics on this response: leave schemas empty, the framework
		// then reports the missing schema itself.
		return
	}
	resp.ProtoV5Schema = rs
	resp.ProtoV5IdentitySchema = is
}

func (r *topicListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	var cfg topicListConfig
	if diags := req.Config.Get(ctx, &cfg); diags.HasError() {
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	client := getSharedClient()
	if client == nil {
		diags := listError("Provider not configured", "The kafka provider block was not configured before listing.")
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	names, err := client.TopicNames()
	if err != nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Listing topics", err.Error()))
		return
	}

	filtered, err := filterTopicNames(names, cfg.NamePrefix.ValueString(), cfg.NameRegex.ValueString(), cfg.IncludeInternal.ValueBool())
	if err != nil {
		stream.Results = list.ListResultsStreamDiagnostics(listError("Invalid name_regex", err.Error()))
		return
	}

	stream.Results = func(push func(list.ListResult) bool) {
		for i, name := range filtered {
			if req.Limit > 0 && int64(i) >= req.Limit {
				return
			}
			result := req.NewListResult(ctx)
			result.DisplayName = name
			result.Diagnostics.Append(result.Identity.SetAttribute(ctx, path.Root("name"), name)...)

			if req.IncludeResource {
				topic, err := client.ReadTopic(name, false)
				if err != nil {
					result.Diagnostics.AddError("Reading topic "+name, err.Error())
				} else {
					config := map[string]string{}
					for k, v := range topic.Config {
						if v != nil {
							config[k] = *v
						}
					}
					result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("id"), name)...)
					result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("name"), name)...)
					result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("partitions"), int64(topic.Partitions))...)
					result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("replication_factor"), int64(topic.ReplicationFactor))...)
					result.Diagnostics.Append(result.Resource.SetAttribute(ctx, path.Root("config"), config)...)
				}
			}

			if !push(result) {
				return
			}
		}
	}
}

// filterTopicNames applies the list block filters and returns names sorted, so
// `terraform query` output and generated config are stable between runs.
func filterTopicNames(names []string, prefix, pattern string, includeInternal bool) ([]string, error) {
	var re *regexp.Regexp
	if pattern != "" {
		var err error
		if re, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if !includeInternal && strings.HasPrefix(n, "__") {
			continue
		}
		if prefix != "" && !strings.HasPrefix(n, prefix) {
			continue
		}
		if re != nil && !re.MatchString(n) {
			continue
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

func listError(summary, detail string) (diags fwdiag.Diagnostics) {
	diags.AddError(summary, detail)
	return diags
}
