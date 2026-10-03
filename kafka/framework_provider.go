package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/list"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	pschema "github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// FrameworkProvider serves only what SDKv2 cannot: list resources
// (`terraform query`, Terraform >= 1.14). Everything else stays in the SDKv2
// provider; both are muxed in main.go.
//
// tf5muxserver requires identical provider schemas, so this provider's schema
// is converted from the SDKv2 one at runtime instead of being written twice.
type FrameworkProvider struct {
	sdk *sdkSchemas
}

func NewFrameworkProvider(sdkProvider *schema.Provider) func() provider.Provider {
	s := &sdkSchemas{provider: sdkProvider}
	return func() provider.Provider { return &FrameworkProvider{sdk: s} }
}

var _ provider.ProviderWithListResources = (*FrameworkProvider)(nil)

func (p *FrameworkProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "kafka"
}

func (p *FrameworkProvider) Schema(ctx context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	s, err := p.sdk.get(ctx)
	if err != nil {
		resp.Diagnostics.AddError("Building provider schema", err.Error())
		return
	}
	converted, err := providerSchemaFromProto(s.Provider)
	if err != nil {
		resp.Diagnostics.AddError("Converting the SDKv2 provider schema", err.Error())
		return
	}
	resp.Schema = converted
}

// Configure does not parse the config: the SDKv2 server (configured first by
// the mux) already built the client and shared it.
func (p *FrameworkProvider) Configure(_ context.Context, _ provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	resp.ListResourceData = p
}

func (p *FrameworkProvider) Resources(context.Context) []func() resource.Resource { return nil }

func (p *FrameworkProvider) DataSources(context.Context) []func() datasource.DataSource { return nil }

func (p *FrameworkProvider) ListResources(context.Context) []func() list.ListResource {
	return []func() list.ListResource{
		func() list.ListResource { return &topicListResource{sdk: p.sdk} },
		func() list.ListResource { return &aclListResource{sdk: p.sdk} },
	}
}

// sdkSchemas caches the proto schemas the SDKv2 provider advertises.
type sdkSchemas struct {
	provider *schema.Provider
	once     sync.Once
	schemas  *tfprotov5.GetProviderSchemaResponse
	identity *tfprotov5.GetResourceIdentitySchemasResponse
	err      error
}

func (s *sdkSchemas) load(ctx context.Context) {
	s.once.Do(func() {
		srv := schema.NewGRPCProviderServer(s.provider)
		s.schemas, s.err = srv.GetProviderSchema(ctx, &tfprotov5.GetProviderSchemaRequest{})
		if s.err != nil {
			return
		}
		s.identity, s.err = srv.GetResourceIdentitySchemas(ctx, &tfprotov5.GetResourceIdentitySchemasRequest{})
	})
}

func (s *sdkSchemas) get(ctx context.Context) (*tfprotov5.GetProviderSchemaResponse, error) {
	s.load(ctx)
	return s.schemas, s.err
}

func (s *sdkSchemas) resource(ctx context.Context, typeName string) (*tfprotov5.Schema, *tfprotov5.ResourceIdentitySchema, error) {
	s.load(ctx)
	if s.err != nil {
		return nil, nil, s.err
	}
	rs, ok := s.schemas.ResourceSchemas[typeName]
	if !ok {
		return nil, nil, fmt.Errorf("SDKv2 provider has no resource %q", typeName)
	}
	is, ok := s.identity.IdentitySchemas[typeName]
	if !ok {
		return nil, nil, fmt.Errorf("SDKv2 resource %q has no identity schema", typeName)
	}
	return rs, is, nil
}

// providerSchemaFromProto mirrors a flat SDKv2 provider schema. It only
// supports what this provider uses (primitives and lists of primitives) and
// fails loudly on anything else, so a new block type cannot drift silently.
func providerSchemaFromProto(s *tfprotov5.Schema) (pschema.Schema, error) {
	out := pschema.Schema{Attributes: map[string]pschema.Attribute{}}
	if s == nil || s.Block == nil {
		return out, nil
	}
	if len(s.Block.BlockTypes) > 0 {
		return out, fmt.Errorf("nested blocks in the provider schema are not supported by the framework mirror")
	}
	for _, a := range s.Block.Attributes {
		conv, err := attributeFromProto(a)
		if err != nil {
			return out, fmt.Errorf("attribute %q: %w", a.Name, err)
		}
		out.Attributes[a.Name] = conv
	}
	return out, nil
}

func attributeFromProto(a *tfprotov5.SchemaAttribute) (pschema.Attribute, error) {
	desc, md := a.Description, ""
	if a.DescriptionKind == tfprotov5.StringKindMarkdown {
		desc, md = "", a.Description
	}
	dep := a.DeprecationMessage
	if a.Deprecated && dep == "" {
		dep = "deprecated"
	}
	switch {
	case a.Type.Is(tftypes.String):
		return pschema.StringAttribute{Required: a.Required, Optional: a.Optional, Sensitive: a.Sensitive, Description: desc, MarkdownDescription: md, DeprecationMessage: dep}, nil
	case a.Type.Is(tftypes.Bool):
		return pschema.BoolAttribute{Required: a.Required, Optional: a.Optional, Sensitive: a.Sensitive, Description: desc, MarkdownDescription: md, DeprecationMessage: dep}, nil
	case a.Type.Is(tftypes.Number):
		return pschema.NumberAttribute{Required: a.Required, Optional: a.Optional, Sensitive: a.Sensitive, Description: desc, MarkdownDescription: md, DeprecationMessage: dep}, nil
	case a.Type.Is(tftypes.List{}):
		elem, err := primitiveType(a.Type.(tftypes.List).ElementType)
		if err != nil {
			return nil, err
		}
		return pschema.ListAttribute{ElementType: elem, Required: a.Required, Optional: a.Optional, Sensitive: a.Sensitive, Description: desc, MarkdownDescription: md, DeprecationMessage: dep}, nil
	case a.Type.Is(tftypes.Set{}):
		elem, err := primitiveType(a.Type.(tftypes.Set).ElementType)
		if err != nil {
			return nil, err
		}
		return pschema.SetAttribute{ElementType: elem, Required: a.Required, Optional: a.Optional, Sensitive: a.Sensitive, Description: desc, MarkdownDescription: md, DeprecationMessage: dep}, nil
	}
	return nil, fmt.Errorf("unsupported type %s", a.Type)
}

func primitiveType(t tftypes.Type) (attr.Type, error) {
	switch {
	case t.Is(tftypes.String):
		return types.StringType, nil
	case t.Is(tftypes.Bool):
		return types.BoolType, nil
	case t.Is(tftypes.Number):
		return types.NumberType, nil
	}
	return nil, fmt.Errorf("unsupported element type %s", t)
}
