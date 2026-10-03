package kafka

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-mux/tf5muxserver"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// MuxServer combines the SDKv2 provider (all resources and data sources) with
// the framework provider (list resources). The SDKv2 server comes first: it is
// configured first, and the framework side reads the Kafka client from it.
func MuxServer(ctx context.Context) (func() tfprotov5.ProviderServer, error) {
	return NewMuxServer(ctx, Provider())
}

// NewMuxServer muxes the given SDKv2 provider instance; tests use it to keep a
// handle on the instance that Terraform configures.
func NewMuxServer(ctx context.Context, sdk *schema.Provider) (func() tfprotov5.ProviderServer, error) {
	mux, err := tf5muxserver.NewMuxServer(ctx,
		sdk.GRPCProvider,
		providerserver.NewProtocol5(NewFrameworkProvider(sdk)()),
	)
	if err != nil {
		return nil, err
	}
	return mux.ProviderServer, nil
}
