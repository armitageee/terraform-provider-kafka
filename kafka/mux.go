package kafka

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-mux/tf5muxserver"
)

// MuxServer combines the SDKv2 provider (all resources and data sources) with
// the framework provider (list resources). The SDKv2 server comes first: it is
// configured first and shares the Kafka client with the framework side.
func MuxServer(ctx context.Context) (func() tfprotov5.ProviderServer, error) {
	sdk := Provider()
	mux, err := tf5muxserver.NewMuxServer(ctx,
		sdk.GRPCProvider,
		providerserver.NewProtocol5(NewFrameworkProvider(sdk)()),
	)
	if err != nil {
		return nil, err
	}
	return mux.ProviderServer, nil
}
