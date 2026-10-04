package main

import (
	"context"
	"flag"
	"log"

	"github.com/Mongey/terraform-provider-kafka/kafka"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

// version is set by goreleaser (-X main.version=...).
var version = "dev"

// Run "go generate" to format example terraform files and generate the docs for the registry/website

// If you do not have terraform installed, you can remove the formatting command, but its suggested to
// ensure the documentation is formatted properly.
//go:generate terraform fmt -recursive ./examples/

// Run the docs generation tool, check its repository for more information on how it works and how docs
// can be customized. Terraform is pinned: list resources need >= 1.14, and a fixed version keeps the
// output stable between machines and CI.
//
//go:generate go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs generate --provider-name kafka --rendered-provider-name terraform-provider-kafka --tf-version 1.16.5
func main() {
	var debugMode bool

	flag.BoolVar(&debugMode, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	// Protocol 6 (Terraform >= 1.0, every OpenTofu): nested attributes.
	err := providerserver.Serve(context.Background(), kafka.New(version), providerserver.ServeOpts{
		Address:         "registry.terraform.io/armitageee/kafka",
		Debug:           debugMode,
		ProtocolVersion: 6,
	})
	if err != nil {
		log.Fatal(err)
	}
}
