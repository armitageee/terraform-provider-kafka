package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// The provider under test is registered as registry.terraform.io/armitageee/kafka,
// the published address, for Terraform and OpenTofu alike: OpenTofu rejects
// terraform-plugin-testing's default (legacy "-" namespace), and the compat
// tests continue state written by the published provider at that address.
func init() {
	if os.Getenv("TF_ACC_PROVIDER_NAMESPACE") == "" {
		os.Setenv("TF_ACC_PROVIDER_NAMESPACE", "armitageee")
	}
}

func testProviderSource() string {
	host := os.Getenv("TF_ACC_PROVIDER_HOST")
	if host == "" {
		host = "registry.terraform.io"
	}
	return host + "/" + os.Getenv("TF_ACC_PROVIDER_NAMESPACE") + "/kafka"
}

// requiredProviders pins the provider address in test configs. Without it an
// import step (its config gets no required_providers from the framework)
// resolves the implicit hashicorp/kafka.
func requiredProviders() string {
	return fmt.Sprintf(`
terraform {
  required_providers {
    kafka = {
      source = %q
    }
  }
}
`, testProviderSource())
}

// testProvider gives acceptance-test checks a Kafka client configured like
// the provider in the tests: bootstrap servers from KAFKA_BOOTSTRAP_SERVERS,
// everything else from the same KAFKA_* environment variables.
var testProvider = accProvider{client: newAccTestClient()}
var testBootstrapServers []string = bootstrapServersFromEnv()

type accProvider struct{ client *LazyClient }

func (p accProvider) Meta() any { return p.client }

func newAccTestClient() *LazyClient {
	ctx := context.Background()
	brokers, _ := types.ListValueFrom(ctx, types.StringType, bootstrapServersFromEnv())
	var diags diag.Diagnostics
	config := buildConfig(ctx, providerModel{
		BootstrapServers: brokers,
		KafkaVersion:     types.StringValue("3.8.0"),
	}, &diags)
	if diags.HasError() {
		log.Printf("[ERROR] acceptance test client: %v", diags)
		return nil
	}
	return &LazyClient{Config: config}
}

func testAccPreCheck(t *testing.T) {
	client := testProvider.client
	if client == nil {
		t.Fatal("Could not construct client")
	}
	if err := client.init(); err != nil {
		t.Fatalf("Client could not be initialized %v", err)
	}
}

// protoV6ProviderFactories serves the provider exactly as main.go does.
// Terraform configures it from the test config and KAFKA_* env.
func protoV6ProviderFactories() map[string]func() (tfprotov6.ProviderServer, error) {
	return map[string]func() (tfprotov6.ProviderServer, error){
		"kafka": providerserver.NewProtocol6WithError(New("test")()),
	}
}

func bootstrapServersFromEnv() []string {
	fromEnv := strings.Split(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"), ",")
	fromEnv = nonEmptyAndTrimmed(fromEnv)

	if len(fromEnv) == 0 {
		fromEnv = []string{"localhost:9092"}
	}

	bootstrapServers := make([]string, 0)
	for _, bs := range fromEnv {
		if bs != "" {
			bootstrapServers = append(bootstrapServers, bs)
		}
	}

	return bootstrapServers
}
