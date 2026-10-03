package kafka

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
)

// The mux refuses to start when the SDKv2 and framework provider schemas
// differ; the framework one is generated from SDKv2, so this guards the
// converter against any future provider attribute.
func TestMuxServerSchemas(t *testing.T) {
	ctx := context.Background()
	newServer, err := MuxServer(ctx)
	if err != nil {
		t.Fatalf("MuxServer: %v", err)
	}
	srv := newServer()

	schemas, err := srv.GetProviderSchema(ctx, &tfprotov5.GetProviderSchemaRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range schemas.Diagnostics {
		t.Errorf("GetProviderSchema diagnostic: %s: %s", d.Summary, d.Detail)
	}
	if _, ok := schemas.ResourceSchemas["kafka_topic"]; !ok {
		t.Error("kafka_topic resource schema missing (SDKv2 side)")
	}
	ls, ok := schemas.ListResourceSchemas["kafka_topic"]
	if !ok {
		t.Fatal("kafka_topic list resource schema missing (framework side)")
	}
	got := map[string]bool{}
	for _, a := range ls.Block.Attributes {
		got[a.Name] = true
	}
	for _, want := range []string{"name_prefix", "name_regex", "include_internal"} {
		if !got[want] {
			t.Errorf("list schema lacks %q", want)
		}
	}

	ids, err := srv.GetResourceIdentitySchemas(ctx, &tfprotov5.GetResourceIdentitySchemasRequest{})
	if err != nil {
		t.Fatal(err)
	}
	id, ok := ids.IdentitySchemas["kafka_topic"]
	if !ok || len(id.IdentityAttributes) != 1 || id.IdentityAttributes[0].Name != "name" || !id.IdentityAttributes[0].RequiredForImport {
		t.Fatalf("kafka_topic identity schema = %+v", id)
	}

	meta, err := srv.GetMetadata(ctx, &tfprotov5.GetMetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range meta.ListResources {
		found = found || l.TypeName == "kafka_topic"
	}
	if !found {
		t.Error("GetMetadata does not advertise the kafka_topic list resource")
	}
}

func TestFilterTopicNames(t *testing.T) {
	names := []string{"orders.v1", "__consumer_offsets", "audit", "orders.v2", "payments"}
	cases := []struct {
		prefix, re string
		internal   bool
		want       []string
	}{
		{"", "", false, []string{"audit", "orders.v1", "orders.v2", "payments"}},
		{"", "", true, []string{"__consumer_offsets", "audit", "orders.v1", "orders.v2", "payments"}},
		{"orders.", "", false, []string{"orders.v1", "orders.v2"}},
		{"", `v2$|^pay`, false, []string{"orders.v2", "payments"}},
	}
	for _, c := range cases {
		got, err := filterTopicNames(names, c.prefix, c.re, c.internal)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(c.want) {
			t.Errorf("filter(%q,%q,%v) = %v, want %v", c.prefix, c.re, c.internal, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("filter(%q,%q,%v) = %v, want %v", c.prefix, c.re, c.internal, got, c.want)
				break
			}
		}
	}
	if _, err := filterTopicNames(names, "", "(", false); err == nil {
		t.Error("invalid regex must be an error")
	}
}
