package kafka

import (
	"context"
	"strings"
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

	al, ok := schemas.ListResourceSchemas["kafka_acl"]
	if !ok {
		t.Fatal("kafka_acl list resource schema missing (framework side)")
	}
	if len(al.Block.Attributes) != 3 {
		t.Errorf("kafka_acl list schema has %d attributes, want 3", len(al.Block.Attributes))
	}

	ids, err := srv.GetResourceIdentitySchemas(ctx, &tfprotov5.GetResourceIdentitySchemasRequest{})
	if err != nil {
		t.Fatal(err)
	}
	id, ok := ids.IdentitySchemas["kafka_topic"]
	if !ok || len(id.IdentityAttributes) != 1 || id.IdentityAttributes[0].Name != "name" || !id.IdentityAttributes[0].RequiredForImport {
		t.Fatalf("kafka_topic identity schema = %+v", id)
	}

	aid, ok := ids.IdentitySchemas["kafka_acl"]
	if !ok || len(aid.IdentityAttributes) != len(aclIdentityAttributes) {
		t.Fatalf("kafka_acl identity schema = %+v", aid)
	}
	for _, a := range aid.IdentityAttributes {
		if !a.RequiredForImport {
			t.Errorf("kafka_acl identity %q is not RequiredForImport", a.Name)
		}
	}

	for typ, want := range map[string]int{"kafka_quota": 2, "kafka_user_scram_credential": 2} {
		is, ok := ids.IdentitySchemas[typ]
		if !ok || len(is.IdentityAttributes) != want {
			t.Errorf("%s identity schema = %+v, want %d attributes", typ, is, want)
		}
	}

	meta, err := srv.GetMetadata(ctx, &tfprotov5.GetMetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	advertised := map[string]bool{}
	for _, l := range meta.ListResources {
		advertised[l.TypeName] = true
	}
	for _, want := range []string{"kafka_topic", "kafka_acl", "kafka_quota", "kafka_user_scram_credential"} {
		if !advertised[want] {
			t.Errorf("GetMetadata does not advertise the %s list resource", want)
		}
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

func TestFilterACLs(t *testing.T) {
	mk := func(principal, typ, name string) StringlyTypedACL {
		return StringlyTypedACL{
			ACL:      ACL{Principal: principal, Host: "*", Operation: "Read", PermissionType: "Allow"},
			Resource: Resource{Type: typ, Name: name, PatternTypeFilter: "Literal"},
		}
	}
	in := []StringlyTypedACL{
		mk("User:bob", "Topic", "orders.v1"),
		mk("User:alice", "Group", "orders-consumers"),
		mk("User:alice", "Topic", "payments"),
		mk("User:alice", "Topic", "orders.v1"),
	}
	cases := []struct {
		f    aclFilter
		want []string
	}{
		{aclFilter{}, []string{
			"User:alice|*|Read|Allow|Group|orders-consumers|Literal",
			"User:alice|*|Read|Allow|Topic|orders.v1|Literal",
			"User:alice|*|Read|Allow|Topic|payments|Literal",
			"User:bob|*|Read|Allow|Topic|orders.v1|Literal",
		}},
		{aclFilter{principal: "User:bob"}, []string{"User:bob|*|Read|Allow|Topic|orders.v1|Literal"}},
		{aclFilter{resourceType: "topic", namePrefix: "orders"}, []string{
			"User:alice|*|Read|Allow|Topic|orders.v1|Literal",
			"User:bob|*|Read|Allow|Topic|orders.v1|Literal",
		}},
	}
	for _, c := range cases {
		var got []string
		for _, a := range filterACLs(in, c.f) {
			got = append(got, a.String())
		}
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("filterACLs(%+v) = %v, want %v", c.f, got, c.want)
		}
	}
}

func TestFilterQuotas(t *testing.T) {
	in := []Quota{
		{EntityType: "user", EntityName: "bob"},
		{EntityType: "client-id", EntityName: "orders-app"},
		{EntityType: "user", EntityName: ""},
		{EntityType: "user", EntityName: "alice", Ops: []QuotaOp{{Key: "producer_byte_rate", Value: 1048576}}},
	}
	ids := func(qs []Quota) string {
		var out []string
		for _, q := range qs {
			out = append(out, q.ID())
		}
		return strings.Join(out, ",")
	}
	if got := ids(filterQuotas(in, "", "")); got != "alice|user,bob|user,entity-default|user,orders-app|client-id" {
		t.Errorf("all: %s", got)
	}
	if got := ids(filterQuotas(in, "user", "")); got != "alice|user,bob|user,entity-default|user" {
		t.Errorf("user: %s", got)
	}
	// A name prefix never matches the default quota.
	if got := ids(filterQuotas(in, "", "a")); got != "alice|user" {
		t.Errorf("prefix: %s", got)
	}
	if got := quotaDisplayName(in[3]); got != "user alice: producer_byte_rate=1.048576e+06" {
		t.Errorf("display name: %s", got)
	}
	if got := quotaDisplayName(Quota{EntityType: "ip"}); got != "default ip: " {
		t.Errorf("default display name: %q", got)
	}
}

func TestImportQuotaFormats(t *testing.T) {
	cases := map[string]struct{ typ, name, id string }{
		"orders-app|client-id": {"client-id", "orders-app", "orders-app|client-id"},
		"entity-default|user":  {"user", "", "entity-default|user"},
		"client-id:orders-app": {"client-id", "orders-app", "orders-app|client-id"},
		"user:":                {"user", "", "entity-default|user"},
		"ip:10.0.0.1":          {"ip", "10.0.0.1", "10.0.0.1|ip"},
	}
	for in, want := range cases {
		d := kafkaQuotaResource().TestResourceData()
		d.SetId(in)
		out, err := importQuota(context.Background(), d, nil)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		got := out[0]
		if got.Get("entity_type") != want.typ || got.Get("entity_name") != want.name || got.Id() != want.id {
			t.Errorf("%q: got type=%v name=%v id=%v", in, got.Get("entity_type"), got.Get("entity_name"), got.Id())
		}
	}
	for _, bad := range []string{"just-a-name", "group:x", "a|b|c"} {
		d := kafkaQuotaResource().TestResourceData()
		d.SetId(bad)
		if _, err := importQuota(context.Background(), d, nil); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}
