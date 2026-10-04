package kafka

import (
	"context"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// The provider serves every resource, data source, identity and list
// resource from terraform-plugin-framework over protocol 6.
func TestProviderSchemas(t *testing.T) {
	ctx := context.Background()
	srv, err := protoV6ProviderFactories()["kafka"]()
	if err != nil {
		t.Fatal(err)
	}

	schemas, err := srv.GetProviderSchema(ctx, &tfprotov6.GetProviderSchemaRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range schemas.Diagnostics {
		t.Errorf("GetProviderSchema diagnostic: %s: %s", d.Summary, d.Detail)
	}
	for _, r := range []string{"kafka_topic", "kafka_acl", "kafka_quota", "kafka_user_scram_credential"} {
		if _, ok := schemas.ResourceSchemas[r]; !ok {
			t.Errorf("resource %s missing", r)
		}
		if _, ok := schemas.ListResourceSchemas[r]; !ok {
			t.Errorf("list resource %s missing", r)
		}
	}
	for _, d := range []string{"kafka_topic", "kafka_topics", "kafka_cluster", "kafka_acls", "kafka_quotas", "kafka_user_scram_credentials"} {
		if _, ok := schemas.DataSourceSchemas[d]; !ok {
			t.Errorf("data source %s missing", d)
		}
	}
	if v := schemas.ResourceSchemas["kafka_acl"].Version; v != 1 {
		t.Errorf("kafka_acl schema version = %d, want 1 (state upgrade from 0)", v)
	}
	if n := len(schemas.Provider.Block.Attributes); n != 37 {
		t.Errorf("provider has %d attributes, want 37", n)
	}

	ids, err := srv.GetResourceIdentitySchemas(ctx, &tfprotov6.GetResourceIdentitySchemasRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for typ, want := range map[string]int{"kafka_topic": 1, "kafka_acl": 7, "kafka_quota": 2, "kafka_user_scram_credential": 2} {
		is, ok := ids.IdentitySchemas[typ]
		if !ok || len(is.IdentityAttributes) != want {
			t.Errorf("%s identity schema = %+v, want %d attributes", typ, is, want)
		}
	}
}

func TestTopicEqualNegativeReplicationFactor(t *testing.T) {
	want := Topic{Name: "t", Partitions: 3, ReplicationFactor: -1}
	if !want.Equal(Topic{Name: "t", Partitions: 3, ReplicationFactor: 3}) {
		t.Error("-1 must accept any replication factor Kafka reports")
	}
	want.ReplicationFactor = 2
	if want.Equal(Topic{Name: "t", Partitions: 3, ReplicationFactor: 3}) {
		t.Error("2 != 3")
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
	cases := map[string]struct{ typ, name string }{
		"orders-app|client-id": {"client-id", "orders-app"},
		"entity-default|user":  {"user", ""},
		"client-id:orders-app": {"client-id", "orders-app"},
		"user:":                {"user", ""},
		"ip:10.0.0.1":          {"ip", "10.0.0.1"},
	}
	for in, want := range cases {
		typ, name, err := parseQuotaImportID(in)
		if err != nil || typ != want.typ || name != want.name {
			t.Errorf("%q: got %q %q %v", in, typ, name, err)
		}
	}
	for _, bad := range []string{"just-a-name", "group:x", "a|b|c"} {
		if _, _, err := parseQuotaImportID(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}
