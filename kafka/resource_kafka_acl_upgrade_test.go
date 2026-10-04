package kafka

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

func TestUpgradeACLStateV0(t *testing.T) {
	base := map[string]string{
		"id": "User:Alice|*|Write|Allow|Topic|syslog", "acl_principal": "User:Alice", "acl_host": "*",
		"acl_operation": "Write", "acl_permission_type": "Allow", "resource_type": "Topic", "resource_name": "syslog",
	}
	raw, _ := json.Marshal(base)
	withFilter := map[string]string{"resource_pattern_type_filter": "Prefixed"}
	for k, v := range base {
		withFilter[k] = v
	}
	rawPrefixed, _ := json.Marshal(withFilter)

	cases := map[string]struct {
		state *tfprotov6.RawState
		want  string
	}{
		"json without filter":    {&tfprotov6.RawState{JSON: raw}, "Literal"},
		"json with filter":       {&tfprotov6.RawState{JSON: rawPrefixed}, "Prefixed"},
		"flatmap without filter": {&tfprotov6.RawState{Flatmap: base}, "Literal"},
	}
	for name, c := range cases {
		resp := &resource.UpgradeStateResponse{}
		upgradeACLStateV0(context.Background(), resource.UpgradeStateRequest{RawState: c.state}, resp)
		if resp.Diagnostics.HasError() {
			t.Fatalf("%s: %v", name, resp.Diagnostics)
		}
		out := map[string]any{}
		if err := json.Unmarshal(resp.DynamicValue.JSON, &out); err != nil {
			t.Fatal(err)
		}
		if out["resource_pattern_type_filter"] != c.want || out["acl_principal"] != "User:Alice" || out["id"] == nil {
			t.Errorf("%s: got %v", name, out)
		}
	}
}
