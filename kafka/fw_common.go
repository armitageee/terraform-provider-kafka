package kafka

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var pathRoot = path.Root

// clientFrom unwraps the provider data given to Configure of resources, data
// sources and list resources. nil data means the provider is not configured
// yet (e.g. during validation); callers then do nothing.
func clientFrom(data any, diags *diag.Diagnostics) *LazyClient {
	if data == nil {
		return nil
	}
	c, ok := data.(*LazyClient)
	if !ok {
		diags.AddError("Unexpected provider data", fmt.Sprintf("expected *LazyClient, got %T", data))
		return nil
	}
	return c
}

// waitFor polls check until it reports done, fails, or the timeout passes.
// Kafka admin calls return before every broker applied the change, so
// resources wait until the change is visible (create, update and delete).
func waitFor(ctx context.Context, what string, timeout, delay, interval time.Duration, check func() (bool, error)) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}
	deadline := time.Now().Add(timeout)
	for {
		done, err := check()
		if err != nil {
			return fmt.Errorf("waiting for %s: %w", what, err)
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %s waiting for %s", timeout, what)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (c *LazyClient) timeout() time.Duration {
	if c == nil || c.Config == nil || c.Config.Timeout <= 0 {
		return 120 * time.Second
	}
	return time.Duration(c.Config.Timeout) * time.Second
}

// replicationFactorValidator: -1 (Confluent placement constraints) or >= 1.
type replicationFactorValidator struct{}

func (replicationFactorValidator) Description(context.Context) string {
	return "must be -1 or at least 1"
}

func (v replicationFactorValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (replicationFactorValidator) ValidateInt64(_ context.Context, req validator.Int64Request, resp *validator.Int64Response) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if n := req.ConfigValue.ValueInt64(); n != -1 && n < 1 {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid replication_factor",
			fmt.Sprintf("replication_factor must be -1 or at least 1, got %d", n))
	}
}

// stringMap converts a framework map of strings; null → nil.
func stringMap(ctx context.Context, m types.Map, diags *diag.Diagnostics) map[string]string {
	if m.IsNull() || m.IsUnknown() {
		return nil
	}
	out := map[string]string{}
	diags.Append(m.ElementsAs(ctx, &out, false)...)
	return out
}

// float64Map converts a framework map of numbers; null → nil.
func float64Map(ctx context.Context, m types.Map, diags *diag.Diagnostics) map[string]float64 {
	if m.IsNull() || m.IsUnknown() {
		return nil
	}
	out := map[string]float64{}
	diags.Append(m.ElementsAs(ctx, &out, false)...)
	return out
}

// nonBlank matches strings with at least one non-whitespace character.
var nonBlank = regexp.MustCompile(`\S`)
