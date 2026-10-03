package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func kafkaACLResource() *schema.Resource {
	//lintignore:R011
	return &schema.Resource{
		CreateContext: aclCreate,
		ReadContext:   aclRead,
		DeleteContext: aclDelete,
		Importer: &schema.ResourceImporter{
			StateContext: importACL,
		},
		// Identity: the seven fields that make an ACL unique (the same ones the
		// pipe-delimited ID is built from). Used by `terraform query` and import blocks.
		Identity: &schema.ResourceIdentity{
			SchemaFunc: func() map[string]*schema.Schema {
				out := map[string]*schema.Schema{}
				for _, k := range aclIdentityAttributes {
					out[k] = &schema.Schema{Type: schema.TypeString, RequiredForImport: true}
				}
				return out
			},
		},
		SchemaVersion: 1,
		// Kept for states written before SchemaVersion 1 (StateUpgraders start after it).
		MigrateState: migrateKafkaAclState, //nolint:staticcheck // SA1019: still required for old states
		Schema: map[string]*schema.Schema{
			"resource_name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the resource",
			},
			"resource_type": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"resource_pattern_type_filter": {
				Type:             schema.TypeString,
				Default:          "Literal",
				Optional:         true,
				ForceNew:         true,
				ValidateDiagFunc: validateDiagFunc(validation.StringInSlice([]string{"Literal", "Prefixed"}, false)),
				Description:      "How to match the resource name. Valid values: Literal (exact match) or Prefixed (match resources with the given prefix).",
			},
			"acl_principal": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"acl_host": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"acl_operation": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"acl_permission_type": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
		},
	}
}

func aclCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*LazyClient)
	a := aclInfo(d)

	log.Printf("[INFO] Creating ACL %s", a)
	err := c.CreateACL(a)

	if err != nil {
		log.Println("[ERROR] Failed to create ACL")
		return diag.FromErr(err)
	}

	d.SetId(a.String())
	if err := setACLIdentity(d, a); err != nil {
		return diag.FromErr(err)
	}

	// Wait for ACL to be visible in Kafka before returning
	// This handles eventual consistency and ensures the ACL is actually created
	log.Printf("[INFO] Waiting for ACL %s to be visible in Kafka", a)
	err = waitForACLToBeVisible(ctx, c, a)
	if err != nil {
		log.Printf("[ERROR] ACL created but not visible: %v", err)
		return diag.FromErr(err)
	}

	return nil
}

func aclDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*LazyClient)
	a := aclInfo(d)
	log.Printf("[INFO] Deleting ACL %s", a)

	err := c.DeleteACL(a)
	if err != nil {
		return diag.FromErr(err)
	}

	// Wait for ACL to be removed from Kafka before returning
	// This handles eventual consistency and ensures the ACL is actually deleted
	log.Printf("[INFO] Waiting for ACL %s to be removed from Kafka", a)
	err = waitForACLToBeDeleted(ctx, c, a)
	if err != nil {
		log.Printf("[ERROR] ACL deletion requested but still visible: %v", err)
		return diag.FromErr(err)
	}

	return nil
}

func aclRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	log.Println("[INFO] Reading ACL")
	c := meta.(*LazyClient)
	a := aclInfo(d)
	log.Printf("[INFO] Reading ACL %s", a)

	currentACLs, err := c.ListACLs()
	if err != nil {
		return diag.FromErr(err)
	}

	for _, foundACLs := range currentACLs {
		// find only ACLs where ResourceName matches
		if foundACLs.ResourceName != a.Name {
			continue
		}
		if len(foundACLs.Acls) < 1 {
			continue
		}
		log.Printf("[INFO] Found (%d) ACL(s) for Resource %s: %+v.", len(foundACLs.Acls), foundACLs.ResourceName, foundACLs)

		for _, acl := range foundACLs.Acls {
			aclID := StringlyTypedACL{
				ACL: ACL{
					Principal:      acl.Principal,
					Host:           acl.Host,
					Operation:      ACLOperationToString(acl.Operation),
					PermissionType: ACLPermissionTypeToString(acl.PermissionType),
				},
				Resource: Resource{
					Type:              ACLResourceToString(foundACLs.ResourceType),
					Name:              foundACLs.ResourceName,
					PatternTypeFilter: foundACLs.ResourcePatternType.String(),
				},
			}

			// Found the ACL, so no need to remove it from state
			if a.String() == aclID.String() {
				return diag.FromErr(setACLIdentity(d, a))
			}
		}
	}

	// If we get here, the ACL was not found
	log.Printf("[INFO] Did not find ACL %s", a.String())
	d.SetId("")

	return nil
}

func importACL(ctx context.Context, d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	var parts []string
	if d.Id() != "" {
		parts = strings.Split(d.Id(), "|")
	} else {
		// Import by identity (import block with `identity`, or `terraform query`).
		identity, err := d.Identity()
		if err != nil {
			return nil, err
		}
		for _, k := range aclIdentityAttributes {
			parts = append(parts, identity.Get(k).(string))
		}
	}
	if len(parts) != len(aclIdentityAttributes) {
		return nil, fmt.Errorf("failed importing resource; expected format is acl_principal|acl_host|acl_operation|acl_permission_type|resource_type|resource_name|resource_pattern_type_filter - got %v segments instead of 7", len(parts))
	}

	errSet := errSetter{d: d}
	for i, k := range aclIdentityAttributes {
		errSet.Set(k, parts[i])
	}
	if errSet.err != nil {
		return nil, errSet.err
	}
	a := aclInfo(d)
	d.SetId(a.String())
	if err := setACLIdentity(d, a); err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}

// aclIdentityAttributes are in ID order (see StringlyTypedACL.String).
var aclIdentityAttributes = []string{
	"acl_principal",
	"acl_host",
	"acl_operation",
	"acl_permission_type",
	"resource_type",
	"resource_name",
	"resource_pattern_type_filter",
}

func aclAttributes(a StringlyTypedACL) map[string]string {
	return map[string]string{
		"acl_principal":                a.ACL.Principal,
		"acl_host":                     a.ACL.Host,
		"acl_operation":                a.ACL.Operation,
		"acl_permission_type":          a.ACL.PermissionType,
		"resource_type":                a.Type,
		"resource_name":                a.Name,
		"resource_pattern_type_filter": a.PatternTypeFilter,
	}
}

func setACLIdentity(d *schema.ResourceData, a StringlyTypedACL) error {
	identity, err := d.Identity()
	if err != nil {
		return err
	}
	for k, v := range aclAttributes(a) {
		if err := identity.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

type errSetter struct {
	err error
	d   *schema.ResourceData
}

func (es *errSetter) Set(key string, value interface{}) {
	if es.err != nil {
		return
	}
	//lintignore:R001
	es.err = es.d.Set(key, value)
}

func aclInfo(d *schema.ResourceData) StringlyTypedACL {
	s := StringlyTypedACL{
		ACL: ACL{
			Principal:      d.Get("acl_principal").(string),
			Host:           d.Get("acl_host").(string),
			Operation:      d.Get("acl_operation").(string),
			PermissionType: d.Get("acl_permission_type").(string),
		},
		Resource: Resource{
			Type:              d.Get("resource_type").(string),
			Name:              d.Get("resource_name").(string),
			PatternTypeFilter: d.Get("resource_pattern_type_filter").(string),
		},
	}
	return s
}

// waitForACLToBeVisible waits for an ACL to be visible in Kafka after creation
// This handles eventual consistency issues with Kafka ACL propagation
func waitForACLToBeVisible(ctx context.Context, c *LazyClient, expectedACL StringlyTypedACL) error {
	maxRetries := 10
	retryInterval := 200 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Invalidate cache to ensure we get fresh data
		err := c.InvalidateACLCache()
		if err != nil {
			return fmt.Errorf("failed to invalidate ACL cache: %w", err)
		}

		// List all ACLs
		acls, err := c.ListACLs()
		if err != nil {
			return fmt.Errorf("failed to list ACLs: %w", err)
		}

		// Check if our ACL exists
		for _, foundACLs := range acls {
			if foundACLs.ResourceName != expectedACL.Name {
				continue
			}

			for _, acl := range foundACLs.Acls {
				foundACL := StringlyTypedACL{
					ACL: ACL{
						Principal:      acl.Principal,
						Host:           acl.Host,
						Operation:      ACLOperationToString(acl.Operation),
						PermissionType: ACLPermissionTypeToString(acl.PermissionType),
					},
					Resource: Resource{
						Type:              ACLResourceToString(foundACLs.ResourceType),
						Name:              foundACLs.ResourceName,
						PatternTypeFilter: foundACLs.ResourcePatternType.String(),
					},
				}

				// Check for exact match
				if expectedACL.String() == foundACL.String() {
					log.Printf("[INFO] ACL %s is now visible in Kafka (attempt %d)", expectedACL, i+1)
					return nil
				}
			}
		}

		// If not found and not the last attempt, wait before retrying
		if i < maxRetries-1 {
			log.Printf("[DEBUG] ACL %s not yet visible, retrying in %v (attempt %d/%d)", expectedACL, retryInterval, i+1, maxRetries)
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("ACL %s was not visible in Kafka after %d attempts over %v", expectedACL, maxRetries, time.Duration(maxRetries)*retryInterval)
}

// waitForACLToBeDeleted waits for an ACL to be removed from Kafka after deletion
// This handles eventual consistency issues with Kafka ACL propagation
func waitForACLToBeDeleted(ctx context.Context, c *LazyClient, deletedACL StringlyTypedACL) error {
	maxRetries := 10
	retryInterval := 200 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Invalidate cache to ensure we get fresh data
		err := c.InvalidateACLCache()
		if err != nil {
			return fmt.Errorf("failed to invalidate ACL cache: %w", err)
		}

		// List all ACLs
		acls, err := c.ListACLs()
		if err != nil {
			return fmt.Errorf("failed to list ACLs: %w", err)
		}

		// Check if our ACL still exists
		found := false
		for _, foundACLs := range acls {
			if foundACLs.ResourceName != deletedACL.Name {
				continue
			}

			for _, acl := range foundACLs.Acls {
				foundACL := StringlyTypedACL{
					ACL: ACL{
						Principal:      acl.Principal,
						Host:           acl.Host,
						Operation:      ACLOperationToString(acl.Operation),
						PermissionType: ACLPermissionTypeToString(acl.PermissionType),
					},
					Resource: Resource{
						Type:              ACLResourceToString(foundACLs.ResourceType),
						Name:              foundACLs.ResourceName,
						PatternTypeFilter: foundACLs.ResourcePatternType.String(),
					},
				}

				// Check for exact match
				if deletedACL.String() == foundACL.String() {
					found = true
					break
				}
			}

			if found {
				break
			}
		}

		// If not found, the ACL has been successfully deleted
		if !found {
			log.Printf("[INFO] ACL %s has been removed from Kafka (attempt %d)", deletedACL, i+1)
			return nil
		}

		// If still found and not the last attempt, wait before retrying
		if i < maxRetries-1 {
			log.Printf("[DEBUG] ACL %s still visible, retrying in %v (attempt %d/%d)", deletedACL, retryInterval, i+1, maxRetries)
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("ACL %s was still visible in Kafka after %d attempts over %v", deletedACL, maxRetries, time.Duration(maxRetries)*retryInterval)
}
