# Documentation Maintenance Guide

This guide explains how to maintain the enhanced documentation when regenerating with `terraform-plugin-docs`.

## Directory Structure

To preserve custom documentation when running `terraform-plugin-docs generate`, use this structure:

```
terraform-provider-kafka/
├── templates/                      # Custom templates (preserved)
│   ├── index.md.tmpl              # Provider index template
│   ├── resources/
│   │   ├── topic.md.tmpl
│   │   ├── acl.md.tmpl
│   │   ├── quota.md.tmpl
│   │   └── user_scram_credential.md.tmpl
│   ├── data-sources/
│   │   └── topic.md.tmpl
│   └── guides/                    # Custom guides (preserved)
│       ├── authentication.md
│       ├── aws-msk-integration.md
│       ├── quick-start.md
│       ├── troubleshooting.md
│       └── migration.md
├── examples/                      # Example files (preserved)
│   ├── provider/
│   │   ├── provider-tls.tf
│   │   ├── provider-sasl-plain.tf
│   │   └── ...
│   ├── resources/
│   │   ├── kafka_topic/
│   │   │   ├── resource.tf
│   │   │   ├── logs.tf
│   │   │   ├── compacted.tf
│   │   │   └── import.sh
│   │   └── ...
│   └── data-sources/
│       └── kafka_topic/
│           └── data-source.tf
└── docs/                         # Generated (overwritten)
```

## Key Points

1. **Templates Directory**: Files in `templates/` override the default generation
2. **Examples Directory**: Referenced by templates using `{{tffile}}` and `{{codefile}}`
3. **Guides**: Place static guides directly in `templates/guides/` - they'll be copied as-is
4. **Schema Sections**: Use `{{ .SchemaMarkdown | trimspace }}` to include auto-generated schema

## Template Variables

Available in templates:
- `{{.Name}}` - Resource/data source name
- `{{.Type}}` - "Resource" or "Data Source"
- `{{.ProviderName}}` - Full provider name
- `{{.ProviderShortName}}` - Short provider name
- `{{.SchemaMarkdown}}` - Auto-generated schema documentation

## Regenerating Documentation

```bash
go generate ./...
```

This formats `examples/` and runs `tfplugindocs` with the flags pinned in `main.go`
(including the Terraform version: list resources need Terraform >= 1.14). Commit
`docs/` together with the change; the `docs` CI job fails when the generated
output differs from what is committed.

Never edit files in `docs/` by hand (except this guide): the next generation
overwrites them. Change `templates/`, `examples/` or the schema descriptions.

## Adding New Documentation

1. Create template: `templates/resources/new_resource.md.tmpl`
2. Create examples: `examples/resources/new_resource/*.tf`
3. Run generation: `go generate ./...`

List resources (`terraform query`): example in `examples/list-resources/<name>/list-resource.tfquery.hcl`, optional template `templates/list-resources/<short_name>.md.tmpl`.

## Migrating Current Enhanced Docs

To preserve the current enhanced documentation:

1. Copy content from `docs/` files to corresponding `templates/` files
2. Replace example code blocks with `{{tffile}}` references
3. Create example `.tf` files in `examples/` directory
4. Add `{{ .SchemaMarkdown | trimspace }}` where schema should appear

## Example Template

```markdown
---
page_title: "{{.Name}} {{.Type}} - {{.ProviderName}}"
subcategory: ""
description: |-
  Your description here
---

# {{.Name}} ({{.Type}})

Description and overview...

## Example Usage

{{tffile "examples/resources/kafka_topic/resource.tf"}}

## Import

{{codefile "shell" "examples/resources/kafka_topic/import.sh"}}

{{ .SchemaMarkdown | trimspace }}

## Additional Information

Custom content that will be preserved...
```