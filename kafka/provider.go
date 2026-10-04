package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/list"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	pschema "github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// KafkaProvider is the terraform-plugin-framework provider. Every attribute
// can also come from an environment variable (see providerEnv); values in the
// provider block win.
type KafkaProvider struct {
	version string
}

func New(version string) func() provider.Provider {
	return func() provider.Provider { return &KafkaProvider{version: version} }
}

var (
	_ provider.Provider                  = (*KafkaProvider)(nil)
	_ provider.ProviderWithListResources = (*KafkaProvider)(nil)
)

type providerModel struct {
	BootstrapServers                       types.List   `tfsdk:"bootstrap_servers"`
	CaCert                                 types.String `tfsdk:"ca_cert"`
	CaCertFile                             types.String `tfsdk:"ca_cert_file"`
	ClientCert                             types.String `tfsdk:"client_cert"`
	ClientCertFile                         types.String `tfsdk:"client_cert_file"`
	ClientKey                              types.String `tfsdk:"client_key"`
	ClientKeyFile                          types.String `tfsdk:"client_key_file"`
	ClientKeyPassphrase                    types.String `tfsdk:"client_key_passphrase"`
	KafkaVersion                           types.String `tfsdk:"kafka_version"`
	SaslAwsAccessKey                       types.String `tfsdk:"sasl_aws_access_key"`
	SaslAwsContainerAuthorizationTokenFile types.String `tfsdk:"sasl_aws_container_authorization_token_file"`
	SaslAwsContainerCredentialsFullUri     types.String `tfsdk:"sasl_aws_container_credentials_full_uri"`
	SaslAwsCredsDebug                      types.Bool   `tfsdk:"sasl_aws_creds_debug"`
	SaslAwsExternalId                      types.String `tfsdk:"sasl_aws_external_id"`
	SaslAwsProfile                         types.String `tfsdk:"sasl_aws_profile"`
	SaslAwsRegion                          types.String `tfsdk:"sasl_aws_region"`
	SaslAwsRoleArn                         types.String `tfsdk:"sasl_aws_role_arn"`
	SaslAwsSecretKey                       types.String `tfsdk:"sasl_aws_secret_key"`
	SaslAwsSharedConfigFiles               types.List   `tfsdk:"sasl_aws_shared_config_files"`
	SaslAwsToken                           types.String `tfsdk:"sasl_aws_token"`
	SaslGssapiCcachePath                   types.String `tfsdk:"sasl_gssapi_ccache_path"`
	SaslGssapiDisablePafxfast              types.Bool   `tfsdk:"sasl_gssapi_disable_pafxfast"`
	SaslGssapiKerberosConfigPath           types.String `tfsdk:"sasl_gssapi_kerberos_config_path"`
	SaslGssapiKeytabPath                   types.String `tfsdk:"sasl_gssapi_keytab_path"`
	SaslGssapiPassword                     types.String `tfsdk:"sasl_gssapi_password"`
	SaslGssapiPrincipal                    types.String `tfsdk:"sasl_gssapi_principal"`
	SaslGssapiRealm                        types.String `tfsdk:"sasl_gssapi_realm"`
	SaslGssapiServiceName                  types.String `tfsdk:"sasl_gssapi_service_name"`
	SaslGssapiUsername                     types.String `tfsdk:"sasl_gssapi_username"`
	SaslMechanism                          types.String `tfsdk:"sasl_mechanism"`
	SaslOauthScopes                        types.List   `tfsdk:"sasl_oauth_scopes"`
	SaslPassword                           types.String `tfsdk:"sasl_password"`
	SaslTokenUrl                           types.String `tfsdk:"sasl_token_url"`
	SaslUsername                           types.String `tfsdk:"sasl_username"`
	SkipTlsVerify                          types.Bool   `tfsdk:"skip_tls_verify"`
	Timeout                                types.Int64  `tfsdk:"timeout"`
	TlsEnabled                             types.Bool   `tfsdk:"tls_enabled"`
}

func (p *KafkaProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "kafka"
	resp.Version = p.version
}

func (p *KafkaProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = pschema.Schema{Attributes: map[string]pschema.Attribute{
		"bootstrap_servers":     pschema.ListAttribute{ElementType: types.StringType, Required: true, Description: "A list of kafka brokers"},
		"ca_cert":               pschema.StringAttribute{Optional: true, Description: "CA certificate file to validate the server's certificate."},
		"ca_cert_file":          pschema.StringAttribute{Optional: true, Description: "Path to a CA certificate file to validate the server's certificate.", DeprecationMessage: "This parameter is now deprecated and will be removed in a later release, please use `ca_cert` instead."},
		"client_cert":           pschema.StringAttribute{Optional: true, Description: "The client certificate."},
		"client_cert_file":      pschema.StringAttribute{Optional: true, Description: "Path to a file containing the client certificate.", DeprecationMessage: "This parameter is now deprecated and will be removed in a later release, please use `client_cert` instead."},
		"client_key":            pschema.StringAttribute{Optional: true, Sensitive: true, Description: "The private key that the certificate was issued for."},
		"client_key_file":       pschema.StringAttribute{Optional: true, Description: "Path to a file containing the private key that the certificate was issued for.", DeprecationMessage: "This parameter is now deprecated and will be removed in a later release, please use `client_key` instead."},
		"client_key_passphrase": pschema.StringAttribute{Optional: true, Sensitive: true, Description: "The passphrase for the private key that the certificate was issued for."},
		"kafka_version":         pschema.StringAttribute{Optional: true, Description: "The version of Kafka protocol to use in `$MAJOR.$MINOR.$PATCH` format. Some features may not be available on older versions. Default is 2.7.0."},
		"sasl_aws_access_key":   pschema.StringAttribute{Optional: true, Sensitive: true, Description: "The AWS access key."},
		"sasl_aws_container_authorization_token_file": pschema.StringAttribute{Optional: true, Description: "Path to a file containing the AWS pod identity authorization token"},
		"sasl_aws_container_credentials_full_uri":     pschema.StringAttribute{Optional: true, Description: "URI to retrieve AWS credentials from"},
		"sasl_aws_creds_debug":                        pschema.BoolAttribute{Optional: true, Description: "Set this to true to turn AWS credentials debug."},
		"sasl_aws_external_id":                        pschema.StringAttribute{Optional: true, Description: "External ID of the AWS IAM role to assume"},
		"sasl_aws_profile":                            pschema.StringAttribute{Optional: true, Description: "AWS profile name to use"},
		"sasl_aws_region":                             pschema.StringAttribute{Optional: true, Description: "AWS region where MSK is deployed."},
		"sasl_aws_role_arn":                           pschema.StringAttribute{Optional: true, Description: "Arn of an AWS IAM role to assume"},
		"sasl_aws_secret_key":                         pschema.StringAttribute{Optional: true, Sensitive: true, Description: "The AWS secret key."},
		"sasl_aws_shared_config_files":                pschema.ListAttribute{ElementType: types.StringType, Optional: true, Description: "List of paths to AWS shared config files."},
		"sasl_aws_token":                              pschema.StringAttribute{Optional: true, Sensitive: true, Description: "The AWS session token. Only required if you are using temporary security credentials."},
		"sasl_gssapi_ccache_path":                     pschema.StringAttribute{Optional: true, Description: "Credentials cache used when neither keytab nor password is set (e.g. after `kinit`). Defaults to `$KRB5CCNAME`, then `/tmp/krb5cc_<uid>`. Only `FILE:` caches are supported."},
		"sasl_gssapi_disable_pafxfast":                pschema.BoolAttribute{Optional: true, Description: "Disable PA-FX-FAST. Required for Active Directory and most KDCs that do not support it."},
		"sasl_gssapi_kerberos_config_path":            pschema.StringAttribute{Optional: true, Description: "Path to krb5.conf. Defaults to `$KRB5_CONFIG`, then `/etc/krb5.conf`."},
		"sasl_gssapi_keytab_path":                     pschema.StringAttribute{Optional: true, Description: "Path to a keytab. When set, keytab authentication is used."},
		"sasl_gssapi_password":                        pschema.StringAttribute{Optional: true, Sensitive: true, Description: "Kerberos password. Used when no keytab is set."},
		"sasl_gssapi_principal":                       pschema.StringAttribute{Optional: true, Description: "Client principal, e.g. `terraform/ci@EXAMPLE.COM`. Username and realm are taken from it. With a credentials cache it may be omitted (read from the cache)."},
		"sasl_gssapi_realm":                           pschema.StringAttribute{Optional: true, Description: "Kerberos realm. Defaults to the principal's realm, the credentials cache, then `default_realm` in krb5.conf."},
		"sasl_gssapi_service_name":                    pschema.StringAttribute{Optional: true, Description: "Kerberos service name of the brokers (the `primary` of their principal), when using sasl mechanism gssapi."},
		"sasl_gssapi_username":                        pschema.StringAttribute{Optional: true, Description: "Principal without the realm. Overrides the one derived from `sasl_gssapi_principal`."},
		"sasl_mechanism":                              pschema.StringAttribute{Optional: true, Description: "SASL mechanism, can be plain, scram-sha512, scram-sha256, aws-iam, oauthbearer, gssapi"},
		"sasl_oauth_scopes":                           pschema.ListAttribute{ElementType: types.StringType, Optional: true, Description: "OAuth scopes to request when using the oauthbearer mechanism"},
		"sasl_password":                               pschema.StringAttribute{Optional: true, Sensitive: true, Description: "Password for SASL authentication."},
		"sasl_token_url":                              pschema.StringAttribute{Optional: true, Description: "The url to retrieve oauth2 tokens from, when using sasl mechanism oauthbearer"},
		"sasl_username":                               pschema.StringAttribute{Optional: true, Description: "Username for SASL authentication."},
		"skip_tls_verify":                             pschema.BoolAttribute{Optional: true, Description: "Set this to true only if the target Kafka server is an insecure development instance."},
		"timeout":                                     pschema.Int64Attribute{Optional: true, Description: "Timeout in seconds"},
		"tls_enabled":                                 pschema.BoolAttribute{Optional: true, Description: "Enable communication with the Kafka Cluster over TLS."},
	}}
}

// Configure builds the (lazily connecting) Kafka client. Precedence for every
// attribute: provider block, then the environment variable, then the default
// — the same variables and defaults as the SDKv2 provider had.
func (p *KafkaProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var m providerModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	config := buildConfig(ctx, m, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	log.Printf("[TRACE] Config @ %v", config.copyWithMaskedSensitiveValues())
	client := &LazyClient{Config: config}
	resp.ResourceData = client
	resp.DataSourceData = client
	resp.ListResourceData = client
}

// buildConfig applies the env variables and defaults to the provider block.
func buildConfig(ctx context.Context, m providerModel, diags *diag.Diagnostics) *Config {
	resp := struct{ Diagnostics *diag.Diagnostics }{diags}

	str := func(v types.String, env, def string) string {
		if !v.IsNull() && !v.IsUnknown() {
			return v.ValueString()
		}
		if env != "" {
			if e, ok := os.LookupEnv(env); ok {
				return e
			}
		}
		return def
	}
	boolean := func(v types.Bool, env string, def bool) bool {
		if !v.IsNull() && !v.IsUnknown() {
			return v.ValueBool()
		}
		if e, ok := os.LookupEnv(env); ok {
			if b, err := strconv.ParseBool(e); err == nil {
				return b
			}
			resp.Diagnostics.AddError("Invalid environment variable", fmt.Sprintf("%s=%q is not a boolean", env, e))
		}
		return def
	}
	strList := func(v types.List, env string) []string {
		if !v.IsNull() && !v.IsUnknown() {
			var out []string
			resp.Diagnostics.Append(v.ElementsAs(ctx, &out, false)...)
			return out
		}
		if e, ok := os.LookupEnv(env); ok && e != "" {
			return nonEmptyAndTrimmed(strings.Split(e, ","))
		}
		return nil
	}

	var brokers []string
	resp.Diagnostics.Append(m.BootstrapServers.ElementsAs(ctx, &brokers, false)...)

	saslMechanism := str(m.SaslMechanism, "KAFKA_SASL_MECHANISM", "plain")
	switch saslMechanism {
	case "scram-sha512", "scram-sha256", "aws-iam", "oauthbearer", "plain", "gssapi":
	default:
		resp.Diagnostics.AddAttributeError(pathRoot("sasl_mechanism"), "Invalid sasl_mechanism",
			fmt.Sprintf("%q: can only be \"scram-sha256\", \"scram-sha512\", \"aws-iam\", \"oauthbearer\", \"gssapi\" or \"plain\"", saslMechanism))
	}

	timeout := 120
	if !m.Timeout.IsNull() && !m.Timeout.IsUnknown() {
		timeout = int(m.Timeout.ValueInt64())
	}

	config := &Config{
		BootstrapServers:                       &brokers,
		CACert:                                 str(m.CaCert, "KAFKA_CA_CERT", ""),
		ClientCert:                             str(m.ClientCert, "KAFKA_CLIENT_CERT", ""),
		ClientCertKey:                          str(m.ClientKey, "KAFKA_CLIENT_KEY", ""),
		ClientCertKeyPassphrase:                str(m.ClientKeyPassphrase, "KAFKA_CLIENT_KEY_PASSPHRASE", ""),
		KafkaVersion:                           str(m.KafkaVersion, "KAFKA_VERSION", "2.7.0"),
		SkipTLSVerify:                          boolean(m.SkipTlsVerify, "KAFKA_SKIP_VERIFY", false),
		SASLAWSRegion:                          str(m.SaslAwsRegion, "KAFKA_SASL_IAM_AWS_REGION", ""),
		SASLAWSContainerAuthorizationTokenFile: str(m.SaslAwsContainerAuthorizationTokenFile, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", ""),
		SASLAWSContainerCredentialsFullUri:     str(m.SaslAwsContainerCredentialsFullUri, "AWS_CONTAINER_CREDENTIALS_FULL_URI", ""),
		SASLUsername:                           str(m.SaslUsername, "KAFKA_SASL_USERNAME", ""),
		SASLPassword:                           str(m.SaslPassword, "KAFKA_SASL_PASSWORD", ""),
		SASLTokenUrl:                           str(m.SaslTokenUrl, "KAFKA_SASL_TOKEN_URL", ""),
		SASLAWSRoleArn:                         str(m.SaslAwsRoleArn, "AWS_ROLE_ARN", ""),
		SASLAWSExternalId:                      str(m.SaslAwsExternalId, "", ""),
		SASLAWSProfile:                         str(m.SaslAwsProfile, "AWS_PROFILE", ""),
		SASLAWSSharedConfigFiles:               ptrIfAny(strList(m.SaslAwsSharedConfigFiles, "AWS_SHARED_CONFIG_FILES")),
		SASLAWSAccessKey:                       str(m.SaslAwsAccessKey, "AWS_ACCESS_KEY_ID", ""),
		SASLAWSSecretKey:                       str(m.SaslAwsSecretKey, "AWS_SECRET_ACCESS_KEY", ""),
		SASLAWSToken:                           str(m.SaslAwsToken, "AWS_SESSION_TOKEN", ""),
		SASLAWSCredsDebug:                      boolean(m.SaslAwsCredsDebug, "AWS_CREDS_DEBUG", false),
		SASLOAuthScopes:                        strList(m.SaslOauthScopes, "KAFKA_SASL_OAUTH_SCOPES"),
		SASLMechanism:                          saslMechanism,
		SASLGSSAPI: GSSAPIConfig{
			ServiceName:        str(m.SaslGssapiServiceName, "KAFKA_SASL_GSSAPI_SERVICE_NAME", defaultGSSAPIServiceName),
			Principal:          str(m.SaslGssapiPrincipal, "KAFKA_SASL_GSSAPI_PRINCIPAL", ""),
			Username:           str(m.SaslGssapiUsername, "KAFKA_SASL_GSSAPI_USERNAME", ""),
			Realm:              str(m.SaslGssapiRealm, "KAFKA_SASL_GSSAPI_REALM", ""),
			Password:           str(m.SaslGssapiPassword, "KAFKA_SASL_GSSAPI_PASSWORD", ""),
			KeyTabPath:         str(m.SaslGssapiKeytabPath, "KAFKA_SASL_GSSAPI_KEYTAB_PATH", ""),
			CCachePath:         str(m.SaslGssapiCcachePath, "KAFKA_SASL_GSSAPI_CCACHE_PATH", ""),
			KerberosConfigPath: str(m.SaslGssapiKerberosConfigPath, "KAFKA_SASL_GSSAPI_KRB5_CONF", ""),
			DisablePAFXFAST:    boolean(m.SaslGssapiDisablePafxfast, "KAFKA_SASL_GSSAPI_DISABLE_PAFXFAST", true),
		},
		TLSEnabled: boolean(m.TlsEnabled, "KAFKA_ENABLE_TLS", true),
		Timeout:    timeout,
	}
	// Deprecated *_file attributes are fallbacks (same env variables).
	if config.CACert == "" {
		config.CACert = str(m.CaCertFile, "KAFKA_CA_CERT", "")
	}
	if config.ClientCert == "" {
		config.ClientCert = str(m.ClientCertFile, "KAFKA_CLIENT_CERT", "")
	}
	if config.ClientCertKey == "" {
		config.ClientCertKey = str(m.ClientKeyFile, "KAFKA_CLIENT_KEY", "")
	}
	return config
}

func (p *KafkaProvider) Resources(context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		newTopicResource,
		newACLResource,
		newQuotaResource,
		newUserScramCredentialResource,
		newBrokerConfigResource,
	}
}

func (p *KafkaProvider) DataSources(context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		newTopicDataSource,
		newTopicsDataSource,
		newClusterDataSource,
		newACLsDataSource,
		newQuotasDataSource,
		newUserScramCredentialsDataSource,
		newBrokerConfigDataSource,
	}
}

func (p *KafkaProvider) ListResources(context.Context) []func() list.ListResource {
	return []func() list.ListResource{
		func() list.ListResource { return &topicListResource{} },
		func() list.ListResource { return &aclListResource{} },
		func() list.ListResource { return &quotaListResource{} },
		func() list.ListResource { return &scramListResource{} },
	}
}

func ptrIfAny(s []string) *[]string {
	if len(s) == 0 {
		return nil
	}
	return &s
}
