package kafka

import (
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
)

const (
	defaultGSSAPIServiceName = "kafka"
	defaultKrb5ConfigPath    = "/etc/krb5.conf"
)

// GSSAPIConfig is the provider-level Kerberos (SASL/GSSAPI) configuration.
//
// Authentication source, first match wins:
//   - KeyTabPath set  → keytab (KRB5_KEYTAB_AUTH)
//   - Password set    → username/password (KRB5_USER_AUTH)
//   - otherwise       → credentials cache (KRB5_CCACHE_AUTH): CCachePath,
//     $KRB5CCNAME or /tmp/krb5cc_<uid>, e.g. after `kinit`.
//
// sarama requires Username and Realm for every auth type; they are resolved
// from Principal ("user@REALM"), explicit Username/Realm, the ccache's own
// default principal, and finally default_realm in krb5.conf.
type GSSAPIConfig struct {
	ServiceName        string
	Principal          string
	Username           string
	Realm              string
	Password           string
	KeyTabPath         string
	CCachePath         string
	KerberosConfigPath string
	DisablePAFXFAST    bool
}

func (g GSSAPIConfig) masked() GSSAPIConfig {
	m := g
	if m.Password != "" {
		m.Password = "*****"
	}
	return m
}

// saramaConfig resolves defaults and returns a configuration that passes
// sarama's GSSAPI validation, or an error that names the provider attribute
// to fix.
func (g GSSAPIConfig) saramaConfig() (sarama.GSSAPIConfig, error) {
	out := sarama.GSSAPIConfig{
		ServiceName:        firstNonEmpty(g.ServiceName, defaultGSSAPIServiceName),
		KerberosConfigPath: firstNonEmpty(g.KerberosConfigPath, os.Getenv("KRB5_CONFIG"), defaultKrb5ConfigPath),
		DisablePAFXFAST:    g.DisablePAFXFAST,
	}

	username, realm := splitPrincipal(g.Principal)
	username = firstNonEmpty(g.Username, username)
	realm = firstNonEmpty(g.Realm, realm)

	switch {
	case g.KeyTabPath != "":
		out.AuthType = sarama.KRB5_KEYTAB_AUTH
		out.KeyTabPath = g.KeyTabPath
	case g.Password != "":
		out.AuthType = sarama.KRB5_USER_AUTH
		out.Password = g.Password
	default:
		out.AuthType = sarama.KRB5_CCACHE_AUTH
		path, err := ccachePath(g.CCachePath)
		if err != nil {
			return out, err
		}
		out.CCachePath = path
		// The cache knows who is logged in: no need to repeat it in HCL.
		if username == "" || realm == "" {
			if cc, err := credentials.LoadCCache(path); err == nil {
				username = firstNonEmpty(username, cc.GetClientPrincipalName().PrincipalNameString())
				realm = firstNonEmpty(realm, cc.GetClientRealm())
			}
		}
	}

	if realm == "" {
		if cfg, err := krb5config.Load(out.KerberosConfigPath); err == nil {
			realm = cfg.LibDefaults.DefaultRealm
		}
	}

	if username == "" {
		return out, fmt.Errorf("gssapi: set sasl_gssapi_principal (user@REALM) or sasl_gssapi_username")
	}
	if realm == "" {
		return out, fmt.Errorf("gssapi: realm unknown: set sasl_gssapi_realm, use a principal with @REALM, or default_realm in %s", out.KerberosConfigPath)
	}
	out.Username, out.Realm = username, realm
	return out, nil
}

// splitPrincipal splits "primary[/instance]@REALM" on the last '@'.
func splitPrincipal(p string) (user, realm string) {
	p = strings.TrimSpace(p)
	if i := strings.LastIndex(p, "@"); i > 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

// ccachePath resolves the credentials cache file. Only FILE: caches are
// readable by gokrb5; KEYRING:/KCM:/API: caches must be exported with
// `KRB5CCNAME=FILE:/tmp/cc kinit ...`.
func ccachePath(explicit string) (string, error) {
	p := firstNonEmpty(explicit, os.Getenv("KRB5CCNAME"))
	if p == "" {
		return fmt.Sprintf("/tmp/krb5cc_%d", os.Getuid()), nil
	}
	if strings.HasPrefix(p, "FILE:") {
		return strings.TrimPrefix(p, "FILE:"), nil
	}
	if i := strings.Index(p, ":"); i > 0 && !strings.Contains(p[:i], "/") {
		return "", fmt.Errorf("gssapi: credentials cache %q is not a file cache; only FILE: is supported (kinit with KRB5CCNAME=FILE:/path)", p)
	}
	return p, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
