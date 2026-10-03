package kafka

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

// gssapiSaramaConfig builds the full sarama config through the provider path
// and runs sarama's own validation on it.
func gssapiSaramaConfig(t *testing.T, g GSSAPIConfig) (*sarama.Config, error) {
	t.Helper()
	brokers := []string{"localhost:9092"}
	c := &Config{BootstrapServers: &brokers, Timeout: 120, SASLMechanism: "gssapi", SASLGSSAPI: g}
	kc, err := c.newKafkaConfig()
	if err != nil {
		return nil, err
	}
	if err := kc.Validate(); err != nil {
		t.Fatalf("sarama rejected the config: %v", err)
	}
	return kc, nil
}

func writeKrb5Conf(t *testing.T, realm string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "krb5.conf")
	body := "[libdefaults]\n  default_realm = " + realm + "\n\n[realms]\n  " + realm + " = {\n    kdc = kdc.example.com\n  }\n"
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// writeCCache writes a minimal MIT credentials cache (format 0x0504) that holds
// only the default principal — enough for the provider to learn who is logged in.
func writeCCache(t *testing.T, realm string, components ...string) string {
	t.Helper()
	var b bytes.Buffer
	be := func(v any) { _ = binary.Write(&b, binary.BigEndian, v) }
	str := func(s string) { be(uint32(len(s))); b.WriteString(s) }
	be(uint16(0x0504)) // version
	be(uint16(0))      // header length
	be(uint32(1))      // name type KRB5_NT_PRINCIPAL
	be(uint32(len(components)))
	str(realm)
	for _, c := range components {
		str(c)
	}
	p := filepath.Join(t.TempDir(), "krb5cc")
	if err := os.WriteFile(p, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestGSSAPIKeytabFromPrincipal(t *testing.T) {
	kc, err := gssapiSaramaConfig(t, GSSAPIConfig{
		Principal:          "terraform/ci@EXAMPLE.COM",
		KeyTabPath:         "/etc/security/terraform.keytab",
		KerberosConfigPath: "/etc/krb5.conf",
		DisablePAFXFAST:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	g := kc.Net.SASL.GSSAPI
	if kc.Net.SASL.Mechanism != sarama.SASLTypeGSSAPI || !kc.Net.SASL.Enable {
		t.Fatalf("mechanism = %q enabled=%v", kc.Net.SASL.Mechanism, kc.Net.SASL.Enable)
	}
	if g.AuthType != sarama.KRB5_KEYTAB_AUTH || g.Username != "terraform/ci" || g.Realm != "EXAMPLE.COM" || g.ServiceName != "kafka" {
		t.Fatalf("unexpected gssapi config: %+v", g)
	}
	if kc.Net.SASL.Password != "" {
		t.Fatal("no dummy SASL password should be needed for GSSAPI")
	}
}

func TestGSSAPIPasswordAuth(t *testing.T) {
	kc, err := gssapiSaramaConfig(t, GSSAPIConfig{
		Principal:          "alice@EXAMPLE.COM",
		Password:           "s3cret",
		ServiceName:        "kafka-broker",
		KerberosConfigPath: "/etc/krb5.conf",
	})
	if err != nil {
		t.Fatal(err)
	}
	g := kc.Net.SASL.GSSAPI
	if g.AuthType != sarama.KRB5_USER_AUTH || g.Password != "s3cret" || g.ServiceName != "kafka-broker" {
		t.Fatalf("unexpected gssapi config: %+v", g)
	}
}

func TestGSSAPIKeytabWinsOverPassword(t *testing.T) {
	kc, err := gssapiSaramaConfig(t, GSSAPIConfig{
		Principal: "alice@EXAMPLE.COM", Password: "x", KeyTabPath: "/k.keytab", KerberosConfigPath: "/etc/krb5.conf",
	})
	if err != nil {
		t.Fatal(err)
	}
	if kc.Net.SASL.GSSAPI.AuthType != sarama.KRB5_KEYTAB_AUTH {
		t.Fatalf("keytab must take precedence, got auth type %d", kc.Net.SASL.GSSAPI.AuthType)
	}
}

func TestGSSAPIRealmFromKrb5Conf(t *testing.T) {
	conf := writeKrb5Conf(t, "CORP.EXAMPLE")
	kc, err := gssapiSaramaConfig(t, GSSAPIConfig{
		Username: "svc-terraform", KeyTabPath: "/k.keytab", KerberosConfigPath: conf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if kc.Net.SASL.GSSAPI.Realm != "CORP.EXAMPLE" {
		t.Fatalf("realm = %q, want default_realm from krb5.conf", kc.Net.SASL.GSSAPI.Realm)
	}
}

func TestGSSAPICCacheFromEnvAndPrincipalFromCache(t *testing.T) {
	cache := writeCCache(t, "EXAMPLE.COM", "terraform", "ci")
	t.Setenv("KRB5CCNAME", "FILE:"+cache)
	kc, err := gssapiSaramaConfig(t, GSSAPIConfig{KerberosConfigPath: "/etc/krb5.conf"})
	if err != nil {
		t.Fatal(err)
	}
	g := kc.Net.SASL.GSSAPI
	if g.AuthType != sarama.KRB5_CCACHE_AUTH || g.CCachePath != cache {
		t.Fatalf("want ccache auth at %s, got %+v", cache, g)
	}
	if g.Username != "terraform/ci" || g.Realm != "EXAMPLE.COM" {
		t.Fatalf("principal must come from the cache, got %q @ %q", g.Username, g.Realm)
	}
}

func TestGSSAPINonFileCCacheIsAnError(t *testing.T) {
	t.Setenv("KRB5CCNAME", "KEYRING:persistent:1000")
	_, err := GSSAPIConfig{Principal: "a@B"}.saramaConfig()
	if err == nil || !strings.Contains(err.Error(), "FILE:") {
		t.Fatalf("want a FILE:-only error, got %v", err)
	}
}

func TestGSSAPIMissingIdentity(t *testing.T) {
	t.Setenv("KRB5CCNAME", filepath.Join(t.TempDir(), "absent"))
	_, err := GSSAPIConfig{KerberosConfigPath: filepath.Join(t.TempDir(), "none.conf")}.saramaConfig()
	if err == nil || !strings.Contains(err.Error(), "sasl_gssapi_principal") {
		t.Fatalf("want an error naming sasl_gssapi_principal, got %v", err)
	}

	_, err = GSSAPIConfig{Username: "bob", KeyTabPath: "/k", KerberosConfigPath: filepath.Join(t.TempDir(), "none.conf")}.saramaConfig()
	if err == nil || !strings.Contains(err.Error(), "realm") {
		t.Fatalf("want a realm error, got %v", err)
	}
}

func TestGSSAPIPasswordIsMasked(t *testing.T) {
	brokers := []string{"localhost:9092"}
	c := &Config{BootstrapServers: &brokers, SASLGSSAPI: GSSAPIConfig{Password: "s3cret"}}
	if got := c.copyWithMaskedSensitiveValues().SASLGSSAPI.Password; got != "*****" {
		t.Fatalf("password not masked: %q", got)
	}
}

func TestSplitPrincipal(t *testing.T) {
	for in, want := range map[string][2]string{
		"terraform/ci@EXAMPLE.COM": {"terraform/ci", "EXAMPLE.COM"},
		"alice":                    {"alice", ""},
		"weird@name@REALM":         {"weird@name", "REALM"},
	} {
		u, r := splitPrincipal(in)
		if u != want[0] || r != want[1] {
			t.Errorf("splitPrincipal(%q) = %q, %q; want %q, %q", in, u, r, want[0], want[1])
		}
	}
}
