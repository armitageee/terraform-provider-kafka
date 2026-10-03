package kafka

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestGSSAPIE2E runs against a real KDC + Kafka (SASL_PLAINTEXT/GSSAPI):
//
//	docker compose -f e2e/gssapi/docker-compose.yaml run --rm client
//
// It goes through the provider's own path (Config → NewClient) and creates,
// reads and deletes a topic, once per Kerberos credential source.
func TestGSSAPIE2E(t *testing.T) {
	if os.Getenv("GSSAPI_E2E") != "1" {
		t.Skip("set GSSAPI_E2E=1 (see e2e/gssapi/docker-compose.yaml)")
	}
	brokers := []string{os.Getenv("KAFKA_BOOTSTRAP")}

	cases := map[string]GSSAPIConfig{
		"keytab": {
			Principal:       "terraform/ci@EXAMPLE.TEST",
			KeyTabPath:      os.Getenv("GSSAPI_E2E_KEYTAB"),
			DisablePAFXFAST: true,
		},
		"password, realm from krb5.conf": {
			Principal:       "alice",
			Password:        "alicepw",
			DisablePAFXFAST: true,
		},
	}
	for name, g := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := NewClient(&Config{
				BootstrapServers: &brokers,
				Timeout:          30,
				SASLMechanism:    "gssapi",
				SASLGSSAPI:       g,
			})
			if err != nil {
				t.Fatalf("NewClient over GSSAPI: %v", err)
			}
			topic := fmt.Sprintf("gssapi-e2e-%d", time.Now().UnixNano())
			if err := c.CreateTopic(Topic{Name: topic, Partitions: 1, ReplicationFactor: 1}); err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			got, err := c.ReadTopic(topic, true)
			if err != nil || got.Name != topic {
				t.Fatalf("ReadTopic: %+v, %v", got, err)
			}
			if err := c.DeleteTopic(topic); err != nil {
				t.Fatalf("DeleteTopic: %v", err)
			}
		})
	}
}
