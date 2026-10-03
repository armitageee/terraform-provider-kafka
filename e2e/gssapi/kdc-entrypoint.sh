#!/bin/sh
# Throwaway MIT KDC for the GSSAPI e2e test. Writes keytabs to /keytabs.
set -eu
apk add --no-cache krb5-server krb5 >/dev/null
cp /e2e/krb5.conf /etc/krb5.conf
mkdir -p /var/lib/krb5kdc
cat > /var/lib/krb5kdc/kdc.conf <<KDC
[realms]
  EXAMPLE.TEST = {
    database_name = /var/lib/krb5kdc/principal
    acl_file = /var/lib/krb5kdc/kadm5.acl
    key_stash_file = /var/lib/krb5kdc/.k5.EXAMPLE.TEST
    supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
  }
KDC
export KRB5_KDC_PROFILE=/var/lib/krb5kdc/kdc.conf
kdb5_util create -s -r EXAMPLE.TEST -P masterpw >/dev/null
kadmin.local -q "addprinc -randkey kafka/kafka.example.test@EXAMPLE.TEST" >/dev/null
kadmin.local -q "addprinc -randkey terraform/ci@EXAMPLE.TEST" >/dev/null
kadmin.local -q "addprinc -pw alicepw alice@EXAMPLE.TEST" >/dev/null
rm -f /keytabs/*.keytab
kadmin.local -q "ktadd -k /keytabs/kafka.keytab kafka/kafka.example.test@EXAMPLE.TEST" >/dev/null
kadmin.local -q "ktadd -k /keytabs/terraform.keytab terraform/ci@EXAMPLE.TEST" >/dev/null
chmod 644 /keytabs/*.keytab
touch /keytabs/ready
exec krb5kdc -n
