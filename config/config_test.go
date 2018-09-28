// Copyright 2012, 2013, 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package config_test

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/base64"
	"io/ioutil"
	"path"
	"testing"
	"time"

	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery"

	"gopkg.in/juju/charmstore.v5/config"
)

func TestPackage(t *testing.T) {
	gc.TestingT(t)
}

type ConfigSuite struct {
	jujutesting.IsolationSuite
}

var _ = gc.Suite(&ConfigSuite{})

const testConfig = `
audit-log-file: /var/log/charmstore/audit.log
audit-log-max-size: 500
audit-log-max-age: 1
mongo-url: localhost:23456
api-addr: blah:2324
foo: 1
bar: false
auth-username: myuser
auth-password: mypasswd
identity-location: localhost:18082
identity-public-key: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA=
identity-api-url: "http://example.com/identity"
terms-public-key: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFB=
terms-location: localhost:8092
agent-username: agentuser
agent-key:
  private: lsvcDkapKoFxIyjX9/eQgb3s41KVwPMISFwAJdVCZ70=
  public: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA=
stats-cache-max-age: 1h
search-cache-max-age: 15m
request-timeout: 500ms
max-mgo-sessions: 10
blobstore: swift
swift-auth-url: 'https://foo.com'
swift-username: bob
swift-secret: secret
swift-bucket: bucket
swift-region: somewhere
swift-tenant: a-tenant
swift-authmode: userpass
logging-config: INFO
docker-registry-address: 0.1.3.5:1000
docker-registry-auth-certs: |
  -----BEGIN CERTIFICATE-----
  MIIBSDCB+KADAgECAgEBMAoGCCqGSM49BAMCMA8xDTALBgNVBAMTBHJvb3QwHhcN
  MTgwNTMwMDYxNzQ1WhcNMjMwNTMwMDYxNzQ1WjAPMQ0wCwYDVQQDEwR0ZXN0ME4w
  EAYHKoZIzj0CAQYFK4EEACEDOgAEZVrQP4knlGBQ2cOMsYmgc0VEWu8DmOFlFa8s
  /ym8yiBvsCfa7/t/V53VzepLnvTYb6j0LeMcnXajUDBOMAwGA1UdEwEB/wQCMAAw
  HQYDVR0OBBYEFG1euQX6O6FbNV4lTu0CYAnFCpc8MB8GA1UdIwQYMBaAFNopWnFZ
  iUBhd2W9d8NKbkRf8gujMAoGCCqGSM49BAMCAz8AMDwCHEPZ9X8JQRe5KBAMUTfo
  wngH3J2yXb1nQXzLR4cCHEbutF5CmWNzWzcek2JfQMOl7aFjcBxAerJGgRU=
  -----END CERTIFICATE-----
  -----BEGIN CERTIFICATE-----
  MIIBKzCB2qADAgECAgEAMAoGCCqGSM49BAMCMA8xDTALBgNVBAMTBHJvb3QwHhcN
  MTgwNTMwMDYxNDQyWhcNMjgwNTI5MDYxNDQyWjAPMQ0wCwYDVQQDEwRyb290ME4w
  EAYHKoZIzj0CAQYFK4EEACEDOgAEp5HUPVxs3wdpFF/HrimbFPVWkG+v6RacjFyP
  ujEylCfsONDOvYFzzz3x6/kxpQBl0ZYCHSJSDzKjMjAwMA8GA1UdEwEB/wQFMAMB
  Af8wHQYDVR0OBBYEFNopWnFZiUBhd2W9d8NKbkRf8gujMAoGCCqGSM49BAMCA0AA
  MD0CHQC7z3ryynKOXgm/flVbOytXmAgnc8n2I7jLGMKhAhwmW2IwwXFWcH/nX9K/
  e9AIP3l4dkWUxrNGRqwW
  -----END CERTIFICATE-----
docker-registry-auth-key: |
  -----BEGIN EC PRIVATE KEY-----
  MGgCAQEEHM9ekg7h0LAhNBaiSJolcfDNtyfS94DyUblrFu+gBwYFK4EEACGhPAM6
  AARlWtA/iSeUYFDZw4yxiaBzRURa7wOY4WUVryz/KbzKIG+wJ9rv+39XndXN6kue
  9NhvqPQt4xyddg==
  -----END EC PRIVATE KEY-----
docker-registry-token-duration: 1h10m
tempdir: /var/tmp/charmstore
disable-slow-metadata: true
`

func (s *ConfigSuite) readConfig(c *gc.C, content string) (*config.Config, error) {
	// Write the configuration content to file.
	path := path.Join(c.MkDir(), "charmd.conf")
	err := ioutil.WriteFile(path, []byte(content), 0666)
	c.Assert(err, gc.Equals, nil)

	// Read the configuration.
	return config.Read(path)
}

func (s *ConfigSuite) TestRead(c *gc.C) {
	conf, err := s.readConfig(c, testConfig)
	c.Assert(err, gc.Equals, nil)
	c.Assert(conf, jc.DeepEquals, &config.Config{
		AuditLogFile:     "/var/log/charmstore/audit.log",
		AuditLogMaxAge:   1,
		AuditLogMaxSize:  500,
		MongoURL:         "localhost:23456",
		APIAddr:          "blah:2324",
		AuthUsername:     "myuser",
		AuthPassword:     "mypasswd",
		IdentityLocation: "localhost:18082",
		IdentityPublicKey: &bakery.PublicKey{
			Key: mustParseKey("+qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA="),
		},
		TermsLocation: "localhost:8092",
		TermsPublicKey: &bakery.PublicKey{
			Key: mustParseKey("+qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFB="),
		},
		AgentUsername: "agentuser",
		AgentKey: &bakery.KeyPair{
			Public: bakery.PublicKey{
				Key: mustParseKey("+qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA="),
			},
			Private: bakery.PrivateKey{
				mustParseKey("lsvcDkapKoFxIyjX9/eQgb3s41KVwPMISFwAJdVCZ70="),
			},
		},
		StatsCacheMaxAge:      config.DurationString{time.Hour},
		RequestTimeout:        config.DurationString{500 * time.Millisecond},
		MaxMgoSessions:        10,
		SearchCacheMaxAge:     config.DurationString{15 * time.Minute},
		BlobStore:             config.SwiftBlobStore,
		SwiftAuthURL:          "https://foo.com",
		SwiftUsername:         "bob",
		SwiftSecret:           "secret",
		SwiftBucket:           "bucket",
		SwiftRegion:           "somewhere",
		SwiftTenant:           "a-tenant",
		SwiftAuthMode:         &config.SwiftAuthMode{identity.AuthUserPass},
		LoggingConfig:         "INFO",
		DockerRegistryAddress: "0.1.3.5:1000",
		DockerRegistryAuthCertificates: config.X509Certificates{
			Certificates: []*x509.Certificate{
				mustParseCertificate("MIIBSDCB+KADAgECAgEBMAoGCCqGSM49BAMCMA8xDTALBgNVBAMTBHJvb3QwHhcNMTgwNTMwMDYxNzQ1WhcNMjMwNTMwMDYxNzQ1WjAPMQ0wCwYDVQQDEwR0ZXN0ME4wEAYHKoZIzj0CAQYFK4EEACEDOgAEZVrQP4knlGBQ2cOMsYmgc0VEWu8DmOFlFa8s/ym8yiBvsCfa7/t/V53VzepLnvTYb6j0LeMcnXajUDBOMAwGA1UdEwEB/wQCMAAwHQYDVR0OBBYEFG1euQX6O6FbNV4lTu0CYAnFCpc8MB8GA1UdIwQYMBaAFNopWnFZiUBhd2W9d8NKbkRf8gujMAoGCCqGSM49BAMCAz8AMDwCHEPZ9X8JQRe5KBAMUTfowngH3J2yXb1nQXzLR4cCHEbutF5CmWNzWzcek2JfQMOl7aFjcBxAerJGgRU="),
				mustParseCertificate("MIIBKzCB2qADAgECAgEAMAoGCCqGSM49BAMCMA8xDTALBgNVBAMTBHJvb3QwHhcNMTgwNTMwMDYxNDQyWhcNMjgwNTI5MDYxNDQyWjAPMQ0wCwYDVQQDEwRyb290ME4wEAYHKoZIzj0CAQYFK4EEACEDOgAEp5HUPVxs3wdpFF/HrimbFPVWkG+v6RacjFyPujEylCfsONDOvYFzzz3x6/kxpQBl0ZYCHSJSDzKjMjAwMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFNopWnFZiUBhd2W9d8NKbkRf8gujMAoGCCqGSM49BAMCA0AAMD0CHQC7z3ryynKOXgm/flVbOytXmAgnc8n2I7jLGMKhAhwmW2IwwXFWcH/nX9K/e9AIP3l4dkWUxrNGRqwW"),
			},
		},
		DockerRegistryAuthKey: config.X509PrivateKey{
			Key: mustParseECPrivateKey("MGgCAQEEHM9ekg7h0LAhNBaiSJolcfDNtyfS94DyUblrFu+gBwYFK4EEACGhPAM6AARlWtA/iSeUYFDZw4yxiaBzRURa7wOY4WUVryz/KbzKIG+wJ9rv+39XndXN6kue9NhvqPQt4xyddg=="),
		},
		DockerRegistryTokenDuration: config.DurationString{time.Hour + 10*time.Minute},
		TempDir:                     "/var/tmp/charmstore",
		DisableSlowMetadata:         true,
	})
}

func (s *ConfigSuite) TestReadConfigError(c *gc.C) {
	cfg, err := config.Read(path.Join(c.MkDir(), "charmd.conf"))
	c.Assert(err, gc.ErrorMatches, ".* no such file or directory")
	c.Assert(cfg, gc.IsNil)
}

func (s *ConfigSuite) TestReadConfigWithNonEmptyCertsWithNoCerts(c *gc.C) {
	configText := `
audit-log-file: /var/log/charmstore/audit.log
audit-log-max-size: 500
audit-log-max-age: 1
mongo-url: localhost:23456
api-addr: blah:2324
foo: 1
bar: false
auth-username: myuser
auth-password: mypasswd
identity-location: localhost:18082
identity-public-key: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA=
identity-api-url: "http://example.com/identity"
terms-public-key: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFB=
terms-location: localhost:8092
agent-username: agentuser
agent-key:
  private: lsvcDkapKoFxIyjX9/eQgb3s41KVwPMISFwAJdVCZ70=
  public: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA=
stats-cache-max-age: 1h
search-cache-max-age: 15m
request-timeout: 500ms
max-mgo-sessions: 10
blobstore: swift
swift-auth-url: 'https://foo.com'
swift-username: bob
swift-secret: secret
swift-bucket: bucket
swift-region: somewhere
swift-tenant: a-tenant
swift-authmode: userpass
logging-config: INFO
docker-registry-address: 0.1.3.5:1000
docker-registry-auth-certs: some text
docker-registry-auth-key: |
  -----BEGIN EC PRIVATE KEY-----
  MGgCAQEEHM9ekg7h0LAhNBaiSJolcfDNtyfS94DyUblrFu+gBwYFK4EEACGhPAM6
  AARlWtA/iSeUYFDZw4yxiaBzRURa7wOY4WUVryz/KbzKIG+wJ9rv+39XndXN6kue
  9NhvqPQt4xyddg==
  -----END EC PRIVATE KEY-----
`

	_, err := s.readConfig(c, configText)
	c.Assert(err, gc.ErrorMatches, `cannot parse .*: no certificates found`)
}

func (s *ConfigSuite) TestReadConfigWithNonEmptyKeyWithNoKey(c *gc.C) {
	configText := `
audit-log-file: /var/log/charmstore/audit.log
audit-log-max-size: 500
audit-log-max-age: 1
mongo-url: localhost:23456
api-addr: blah:2324
foo: 1
bar: false
auth-username: myuser
auth-password: mypasswd
identity-location: localhost:18082
identity-public-key: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA=
identity-api-url: "http://example.com/identity"
terms-public-key: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFB=
terms-location: localhost:8092
agent-username: agentuser
agent-key:
  private: lsvcDkapKoFxIyjX9/eQgb3s41KVwPMISFwAJdVCZ70=
  public: +qNbDWly3kRTDVv2UN03hrv/CBt4W6nxY5dHdw+KJFA=
stats-cache-max-age: 1h
search-cache-max-age: 15m
request-timeout: 500ms
max-mgo-sessions: 10
blobstore: swift
swift-auth-url: 'https://foo.com'
swift-username: bob
swift-secret: secret
swift-bucket: bucket
swift-region: somewhere
swift-tenant: a-tenant
swift-authmode: userpass
logging-config: INFO
docker-registry-address: 0.1.3.5:1000
docker-registry-auth-certs: |
  -----BEGIN CERTIFICATE-----
  MIIBSDCB+KADAgECAgEBMAoGCCqGSM49BAMCMA8xDTALBgNVBAMTBHJvb3QwHhcN
  MTgwNTMwMDYxNzQ1WhcNMjMwNTMwMDYxNzQ1WjAPMQ0wCwYDVQQDEwR0ZXN0ME4w
  EAYHKoZIzj0CAQYFK4EEACEDOgAEZVrQP4knlGBQ2cOMsYmgc0VEWu8DmOFlFa8s
  /ym8yiBvsCfa7/t/V53VzepLnvTYb6j0LeMcnXajUDBOMAwGA1UdEwEB/wQCMAAw
  HQYDVR0OBBYEFG1euQX6O6FbNV4lTu0CYAnFCpc8MB8GA1UdIwQYMBaAFNopWnFZ
  iUBhd2W9d8NKbkRf8gujMAoGCCqGSM49BAMCAz8AMDwCHEPZ9X8JQRe5KBAMUTfo
  wngH3J2yXb1nQXzLR4cCHEbutF5CmWNzWzcek2JfQMOl7aFjcBxAerJGgRU=
  -----END CERTIFICATE-----
  -----BEGIN CERTIFICATE-----
  MIIBKzCB2qADAgECAgEAMAoGCCqGSM49BAMCMA8xDTALBgNVBAMTBHJvb3QwHhcN
  MTgwNTMwMDYxNDQyWhcNMjgwNTI5MDYxNDQyWjAPMQ0wCwYDVQQDEwRyb290ME4w
  EAYHKoZIzj0CAQYFK4EEACEDOgAEp5HUPVxs3wdpFF/HrimbFPVWkG+v6RacjFyP
  ujEylCfsONDOvYFzzz3x6/kxpQBl0ZYCHSJSDzKjMjAwMA8GA1UdEwEB/wQFMAMB
  Af8wHQYDVR0OBBYEFNopWnFZiUBhd2W9d8NKbkRf8gujMAoGCCqGSM49BAMCA0AA
  MD0CHQC7z3ryynKOXgm/flVbOytXmAgnc8n2I7jLGMKhAhwmW2IwwXFWcH/nX9K/
  e9AIP3l4dkWUxrNGRqwW
  -----END CERTIFICATE-----
docker-registry-auth-key: some key text
`

	_, err := s.readConfig(c, configText)
	c.Assert(err, gc.ErrorMatches, `cannot parse .*: no private key found`)
}

func (s *ConfigSuite) TestValidateConfigError(c *gc.C) {
	cfg, err := s.readConfig(c, "")
	c.Assert(err, gc.ErrorMatches, "missing fields mongo-url, api-addr, auth-username, auth-password in config file")
	c.Assert(cfg, gc.IsNil)

	cfg, err = s.readConfig(c, "blobstore: swift\n")
	c.Assert(err, gc.ErrorMatches, "missing fields mongo-url, api-addr, auth-username, auth-password, swift-auth-url, swift-username, swift-secret, swift-bucket, swift-region, swift-tenant, swift-auth-mode in config file")
	c.Assert(cfg, gc.IsNil)
}

func mustParseKey(s string) bakery.Key {
	var k bakery.Key
	err := k.UnmarshalText([]byte(s))
	if err != nil {
		panic(err)
	}
	return k
}

func mustParseCertificate(s string) *x509.Certificate {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	cert, err := x509.ParseCertificate(b)
	if err != nil {
		panic(err)
	}
	return cert
}

func mustParseECPrivateKey(s string) *ecdsa.PrivateKey {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	key, err := x509.ParseECPrivateKey(b)
	if err != nil {
		panic(err)
	}
	return key
}
