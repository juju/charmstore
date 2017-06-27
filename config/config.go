// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// The config package defines configuration parameters for
// the charm store.
package config // import "gopkg.in/juju/charmstore.v5-unstable/config"

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery"
	"gopkg.in/yaml.v2"
)

type Config struct {
	// TODO(rog) rename this to MongoAddr - it's not a URL.
	MongoURL          string            `yaml:"mongo-url,omitempty"`
	AuditLogFile      string            `yaml:"audit-log-file,omitempty"`
	AuditLogMaxSize   int               `yaml:"audit-log-max-size,omitempty"`
	AuditLogMaxAge    int               `yaml:"audit-log-max-age,omitempty"`
	APIAddr           string            `yaml:"api-addr,omitempty"`
	AuthUsername      string            `yaml:"auth-username,omitempty"`
	AuthPassword      string            `yaml:"auth-password,omitempty"`
	ESAddr            string            `yaml:"elasticsearch-addr,omitempty"` // elasticsearch is optional
	IdentityPublicKey *bakery.PublicKey `yaml:"identity-public-key,omitempty"`
	IdentityLocation  string            `yaml:"identity-location"`
	TermsPublicKey    *bakery.PublicKey `yaml:"terms-public-key,omitempty"`
	TermsLocation     string            `yaml:"terms-location,omitempty"`
	AgentUsername     string            `yaml:"agent-username,omitempty"`
	AgentKey          *bakery.KeyPair   `yaml:"agent-key,omitempty"`
	MaxMgoSessions    int               `yaml:"max-mgo-sessions,omitempty"`
	RequestTimeout    DurationString    `yaml:"request-timeout,omitempty"`
	StatsCacheMaxAge  DurationString    `yaml:"stats-cache-max-age,omitempty"`
	SearchCacheMaxAge DurationString    `yaml:"search-cache-max-age,omitempty"`
	Database          string            `yaml:"database,omitempty"`
	AccessLog         string            `yaml:"access-log"`
	MinUploadPartSize int64             `yaml:"min-upload-part-size"`
	MaxUploadPartSize int64             `yaml:"max-upload-part-size"`
	MaxUploadParts    int               `yaml:"max-upload-parts"`
	BlobStore         BlobStoreType     `yaml:"blobstore"`
	SwiftAuthURL      string            `yaml:"swift-auth-url"`
	SwiftUsername     string            `yaml:"swift-username"`
	SwiftSecret       string            `yaml:"swift-secret"`
	SwiftBucket       string            `yaml:"swift-bucket"`
	SwiftRegion       string            `yaml:"swift-region"`
	SwiftTenant       string            `yaml:"swift-tenant"`
	SwiftAuthMode     *SwiftAuthMode    `yaml:"swift-authmode"`
}

type BlobStoreType string

const (
	MongoDBBlobStore BlobStoreType = "mongodb"
	SwiftBlobStore   BlobStoreType = "swift"
)

// SwiftAuthMode implements unmarshaling for
// an identity.AuthMode.
type SwiftAuthMode struct {
	Mode identity.AuthMode
}

func (m *SwiftAuthMode) UnmarshalText(data []byte) error {
	switch string(data) {
	case "legacy":
		m.Mode = identity.AuthLegacy
	case "keypair":
		m.Mode = identity.AuthKeyPair
	case "authuserpassv3":
		m.Mode = identity.AuthUserPassV3
	case "userpass":
		m.Mode = identity.AuthUserPass
	default:
		return errgo.Newf("unknown swift auth mode %q", data)
	}
	return nil
}

func (c *Config) validate() error {
	var missing []string
	needString := func(name, val string) {
		if val == "" {
			missing = append(missing, name)
		}
	}
	needString("mongo-url", c.MongoURL)
	needString("api-addr", c.APIAddr)
	needString("auth-username", c.AuthUsername)
	if strings.Contains(c.AuthUsername, ":") {
		return fmt.Errorf("invalid user name %q (contains ':')", c.AuthUsername)
	}
	needString("auth-password", c.AuthPassword)
	if c.BlobStore == "" {
		c.BlobStore = MongoDBBlobStore
	}
	switch c.BlobStore {
	case SwiftBlobStore:
		needString("swift-auth-url", c.SwiftAuthURL)
		needString("swift-username", c.SwiftUsername)
		needString("swift-secret", c.SwiftSecret)
		needString("swift-bucket", c.SwiftBucket)
		needString("swift-region", c.SwiftRegion)
		needString("swift-tenant", c.SwiftTenant)
		if c.SwiftAuthMode == nil {
			missing = append(missing, "swift-auth-mode")
		}
	case MongoDBBlobStore:
	default:
		return errgo.Newf("invalid blob store type %q", c.BlobStore)
	}
	if len(missing) != 0 {
		return errgo.Newf("missing fields %s in config file", strings.Join(missing, ", "))
	}
	return nil
}

// Read reads a charm store configuration file from the
// given path.
func Read(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errgo.Notef(err, "cannot open config file")
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errgo.Notef(err, "cannot read %q", path)
	}
	var conf Config
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		return nil, errgo.Notef(err, "cannot parse %q", path)
	}
	if err := conf.validate(); err != nil {
		return nil, errgo.Mask(err)
	}
	return &conf, nil
}

// DurationString holds a duration that marshals and
// unmarshals as a friendly string.
type DurationString struct {
	time.Duration
}

func (dp *DurationString) UnmarshalText(data []byte) error {
	d, err := time.ParseDuration(string(data))
	if err != nil {
		return errgo.Mask(err)
	}
	dp.Duration = d
	return nil
}
