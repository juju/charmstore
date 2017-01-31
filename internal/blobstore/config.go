// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

// ProviderConfig holds configuration for where blobs are stored.
type ProviderConfig struct {
	// Type of the provider.  Currently jujublobstore, s3, localfs are supported.
	Type string `yaml:"type"`

	// MongoAddr is the address of the mongodb database holding the gridfs.
	MongoAddr string `yaml:"mongo_addr,omitempty"`

	// MongoDBName is the name of the mongodb database holding the gridfs.
	MongoDBName string `yaml:"mongo_dbname,omitempty"`

	// BucketName to use with S3 or the GridFS Prefix to use with gridfs.
	BucketName string `yaml:"bucket_name,omitempty"`

	// Endpoint for using S3 api with non-S3 store such as swift or Raik CS.
	Endpoint string `yaml:"endpoint,omitempty"`

	// Region to use with S3.
	Region string `yaml:"region,omitempty"`

	// S3ForcePathStyle to use with S3.
	S3ForcePathStyle bool `yaml:"s3_force_path_style,omitempty"`

	// DisableSSL to use with S3.
	DisableSSL bool `yaml:"disable_ssl,omitempty"`

	// Key to use with S3, aka access key.
	Key string `yaml:"key,omitempty"`

	// Secret to use with S3.
	Secret string `yaml:"secret,omitempty"`

	// Mode of the provider. Read-only, Write-only, or Read-Write
	Mode string `yaml:"mode,omitempty"`
}
