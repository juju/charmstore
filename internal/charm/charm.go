// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// Package charm is is a wrapper for the parts of
// github.com/juju/charm/v8 used by the charmstore. This version maintains
// the url parsing bahaviour expected by the charmstore.
package charm

import (
	"io"
	stdurl "net/url"

	"github.com/juju/charm/v8"
)

// Copied constants.
const (
	ScopeGlobal = charm.ScopeGlobal
)

// Alias all necessary types
type Actions = charm.Actions
type ApplicationSpec = charm.ApplicationSpec
type Bundle = charm.Bundle
type BundleArchive = charm.BundleArchive
type BundleData = charm.BundleData
type Charm = charm.Charm
type CharmArchive = charm.CharmArchive
type CharmDir = charm.CharmDir
type Config = charm.Config
type MachineSpec = charm.MachineSpec
type Meta = charm.Meta
type Metric = charm.Metric
type Metrics = charm.Metrics
type Relation = charm.Relation
type URL = charm.URL
type UnitPlacement = charm.UnitPlacement
type VerificationError = charm.VerificationError

// Unmodified functions

func ReadBundle(path string) (Bundle, error) {
	return charm.ReadBundle(path)
}

func ReadBundleArchive(path string) (*BundleArchive, error) {
	return charm.ReadBundleArchive(path)
}

func ReadBundleArchiveBytes(data []byte) (*BundleArchive, error) {
	return charm.ReadBundleArchiveBytes(data)
}

func ReadBundleArchiveFromReader(r io.ReaderAt, size int64) (*BundleArchive, error) {
	return charm.ReadBundleArchiveFromReader(r, size)
}

func ReadCharmArchiveBytes(data []byte) (*CharmArchive, error) {
	return charm.ReadCharmArchiveBytes(data)
}

func ReadCharmArchiveFromReader(r io.ReaderAt, size int64) (*CharmArchive, error) {
	return charm.ReadCharmArchiveFromReader(r, size)
}

func ReadCharmDir(path string) (*CharmDir, error) {
	return charm.ReadCharmDir(path)
}

func ReadMeta(r io.Reader) (*Meta, error) {
	return charm.ReadMeta(r)
}

func ParsePlacement(p string) (*UnitPlacement, error) {
	return charm.ParsePlacement(p)
}

// MustParseURL parses the given URL, it panics is the URL cannot be
// parsed.
func MustParseURL(s string) *URL {
	u, err := ParseURL(s)
	if err != nil {
		panic(err)
	}
	return u
}

// ParseURL parses the given string as
func ParseURL(s string) (*URL, error) {
	if u, err := stdurl.Parse(s); err == nil {
		// Ignore any actual error parsing the URL, the library parse
		// function will report those.
		if u.Scheme == "" {
			// If there is no other scheme then assume "cs"
			s = "cs:" + s
		}
	}
	return charm.ParseURL(s)
}
