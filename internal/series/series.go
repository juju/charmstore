// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// Package series holds information about series supported in the
// charmstore.
package series // import "gopkg.in/juju/charmstore.v5/internal/series"

// Distribution represents a distribution supported by the charmstore.
// Every series will belong to a distribution.
type Distribution string

const (
	Ubuntu     Distribution = "ubuntu"
	CentOS     Distribution = "centos"
	Windows    Distribution = "windows"
	Kubernetes Distribution = "kubernetes"
)

// SeriesInfo contains the information the charmstore knows about a
// series name.
type SeriesInfo struct {
	// CharmSeries holds whether this series name is for charms.
	CharmSeries bool

	// Distribution holds the Distribution this series belongs to.
	Distribution Distribution

	// SearchIndex holds whether charms in this series should be added
	// to the search index.
	SearchIndex bool

	// SearchBoost contains the relative boost given to charms in
	// this series when searching.
	SearchBoost float64
}

const (
	boostBundle     = 1.1255
	boostLTS1       = 1.125
	boostLTS2       = 1.1125
	boostLTS3       = 1.11
	boostS1         = 1.102
	boostS2         = 1.101
	boostS3         = 1.1
	boostUnreleased = 0.9
)

// Series contains the data charmstore knows about series names
var Series = map[string]SeriesInfo{
	// Bundle
	"bundle": {false, "", true, boostBundle},

	// Ubuntu
	"oneiric": {true, Ubuntu, false, 0},
	"precise": {true, Ubuntu, false, 0},
	"quantal": {true, Ubuntu, false, 0},
	"raring":  {true, Ubuntu, false, 0},
	"saucy":   {true, Ubuntu, false, 0},
	"trusty":  {true, Ubuntu, true, boostLTS3},
	"utopic":  {true, Ubuntu, false, 0},
	"vivid":   {true, Ubuntu, false, 0},
	"wily":    {true, Ubuntu, false, 0},
	"xenial":  {true, Ubuntu, true, boostLTS2},
	"yakkety": {true, Ubuntu, false, 0},
	"zesty":   {true, Ubuntu, false, 0},
	"artful":  {true, Ubuntu, false, 0},
	"bionic":  {true, Ubuntu, true, boostLTS1},
	"cosmic":  {true, Ubuntu, false, 0},
	"disco":   {true, Ubuntu, true, boostS2},
	"eoan":    {true, Ubuntu, true, boostS1},
	"focal":   {true, Ubuntu, true, boostUnreleased},

	// Windows
	"win2012hvr2": {true, Windows, true, 1.1},
	"win2012hv":   {true, Windows, true, 1.1},
	"win2012r2":   {true, Windows, true, 1.1},
	"win2012":     {true, Windows, true, 1.1},
	"win7":        {true, Windows, true, 1.1},
	"win8":        {true, Windows, true, 1.1},
	"win81":       {true, Windows, true, 1.1},
	"win10":       {true, Windows, true, 1.1},
	"win2016":     {true, Windows, true, 1.1},
	"win2016hv":   {true, Windows, true, 1.1},
	"win2016nano": {true, Windows, true, 1.1},

	// Centos
	"centos7": {true, CentOS, true, 1.1},

	// Kubernetes
	"kubernetes": {true, Kubernetes, true, 1.1},
}
