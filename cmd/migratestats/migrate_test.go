package main

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	jujutesting "github.com/juju/testing"
	"gopkg.in/juju/charmstore.v5/internal/charmstore"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/mgo.v2/bson"
)

var _ = gc.Suite(&suite{})

var tokens = map[string]string{
	"":   "16o",
	"15": "175",
	"apache-analytics-pig":         "14h",
	"apache-hbase":                 "1cb",
	"archive-delete":               "1fr",
	"archive-download":             "16m",
	"archive-download-promulgated": "1io",
	"archive-failed-upload":        "8v",
	"archive-upload":               "1",
	"arosales":                     "i1",
	"bigdata-charmers":             "1v",
	"bigdata-dev":                  "7b",
	"bootstack-config":             "lb",
	"bundle":                       "144",
	"canonical-bootstack":          "ih",
	"charm-event":                  "235",
	"charm-info":                   "1fj",
	"charm-missing":                "1n8",
	"charmers":                     "7",
	"cherylj":                      "hs",
	"contrail":                     "169",
	"devicehive-bundle":            "16k",
	"django":                       "150",
	"elasticsearch":                "4i",
	"elasticsearch-cluster":        "151",
	"grafana-influxdb-bundle":      "15e",
	"hdp-core-batch-processing":    "14l",
	"heat": "m",
	"high-performance-batch-processing": "14o",
	"isc-dhcp":                          "1b1",
	"jorge":                             "hp",
	"juju-gui":                          "1n",
	"landscape":                         "79",
	"landscape-scalable":                "163",
	"manjiri":                           "85",
	"mediawiki-single":                  "155",
	"neutron-api-odl":                   "1ha",
	"nuage-canonical":                   "at",
	"nuage-vsc":                         "as",
	"openstack-charmers-next":           "1em",
	"project-calico":                    "1b3",
	"storm":                             "i7",
	"swift-proxy":                       "20",
	"trusty":                            "8",
	"vivid":                             "1et",
	"x3v947pl":                          "aj",
	"xenial":                            "1kl",
}

type counterDoc struct {
	T int
	K string
	C int
}

var beforeMigrate = []counterDoc{
	{T: 109157820, K: "8v:8:91:7v:", C: 1},
	{T: 109157880, K: "8v:8:2b:a8:", C: 1},
	{T: 109170540, K: "16m:144:14v:tb:16p:", C: 1},
	{T: 109170540, K: "16m:8:10:16o:16n:", C: 1},
	{T: 109170540, K: "16m:8:10:7:16n:", C: 1},
	{T: 109170840, K: "16m:8:10:16o:16n:", C: 3},
	{T: 109171140, K: "16m:8:1d:16o:16q:", C: 1},
	{T: 109171140, K: "16m:8:1d:7:16q:", C: 1},
	{T: 109171380, K: "16m:3:k:6:16r:", C: 1},
	{T: 109173300, K: "16m:8:10:16o:16n:", C: 7},
	{T: 109173660, K: "16m:8:10:16o:16n:", C: 100},
	{T: 118129140, K: "1fj:3:1q:", C: 1},
	{T: 118371660, K: "1fr:8:pp:pq:16n:", C: 1},
	{T: 118371720, K: "1fr:8:141:pq:16t:", C: 1},
	{T: 118371840, K: "1fr:8:pp:pq:16t:", C: 1},
	{T: 123604860, K: "1io:8:v:16o:17l:", C: 1},
	{T: 123604920, K: "1io:3:6a:16o:16v:", C: 1},
	{T: 123605280, K: "1io:8:12:16o:18d:", C: 1},
	{T: 123605280, K: "1io:8:1d:16o:175:", C: 1},
	{T: 123605280, K: "1io:8:2c:16o:174:", C: 1},
	{T: 123704940, K: "1fj:3:1q:", C: 2},
	{T: 124156440, K: "1fr:8:1if:hb:16t:", C: 1},
	{T: 125072100, K: "1fj:3:1q:", C: 2},
	{T: 131501640, K: "1n8:1n9:1m:", C: 1},
	{T: 132357660, K: "1n8:16o:1nk:8:", C: 1},
	{T: 132590040, K: "1n8:16o:1nk:8:", C: 1},
}

// Sanity check the first document the collection
// to give us some confidence in our squash function/
var expectFirst = counterDoc{
	T: 109170000,
	K: "16m:16o:10:8:16n:",
	C: 1 + 3 + 7,
}

var afterMigrate = []counterDoc{}

type migrationDoc struct {
	Executed []mongodoc.MigrationName `bson:"executed"`
}

type suite struct {
	jujutesting.IsolatedMgoSuite
}

func (s *suite) TestMigrate(c *gc.C) {
	db := s.Session.DB("juju")
	countersColl := db.C("juju.stat.counters")
	for _, doc := range beforeMigrate {
		err := countersColl.Insert(doc)
		c.Assert(err, gc.Equals, nil)
	}
	tokensColl := db.C("juju.stat.tokens")
	for name, idStr := range tokens {
		id, err := strconv.ParseInt(idStr, 32, 32)
		c.Assert(err, gc.Equals, nil)
		err = tokensColl.Insert(bson.M{"_id": id, "t": name})
		c.Assert(err, gc.Equals, nil)
	}
	err := run1(s.Session)
	c.Assert(err, gc.Equals, nil)

	expectedDocs := squash(beforeMigrate)

	checkResult := func() {
		var docs []counterDoc
		err = countersColl.Find(nil).All(&docs)
		c.Assert(err, gc.Equals, nil)
		sortCounters(docs)
		sortCounters(expectedDocs)
		c.Assert(docs, jc.DeepEquals, expectedDocs)
		// Sanity check our calculations.
		c.Assert(docs[0], jc.DeepEquals, expectFirst)
	}
	checkResult()

	var migrateDocs []migrationDoc
	err = db.C(migrationsColl).Find(nil).All(&migrateDocs)
	c.Assert(err, gc.Equals, nil)

	c.Assert(migrateDocs, jc.DeepEquals, []migrationDoc{{
		Executed: []mongodoc.MigrationName{
			charmstore.MigrationStatCounterSquash,
			charmstore.MigrationStatCounterReorderKey,
		},
	}})

	// Check that it's idempotent.
	err = run1(s.Session)
	c.Assert(err, gc.Equals, nil)
	checkResult()
}

func squash(docs []counterDoc) []counterDoc {
	type key struct {
		Key  string
		Time int
	}
	seconds := int(charmstore.StatsGranularity / time.Second)
	counts := make(map[key]int)
	for _, doc := range docs {
		if !shouldKeep(doc.K) {
			continue
		}
		counts[key{
			Key:  reorderKeys(doc.K),
			Time: doc.T - doc.T%seconds,
		}] += doc.C
	}
	var newDocs []counterDoc
	for k, count := range counts {
		newDocs = append(newDocs, counterDoc{
			C: count,
			K: k.Key,
			T: k.Time,
		})
	}
	return newDocs
}

const part = `[^:]+`

var keyRegex = regexp.MustCompile(strings.Replace(`(p:)(p:)(p:)(p:)(p:)?`, "p", part, -1))

func reorderKeys(k string) string {
	m := keyRegex.FindStringSubmatch(k)
	if m == nil {
		panic(fmt.Errorf("mismatched key %q against %q", k, keyRegex))
	}
	return m[1] + m[4] + m[3] + m[2] + m[5]
}

func shouldKeep(key string) bool {
	i := strings.Index(key, ":")
	if i == -1 {
		panic("key with no colon")
	}
	for _, k := range keepKinds {
		if tokens[k] == key[0:i] {
			return true
		}
	}
	return false
}

func sortCounters(counters []counterDoc) {
	sort.Slice(counters, func(i, j int) bool {
		c1, c2 := &counters[i], &counters[j]
		if c1.T != c2.T {
			return c1.T < c2.T
		}
		return c1.K < c2.K
	})
}
