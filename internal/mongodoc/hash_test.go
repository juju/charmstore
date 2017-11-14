// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package mongodoc_test

import (
	"bytes"
	"crypto/sha512"
	"fmt"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"

	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
)

type HashSuite struct{}

var _ = gc.Suite(&HashSuite{})

var unmarshalTests = []struct {
	from        []byte
	expect      mongodoc.Hashes
	expectError string
}{{
	from:   byteRep('a', sha512.Size384, 'b', sha512.Size384),
	expect: mongodoc.Hashes{fakeHash('a'), fakeHash('b')},
}, {
	from:   []byte{},
	expect: mongodoc.Hashes{},
}, {
	from:        byteRep('a', 5),
	expectError: "length 5 not a multiple of hash size",
}}

func (*HashSuite) TestUnmarshal(c *gc.C) {
	type byteRepr struct {
		X []byte
	}
	for i, test := range unmarshalTests {
		c.Logf("test %d", i)
		data, err := bson.Marshal(&byteRepr{test.from})
		c.Assert(err, gc.Equals, nil)
		var x struct {
			X mongodoc.Hashes
		}
		err = bson.Unmarshal(data, &x)
		if test.expectError != "" {
			c.Assert(err, gc.ErrorMatches, test.expectError)
		} else {
			c.Assert(err, gc.Equals, nil)
			c.Assert(x.X, jc.DeepEquals, test.expect)
			// Make sure it round trips OK.
			data1, err := bson.Marshal(x)
			c.Assert(err, gc.Equals, nil)
			c.Assert(data1, jc.DeepEquals, data)
		}
	}
}

var marshalErrorTests = []struct {
	from        mongodoc.Hashes
	expectError string
}{{
	from:        mongodoc.Hashes{""},
	expectError: `invalid hash length of ""`,
}, {
	from:        mongodoc.Hashes{"abc"},
	expectError: `invalid hash length of "abc"`,
}, {
	from:        mongodoc.Hashes{"abcd"},
	expectError: `invalid hash length of "abcd"`,
}, {
	from:        mongodoc.Hashes{"z68412320f7b0aa5812fce428dc4706b3cae50e02a64caa16a782249bfe8efc4b7ef1ccb126255d196047dfedf17a0a9"},
	expectError: `invalid hash encoding "z684.*": encoding/hex: invalid byte: U\+007A 'z'`,
}}

func (*HashSuite) TestMarshalError(c *gc.C) {
	for i, test := range marshalErrorTests {
		c.Logf("test %d", i)
		x := struct {
			X mongodoc.Hashes
		}{test.from}
		_, err := bson.Marshal(x)
		c.Assert(err, gc.ErrorMatches, test.expectError)
	}
}

func fakeHash(b byte) string {
	var r [sha512.Size384]byte
	for i := range r {
		r[i] = b
	}
	return fmt.Sprintf("%x", r[:])
}

func byteRep(n ...int) []byte {
	var b []byte
	for i := 0; i < len(n); i += 2 {
		b = append(b, bytes.Repeat([]byte{byte(n[i])}, n[i+1])...)
	}
	return b
}
