// Copyright 2014-2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"testing/iotest"
	"time"

	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"
	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
	"gopkg.in/goose.v2/testservices/openstackservice"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5/internal/blobstore"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5/internal/monitoring"
)

var _ = gc.Suite(&MongoStoreSuite{})

type MongoStoreSuite struct {
	blobStoreSuite
}

func (s *MongoStoreSuite) SetUpTest(c *gc.C) {
	s.blobStoreSuite.SetUpTest(c, func(db *mgo.Database) blobstore.Backend {
		return blobstore.NewMongoBackend(db, "blobstore")
	})
}

func (s *MongoStoreSuite) TestPutConcurrent(c *gc.C) {
	s.blobStoreSuite.TestPutConcurrent(c)

	// Additionally check that there's only one blob
	// left in the underlying backend.
	fs := blobstore.BackendGridFS(s.store)
	n, err := fs.Find(nil).Count()
	c.Assert(err, gc.Equals, nil)
	c.Assert(n, gc.Equals, 1)
}

type SwiftStoreSuite struct {
	openstack *openstackservice.Openstack
	blobStoreSuite
}

var _ = gc.Suite(&SwiftStoreSuite{})

func (s *SwiftStoreSuite) SetUpTest(c *gc.C) {
	// Set up an Openstack service.
	cred := &identity.Credentials{
		URL:        "http://0.1.2.3/",
		User:       "fred",
		Secrets:    "secret",
		Region:     "some region",
		TenantName: "tenant",
	}
	var logMsg []string
	s.openstack, logMsg = openstackservice.New(cred, identity.AuthUserPass, false)
	for _, msg := range logMsg {
		c.Logf(msg)
	}
	s.openstack.SetupHTTP(nil)

	cred2 := &identity.Credentials{
		URL:        s.openstack.URLs["identity"],
		User:       "fred",
		Secrets:    "secret",
		Region:     "some region",
		TenantName: "tenant",
	}

	client := client.NewClient(cred, identity.AuthUserPass, nil)
	sw := swift.New(client)
	sw.CreateContainer("testc", swift.Private)

	s.blobStoreSuite.SetUpTest(c, func(db *mgo.Database) blobstore.Backend {
		return blobstore.NewSwiftBackend(cred2, identity.AuthUserPass, "testc", c.MkDir())
	})
}

func (s *SwiftStoreSuite) TearDownTest(c *gc.C) {
	s.blobStoreSuite.TearDownTest(c)
	s.openstack.Stop()
}

func (s *SwiftStoreSuite) TestPutInvalidHashBuffered(c *gc.C) {
	content := "some data"
	err := s.store.Put(justReader{strings.NewReader(content)}, hashOf("wrong"), int64(len(content)))
	c.Assert(err, gc.ErrorMatches, "hash mismatch")
}

func (s *SwiftStoreSuite) TestTemporaryDirectory(c *gc.C) {
	cred := &identity.Credentials{
		URL:        s.openstack.URLs["identity"],
		User:       "fred",
		Secrets:    "secret",
		Region:     "some region",
		TenantName: "tenant",
	}

	var r io.Reader
	be := blobstore.NewSwiftBackend(cred, identity.AuthUserPass, "testc", "/no/such/path")
	err := be.Put("test", r, 11*1024*1024, "a hash")
	c.Assert(err, gc.ErrorMatches, `open /no/such/path/test: no such file or directory`)
}

type blobStoreSuite struct {
	jujutesting.IsolatedMgoSuite
	store      *blobstore.Store
	newBackend func(db *mgo.Database) blobstore.Backend
}

func (s *blobStoreSuite) SetUpTest(c *gc.C, newBackend func(db *mgo.Database) blobstore.Backend) {
	s.IsolatedMgoSuite.SetUpTest(c)
	s.newBackend = newBackend
	s.store = s.newBlobStore(s.Session)
}

func (s *blobStoreSuite) TestPut(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), hashOf(content), int64(len(content)))
	c.Assert(err, gc.Equals, nil)

	s.assertBlobContent(c, nil, content)

	err = s.store.Put(strings.NewReader(content), hashOf(content), int64(len(content)))
	c.Assert(err, gc.Equals, nil)
}

func (s *blobStoreSuite) TestGC(c *gc.C) {
	content := func(i int) string {
		return strings.Repeat("0", i)
	}
	const N = 10
	for i := 0; i < N; i++ {
		err := s.store.Put(strings.NewReader(content(i)), hashOf(content(i)), int64(len(content(i))))
		c.Assert(err, gc.Equals, nil)
	}
	refs := blobstore.NewRefs(0)
	refs.Add(hashOf(content(2)))
	refs.Add(hashOf(content(5)))
	stats, err := s.store.GC(refs, time.Now())
	c.Assert(err, gc.Equals, nil)
	c.Assert(stats, jc.DeepEquals, monitoring.BlobStats{
		Count:    2,
		MaxSize:  5,
		MeanSize: (2 + 5) / 2,
	})

	s.assertBlobContent(c, nil, content(2))
	s.assertBlobContent(c, nil, content(5))

	for i := 0; i < N; i++ {
		if i == 2 || i == 5 {
			s.assertBlobContent(c, nil, content(i))
		} else {
			s.assertBlobDoesNotExist(c, content(i))
		}
	}
}

func (s *blobStoreSuite) TestPutInvalidHash(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), hashOf("wrong"), int64(len(content)))
	c.Assert(err, gc.ErrorMatches, "hash mismatch")
}

func (s *blobStoreSuite) TestPutAgainWithWrongData(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), hashOf(content), int64(len(content)))
	c.Assert(err, gc.Equals, nil)

	err = s.store.Put(strings.NewReader("xxxx data"), hashOf(content), int64(len(content)))
	c.Assert(err, gc.ErrorMatches, "blob hash mismatch")
}

func (s *blobStoreSuite) TestPutAgainWithWrongSize(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), hashOf(content), int64(len(content)))
	c.Assert(err, gc.Equals, nil)

	err = s.store.Put(strings.NewReader("dat"), hashOf(content), 4)
	c.Assert(err, gc.ErrorMatches, `unexpected blob size 3 \(expected 4\)`)
}

func (s *blobStoreSuite) TestPutShortHash(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "abc", int64(len(content)))
	c.Assert(err, gc.ErrorMatches, `implausible hash "abc"`)
}

func (s *blobStoreSuite) TestPutConcurrent(c *gc.C) {
	content := "foo"
	rs := make([]*syncReader, 3)
	for i := range rs {
		rs[i] = newSyncReader(strings.NewReader(content))
	}
	done := make(chan struct{})
	for _, r := range rs {
		r := r
		go func() {
			err := s.store.Put(r, hashOf(content), int64(len(content)))
			c.Check(err, gc.Equals, nil)
			done <- struct{}{}
		}()
	}
	// Wait for all the Puts to start reading the data.
	for _, r := range rs {
		<-r.sync
	}
	// Start them all going properly.
	for _, r := range rs {
		close(r.sync)
	}
	// Wait for all the puts to complete.
	for range rs {
		<-done
	}

	// Check that we can get the data.
	s.assertBlobContent(c, nil, content)
}

func (s *blobStoreSuite) TestNewParts(c *gc.C) {
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	c.Assert(id, gc.Not(gc.Equals), "")

	// Verify that the new record looks like we expect.
	var udoc blobstore.UploadDoc
	err = s.Session.DB("db").C("blobstore.upload").FindId(id).One(&udoc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(udoc, jc.DeepEquals, blobstore.UploadDoc{
		Id:      id,
		Expires: expires,
	})
}

func (s *blobStoreSuite) TestPutPartNegativePart(c *gc.C) {
	id := s.newUpload(c)

	err := s.store.PutPart(id, -1, nil, 0, 0, "")
	c.Assert(err, gc.ErrorMatches, "negative part number")
}

func (s *blobStoreSuite) TestPutPartNumberTooBig(c *gc.C) {
	s.store.MaxParts = 100

	id := s.newUpload(c)
	err := s.store.PutPart(id, 100, nil, 0, 0, "")
	c.Assert(err, gc.ErrorMatches, `part number 100 too big \(maximum 99\)`)
}

func (s *blobStoreSuite) TestPutPartSizeNonPositive(c *gc.C) {
	id := s.newUpload(c)
	err := s.store.PutPart(id, 0, strings.NewReader(""), 0, 0, hashOf(""))
	c.Assert(err, gc.ErrorMatches, `non-positive part 0 size 0`)
}

func (s *blobStoreSuite) TestPutPartSizeTooBig(c *gc.C) {
	s.store.MaxPartSize = 5

	id := s.newUpload(c)
	err := s.store.PutPart(id, 0, strings.NewReader(""), 20, 0, hashOf(""))
	c.Assert(err, gc.ErrorMatches, `part 0 too big \(maximum 5\)`)
}

func (s *blobStoreSuite) TestPutPartSingle(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)

	r, size, err := s.store.Open(hashOf(content), nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content))
}

func (s *blobStoreSuite) TestPutPartAgain(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"

	// Perform a Put with mismatching content. This should leave the part in progress
	// but not completed.
	err := s.store.PutPart(id, 0, strings.NewReader("something different"), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.ErrorMatches, `cannot upload part (.|\n)*`)

	// Try again with the correct content this time.
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)

	r, size, err := s.store.Open(hashOf(content), nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content))
}

func (s *blobStoreSuite) TestPutPartAgainWithDifferentHash(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 0, strings.NewReader(content1), int64(len(content1)), 0, hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `hash mismatch for already uploaded part`)
}

func (s *blobStoreSuite) TestPutPartAgainWithSameHash(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)

	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)
}

func (s *blobStoreSuite) TestPutPartOutOfOrder(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content1 := "123456789 123456789 "
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 26, hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	content0 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	r, size, err := s.store.Open(hashOf(content0), nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content0)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content0))

	r, size, err = s.store.Open(hashOf(content1), nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content1)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content1))
}

func (s *blobStoreSuite) TestPutPartTooSmall(c *gc.C) {
	s.store.MinPartSize = 100
	id := s.newUpload(c)

	content0 := "abcdefghijklmnopqrstuvwxyz"
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "123456789 123456789 "
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 26, hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `part 0 too small \(need at least 100 bytes, got 26\)`)
}

func (s *blobStoreSuite) TestPutPartTooSmallOutOfOrder(c *gc.C) {
	s.store.MinPartSize = 100
	id := s.newUpload(c)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 1, hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	content0 := "123456789 123456789 "
	err = s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 26, hashOf(content0))
	c.Assert(err, gc.ErrorMatches, `part 0 too small \(need at least 100 bytes, got 20\)`)
}

func (s *blobStoreSuite) TestPutPartSmallAtEnd(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "1234"
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abc"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 4, hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `part 0 too small \(need at least 10 bytes, got 4\)`)
}

func (s *blobStoreSuite) TestPutPartConcurrent(c *gc.C) {
	id := s.newUpload(c)
	var hash [3]string
	const size = 5 * 1024 * 1024
	for i := range hash {
		hash[i] = hashOfReader(c, newDataSource(int64(i+1), size))
	}
	var wg sync.WaitGroup
	for i := range hash {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Make a copy of the session so we get independent
			// mongo sockets and more concurrency.
			session := s.Session.Copy()
			defer session.Close()
			store := s.newBlobStore(session)
			err := store.PutPart(id, i, newDataSource(int64(i+1), size), size, int64(i)*size, hash[i])
			c.Check(err, gc.Equals, nil)
		}()
	}
	wg.Wait()
	for i, h := range hash {
		r, size, err := s.store.Open(h, nil)
		c.Assert(err, gc.Equals, nil)
		c.Assert(size, gc.Equals, size)
		c.Assert(hashOfReader(c, r), gc.Equals, hash[i])
	}
}

func (s *blobStoreSuite) TestPutPartNotFound(c *gc.C) {
	err := s.store.PutPart("unknownblob", 0, strings.NewReader("x"), 1, 0, hashOf(""))
	c.Assert(err, gc.ErrorMatches, `upload id "unknownblob" not found`)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *blobStoreSuite) TestFinishUploadMismatchedPartCount(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 20, hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}})
	c.Assert(err, gc.ErrorMatches, `part count mismatch \(got 1 but 2 uploaded\)`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *blobStoreSuite) TestFinishUploadMismatchedPartHash(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 20, hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}, {
		Hash: "badhash",
	}})
	c.Assert(err, gc.ErrorMatches, `hash mismatch on part 1 \(got "badhash" want ".+"\)`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *blobStoreSuite) TestFinishUploadPartNotUploaded(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content1 := "123456789 123456789 "
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 26, hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content1),
	}, {
		Hash: hashOf(content1),
	}})
	c.Assert(err, gc.ErrorMatches, `part 0 not uploaded yet`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *blobStoreSuite) TestFinishUploadPartIncomplete(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader("1"), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.ErrorMatches, `cannot upload part (.|\n)*`)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}})
	c.Assert(err, gc.ErrorMatches, `part 0 not uploaded yet`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *blobStoreSuite) TestFinishUploadCheckSizes(c *gc.C) {
	s.store.MinPartSize = 50
	id := s.newUpload(c)
	content := "123456789 123456789 "
	// Upload two small parts concurrently.
	done := make(chan error)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			err := s.store.PutPart(id, i, strings.NewReader(content), int64(len(content)), int64(len(content)*i), hashOf(content))
			done <- err
		}()
	}
	allOK := true
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			c.Assert(err, gc.ErrorMatches, ".*too small.*")
			allOK = false
		}
	}
	if !allOK {
		// Although it's likely that both parts will succeed
		// because they both fetch the upload doc at the same
		// time, there's a possibility that one goroutine will
		// fetch and initialize its update doc before the other
		// one retrieves it, so we skip the test in that case
		c.Skip("concurrent uploads were not very concurrent, so test skipped")
	}
	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content),
	}, {
		Hash: hashOf(content),
	}})
	c.Assert(err, gc.ErrorMatches, `part 0 too small \(need at least 50 bytes, got 20\)`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *blobStoreSuite) TestFinishUploadSuccess(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 20, hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}, {
		Hash: hashOf(content1),
	}})
	c.Assert(err, gc.Equals, nil)
	c.Assert(hash, gc.Equals, hashOf(content0+content1))
	c.Assert(idx, jc.DeepEquals, &mongodoc.MultipartIndex{
		Sizes: []uint32{
			uint32(len(content0)),
			uint32(len(content1)),
		},
		Hashes: []string{
			hashOf(content0),
			hashOf(content1),
		},
	})
}

func (s *blobStoreSuite) TestPutPartWithWrongOffset(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 22, hashOf(content1))
	c.Assert(err, gc.ErrorMatches, "part 1 should start at 20 not at 22")
}

func (s *blobStoreSuite) TestPutPartWithWrongOffsetOutOfOrder(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), 22, hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	content0 := "123456789 123456789 "
	err = s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.ErrorMatches, "part 1 should start at 20 not at 22")
}

func (s *blobStoreSuite) TestFinishUploadSuccessOnePart(c *gc.C) {
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}})
	c.Assert(err, gc.Equals, nil)
	c.Assert(hash, gc.Equals, hashOf(content0))
	c.Assert(idx, jc.DeepEquals, &mongodoc.MultipartIndex{
		Sizes: []uint32{
			uint32(len(content0)),
		},
		Hashes: []string{
			hashOf(content0),
		},
	})
}

func (s *blobStoreSuite) TestFinishUploadNotFound(c *gc.C) {
	_, _, err := s.store.FinishUpload("not-an-id", nil)
	c.Assert(err, gc.ErrorMatches, `upload id "not-an-id" not found`)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *blobStoreSuite) TestFinishUploadAgain(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}})
	c.Assert(err, gc.Equals, nil)
	c.Assert(hash, gc.Equals, hashOf(content0))
	c.Assert(idx, jc.DeepEquals, &mongodoc.MultipartIndex{
		Sizes: []uint32{
			uint32(len(content0)),
		},
		Hashes: []string{
			hashOf(content0),
		},
	})

	// We should get exactly the same thing if we call
	// FinishUpload again.
	idx, hash, err = s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}})
	c.Assert(err, gc.Equals, nil)
	c.Assert(hash, gc.Equals, hashOf(content0))
	c.Assert(idx, jc.DeepEquals, &mongodoc.MultipartIndex{
		Sizes: []uint32{
			uint32(len(content0)),
		},
		Hashes: []string{
			hashOf(content0),
		},
	})
}

func (s *blobStoreSuite) TestFinishUploadCalledWhenCalculatingHash(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	// We need at least two parts so that FinishUpload
	// actually needs to stream the parts again, so
	// upload a small first part and then a large second
	// part that's big enough that there's a strong probability
	// that we'll be able to remove the upload entry before
	// FinishUpload has finished calculating the hash.
	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), 0, hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	const size1 = 20 * 1024 * 1024
	hash1 := hashOfReader(c, newDataSource(1, size1))
	err = s.store.PutPart(id, 1, newDataSource(1, size1), int64(size1), 20, hash1)
	c.Assert(err, gc.Equals, nil)

	done := make(chan error)
	go func() {
		_, _, err := s.store.FinishUpload(id, []blobstore.Part{{
			Hash: hashOf(content0),
		}, {
			Hash: hash1,
		}})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	err = s.store.RemoveUpload(id)
	c.Assert(err, gc.Equals, nil)

	err = <-done
	if err == nil {
		// We didn't delete it fast enough, so skip the test.
		c.Skip("FinishUpload finished before we could interfere with it")
	}
	if errgo.Cause(err) == blobstore.ErrNotFound {
		c.Skip(fmt.Sprintf("FinishUpload started too late, after we removed its doc (cause %#v)", errgo.Cause(err)))
	} else {
		c.Logf("cause %#v", errgo.Cause(err))
	}
	c.Assert(err, gc.ErrorMatches, `upload expired or removed`)
}

func (s *blobStoreSuite) TestRemoveUploadSuccessWithNoPart(c *gc.C) {
	s.store.MinPartSize = 10
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	err = s.store.RemoveUpload(id)
	c.Assert(err, gc.Equals, nil)
	s.assertUploadDoesNotExist(c, id)
}

func (s *blobStoreSuite) TestRemoveUploadOnNonExistingUpload(c *gc.C) {
	err := s.store.RemoveUpload("something")
	c.Assert(err, gc.Equals, nil)
}

func (s *blobStoreSuite) TestRemoveUploadSuccessWithParts(c *gc.C) {
	s.store.MinPartSize = 10
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	content := "123456789 12345"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)
	err = s.store.RemoveUpload(id)
	c.Assert(err, gc.Equals, nil)
	s.assertUploadDoesNotExist(c, id)

	_, err = s.store.GC(blobstore.NewRefs(0), time.Now())
	c.Assert(err, gc.Equals, nil)
	s.assertBlobDoesNotExist(c, content)
}

func (s *blobStoreSuite) TestSetOwner(c *gc.C) {
	s.store.MinPartSize = 10
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	content := "123456789 12345"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)

	// Check that we can't call SetOwner on an incomplete upload.
	err = s.store.SetOwner(id, "something", expires)
	c.Assert(err, gc.ErrorMatches, `cannot set owner on incomplete upload`)

	_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
	c.Assert(err, gc.Equals, nil)

	newExpires := time.Now().Add(5 * time.Minute).Truncate(time.Millisecond)
	err = s.store.SetOwner(id, "something", newExpires)
	c.Assert(err, gc.Equals, nil)

	info, err := s.store.UploadInfo(id)
	c.Assert(err, gc.Equals, nil)
	if !info.Expires.Equal(newExpires) {
		c.Fatalf("unexpected expiry time, got %v want %v", info.Expires, newExpires)
	}

	// Check that we can't set the owner to something else.
	err = s.store.SetOwner(id, "other", newExpires)
	c.Assert(err, gc.ErrorMatches, `upload already used by something else`)

	// Check that we can set the owner to the same thing again.
	err = s.store.SetOwner(id, "something", newExpires)
	c.Assert(err, gc.Equals, nil)

	err = s.store.RemoveUpload(id)
	c.Assert(err, gc.Equals, nil)

	// Check that we get a not-found error when the upload
	// has actually been removed.
	err = s.store.SetOwner(id, "something", newExpires)
	c.Check(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
	c.Assert(err, gc.ErrorMatches, `upload has been removed`)
}

func (s *blobStoreSuite) TestRemoveFinishedUploadRemovesParts(c *gc.C) {
	s.store.MinPartSize = 10

	id, err := s.store.NewUpload(time.Now().Add(time.Minute))
	c.Assert(err, gc.Equals, nil)
	content := "123456789 12345"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
	c.Assert(err, gc.Equals, nil)
	_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
	c.Assert(err, gc.Equals, nil)

	err = s.store.RemoveUpload(id)
	c.Assert(err, gc.Equals, nil)

	s.assertUploadDoesNotExist(c, id)

	// The blob will exist but will be removed after a
	// garbage collection.
	_, err = s.store.GC(blobstore.NewRefs(0), time.Now())
	c.Assert(err, gc.Equals, nil)
	s.assertBlobDoesNotExist(c, content)
}

func (s *blobStoreSuite) TestRemoveExpiredUploads(c *gc.C) {
	s.store.MinPartSize = 10

	expireTimes := []time.Duration{-time.Minute, -time.Second, time.Minute, time.Hour}
	ids := make([]string, len(expireTimes))
	for i, dt := range expireTimes {
		id, err := s.store.NewUpload(time.Now().Add(dt))
		c.Assert(err, gc.Equals, nil)
		content := fmt.Sprintf("%15d", i)
		err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), 0, hashOf(content))
		c.Assert(err, gc.Equals, nil)
		_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
		c.Assert(err, gc.Equals, nil)
		ids[i] = id
	}

	err := s.store.RemoveExpiredUploads()
	c.Assert(err, gc.Equals, nil)

	// Garbage collect all blobs (those still referenced
	// by the uploads collection won't be collected).
	_, err = s.store.GC(blobstore.NewRefs(0), time.Now())
	c.Assert(err, gc.Equals, nil)

	for i, id := range ids {
		content := fmt.Sprintf("%15d", i)
		if expireTimes[i] < 0 {
			s.assertUploadDoesNotExist(c, id)
			s.assertBlobDoesNotExist(c, content)
		} else {
			_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
			c.Assert(err, gc.Equals, nil)
			s.assertBlobContent(c, nil, content)
		}
	}
}

func (s *blobStoreSuite) TestOpenEmptyMultipart(c *gc.C) {
	_, idx := s.putMultipart(c)
	s.assertBlobContent(c, idx, "")
}

func (s *blobStoreSuite) TestMultipartReadAll(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	part1 := "abcdefghijklmnopqrstuvwxyz"
	part2 := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	_, idx := s.putMultipart(c, part0, part1, part2)
	s.assertBlobContent(c, idx, part0+part1+part2)
}

func (s *blobStoreSuite) TestMultipartSmallReads(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	part1 := "abcdefghijklmnopqrstuvwxyz"
	part2 := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	id, idx := s.putMultipart(c, part0, part1, part2)
	r, _, err := s.store.Open(id, idx)
	defer r.Close()
	c.Assert(err, gc.Equals, nil)
	data, err := ioutil.ReadAll(iotest.OneByteReader(r))
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, part0+part1+part2)
}

func (s *blobStoreSuite) TestMultipartSinglePart(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	_, idx := s.putMultipart(c, part0)
	s.assertBlobContent(c, idx, part0)
}

func (s *blobStoreSuite) TestMultipartCloseWithoutReading(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	part1 := "abcdefghijklmnopqrstuvwxyz"
	id, idx := s.putMultipart(c, part0, part1)
	r, _, err := s.store.Open(id, idx)
	c.Assert(err, gc.Equals, nil)
	err = r.Close()
	c.Assert(err, gc.Equals, nil)
}

func (s *blobStoreSuite) TestUploadInfo(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	part1 := "abcdefghijklmnopqrstuvwxyz"
	part2 := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	id, _ := s.putMultipartNoRemove(c, part0, part1, part2)
	info, err := s.store.UploadInfo(id)
	c.Assert(err, gc.Equals, nil)
	if want := time.Now().Add(50 * time.Second); !info.Expires.After(want) {
		c.Errorf("unexpected expiry time %v, want at least %v", info.Expires, want)
	}
	info.Expires = time.Time{}
	c.Assert(info, jc.DeepEquals, blobstore.UploadInfo{
		Parts: []*blobstore.PartInfo{{
			Hash:     hashOf(part0),
			Size:     int64(len(part0)),
			Complete: true,
			Offset:   0,
		}, {
			Hash:     hashOf(part1),
			Size:     int64(len(part1)),
			Complete: true,
			Offset:   int64(len(part0)),
		}, {
			Hash:     hashOf(part2),
			Size:     int64(len(part2)),
			Complete: true,
			Offset:   int64(len(part1)) + int64(len(part0)),
		}},
		Hash: hashOf(part0 + part1 + part2),
	})

	// Check that we can read the blob from the index
	// derived from the UploadInfo.
	idx, ok := info.Index()
	c.Assert(ok, gc.Equals, true)
	s.assertBlobContent(c, idx, part0+part1+part2)
}

var multipartSeekTests = []struct {
	initialOffset int64
	offset        int64
	whence        int
	expectPos     int64
	expect        string
}{{
	offset:    0,
	whence:    0,
	expectPos: 0,
	expect:    "123456789 ",
}, {
	offset:    200,
	whence:    0,
	expectPos: 200,
	expect:    "",
}, {
	offset:    7,
	whence:    0,
	expectPos: 7,
	expect:    "89 12345",
}, {
	offset:    -3,
	whence:    0,
	expectPos: 0,
	expect:    "123456789 ",
}, {
	offset:    3,
	whence:    2,
	expectPos: 15 + 26 + 26 - 3,
	expect:    "XYZ",
}, {
	initialOffset: 20,
	offset:        -10,
	whence:        1,
	expectPos:     10,
	expect:        "12345",
}, {
	initialOffset: 60,
	offset:        0,
	whence:        0,
	expectPos:     0,
	expect:        "123456789 ",
}}

func (s *blobStoreSuite) TestMultipartSeek(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	part1 := "abcdefghijklmnopqrstuvwxyz"
	part2 := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	id, idx := s.putMultipart(c, part0, part1, part2)
	r, _, err := s.store.Open(id, idx)
	defer r.Close()
	c.Assert(err, gc.Equals, nil)

	for i, test := range multipartSeekTests {
		c.Logf("test %d: offset %d whence %d", i, test.offset, test.whence)
		p, err := r.Seek(test.initialOffset, 0)
		c.Assert(err, gc.Equals, nil)
		p, err = r.Seek(test.offset, test.whence)
		c.Assert(err, gc.Equals, nil)
		c.Assert(p, gc.Equals, test.expectPos)
		buf := make([]byte, 10)
		n, err := r.Read(buf)
		if test.expect == "" {
			c.Assert(err, gc.Equals, io.EOF)
			c.Assert(n, gc.Equals, 0)
		} else {
			c.Assert(err, gc.Equals, nil)
			c.Assert(string(buf[0:n]), gc.Equals, test.expect)
		}
	}
}

func (s *blobStoreSuite) putMultipart(c *gc.C, contents ...string) (string, *mongodoc.MultipartIndex) {
	id, idx := s.putMultipartNoRemove(c, contents...)
	err := s.store.RemoveUpload(id)
	c.Assert(err, gc.Equals, nil)
	return id, idx
}

func (s *blobStoreSuite) putMultipartNoRemove(c *gc.C, contents ...string) (string, *mongodoc.MultipartIndex) {
	expires := time.Now().Add(time.Minute)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)

	parts := make([]blobstore.Part, len(contents))
	pos := int64(0)
	for i, content := range contents {
		hash := hashOf(content)
		err = s.store.PutPart(id, i, strings.NewReader(content), int64(len(content)), pos, hash)
		c.Assert(err, gc.Equals, nil)
		parts[i].Hash = hash
		pos += int64(len(content))
	}
	idx, _, err := s.store.FinishUpload(id, parts)
	c.Assert(err, gc.Equals, nil)
	err = s.store.SetOwner(id, "test", expires)
	c.Assert(err, gc.Equals, nil)
	return id, idx
}

func (s *blobStoreSuite) newBlobStore(session *mgo.Session) *blobstore.Store {
	db := session.DB("db")
	return blobstore.New(db, "blobstore", s.newBackend(db))
}

func (s *blobStoreSuite) assertUploadDoesNotExist(c *gc.C, id string) {
	_, err := s.store.UploadInfo(id)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *blobStoreSuite) assertBlobDoesNotExist(c *gc.C, content string) {
	_, _, err := s.store.Open(hashOf(content), nil)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound, gc.Commentf("content %q, hash %s", content, hashOf(content)))
}

func (s *blobStoreSuite) assertBlobContent(c *gc.C, idx *mongodoc.MultipartIndex, content string) {
	hash := hashOf(content)
	r, size, err := s.store.Open(hash, idx)
	c.Assert(err, gc.Equals, nil)
	defer r.Close()
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content)))
	data, err := ioutil.ReadAll(r)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, content)
}

// newUpload returns the id of a new upload instance.
func (s *blobStoreSuite) newUpload(c *gc.C) string {
	expires := time.Now().Add(time.Minute).UTC()
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	return id
}

func hashOfReader(c *gc.C, r io.Reader) string {
	h := blobstore.NewHash()
	_, err := io.Copy(h, r)
	c.Assert(err, gc.Equals, nil)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func hashOf(s string) string {
	h := blobstore.NewHash()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

type dataSource struct {
	buf      []byte
	bufIndex int
	remain   int64
}

// newDataSource returns a stream of size bytes holding
// a repeated number.
func newDataSource(fillWith int64, size int64) io.Reader {
	src := &dataSource{
		remain: size,
	}
	for len(src.buf) < 8*1024 {
		src.buf = strconv.AppendInt(src.buf, fillWith, 10)
		src.buf = append(src.buf, ' ')
	}
	return src
}

func (s *dataSource) Read(buf []byte) (int, error) {
	if int64(len(buf)) > s.remain {
		buf = buf[:int(s.remain)]
	}
	total := len(buf)
	if total == 0 {
		return 0, io.EOF
	}

	for len(buf) > 0 {
		if s.bufIndex == len(s.buf) {
			s.bufIndex = 0
		}
		nb := copy(buf, s.buf[s.bufIndex:])
		s.bufIndex += nb
		buf = buf[nb:]
		s.remain -= int64(nb)
	}
	return total, nil
}

func newSyncReader(r io.Reader) *syncReader {
	return &syncReader{
		sync: make(chan struct{}),
		r:    r,
	}
}

type syncReader struct {
	sync chan struct{}
	r    io.Reader
}

func (r *syncReader) Read(buf []byte) (int, error) {
	if r.sync != nil {
		r.sync <- struct{}{}
		<-r.sync
		r.sync = nil
	}
	return r.r.Read(buf)
}

type justReader struct {
	r io.Reader
}

func (r justReader) Read(b []byte) (int, error) {
	return r.r.Read(b)
}
