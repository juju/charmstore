// Copyright 2014-2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"fmt"
	"io"
	"io/ioutil"
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
	"gopkg.in/goose.v2/testing/httpsuite"
	"gopkg.in/goose.v2/testservices/openstackservice"

	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

type SwiftStoreSuite struct {
	jujutesting.IsolatedMgoSuite
	store       *blobstore.Store
	objectstore blobstore.ObjectStore
	httpsuite.HTTPSuite
	openstack *openstackservice.Openstack
}

var _ = gc.Suite(&SwiftStoreSuite{})

func (s *SwiftStoreSuite) SetUpSuite(c *gc.C) {
	c.Logf("Using identity and swift service test doubles")
	s.HTTPSuite.SetUpSuite(c)
	s.IsolatedMgoSuite.SetUpSuite(c)
}

func (s *SwiftStoreSuite) TearDownSuite(c *gc.C) {
	s.IsolatedMgoSuite.TearDownSuite(c)
	s.HTTPSuite.TearDownSuite(c)
}

func (s *SwiftStoreSuite) SetUpTest(c *gc.C) {
	s.HTTPSuite.SetUpTest(c)
	s.IsolatedMgoSuite.SetUpTest(c)
	// Set up an Openstack service.
	cred := &identity.Credentials{
		URL:        s.Server.URL,
		User:       "fred",
		Secrets:    "secret",
		Region:     "some region",
		TenantName: "tenant",
	}
	var logMsg []string
	s.openstack, logMsg = openstackservice.New(cred,
		identity.AuthUserPass, false)
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
	s.objectstore = blobstore.NewSwiftStore(cred2, identity.AuthUserPass)

	client := client.NewClient(cred, identity.AuthUserPass, nil)
	sw := swift.New(client)
	sw.CreateContainer("testc", swift.Private)
	s.store = blobstore.New(s.Session.DB("db"), "blobstore", "testc", s.objectstore)
}

func (s *SwiftStoreSuite) TearDownTest(c *gc.C) {
	s.HTTPSuite.TearDownTest(c)
	s.openstack.Stop()
	s.IsolatedMgoSuite.TearDownTest(c)
}

func (s *SwiftStoreSuite) TestPutTwice(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	content = "some different data"
	err = s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	s.assertBlobContent(c, "x", nil, content)
}

func (s *SwiftStoreSuite) TestPut(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	s.assertBlobContent(c, "x", nil, content)

	err = s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)
}

func (s *SwiftStoreSuite) TestPutInvalidHash(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf("wrong"))
	c.Assert(err, gc.ErrorMatches, "hash mismatch")
}

func (s *SwiftStoreSuite) TestRemove(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	s.assertBlobContent(c, "x", nil, content)

	err = s.store.Remove("x", nil)
	c.Assert(err, gc.Equals, nil)

	s.assertBlobDoesNotExist(c, "x")
}

func (s *SwiftStoreSuite) TestRemoveNonExistent(c *gc.C) {
	err := s.store.Remove("x", nil)
	c.Check(err.Error(), jc.HasPrefix, `failed to DELETE object`)
	c.Check(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *SwiftStoreSuite) TestNewParts(c *gc.C) {
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

func (s *SwiftStoreSuite) TestPutPartNegativePart(c *gc.C) {
	id := s.newUpload(c)

	err := s.store.PutPart(id, -1, nil, 0, "")
	c.Assert(err, gc.ErrorMatches, "negative part number")
}

func (s *SwiftStoreSuite) TestPutPartNumberTooBig(c *gc.C) {
	s.store.MaxParts = 100

	id := s.newUpload(c)
	err := s.store.PutPart(id, 100, nil, 0, "")
	c.Assert(err, gc.ErrorMatches, `part number 100 too big \(maximum 99\)`)
}

func (s *SwiftStoreSuite) TestPutPartSizeNonPositive(c *gc.C) {
	id := s.newUpload(c)
	err := s.store.PutPart(id, 0, strings.NewReader(""), 0, hashOf(""))
	c.Assert(err, gc.ErrorMatches, `non-positive part 0 size 0`)
}

func (s *SwiftStoreSuite) TestPutPartSizeTooBig(c *gc.C) {
	s.store.MaxPartSize = 5

	id := s.newUpload(c)
	err := s.store.PutPart(id, 0, strings.NewReader(""), 20, hashOf(""))
	c.Assert(err, gc.ErrorMatches, `part 0 too big \(maximum 5\)`)
}

func (s *SwiftStoreSuite) TestPutPartSingle(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	r, size, err := s.store.Open(id+"/0", nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content))
}

func (s *SwiftStoreSuite) TestPutPartAgain(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"

	// Perform a Put with mismatching content. This should leave the part in progress
	// but not completed.
	err := s.store.PutPart(id, 0, strings.NewReader("something different"), int64(len(content)), hashOf(content))
	c.Assert(err, gc.ErrorMatches, `cannot upload part ".+": hash mismatch`)

	// Try again with the correct content this time.
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	r, size, err := s.store.Open(id+"/0", nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content))
}

func (s *SwiftStoreSuite) TestPutPartAgainWithDifferentHash(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 0, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `hash mismatch for already uploaded part`)
}

func (s *SwiftStoreSuite) TestPutPartAgainWithSameHash(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)
}

func (s *SwiftStoreSuite) TestPutPartOutOfOrder(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content1 := "123456789 123456789 "
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	content0 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	r, size, err := s.store.Open(id+"/0", nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content0)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content0))

	r, size, err = s.store.Open(id+"/1", nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content1)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content1))
}

func (s *SwiftStoreSuite) TestPutPartTooSmall(c *gc.C) {
	s.store.MinPartSize = 100
	id := s.newUpload(c)

	content0 := "abcdefghijklmnopqrstuvwxyz"
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "123456789 123456789 "
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `part 0 was too small \(need at least 100 bytes, got 26\)`)
}

func (s *SwiftStoreSuite) TestPutPartTooSmallOutOfOrder(c *gc.C) {
	s.store.MinPartSize = 100
	id := s.newUpload(c)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	content0 := "123456789 123456789 "
	err = s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.ErrorMatches, `part too small \(need at least 100 bytes, got 20\)`)
}

func (s *SwiftStoreSuite) TestPutPartSmallAtEnd(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "1234"
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abc"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `part 0 was too small \(need at least 10 bytes, got 4\)`)
}

func (s *SwiftStoreSuite) TestPutPartConcurrent(c *gc.C) {
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
			db := s.Session.Copy().DB("db")
			defer db.Session.Close()
			store := blobstore.New(db, "blobstore", "testc", s.objectstore)
			err := store.PutPart(id, i, newDataSource(int64(i+1), size), size, hash[i])
			c.Check(err, gc.Equals, nil)
		}()
	}
	wg.Wait()
	for i := range hash {
		r, size, err := s.store.Open(fmt.Sprintf("%s/%d", id, i), nil)
		c.Assert(err, gc.Equals, nil)
		c.Assert(size, gc.Equals, size)
		c.Assert(hashOfReader(c, r), gc.Equals, hash[i])
	}
}

func (s *SwiftStoreSuite) TestPutPartNotFound(c *gc.C) {
	err := s.store.PutPart("unknownblob", 0, strings.NewReader("x"), 1, hashOf(""))
	c.Assert(err, gc.ErrorMatches, `upload id "unknownblob" not found`)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *SwiftStoreSuite) TestFinishUploadMismatchedPartCount(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}})
	c.Assert(err, gc.ErrorMatches, `part count mismatch \(got 1 but 2 uploaded\)`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *SwiftStoreSuite) TestFinishUploadMismatchedPartHash(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
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

func (s *SwiftStoreSuite) TestFinishUploadPartNotUploaded(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content1 := "123456789 123456789 "
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
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

func (s *SwiftStoreSuite) TestFinishUploadPartIncomplete(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(""), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.ErrorMatches, `cannot upload part ".+/0": hash mismatch`)

	idx, hash, err := s.store.FinishUpload(id, []blobstore.Part{{
		Hash: hashOf(content0),
	}})
	c.Assert(err, gc.ErrorMatches, `part 0 not uploaded yet`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *SwiftStoreSuite) TestFinishUploadCheckSizes(c *gc.C) {
	s.store.MinPartSize = 50
	id := s.newUpload(c)
	content := "123456789 123456789 "
	// Upload two small parts concurrently.
	done := make(chan error)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			err := s.store.PutPart(id, i, strings.NewReader(content), int64(len(content)), hashOf(content))
			done <- err
		}()
	}
	allOK := true
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			c.Assert(err, gc.ErrorMatches, ".*too small.*")
			allOK = allOK && err == nil
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
	c.Assert(err, gc.ErrorMatches, `part 0 was too small \(need at least 50 bytes, got 20\)`)
	c.Assert(idx, gc.IsNil)
	c.Assert(hash, gc.Equals, "")
}

func (s *SwiftStoreSuite) TestFinishUploadSuccess(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
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
	})
}

func (s *SwiftStoreSuite) TestFinishUploadSuccessOnePart(c *gc.C) {
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
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
	})
}

func (s *SwiftStoreSuite) TestFinishUploadNotFound(c *gc.C) {
	_, _, err := s.store.FinishUpload("not-an-id", nil)
	c.Assert(err, gc.ErrorMatches, `upload id "not-an-id" not found`)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *SwiftStoreSuite) TestFinishUploadAgain(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
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
	})
}

func (s *SwiftStoreSuite) TestFinishUploadCalledWhenCalculatingHash(c *gc.C) {
	s.store.MinPartSize = 10
	id := s.newUpload(c)

	// We need at least two parts so that FinishUpload
	// actually needs to stream the parts again, so
	// upload a small first part and then a large second
	// part that's big enough that there's a strong probability
	// that we'll be able to remove the upload entry before
	// FinishUpload has finished calculating the hash.
	content0 := "123456789 123456789 "
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	const size1 = 2 * 1024 * 1024
	hash1 := hashOfReader(c, newDataSource(1, size1))
	err = s.store.PutPart(id, 1, newDataSource(1, size1), int64(size1), hash1)
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
	err = s.store.RemoveUpload(id, func(_, _ string) (bool, error) {
		return false, nil
	})
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

func (s *SwiftStoreSuite) TestRemoveUploadSuccessWithNoPart(c *gc.C) {
	s.store.MinPartSize = 10
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	err = s.store.RemoveUpload(id, isOwnedByNotCalled(c))
	c.Assert(err, gc.Equals, nil)
	s.assertUploadDoesNotExist(c, id)
}

func (s *SwiftStoreSuite) TestRemoveUploadOnNonExistingUpload(c *gc.C) {
	err := s.store.RemoveUpload("something", isOwnedByNotCalled(c))
	c.Assert(err, gc.Equals, nil)
}

func (s *SwiftStoreSuite) TestRemoveUploadSuccessWithParts(c *gc.C) {
	s.store.MinPartSize = 10
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	content := "123456789 12345"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)
	err = s.store.RemoveUpload(id, isOwnedByNotCalled(c))
	c.Assert(err, gc.Equals, nil)
	s.assertUploadDoesNotExist(c, id)
	s.assertBlobDoesNotExist(c, id+"/0")
}

func (s *SwiftStoreSuite) TestSetOwner(c *gc.C) {
	s.store.MinPartSize = 10
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	content := "123456789 12345"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
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

	err = s.store.RemoveUpload(id, nil)
	c.Assert(err, gc.Equals, nil)

	// Check that we get a not-found error when the upload
	// has actually been removed.
	err = s.store.SetOwner(id, "something", newExpires)
	c.Check(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
	c.Assert(err, gc.ErrorMatches, `upload has been removed`)
}

func (s *SwiftStoreSuite) TestRemoveFinishedUploadRemovesParts(c *gc.C) {
	s.store.MinPartSize = 10

	id, err := s.store.NewUpload(time.Now().Add(time.Minute))
	c.Assert(err, gc.Equals, nil)
	content := "123456789 12345"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)
	_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
	c.Assert(err, gc.Equals, nil)

	err = s.store.RemoveUpload(id, isOwnedByNotCalled(c))
	c.Assert(err, gc.Equals, nil)

	s.assertUploadDoesNotExist(c, id)
	s.assertBlobDoesNotExist(c, id+"/0")
}

func (s *SwiftStoreSuite) TestRemoveOwnedBlobWithOwnershipCheckReturningTrue(c *gc.C) {
	s.store.MinPartSize = 10

	content0 := "123456789 12345"
	content1 := "abcdefghijklmnopqrstuvwxyz"
	// Note: putMultipartNoRemove sets the owner to "test".
	id, idx := s.putMultipartNoRemove(c, content0, content1)

	called := 0
	err := s.store.RemoveUpload(id, func(uploadId, owner string) (bool, error) {
		c.Check(uploadId, gc.Equals, id)
		c.Check(owner, gc.Equals, "test")
		called++
		return true, nil
	})
	c.Assert(err, gc.Equals, nil)
	c.Assert(called, gc.Equals, 1)

	// Because the document was owned, only the
	// upload document is removed.
	s.assertUploadDoesNotExist(c, id)
	s.assertBlobContent(c, id, idx, content0+content1)
}

func (s *SwiftStoreSuite) TestRemoveOwnedBlobWithOwnershipCheckReturningFalse(c *gc.C) {
	s.store.MinPartSize = 10

	content0 := "123456789 12345"
	content1 := "abcdefghijklmnopqrstuvwxyz"
	// Note: putMultipartNoRemove sets the owner to "test".
	id, _ := s.putMultipartNoRemove(c, content0, content1)

	called := 0
	err := s.store.RemoveUpload(id, func(uploadId, owner string) (bool, error) {
		c.Check(uploadId, gc.Equals, id)
		c.Check(owner, gc.Equals, "test")
		called++
		return false, nil
	})
	c.Assert(err, gc.Equals, nil)
	c.Assert(called, gc.Equals, 1)

	// Because the document was not owned, the
	// parts are removed too.
	s.assertUploadDoesNotExist(c, id)
	s.assertBlobDoesNotExist(c, id+"/0")
	s.assertBlobDoesNotExist(c, id+"/1")
}

func (s *SwiftStoreSuite) TestRemoveExpiredUploads(c *gc.C) {
	s.store.MinPartSize = 10

	expireTimes := []time.Duration{-time.Minute, -time.Second, time.Minute, time.Hour}
	ids := make([]string, len(expireTimes))
	content := "123456789 12345"
	for i, dt := range expireTimes {
		id, err := s.store.NewUpload(time.Now().Add(dt))
		c.Assert(err, gc.Equals, nil)
		err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
		c.Assert(err, gc.Equals, nil)
		_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
		c.Assert(err, gc.Equals, nil)
		ids[i] = id
	}

	err := s.store.RemoveExpiredUploads(func(uploadId, owner string) (bool, error) {
		c.Errorf("isOwnedBy called unexpectedly")
		return false, nil
	})
	c.Assert(err, gc.Equals, nil)
	for i, id := range ids {
		if expireTimes[i] < 0 {
			s.assertUploadDoesNotExist(c, id)
			s.assertBlobDoesNotExist(c, id+"/0")
		} else {
			_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
			c.Assert(err, gc.Equals, nil)
			s.assertBlobContent(c, id+"/0", nil, content)
		}
	}
}

func (s *SwiftStoreSuite) TestRemoveExpiredUploadsRemovesOrphanedBlobs(c *gc.C) {
	id, err := s.store.NewUpload(time.Now().Add(-time.Minute))
	c.Assert(err, gc.Equals, nil)
	content := "abcdefghiljklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)
	_, _, err = s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
	c.Assert(err, gc.Equals, nil)
	newExpires := time.Now().Add(5 * time.Second)
	err = s.store.SetOwner(id, "test", newExpires)
	c.Assert(err, gc.Equals, nil)

	called := 0
	err = blobstore.RemoveExpiredUploads(s.store, func(uploadId, owner string) (bool, error) {
		called++
		c.Check(uploadId, gc.Equals, id)
		c.Check(owner, gc.Equals, "test")
		// Note: return false to indicate that the blob is orphaned.
		return false, nil
	}, newExpires.Add(time.Millisecond))
	c.Assert(err, gc.Equals, nil)
	c.Check(called, gc.Equals, 1)

	s.assertUploadDoesNotExist(c, id)
	s.assertBlobDoesNotExist(c, id+"/0")
}

func (s *SwiftStoreSuite) TestRemoveExpiredUploadsDoesNotRemoveNonOrphanBlobs(c *gc.C) {
	id, err := s.store.NewUpload(time.Now().Add(-time.Minute))
	c.Assert(err, gc.Equals, nil)
	content := "abcdefghiljklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)
	idx, _, err := s.store.FinishUpload(id, []blobstore.Part{{Hash: hashOf(content)}})
	c.Assert(err, gc.Equals, nil)
	newExpires := time.Now().Add(5 * time.Second)
	err = s.store.SetOwner(id, "test", newExpires)
	c.Assert(err, gc.Equals, nil)

	called := 0
	err = blobstore.RemoveExpiredUploads(s.store, func(uploadId, owner string) (bool, error) {
		called++
		c.Check(uploadId, gc.Equals, id)
		c.Check(owner, gc.Equals, "test")
		// Note: return true to indicate that the blob is owned.
		return true, nil
	}, newExpires.Add(time.Millisecond))
	c.Assert(err, gc.Equals, nil)
	c.Check(called, gc.Equals, 1)

	s.assertUploadDoesNotExist(c, id)
	s.assertBlobContent(c, id, idx, content)
}

func (s *SwiftStoreSuite) TestOpenEmptyMultipart(c *gc.C) {
	id, idx := s.putMultipart(c)
	s.assertBlobContent(c, id, idx, "")
}

func (s *SwiftStoreSuite) TestMultipartReadAll(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	part1 := "abcdefghijklmnopqrstuvwxyz"
	part2 := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	id, idx := s.putMultipart(c, part0, part1, part2)
	s.assertBlobContent(c, id, idx, part0+part1+part2)
}

func (s *SwiftStoreSuite) TestMultipartSmallReads(c *gc.C) {
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

func (s *SwiftStoreSuite) TestMultipartSinglePart(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	id, idx := s.putMultipart(c, part0)
	s.assertBlobContent(c, id, idx, part0)
}

func (s *SwiftStoreSuite) TestMultipartCloseWithoutReading(c *gc.C) {
	s.store.MinPartSize = 10
	part0 := "123456789 12345"
	part1 := "abcdefghijklmnopqrstuvwxyz"
	id, idx := s.putMultipart(c, part0, part1)
	r, _, err := s.store.Open(id, idx)
	c.Assert(err, gc.Equals, nil)
	err = r.Close()
	c.Assert(err, gc.Equals, nil)
}

func (s *SwiftStoreSuite) TestUploadInfo(c *gc.C) {
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
		}, {
			Hash:     hashOf(part1),
			Size:     int64(len(part1)),
			Complete: true,
		}, {
			Hash:     hashOf(part2),
			Size:     int64(len(part2)),
			Complete: true,
		}},
		Hash: hashOf(part0 + part1 + part2),
	})

	// Check that we can read the blob from the index
	// derived from the UploadInfo.
	idx, ok := info.Index()
	c.Assert(ok, gc.Equals, true)
	s.assertBlobContent(c, id, idx, part0+part1+part2)
}

func (s *SwiftStoreSuite) TestMultipartSeek(c *gc.C) {
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

func (s *SwiftStoreSuite) putMultipart(c *gc.C, contents ...string) (string, *mongodoc.MultipartIndex) {
	id, idx := s.putMultipartNoRemove(c, contents...)
	err := s.store.RemoveUpload(id, nil)
	c.Assert(err, gc.Equals, nil)
	return id, idx
}

func (s *SwiftStoreSuite) putMultipartNoRemove(c *gc.C, contents ...string) (string, *mongodoc.MultipartIndex) {
	expires := time.Now().Add(time.Minute)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)

	parts := make([]blobstore.Part, len(contents))
	for i, content := range contents {
		hash := hashOf(content)
		err = s.store.PutPart(id, i, strings.NewReader(content), int64(len(content)), hash)
		c.Assert(err, gc.Equals, nil)
		parts[i].Hash = hash
	}
	idx, _, err := s.store.FinishUpload(id, parts)
	c.Assert(err, gc.Equals, nil)
	err = s.store.SetOwner(id, "test", expires)
	c.Assert(err, gc.Equals, nil)
	return id, idx
}

func (s *SwiftStoreSuite) assertUploadDoesNotExist(c *gc.C, id string) {
	_, err := s.store.UploadInfo(id)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *SwiftStoreSuite) assertBlobDoesNotExist(c *gc.C, name string) {
	_, _, err := s.store.Open(name, nil)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

func (s *SwiftStoreSuite) assertBlobContent(c *gc.C, name string, idx *mongodoc.MultipartIndex, content string) {
	r, size, err := s.store.Open(name, idx)
	c.Assert(err, gc.Equals, nil)
	defer r.Close()
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content)))
	data, err := ioutil.ReadAll(r)
	c.Assert(err, gc.Equals, nil)
	c.Assert(string(data), gc.Equals, content)
}

// newUpload returns the id of a new upload instance.
func (s *SwiftStoreSuite) newUpload(c *gc.C) string {
	expires := time.Now().Add(time.Minute).UTC()
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	return id
}
