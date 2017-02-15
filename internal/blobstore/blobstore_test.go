// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore_test // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	jujutesting "github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/errgo.v1"

	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
)

func TestPackage(t *testing.T) {
	jujutesting.MgoTestPackage(t, nil)
}

type BlobStoreSuite struct {
	jujutesting.IsolatedMgoSuite
	store *blobstore.Store
}

var _ = gc.Suite(&BlobStoreSuite{})

func (s *BlobStoreSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	s.store = blobstore.New(s.Session.DB("db"), "blobstore")
}

func (s *BlobStoreSuite) TestPutTwice(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	content = "some different data"
	err = s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := s.store.Open("x", nil)
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)
}

func (s *BlobStoreSuite) TestPut(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := s.store.Open("x", nil)
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)

	err = s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)
}

func (s *BlobStoreSuite) TestPutInvalidHash(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf("wrong"))
	c.Assert(err, gc.ErrorMatches, "hash mismatch")
}

func (s *BlobStoreSuite) TestRemove(c *gc.C) {
	content := "some data"
	err := s.store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := s.store.Open("x", nil)
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))
	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)

	err = s.store.Remove("x", nil)
	c.Assert(err, gc.IsNil)

	rc, length, err = s.store.Open("x", nil)
	c.Assert(err, gc.ErrorMatches, `resource at path "[^"]+" not found`)
}

func (s *BlobStoreSuite) TestNewParts(c *gc.C) {
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	c.Assert(id, gc.Not(gc.Equals), "")

	// Verify that the new record looks like we expect.
	var udoc blobstore.UploadDoc
	db := s.Session.DB("db")
	err = db.C("blobstore.upload").FindId(id).One(&udoc)
	c.Assert(err, gc.Equals, nil)
	c.Assert(udoc, jc.DeepEquals, blobstore.UploadDoc{
		Id:      id,
		Expires: expires,
	})
}

func (s *BlobStoreSuite) TestPutPartNegativePart(c *gc.C) {
	id := s.newUpload(c)

	err := s.store.PutPart(id, -1, nil, 0, "")
	c.Assert(err, gc.ErrorMatches, "negative part number")
}

func (s *BlobStoreSuite) TestPutPartNumberTooBig(c *gc.C) {
	s.PatchValue(blobstore.MaxParts, 100)

	id := s.newUpload(c)
	err := s.store.PutPart(id, 100, nil, 0, "")
	c.Assert(err, gc.ErrorMatches, `part number 100 too big \(maximum 99\)`)
}

func (s *BlobStoreSuite) TestPutPartSingle(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	r, size, err := s.store.Open(id+"/0", nil)
	c.Assert(err, gc.Equals, nil)
	c.Assert(size, gc.Equals, int64(len(content)))
	c.Assert(hashOfReader(c, r), gc.Equals, hashOf(content))
}

func (s *BlobStoreSuite) TestPutPartAgain(c *gc.C) {
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

func (s *BlobStoreSuite) TestPutPartAgainWithDifferentHash(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err = s.store.PutPart(id, 0, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `hash mismatch for already uploaded part`)
}

func (s *BlobStoreSuite) TestPutPartAgainWithSameHash(c *gc.C) {
	id := s.newUpload(c)

	content := "123456789 12345"
	err := s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)

	err = s.store.PutPart(id, 0, strings.NewReader(content), int64(len(content)), hashOf(content))
	c.Assert(err, gc.Equals, nil)
}

func (s *BlobStoreSuite) TestPutPartOutOfOrder(c *gc.C) {
	s.PatchValue(blobstore.MinPartSize, int64(10))
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

func (s *BlobStoreSuite) TestPutPartTooSmall(c *gc.C) {
	s.PatchValue(blobstore.MinPartSize, int64(100))
	id := s.newUpload(c)

	content0 := "abcdefghijklmnopqrstuvwxyz"
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "123456789 123456789 "
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `part 0 was too small \(need at least 100 bytes, got 26\)`)
}

func (s *BlobStoreSuite) TestPutPartTooSmallOutOfOrder(c *gc.C) {
	s.PatchValue(blobstore.MinPartSize, int64(100))
	id := s.newUpload(c)

	content1 := "abcdefghijklmnopqrstuvwxyz"
	err := s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.Equals, nil)

	content0 := "123456789 123456789 "
	err = s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.ErrorMatches, `part too small \(need at least 100 bytes, got 20\)`)
}

func (s *BlobStoreSuite) TestPutPartSmallAtEnd(c *gc.C) {
	s.PatchValue(blobstore.MinPartSize, int64(10))
	id := s.newUpload(c)

	content0 := "1234"
	err := s.store.PutPart(id, 0, strings.NewReader(content0), int64(len(content0)), hashOf(content0))
	c.Assert(err, gc.Equals, nil)

	content1 := "abc"
	err = s.store.PutPart(id, 1, strings.NewReader(content1), int64(len(content1)), hashOf(content1))
	c.Assert(err, gc.ErrorMatches, `part 0 was too small \(need at least 10 bytes, got 4\)`)
}

func (s *BlobStoreSuite) TestPutPartConcurrent(c *gc.C) {
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
			store := blobstore.New(db, "blobstore")
			err := store.PutPart(id, i, newDataSource(int64(i+1), size), size, hash[i])
			c.Check(err, gc.IsNil)
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

func (s *BlobStoreSuite) TestPutPartNotFound(c *gc.C) {

	err := s.store.PutPart("unknownblob", 0, strings.NewReader(""), 0, hashOf(""))
	c.Assert(err, gc.ErrorMatches, `upload id "unknownblob" not found`)
	c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound)
}

// newUpload returns the id of a new upload instance.
func (s *BlobStoreSuite) newUpload(c *gc.C) string {
	expires := time.Now().Add(time.Minute).UTC()
	id, err := s.store.NewUpload(expires)
	c.Assert(err, gc.Equals, nil)
	return id
}

func hashOfReader(c *gc.C, r io.Reader) string {
	h := blobstore.NewHash()
	_, err := io.Copy(h, r)
	c.Assert(err, gc.IsNil)
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
