// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"fmt"
	"io/ioutil"
	"strings"

	"gopkg.in/amz.v3/aws"
	"gopkg.in/amz.v3/s3"
	"gopkg.in/amz.v3/s3/s3test"

	gc "gopkg.in/check.v1"
)

type s3StoreSuite struct {
	s3srv *s3test.Server
}

var _ = gc.Suite(&s3StoreSuite{})

func (s *s3StoreSuite) SetUpSuite(c *gc.C) {
	var err error
	s.s3srv, err = s3test.NewServer(&s3test.Config{})
	if err != nil {
		c.Fatalf("cannot start s3 test server: %v", err)
	}
	aws.Regions["test"] = aws.Region{
		Name:                 "test",
		S3Endpoint:           s.s3srv.URL(),
		S3LocationConstraint: true,
	}
}

func (s *s3StoreSuite) TearDownSuite(c *gc.C) {
	s.s3srv.Quit()
}

func (s *s3StoreSuite) TestPutOpen(c *gc.C) {
	store := newS3(&ProviderConfig{BucketName: "charmstoretestbucket"})
	store.getS3 = testGetS3
	store.createBucket()
	content := "some data"
	err := store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := store.Open("x")
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)

}

func hashOf(s string) string {
	h := NewHash()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// testGetS3 returns a fake s3.S3.
func testGetS3() *s3.S3 {
	region := aws.Regions["test"]
	auth := aws.Auth{
		AccessKey: "doesnot",
		SecretKey: "matter",
	}
	return s3.New(auth, region)
}
