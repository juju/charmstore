// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"

	"gopkg.in/amz.v3/aws"
	"gopkg.in/amz.v3/s3"
	"gopkg.in/errgo.v1"
)

type s3Store struct {
	bucket string
	getS3  func() *s3.S3
}

// NewS3 createa a new S3 backed blobstore Store
func NewS3(pc *ProviderConfig) *Store {
	return &Store{newS3(pc)}
}

func newS3(pc *ProviderConfig) *s3Store {
	getter := getS3(pc)
	s := &s3Store{
		bucket: pc.BucketName,
		getS3:  getter,
	}
	s.createBucket()
	return s
}

func (s *s3Store) createBucket() {
	bucket, err := s.getS3().Bucket(s.bucket)
	if err != nil { //this only happens on invalid bucket name that is when a name has any of /:@
		panic(err)
	}
	err = bucket.PutBucket(s3.Private)
	if err != nil {
		log.Println("Failed to create bucket", err)
	}
}

func (s *s3Store) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (_ *ContentChallenge, err error) {
	err = s.PutUnchallenged(r, name, size, hash)
	return
}

func (s *s3Store) PutUnchallenged(r io.Reader, name string, size int64, hash string) error {
	svc := s.getS3()
	bucket, _ := svc.Bucket(s.bucket) // Ignoring the error because we know this bucket name is valid.
	err := bucket.PutReader(name, r, size, "application/octet-stream", s3.Private)
	if err != nil {
		logger.Errorf("put failed :%s", err)
		return errgo.Mask(err)
	}
	logger.Debugf("successful put %s in bucket %s", name, s.bucket)
	return nil
}

func (s *s3Store) Open(name string) (ReadSeekCloser, int64, error) {
	svc := s.getS3()
	bucket, _ := svc.Bucket(s.bucket)
	rc, err := bucket.GetReader(name)
	if err != nil {
		return nil, 0, errgo.Mask(err)
	}
	data, err := ioutil.ReadAll(rc) // JRW: If only rc were Seeker
	if err != nil {
		return nil, 0, errgo.Mask(err)
	}
	r := nopCloser(bytes.NewReader(data)) // JRW: *cringe*
	return r, int64(len(data)), nil
}

func (s *s3Store) Remove(name string) error {
	bucket, _ := s.getS3().Bucket(s.bucket)
	return bucket.Del(name)
}

func getS3(pc *ProviderConfig) func() *s3.S3 {
	regionName := "us-east-1"
	if "" != pc.Region {
		regionName = pc.Region
	}
	region := aws.Regions[regionName]

	if "" != pc.Endpoint {
		region = aws.Region{
			Name:       regionName,
			S3Endpoint: pc.Endpoint,
		}
	}

	auth := aws.Auth{
		AccessKey: pc.Key,
		SecretKey: pc.Secret,
	}
	return func() *s3.S3 {
		return s3.New(auth, region)
	}
}

type nopCloserReadSeeker struct {
	io.ReadSeeker
}

func (nopCloserReadSeeker) Close() error {
	return nil
}

// nopCloser returns a ReadSeekCloser with a no-op Close method
// wrapping the provided ReadSeeker r.
func nopCloser(r io.ReadSeeker) ReadSeekCloser {
	return nopCloserReadSeeker{r}
}
