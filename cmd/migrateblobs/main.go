// Copyright 2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// This command migrates blobstore blobs of charms and resources for all
// entities.  This command is intended to be run on the production db and then
// discarded.  The first time this command is executed, all the entities are
// updated.  Subsequent runs migrate missing destination blobs.
package main // import "gopkg.in/juju/charmstore.v5/cmd/migrateblobs"

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/juju/loggo"
	"github.com/juju/utils/parallel"
	"github.com/ncw/swift"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5/config"
)

var (
	logger        = loggo.GetLogger("migrateblobs")
	loggingConfig = flag.String("logging-config", "INFO", "specify log levels for modules e.g. <root>=TRACE")
	numParallel   = flag.Int("p", 1, "the number of parallel copiers")
)

const maxRetries = 10

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <config path> \n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "Config path is used to it read charmstore data and writing new blobs.\n")
		os.Exit(2)
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
	}
	if *loggingConfig != "" {
		if err := loggo.ConfigureLoggers(*loggingConfig); err != nil {
			fmt.Fprintf(os.Stderr, "cannot configure loggers: %v", err)
			os.Exit(1)
		}
	}
	if err := run(flag.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(confPath string) error {
	logger.Infof("reading configuration")
	config, err := config.Read(confPath)
	if err != nil {
		return errgo.Notef(err, "cannot read config file %q", confPath)
	}

	logger.Infof("connecting to mongo")
	session, err := mgo.Dial(config.MongoURL)
	if err != nil {
		return errgo.Notef(err, "cannot dial mongo at %q", config.MongoURL)
	}
	defer session.Close()
	db := session.DB("juju")

	dst := &swift.Connection{
		ApiKey:     config.SwiftSecret,
		AuthUrl:    config.SwiftAuthURL,
		Region:     config.SwiftRegion,
		StorageUrl: config.SwiftEndpointURL,
		Tenant:     config.SwiftTenant,
		UserName:   config.SwiftUsername,
	}

	logger.Infof("migrating entity blobs")
	counter, alreadyExistsCounter, err := migrate(db.GridFS("entitystore"), dst, config.SwiftBucket)
	logger.Infof("Total entities migrated %d, already existing %d", counter, alreadyExistsCounter)
	if err != nil {
		return errgo.Notef(err, "cannot migrate entity blobs")
	}
	logger.Infof("done")
	return nil
}

func migrate(gridfs *mgo.GridFS, sc *swift.Connection, dstContainerName string) (counter int32, alreadyExistsCounter int32, err error) {
	iter := gridfs.Find(nil).Sort("-uploadDate").Iter()
	defer iter.Close()
	run := parallel.NewRun(*numParallel)
	var file *mgo.GridFile
	for gridfs.OpenNext(iter, &file) {
		fileId := file.Id()
		run.Do(func() error {
			// Avoid session issue if the main session stop working
			// Copy the existing session
			session := gridfs.Files.Database.Session.Copy()
			defer session.Close()
			gridfs2 := &mgo.GridFS{
				Files:  gridfs.Files.With(session),
				Chunks: gridfs.Chunks.With(session),
			}
			file1, err := gridfs2.OpenId(fileId)
			if err != nil {
				return errgo.Mask(err)
			}
			found := false
			err = retry(func() error {
				_, _, err := sc.Object(dstContainerName, file1.Name())
				if err == nil {
					found = true
					logger.Infof("- skipping/existing %s [%d] %v",
						file1.Name(), file1.Size(),
						file1.UploadDate().Format("2006-01-02 15:04:05"))
					return nil
				}
				if err != swift.ObjectNotFound {
					return errgo.Mask(err)
				}
				return nil
			})
			if err != nil {
				logger.Errorf("cannot stat: %s", err)
				return errgo.Mask(err)
			}
			if found {
				atomic.AddInt32(&alreadyExistsCounter, 1)
				return nil
			}
			err = copyObject(file1, sc, dstContainerName)
			if err != nil {
				logger.Errorf("cannot copy: %s", err)
				return errgo.Mask(err)
			}
			counter1 := atomic.AddInt32(&counter, 1)
			logger.Infof("%d Migrated %s [%d] %v", counter1, file1.Name(), file1.Size(),
				file1.UploadDate().Format("2006-01-02 15:04:05"))
			return nil
		})
	}
	if err := run.Wait(); err != nil {
		for _, err1 := range err.(parallel.Errors) {
			logger.Infof("error when migrating %s", err1)
		}
		return counter, alreadyExistsCounter, errgo.Mask(err)
	}
	if err := iter.Err(); err != nil {
		return counter, alreadyExistsCounter, errgo.Notef(err, "cannot iterate entities")
	}
	return counter, alreadyExistsCounter, nil
}

func copyObject(file *mgo.GridFile, sc *swift.Connection, dstContainerName string) error {
	err := retry(func() error {
		file.Seek(0, 0) // If file was read and we are retrying, we need to seek to start of file.
		dst, err := sc.ObjectCreate(dstContainerName, file.Name(), true, file.MD5(), file.ContentType(), nil)
		if err != nil {
			return errgo.Mask(err)
		}
		if _, err := io.Copy(dst, file); err != nil {
			return errgo.Mask(err)
		}
		return errgo.Mask(dst.Close())
	})
	if err != nil {
		return errgo.Notef(err, "cannot put archive for %s", file.Name())
	}
	return nil
}

func retry(callback func() error) (err error) {
	for i := 0; i < maxRetries; i++ {
		if err = callback(); err == nil {
			return
		}
	}
	return errgo.Notef(err, "failed after %d attempts", maxRetries)
}
