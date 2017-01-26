// Copyright 2014-2017 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/config"
	"gopkg.in/juju/charmstore.v5-unstable/elasticsearch"
	"gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/mgo.v2/bson"
)

var logger = loggo.GetLogger("charmdelete")

var (
	index             = flag.String("index", "cs", "Name of index to charmDelete.")
	loggingConfig     = flag.String("logging-config", "", "specify log levels for modules e.g. <root>=TRACE")
	user              = flag.String("user", "", "Delete all charms for a user. Do not mix with charmMatch.")
	charmMatch        = flag.String("charm-match", "", "Delete all charms maching this expression. Do not mix with user.")
	dryrun            = flag.Bool("dry-run", false, "Don't actually delete; just print them.")
	verbose           = flag.Bool("verbose", false, "")
	deletePromulgated = flag.Bool("delete-promulgated", false, "Delete a charm even if it is promulgated.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <config path>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
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
		logger.Errorf("cannot run: %v", err)
		os.Exit(1)
	}
}

func run(confPath string) error {
	logger.Debugf("reading config file %q", confPath)
	conf, err := config.Read(confPath)
	if err != nil {
		return errgo.Notef(err, "cannot read config file %q", confPath)
	}
	if conf.ESAddr == "" {
		return errgo.Newf("no elasticsearch-addr specified in config file %q", confPath)
	}
	si := &charmstore.SearchIndex{
		Database: &elasticsearch.Database{
			conf.ESAddr,
		},
		Index: *index,
	}
	session, err := mgo.Dial(conf.MongoURL)
	if err != nil {
		return errgo.Notef(err, "cannot dial mongo at %q", conf.MongoURL)
	}
	defer session.Close()
	db := session.DB("juju")

	pool, err := charmstore.NewPool(db, si, nil, charmstore.ServerParams{})
	if err != nil {
		return errgo.Notef(err, "cannot create a new store")
	}
	store := pool.Store()
	defer store.Close()
	entities := store.DB.Entities()

	query := bson.D{{"user", *user}}
	if *user == "" {
		fmt.Printf("using match _id $regex %s\n", *charmMatch)
		query = bson.D{{"_id", bson.D{{"$regex", *charmMatch}}}}

		if *charmMatch == "" {
			query = nil
		}
	}
	if query != nil {
		var entity mongodoc.Entity
		iter := entities.Find(query).Iter()
		defer iter.Close()

		counter := 0
		for iter.Next(&entity) {
			if entity.PromulgatedURL != nil && !*deletePromulgated {
				fmt.Printf("not deleting promulgated charm %s\n", entity.URL)
				continue
			}
			if *verbose {
				fmt.Printf("deleting %s\n", entity.URL)
			}
			if !*dryrun {
				deleteEntity(&entity, store)
			}
			counter++
			if counter%100 == 0 {
				logger.Infof("%d entities deleted", counter)
			}
		}
	}

	return nil
}

func deleteEntity(entity *mongodoc.Entity, store *charmstore.Store) {
	err := store.BlobStore.Remove(entity.BlobName)
	if err != nil {
		logger.Errorf("could not remove blob for charm %s %s", entity.URL, err)
	} else if *verbose {
		fmt.Printf("deleted blob %s\n", entity.BlobName)
	}
	err = store.BlobStore.Remove(entity.BlobName + ".pre-v5-suffix")
	if err != nil {
		logger.Errorf("could not remove .pre-v5-suffix blob for charm %s %s", entity.URL, err)
	} else if *verbose {
		fmt.Printf("deleted blob %s\n", entity.BlobName+".pre-v5-suffix")
	}

	if entity.CharmMeta != nil && entity.CharmMeta.Resources != nil {
		for _, r := range entity.CharmMeta.Resources {
			err = store.BlobStore.Remove(r.Name)
			if err != nil {
				logger.Errorf("could not remove %s resource blob for charm %s %s", r.Name, entity.URL, err)
			} else if *verbose {
				fmt.Printf("deleted resource blob %s\n", r.Name)
			}
		}
	}
	err = store.DB.Entities().Remove(bson.D{{"_id", entity.URL}})
	if err != nil {
		logger.Errorf("could not remove entity for charm %s %s", entity.URL, err)
	} else if *verbose {
		fmt.Printf("deleted entity %s\n", entity.URL)
	}
	c, err := store.DB.Entities().Find(bson.D{{"baseurl", entity.BaseURL}}).Count()

	if c == 0 {
		err = store.DB.BaseEntities().Remove(bson.D{{"_id", entity.BaseURL}})
		if err != nil {
			logger.Errorf("could not remove base_entity for charm %s %s", entity.BaseURL, err)
		} else if *verbose {
			fmt.Printf("deleted base entity %s\n", entity.BaseURL)
		}
	}
}
