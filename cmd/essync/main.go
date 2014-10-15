// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"

	"github.com/juju/charmstore/config"
	"github.com/juju/charmstore/internal/charmstore"
	"github.com/juju/charmstore/internal/elasticsearch"
)

var index = flag.String("index", "charmstore", "Name of index to populate.")
var settings = flag.String("settings", "", "File to use to configure the index.")
var mapping = flag.String("mapping", "", "File to use to configure the entity mapping.")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <config path>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
	}
	if err := populate(flag.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "cannot populate elasticsearch: %v", err)
		os.Exit(1)
	}
}

func populate(confPath string) error {
	conf, err := config.Read(confPath)
	if err != nil {
		return err
	}
	if conf.ESAddr == "" {
		return fmt.Errorf("no elasticsearch-addr specified in %s", confPath)
	}
	es := &elasticsearch.Database{conf.ESAddr}
	session, err := mgo.Dial(conf.MongoURL)
	if err != nil {
		return err
	}
	defer session.Close()
	db := session.DB("juju")
	store, err := charmstore.NewStore(db, &charmstore.StoreElasticSearch{es, *index})
	if err != nil {
		return err
	}
	if *settings != "" {
		err = writeSettings(es, *index, *settings)
		if err != nil {
			return err
		}
	}
	if *mapping != "" {
		err = writeMapping(es, *index, "entity", *mapping)
		if err != nil {
			return err
		}
	}
	return store.ExportToElasticSearch()
}

func writeSettings(es *elasticsearch.Database, index, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return errgo.NoteMask(err, "cannot read index settings")
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var data map[string]interface{}
	err = dec.Decode(&data)
	if err != nil {
		return errgo.NoteMask(err, "cannot read index settings")
	}
	err = es.PutIndex(index, data)
	if err != nil {
		return errgo.NoteMask(err, "cannot set index settings")
	}
	return nil
}

func writeMapping(es *elasticsearch.Database, index, type_, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return errgo.Notef(err, "cannot read %s mapping", type_)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var data map[string]interface{}
	err = dec.Decode(&data)
	if err != nil {
		return errgo.Notef(err, "cannot read %s mapping", type_)
	}
	err = es.PutMapping(index, type_, data)
	if err != nil {
		return errgo.Notef(err, "cannot set %s mapping", type_)
	}
	return nil
}
