// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

// elasticsearch package api attempts to name methods to match the
// corresponding elasticsearch endpoint. Methods names like CatIndices are
// named as such because they correspond to /_cat/indices elasticsearch
// endpoint.
// There is no reason to use different vocabulary from that of elasticsearch.
// Use the elasticsearch terminology and avoid mapping names of things.

package elasticsearch // import "gopkg.in/juju/charmstore.v5/elasticsearch"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
)

const (
	// Internal provides elasticsearche's "internal" versioning system, as described in
	// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#_version_types
	Internal = "internal"

	// External provides elasticsearche's "external" versioning system, as described in
	// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#_version_types
	External = "external"

	// ExternalGTE provides elasticsearche's "external_gte" versioning system, as described in
	// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#_version_types
	ExternalGTE = "external_gte"
)

var logger = loggo.GetLogger("charmstore.elasticsearch")

// An ElasticsearchError represents an error returned by the
// elasticsearch server. All methods in this package return errors of
// this type where the error was return from the elasticsearch service.
type ElasticsearchError struct {
	// Status contains the status code returned from the
	// elasticsearch server.
	Status int

	// body contains the body of the error response.
	body errorBody
}

func (e *ElasticsearchError) Error() string {
	s := strings.ToLower(http.StatusText(e.Status))
	if e.body.Error.Reason != "" {
		s += ": " + e.body.Error.Reason
	}
	return s
}

// Type returns the type specified in the elasticsearch error response,
// if any.
func (e *ElasticsearchError) Type() string {
	return e.body.Error.Type
}

type errorBody struct {
	Status int       `json:"status"`
	Error  errorInfo `json:"error"`
}

type errorInfo struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// UnmarshalJSON implements json.Unmarshaler
func (e *errorInfo) UnmarshalJSON(b []byte) error {
	type ei errorInfo
	if b[0] == '{' {
		return json.Unmarshal(b, (*ei)(e))
	}
	return json.Unmarshal(b, &e.Reason)
}

// ErrorStatus returns the elasticsearch status code associated with the
// given error, if any.
func ErrorStatus(err error) int {
	var status int
	if eserr, ok := err.(*ElasticsearchError); ok {
		status = eserr.Status
	}
	return status
}

// IsElasticsearchError reports whether the given error is an instance of
// *ElasticsearchError.
func IsElasticsearchError(err error) bool {
	_, ok := err.(*ElasticsearchError)
	return ok
}

// IsConflictError reports whether the given error is
// returned from elasticsearch and represents a conflict.
func IsConflictError(err error) bool {
	return ErrorStatus(err) == http.StatusConflict
}

// IsNotFoundError reports whether the given error is
// returned from elasticsearch and represents not found.
func IsNotFoundError(err error) bool {
	return ErrorStatus(err) == http.StatusNotFound
}

// Database represents a connection to an elasticsearch database.
type Database struct {
	Addr string
}

// Document represents a document in the elasticsearch database.
type Document struct {
	Found   bool            `json:"found"`
	Id      string          `json:"_id"`
	Index   string          `json:"_index"`
	Type    string          `json:"_type"`
	Version int64           `json:"_version"`
	Source  json.RawMessage `json:"_source"`
}

// ClusterHealth represents the response from _cluster/health on elastic search
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-health.html
type ClusterHealth struct {
	ClusterName         string `json:"cluster_name"`
	Status              string `json:"status"`
	TimedOut            bool   `json:"timed_out"`
	NumberOfNodes       int64  `json:"number_of_nodes"`
	NumberOfDataNodes   int64  `json:"number_of_data_nodes"`
	ActivePrimaryShards int64  `json:"active_primary_shards"`
	ActiveShards        int64  `json:"active_shards"`
	RelocatingShards    int64  `json:"relocating_shards"`
	InitializingShards  int64  `json:"initializing_shards"`
	UnassignedShards    int64  `json:"unassigned_shards"`
}

// String implements fmt.Stringer.
func (h *ClusterHealth) String() string {
	return fmt.Sprintf("cluster_name: %s, status: %s, timed_out: %t"+
		", number_of_nodes: %d, number_of_data_nodes: %d"+
		", active_primary_shards: %d, active_shards: %d"+
		", relocating_shards: %d, initializing_shards: %d"+
		", unassigned_shards:%d", h.ClusterName, h.Status,
		h.TimedOut, h.NumberOfNodes, h.NumberOfDataNodes,
		h.ActivePrimaryShards, h.ActiveShards,
		h.RelocatingShards, h.InitializingShards,
		h.UnassignedShards)
}

// Alias creates or updates an index alias. An alias a is created, or
// modified if it already exists, to point to i. See
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-aliases.html#indices-aliases
// for further details. If an error is returned by the elasticsearch
// server then the returned error will have a cause of type *ElasticsearchError.
func (db *Database) Alias(i, a string) error {
	indexes, err := db.ListIndexesForAlias(a)
	if err != nil {
		return errgo.NoteMask(err, "cannot retrieve current aliases", IsElasticsearchError)
	}
	var actions struct {
		Actions []action `json:"actions"`
	}
	for _, i := range indexes {
		actions.Actions = append(actions.Actions, action{Remove: &alias{Index: i, Alias: a}})
	}
	if i != "" {
		actions.Actions = append(actions.Actions, action{Add: &alias{Index: i, Alias: a}})
	}
	if len(actions.Actions) == 0 {
		return nil
	}
	if err := db.post(db.url("_aliases"), actions, nil); err != nil {
		return errgo.Notef(err, "error updating aliases")
	}
	return nil
}

// Create document attempts to create a new document at index/type_/id with the
// contents in doc. If the document already exists then CreateDocument will return
// an error with a cause that satisfies IsConflictError.
// See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/create-doc.html#create-doc
// for further details.
func (db *Database) CreateDocument(index, type_, id string, doc interface{}) error {
	return errgo.Mask(db.put(db.url(index, type_, id, "_create"), doc, nil), IsElasticsearchError)
}

// DeleteDocument deletes the document at index/type_/id from the elasticsearch
// database. See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/delete-doc.html#delete-doc
// for further details.
func (db *Database) DeleteDocument(index, type_, id string) error {
	return errgo.Mask(db.delete(db.url(index, type_, id), nil, nil), IsElasticsearchError)
}

// DeleteIndex deletes the index with the given name from the database.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-delete-index.html
// If the index does not exist or if the database cannot be
// reached, then an error is returned.
func (db *Database) DeleteIndex(index string) error {
	return errgo.Mask(db.delete(db.url(index), nil, nil), IsElasticsearchError)
}

// GetDocument retrieves the document with the given index, type_ and id
// and unmarshals the json response into v. GetDocument returns an error
// with a cause that satisfies IsNotFoundError if the requested document
// is not present, and returns a non-nil error if any other error occurs.
func (db *Database) GetDocument(index, type_, id string, v interface{}) error {
	d, err := db.GetESDocument(index, type_, id)
	if err != nil {
		return errgo.Mask(err, IsElasticsearchError)
	}
	if !d.Found {
		return errgo.WithCausef(nil, &ElasticsearchError{Status: http.StatusNotFound}, "")
	}
	if err := json.Unmarshal([]byte(d.Source), &v); err != nil {
		return errgo.Mask(err)
	}
	return nil
}

// GetESDocument returns elasticsearch's view of the document stored at
// index/type_/id. It is not an error if this document does not exist, in that case
// the Found field of the returned Document will be false.
func (db *Database) GetESDocument(index, type_, id string) (Document, error) {
	var d Document
	if err := db.get(db.url(index, type_, id), nil, &d); err != nil {
		return Document{}, errgo.Mask(err, IsElasticsearchError)
	}
	return d, nil
}

// HasDocument tests to see a document of the given index, type_, and id exists
// in the elasticsearch database. A non-nil error is returned if there is an error
// communicating with the elasticsearch database.
func (db *Database) HasDocument(index, type_, id string) (bool, error) {
	var d Document
	if err := db.get(db.url(index, type_, id)+"?_source=false", nil, &d); err != nil {
		return false, errgo.Mask(err, IsElasticsearchError)
	}
	return d.Found, nil
}

// Check the health status of Elastic search and retrieve general data from it.
// Calling get on /_cluster/health to retrieve status.
func (db *Database) Health() (ClusterHealth, error) {
	var result ClusterHealth
	if err := db.get(db.url("_cluster", "health"), nil, &result); err != nil {
		return ClusterHealth{}, errgo.Mask(err, IsElasticsearchError)
	}

	return result, nil
}

// ListAllIndexes retreieves the list of all user indexes in the elasticsearch database.
// indexes that are generated to to support plugins are filtered out of the list that
// is returned.
func (db *Database) ListAllIndexes() ([]string, error) {
	var result map[string]interface{}
	if err := db.get(db.url("_aliases"), nil, &result); err != nil {
		return nil, errgo.Mask(err, IsElasticsearchError)
	}
	var indexes []string
	for key := range result {
		// Some ElasticSearch plugins create indexes (e.g. ".marvel...") for their
		// use.  Ignore any that start with a dot.
		if !strings.HasPrefix(key, ".") {
			indexes = append(indexes, key)
		}
	}
	return indexes, nil
}

// ListIndexesForAlias retreieves the list of all indexes in the elasticsearch database
// that have the alias a.
func (db *Database) ListIndexesForAlias(a string) ([]string, error) {
	var result map[string]struct{}
	if err := db.get(db.url("*", "_alias", a), nil, &result); err != nil {
		return nil, errgo.Mask(err, IsElasticsearchError)
	}
	var indexes []string
	for key := range result {
		indexes = append(indexes, key)
	}
	return indexes, nil
}

// PostDocument creates a new auto id document with the given index and _type
// and returns the generated id of the document. The type_ parameter controls how
// the document will be mapped in the index. See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html
// for more details.
func (db *Database) PostDocument(index, type_ string, doc interface{}) (string, error) {
	var resp struct {
		ID string `json:"_id"`
	}
	if err := db.post(db.url(index, type_), doc, &resp); err != nil {
		return "", errgo.Mask(err, IsElasticsearchError)
	}
	return resp.ID, nil
}

// PutDocument creates or updates the document with the given index, type_ and
// id. The type_ parameter controls how the document will be mapped in the index.
// See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html
// for more details.
func (db *Database) PutDocument(index, type_, id string, doc interface{}) error {
	if err := db.put(db.url(index, type_, id), doc, nil); err != nil {
		return errgo.Mask(err, IsElasticsearchError)
	}
	return nil
}

// PutDocumentVersion creates or updates the document in the given index
// if the version parameter is the same as the currently stored version.
// The type_ parameter controls how the document will be indexed.
// PutDocumentVersion returns an error with a cause that satisfies
// IsConflictError if the data cannot be stored due to a version
// mismatch, and a non-nil error if any other error occurs. See
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning
// for more information.
func (db *Database) PutDocumentVersion(index, type_, id string, version int64, doc interface{}) error {
	return errgo.Mask(db.PutDocumentVersionWithType(index, type_, id, version, "internal", doc), IsElasticsearchError)
}

// PutDocumentVersionWithType creates or updates the document in the
// given index if the version parameter is the same as the currently
// stored version. The type_ parameter controls how the document will be
// indexed. PutDocumentVersionWithType returns an error with a cause that
// satisfies IsConflictError if the data cannot be stored due to a
// version mismatch, and a non-nil error if any other error occurs.
//
// The constants Internal, External and ExternalGTE represent some of the
// available version types. Other version types may also be available,
// plese check the elasticsearch documentation.
//
// See
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning
// and
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#_version_types
// for more information.
func (db *Database) PutDocumentVersionWithType(
	index, type_, id string,
	version int64,
	versionType string,
	doc interface{}) error {
	url := fmt.Sprintf("%s?version=%d&version_type=%s", db.url(index, type_, id), version, versionType)
	return errgo.Mask(db.put(url, doc, nil), IsElasticsearchError)
}

// PutIndex creates the index with the given configuration.
func (db *Database) PutIndex(index string, config interface{}) error {
	return errgo.Mask(db.put(db.url(index), config, nil), IsElasticsearchError)
}

// PutMapping creates or updates the mapping with the given configuration.
func (db *Database) PutMapping(index, type_ string, config interface{}) error {
	return errgo.Mask(db.put(db.url(index, "_mapping", type_), config, nil), IsElasticsearchError)
}

// RefreshIndex posts a _refresh to the index in the database.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-refresh.html
func (db *Database) RefreshIndex(index string) error {
	return errgo.Mask(db.post(db.url(index, "_refresh"), nil, nil), IsElasticsearchError)
}

// Search performs the query specified in q on the values in index/type_ and returns a
// SearchResult.
func (db *Database) Search(index, type_ string, q QueryDSL) (SearchResult, error) {
	var sr SearchResult
	if err := db.get(db.url(index, type_, "_search"), q, &sr); err != nil {
		return SearchResult{}, errgo.NoteMask(err, "search failed", IsElasticsearchError)
	}
	return sr, nil
}

// do performs a request on the elasticsearch server. If body is not nil it will be
// marshaled as a json object and sent with the request. If v is non nil the response
// body will be unmarshalled into the value it points to.
func (db *Database) do(method, url string, body, v interface{}) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return errgo.Notef(err, "cannot marshaling body")
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return errgo.Notef(err, "cannot create request")
	}
	if body != nil {
		req.Header.Add("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errgo.Mask(err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errgo.Notef(err, "cannot read response")
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		if v != nil {
			return errgo.Mask(json.Unmarshal(b, &v))
		}
		return nil
	}
	// There was potentially an error.
	var errorBody errorBody
	err = json.Unmarshal(b, &errorBody)

	// In some circumstances Not Found will be returned when it isn't an error, handle that.
	if errorBody.Status == 0 && resp.StatusCode == http.StatusNotFound && v != nil {
		return errgo.Mask(json.Unmarshal(b, &v))
	}

	if err != nil {
		logger.Errorf("cannot unmarshal elasticsearch error: %s", err)
	}

	return &ElasticsearchError{
		Status: resp.StatusCode,
		body:   errorBody,
	}
}

// delete makes a DELETE request to the database url. A non-nil body will be
// sent with the request and if v is not nill then the response will be unmarshaled
// into tha value it points to.
func (db *Database) delete(url string, body, v interface{}) error {
	return db.do("DELETE", url, body, v)
}

// get makes a GET request to the database url. A non-nil body will be
// sent with the request and if v is not nill then the response will be unmarshaled
// into tha value it points to.
func (db *Database) get(url string, body, v interface{}) error {
	return db.do("GET", url, body, v)
}

// post makes a POST request to the database url. A non-nil body will be
// sent with the request and if v is not nill then the response will be unmarshaled
// into tha value it points to.
func (db *Database) post(url string, body, v interface{}) error {
	return db.do("POST", url, body, v)
}

// put makes a PUT request to the database url. A non-nil body will be
// sent with the request and if v is not nill then the response will be unmarshaled
// into tha value it points to.
func (db *Database) put(url string, body, v interface{}) error {
	return db.do("PUT", url, body, v)
}

// url constructs the URL for accessing the database.
func (db *Database) url(pathParts ...string) string {
	path := path.Join(pathParts...)
	url := &url.URL{
		Scheme: "http",
		Host:   db.Addr,
		Path:   path,
	}
	return url.String()

}

// SearchResult is the result returned after performing a search in elasticsearch
type SearchResult struct {
	Hits struct {
		Total    int     `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []Hit   `json:"hits"`
	} `json:"hits"`
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
}

// Hit represents an individual search hit returned from elasticsearch
type Hit struct {
	Index  string          `json:"_index"`
	Type   string          `json:"_type"`
	ID     string          `json:"_id"`
	Score  float64         `json:"_score"`
	Source json.RawMessage `json:"_source"`
	Fields Fields          `json:"fields"`
}

type Fields map[string][]interface{}

// Get retrieves the first value of key in the fields map. If no such value
// exists then it will return nil.
func (f Fields) Get(key string) interface{} {
	if len(f[key]) < 1 {
		return nil
	}
	return f[key][0]
}

// Get retrieves the first value of key in the fields map, and coerces it into a
// string. If no such value exists or the value is not a string, then "" will be returned.
func (f Fields) GetString(key string) string {
	s, ok := f.Get(key).(string)
	if !ok {
		return ""
	}
	return s
}

// EscapeRegexp returns the supplied string with any special characters escaped.
// A regular expression match on the returned string will match exactly the characters
// in the supplied string.
func EscapeRegexp(s string) string {
	return regexpReplacer.Replace(s)
}

var regexpReplacer = strings.NewReplacer(
	`.`, `\.`,
	`?`, `\?`,
	`+`, `\+`,
	`*`, `\*`,
	`|`, `\|`,
	`{`, `\{`,
	`}`, `\}`,
	`[`, `\[`,
	`]`, `\]`,
	`(`, `\(`,
	`)`, `\)`,
	`"`, `\"`,
	`\`, `\\`,
	`#`, `\#`,
	`@`, `\@`,
	`&`, `\&`,
	`<`, `\<`,
	`>`, `\>`,
	`~`, `\~`,
)

// alias describes an alias in elasticsearch.
type alias struct {
	Index string `json:"index"`
	Alias string `json:"alias"`
}

// action is an action that can be performed on an alias
type action struct {
	Remove *alias `json:"remove,omitempty"`
	Add    *alias `json:"add,omitempty"`
}
