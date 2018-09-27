package monitoring

// SetElasticSearchSyncing sets the charmstore_elastic_search_syncing gauge to
// 1 if inProgress, 0 otherwise (as per common practice for tracking
// "booleans" in ElasticSearch).
func SetElasticSearchSyncing(inProgress bool) {
	var f float64
	if inProgress {
		f = 1.0
	}
	esSyncing.Set(f)
}
