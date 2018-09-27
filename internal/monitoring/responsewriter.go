package monitoring

import "net/http"

// ResponseWriter wraps the http.ResponseWriter to give us the ability to
// retrieve the HTTP status code at any point after the response has been written.
//
// Note: This is does not implement any of the optional methods implemented by
// ResponseWriters. (i.e. CloseNotifier, Flusher, Hijacker)
type ResponseWriter struct {
	http.ResponseWriter
	status int
}

// NewResponseWriter creates a new *ResponseWriter from a provided http.ResponseWriter
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{ResponseWriter: w}
}

// WriteHeader records the HTTP status before calling WriteHeader on the underlying
// http.ResponseWriter.
func (w *ResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Write records the HTTP status before calling Write on the underlying
// http.ResponseWriter. If the status has not been set by this point, it is
// set to 200 OK (something the underlying http.ResponseWriter will do anyway).
func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Status returns the status of the ResponseWriter. It will be 0 if not set yet.
func (w *ResponseWriter) Status() int {
	return w.status
}
