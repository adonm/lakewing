package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestMeterPreservesSignedHostAndCountsActualBytes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "direct-s3.lake-bench.svc:8080" || r.Header.Get("Range") != "bytes=2-4" || r.Header.Get("Authorization") != "signature" {
			t.Error("proxy changed signed request")
		}
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusPartialContent)
		if r.Method != "HEAD" {
			io.WriteString(w, "abc")
		}
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	m := &meter{counts: map[string]counter{}}
	h := m.handler(target)
	for _, method := range []string{"GET", "HEAD"} {
		r := httptest.NewRequest(method, "http://direct-s3.lake-bench.svc:8080/lake/nw/data/file.parquet", nil)
		r.Header.Set("Range", "bytes=2-4")
		r.Header.Set("Authorization", "signature")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 206 {
			t.Fatalf("status %d", w.Code)
		}
	}
	if m.active != 0 || m.counts["direct-s3/GET/data/206"] != (counter{1, 3}) || m.counts["direct-s3/HEAD/data/206"] != (counter{1, 0}) {
		t.Fatalf("incorrect completed-transfer accounting: %+v", m)
	}
}
