package s3cache

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// origin is a minimal S3-ish upstream: HEAD lengths, ranged 206s, auth log.
type origin struct {
	data   []byte
	gets   atomic.Int64
	heads  atomic.Int64
	ranges []string
	mu     sync.Mutex
	auths  map[string]int
}

func newOrigin(size int) *origin {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return &origin{data: data, auths: map[string]int{}}
}

func (o *origin) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		o.auths[r.Header.Get("Authorization")]++
		o.mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			o.heads.Add(1)
			w.Header().Set("Content-Length", strconv.Itoa(len(o.data)))
			w.WriteHeader(http.StatusOK)
			return
		case http.MethodGet:
			q, _ := url.ParseQuery(r.URL.RawQuery)
			q.Del("x-id")
			if len(q) > 0 {
				// LIST-style: tiny body, passthrough path.
				w.Header().Set("Content-Length", "11")
				io.WriteString(w, "list-result")
				return
			}
			o.gets.Add(1)
			rg := r.Header.Get("Range")
			o.mu.Lock()
			o.ranges = append(o.ranges, rg)
			o.mu.Unlock()
			if rg == "" {
				w.Header().Set("Content-Length", strconv.Itoa(len(o.data)))
				w.Write(o.data)
				return
			}
			var lo, hi int
			fmt.Sscanf(rg, "bytes=%d-%d", &lo, &hi)
			if hi >= len(o.data) {
				hi = len(o.data) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi, len(o.data)))
			w.Header().Set("Content-Length", strconv.Itoa(hi-lo+1))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(o.data[lo : hi+1])
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func testProxy(t *testing.T, o *origin, maxBytes int64) (*Proxy, string) {
	t.Helper()
	srv := httptest.NewServer(o.handler())
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	proxy, err := New(Config{
		Upstream:   target,
		CacheDir:   t.TempDir(),
		MaxBytes:   maxBytes,
		SliceBytes: 1024,
		Fetchers:   8,
	})
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(proxy)
	t.Cleanup(front.Close)
	return proxy, front.URL
}

func get(t *testing.T, url, rg, auth string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if rg != "" {
		req.Header.Set("Range", rg)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func TestSliceReuse(t *testing.T) {
	o := newOrigin(3000)
	_, front := testProxy(t, o, 1<<20)
	want := o.data[100:2900]
	if code, body := get(t, front+"/b/f.parquet", "bytes=100-2899", "sig1"); code != 206 || string(body) != string(want) {
		t.Fatalf("first range: code=%d len=%d", code, len(body))
	}
	first := o.gets.Load()
	if code, body := get(t, front+"/b/f.parquet", "bytes=100-2899", "sig1"); code != 206 || string(body) != string(want) {
		t.Fatalf("repeat range: code=%d", code)
	}
	if o.gets.Load() != first {
		t.Fatalf("repeat fetched upstream: %d -> %d", first, o.gets.Load())
	}
}

func TestAuthExcludedFromKey(t *testing.T) {
	o := newOrigin(3000)
	_, front := testProxy(t, o, 1<<20)
	get(t, front+"/b/f.parquet", "bytes=0-99", "pod-a")
	before := o.gets.Load()
	get(t, front+"/b/f.parquet", "bytes=0-99", "pod-b")
	if o.gets.Load() != before {
		t.Fatal("different Authorization caused upstream refetch")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.auths) == 0 || o.auths["pod-a"] == 0 {
		t.Fatal("upstream never received client auth")
	}
}

func TestFullGetAssembledFromSlices(t *testing.T) {
	o := newOrigin(2500)
	_, front := testProxy(t, o, 1<<20)
	code, body := get(t, front+"/b/f.parquet", "", "sig")
	if code != 200 || string(body) != string(o.data) {
		t.Fatalf("full get: code=%d len=%d", code, len(body))
	}
}

func TestSingleflightCollapses(t *testing.T) {
	o := newOrigin(2048)
	_, front := testProxy(t, o, 1<<20)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body := get(t, front+"/b/f.parquet", "bytes=0-2047", "sig")
			if code != 206 || string(body) != string(o.data) {
				t.Errorf("concurrent range: code=%d", code)
			}
		}()
	}
	wg.Wait()
	if n := o.gets.Load(); n != 2 {
		t.Fatalf("expected 2 slice fetches, got %d", n)
	}
}

func TestEvictionBounded(t *testing.T) {
	o := newOrigin(4096)
	proxy, front := testProxy(t, o, 1500)
	get(t, front+"/b/a.parquet", "", "sig")
	get(t, front+"/b/b.parquet", "", "sig")
	var total int64
	filepath.Walk(proxy.cfg.CacheDir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if total > 1500+2048 {
		t.Fatalf("disk use %d exceeds bound + one in-flight object", total)
	}
}

func TestListPassthroughAndPutForbidden(t *testing.T) {
	o := newOrigin(100)
	_, front := testProxy(t, o, 1<<20)
	code, _ := get(t, front+"/b/?list-type=2", "", "sig")
	if code != 200 {
		t.Fatalf("LIST passthrough: %d", code)
	}
	put, _ := http.NewRequest(http.MethodPut, front+"/b/f.parquet", strings.NewReader("x"))
	resp, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT status=%d, want 403", resp.StatusCode)
	}
}

func TestUnsatisfiableRange(t *testing.T) {
	o := newOrigin(100)
	_, front := testProxy(t, o, 1<<20)
	code, _ := get(t, front+"/b/f.parquet", "bytes=200-300", "sig")
	if code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status=%d, want 416", code)
	}
}

// SDK telemetry query (?x-id=…) is stripped from the key but forwarded
// upstream, so rclone-style clients share slice entries.
func TestXIdTelemetryShared(t *testing.T) {
	o := newOrigin(3000)
	_, front := testProxy(t, o, 1<<20)
	code, body := get(t, front+"/b/f.parquet?x-id=GetObject", "bytes=0-2047", "sig")
	if code != 206 || string(body) != string(o.data[0:2048]) {
		t.Fatalf("telemetry GET: code=%d len=%d", code, len(body))
	}
	n := o.gets.Load()
	code, body = get(t, front+"/b/f.parquet?x-id=GetObject", "bytes=0-99", "sig")
	if code != 206 || string(body) != string(o.data[0:100]) {
		t.Fatalf("sub-range: code=%d", code)
	}
	if o.gets.Load() != n {
		t.Fatal("sub-range of cached slices refetched upstream")
	}
}

// TestSignedHeaderTransparency mirrors rclone: every signed header
// (including Accept-Encoding and Amz-Sdk-*) must reach the origin
// byte-identical or SigV4 verification fails.
func TestSignedHeaderTransparency(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		host := r.Host
		if host != "127.0.0.1:1" {
			t.Errorf("host=%q, want incoming host preserved", host)
		}
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	target, _ := url.Parse(srv.URL)
	proxy, err := New(Config{Upstream: target, CacheDir: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(proxy)
	defer front.Close()
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/b/?list-type=2", nil)
	req.Host = "127.0.0.1:1"
	for k, v := range map[string]string{
		"Authorization":         "AWS4-HMAC-SHA256 Credential=x",
		"Accept-Encoding":       "identity",
		"Amz-Sdk-Invocation-Id": "abc",
		"Amz-Sdk-Request":       "attempt=1",
		"X-Amz-Content-Sha256":  "e3b0",
		"X-Amz-Date":            "20260920T000000Z",
		"User-Agent":            "rclone/v1.74.3",
	} {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for k, v := range map[string]string{
		"Authorization":         "AWS4-HMAC-SHA256 Credential=x",
		"Accept-Encoding":       "identity",
		"Amz-Sdk-Invocation-Id": "abc",
		"Amz-Sdk-Request":       "attempt=1",
		"X-Amz-Content-Sha256":  "e3b0",
		"X-Amz-Date":            "20260920T000000Z",
		"User-Agent":            "rclone/v1.74.3",
	} {
		if got.Get(k) != v {
			t.Errorf("header %s=%q, want %q", k, got.Get(k), v)
		}
	}
}
