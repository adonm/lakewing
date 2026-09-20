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
	"time"
)

// origin is a minimal S3-ish upstream: HEAD lengths, ranged 206s, LIST
// bodies, auth enforcement and (optional) per-GET latency with peak
// in-flight tracking.
type origin struct {
	data []byte
	// requireAuth: requests whose Authorization does not contain the
	// proxy credential fragment are rejected, like a real S3.
	requireAuth string

	gets     atomic.Int64
	heads    atomic.Int64
	inflight atomic.Int64
	peak     atomic.Int64
	latency  time.Duration

	mu     sync.Mutex
	ranges []string
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
		if o.requireAuth != "" && !strings.Contains(r.Header.Get("Authorization"), o.requireAuth) {
			http.Error(w, "bad signature", http.StatusForbidden)
			return
		}
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
			o.inflight.Add(1)
			if o.latency > 0 {
				time.Sleep(o.latency)
			}
			defer func() {
				n := o.inflight.Add(-1)
				for {
					p := o.peak.Load()
					if n <= p || o.peak.CompareAndSwap(p, n) {
						break
					}
				}
			}()
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

// testProxy wires an origin to a proxy; requireAuth makes the origin
// verify that upstream requests carry the proxy's signature.
func testProxy(t *testing.T, o *origin, maxBytes int64, extra func(*Config)) (*Proxy, string) {
	t.Helper()
	srv := httptest.NewServer(o.handler())
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	cfg := Config{
		Upstream:   target,
		CacheDir:   t.TempDir(),
		MaxBytes:   maxBytes,
		SliceBytes: 1024,
		Fetchers:   8,
		KeyID:      "proxykey",
		Secret:     "proxysecret",
		Region:     "us-east-1",
	}
	if extra != nil {
		extra(&cfg)
	}
	proxy, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(proxy)
	t.Cleanup(front.Close)
	o.requireAuth = "Credential=proxykey"
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
	_, front := testProxy(t, o, 1<<20, nil)
	want := o.data[100:2900]
	if code, body := get(t, front+"/b/f.parquet", "bytes=100-2899", "pod-a"); code != 206 || string(body) != string(want) {
		t.Fatalf("first range: code=%d len=%d", code, len(body))
	}
	first := o.gets.Load()
	// A different worker credential must still hit the same slices:
	// client auth is not part of the key and is never forwarded.
	if code, body := get(t, front+"/b/f.parquet", "bytes=100-2899", "pod-b"); code != 206 || string(body) != string(want) {
		t.Fatalf("repeat range: code=%d", code)
	}
	if o.gets.Load() != first {
		t.Fatalf("repeat fetched upstream: %d -> %d", first, o.gets.Load())
	}
}

// Workers may authenticate however they like (or not at all): only the
// proxy's credential ever reaches the origin, and every upstream request
// carries it — proving the proxy signs everything it forwards.
func TestProxyIsSoleSigner(t *testing.T) {
	o := newOrigin(3000)
	_, front := testProxy(t, o, 1<<20, nil)
	for _, auth := range []string{"", "evil", "AWS4-HMAC-SHA256 Credential=worker/..."} {
		if code, _ := get(t, front+"/b/f.parquet", "bytes=0-99", auth); code != 206 {
			t.Fatalf("auth %q: code=%d", auth, code)
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.auths) != 1 || o.auths["Credential=proxykey"] == 0 {
		// map keys are full Authorization values; verify no client auth leaked
		for k := range o.auths {
			if strings.Contains(k, "worker") || strings.Contains(k, "evil") {
				t.Fatalf("client auth leaked upstream: %q", k)
			}
			if !strings.Contains(k, "Credential=proxykey") {
				t.Fatalf("unexpected upstream auth: %q", k)
			}
		}
	}
}

func TestFullGetAssembledFromSlices(t *testing.T) {
	o := newOrigin(2500)
	_, front := testProxy(t, o, 1<<20, nil)
	code, body := get(t, front+"/b/f.parquet", "", "sig")
	if code != 200 || string(body) != string(o.data) {
		t.Fatalf("full get: code=%d len=%d", code, len(body))
	}
}

func TestSingleflightCollapses(t *testing.T) {
	o := newOrigin(2048)
	_, front := testProxy(t, o, 1<<20, nil)
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

// Slices within one request fetch in parallel: peak in-flight upstream
// GETs must exceed 1 for a multi-slice range behind a slow origin.
func TestParallelSliceFetch(t *testing.T) {
	o := newOrigin(8192)
	o.latency = 30 * time.Millisecond
	_, front := testProxy(t, o, 1<<20, func(c *Config) { c.Fetchers = 8 })
	start := time.Now()
	code, body := get(t, front+"/b/f.parquet", "bytes=0-8191", "sig")
	if code != 206 || string(body) != string(o.data) {
		t.Fatalf("parallel range: code=%d len=%d", code, len(body))
	}
	if elapsed := time.Since(start); elapsed > 8*o.latency {
		t.Fatalf("8 slices took %s; fetches look serial", elapsed)
	}
	if peak := o.peak.Load(); peak < 2 {
		t.Fatalf("peak in-flight=%d; slices fetched serially", peak)
	}
}

// Read-ahead: after serving a range, the next slices of the same object
// are prefetched off the serving path.
func TestReadAheadPrefetches(t *testing.T) {
	o := newOrigin(8192)
	proxy, front := testProxy(t, o, 1<<20, func(c *Config) { c.ReadAhead = 2 })
	if code, _ := get(t, front+"/b/f.parquet", "bytes=0-1023", "sig"); code != 206 {
		t.Fatal("range failed")
	}
	deadline := time.Now().Add(3 * time.Second)
	for o.gets.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := o.gets.Load(); n != 3 { // slice 0 (probe) + slices 1,2
		t.Fatalf("read-ahead did not prefetch: origin GETs=%d", n)
	}
	_ = proxy
}

func TestEvictionBounded(t *testing.T) {
	o := newOrigin(4096)
	proxy, front := testProxy(t, o, 1500, func(c *Config) { c.ReadAhead = 0 })
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
	_, front := testProxy(t, o, 1<<20, nil)
	// LIST must be signed by the proxy too: origin rejects unsigned.
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
	_, front := testProxy(t, o, 1<<20, func(c *Config) { c.ReadAhead = 0 })
	code, _ := get(t, front+"/b/f.parquet", "bytes=200-300", "sig")
	if code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status=%d, want 416", code)
	}
}

// SDK telemetry query (?x-id=…) is stripped from the key but forwarded
// upstream, so rclone-style clients share slice entries.
func TestXIdTelemetryShared(t *testing.T) {
	o := newOrigin(3000)
	_, front := testProxy(t, o, 1<<20, func(c *Config) { c.ReadAhead = 0 })
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
