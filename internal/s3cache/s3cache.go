// Package s3cache is a node-local read-through S3 cache for DuckDB
// workers: fixed-size immutable slices shared by every pod on the node.
//
// Trust boundary: the proxy is the sole SigV4 signer. Workers may
// authenticate to it however the cluster likes (or not at all) — their
// Authorization never travels upstream and is never part of the cache
// key. The proxy holds the single S3 credential and signs everything it
// forwards, so client auth is one env var on the DaemonSet instead of
// credentials in every worker pod.
package s3cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Config wires the proxy. Upstream is the S3 origin base URL
// (scheme://host[:port], no bucket path).
type Config struct {
	Upstream   *url.URL
	CacheDir   string
	MaxBytes   int64
	SliceBytes int64
	// Fetchers bounds concurrent upstream slice fetches within one
	// request's ensureSlices fan-out. Sized for RTT-bound origins:
	// 8-way at 25 ms RTT caps at ~320 slices/s, while a FULL scan
	// needs ~1,700. Read-ahead lanes add up to READAHEAD more.
	Fetchers int
	// ReadAhead prefetches this many subsequent slices after a fully
	// cached range is served, off the serving path. 0 disables.
	ReadAhead int
	Timeout   time.Duration
	Client    *http.Client
	// KeyID/Secret/Region sign upstream requests. Empty KeyID means an
	// unsigned (anonymous) origin.
	KeyID  string
	Secret string
	Region string
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.SliceBytes <= 0 {
		out.SliceBytes = 4 << 20
	}
	if out.Fetchers <= 0 {
		out.Fetchers = 32
	}
	if out.Timeout <= 0 {
		out.Timeout = 60 * time.Second
	}
	if out.Client == nil {
		out.Client = &http.Client{Timeout: out.Timeout, Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
			// Byte-exact accounting below assumes the body arrives as
			// sent; S3 parquet is never content-encoded, and transparent
			// gzip would invalidate Content-Length/Content-Range math.
			DisableCompression: true,
		}}
	}
	return out
}

// Proxy serves S3 GETs from disk slices, passing everything else through.
type Proxy struct {
	cfg    Config
	mux    *http.ServeMux
	signer *signer

	// Lock discipline: mu is the ONLY index/metrics lock. A second lock
	// once deadlocked serveMetrics against insert's eviction path under
	// churn (ABBA); never reintroduce one around index or counters.
	// Nothing that can block (network, disk data I/O) runs under it:
	// eviction unlinks happen after unlock.
	mu        sync.Mutex
	lru       *list.List
	index     map[string]*list.Element
	used      int64
	hits      uint64
	hitBytes  uint64
	misses    uint64 // upstream slice fetches
	missBytes uint64
	fetchSecs float64 // total origin fetch latency (avg = fetchSecs/misses)
	evictions uint64
	originErr uint64

	// fetch collapses same-slice concurrent misses onto one origin GET.
	fetch singleflight.Group

	prefetch chan struct{}

	totalsMu sync.Mutex
	totals   map[string]int64 // object key -> length from Content-Range
}

// New validates config, prepares the cache dir and returns the handler.
func New(cfg Config) (*Proxy, error) {
	cfg = cfg.withDefaults()
	if cfg.Upstream == nil || (cfg.Upstream.Scheme != "http" && cfg.Upstream.Scheme != "https") {
		return nil, fmt.Errorf("upstream must be http(s)")
	}
	if cfg.MaxBytes <= 0 {
		return nil, fmt.Errorf("max bytes must be positive")
	}
	if err := os.MkdirAll(cfg.CacheDir, 0755); err != nil {
		return nil, err
	}
	p := &Proxy{
		cfg:    cfg,
		mux:    http.NewServeMux(),
		signer: newSigner(cfg.KeyID, cfg.Secret, cfg.Region),
		lru:    list.New(),
		index:  map[string]*list.Element{},
		totals: map[string]int64{},
	}
	if cfg.ReadAhead > 0 {
		p.prefetch = make(chan struct{}, cfg.ReadAhead)
	}
	p.recover()
	p.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	p.mux.HandleFunc("GET /metrics", p.serveMetrics)
	p.mux.HandleFunc("/", p.serve)
	return p, nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.mux.ServeHTTP(w, r) }

// objectKey identifies the S3 object. Only plain object GETs are sliced:
// LIST and other query-string operations pass through. The SDK telemetry
// param (?x-id=…) is stripped from the key but still forwarded upstream.
func objectKey(r *http.Request) (string, bool) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return "", false
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return "", false
	}
	q.Del("x-id")
	if len(q) > 0 {
		return "", false
	}
	sum := sha256.Sum256([]byte("GET\n" + r.URL.EscapedPath()))
	return hex.EncodeToString(sum[:]), true
}

func sliceKey(obj string, i int64) string { return obj + "/" + strconv.FormatInt(i, 10) }

func slicePath(dir, key string) string {
	return filepath.Join(dir, key[:2], key[2:]+".slice")
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request) {
	// Read-only cache: writers go direct to S3. Fail closed.
	switch r.Method {
	case http.MethodGet:
	case http.MethodHead:
		// Recurring HEADs (existence/size probes per file open) are
		// answered from learned lengths: zero RTT once touched.
		// Snapshots are immutable, so lengths never change.
		if obj, ok := objectKey(r); ok {
			if length, known := p.cachedTotal(obj); known && length >= 0 {
				w.Header().Set("Accept-Ranges", "bytes")
				w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
				w.Header().Set("Content-Type", "application/octet-stream")
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		p.passthrough(w, r)
		return
	default:
		http.Error(w, "read-only proxy", http.StatusForbidden)
		return
	}
	// LIST and other query-string operations are mutable/small: pass through.
	obj, ok := objectKey(r)
	if !ok {
		p.passthrough(w, r)
		return
	}
	path := r.URL.EscapedPath()
	ctx := r.Context()
	// Learn the object length from the first needed slice's Content-Range
	// instead of an upstream HEAD: fewer origin trips and no second
	// request shape to get wrong.
	probe := int64(0)
	if lo, hi, multipart, err := splitRange(r.Header.Get("Range")); err == nil && !multipart && hi >= 0 {
		probe = lo / p.cfg.SliceBytes
	}
	if err := p.fetchSlice(ctx, obj, path, probe); err != nil {
		p.passthrough(w, r)
		return
	}
	length, known := p.cachedTotal(obj)
	if !known || length < 0 {
		p.passthrough(w, r)
		return
	}
	start, end, err := parseRange(r.Header.Get("Range"), length)
	switch {
	case err == nil:
	case errors.Is(err, errMultipartRange):
		p.serveFull(w, r, obj, path, length)
		return
	default:
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", length))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if r.Header.Get("Range") == "" {
		p.serveFull(w, r, obj, path, length)
		return
	}
	p.serveRange(w, r, obj, path, length, start, end)
}

func (p *Proxy) serveFull(w http.ResponseWriter, r *http.Request, obj, path string, length int64) {
	if length == 0 {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "0")
		w.Header().Set("Content-Type", "application/octet-stream")
		return
	}
	allHit, err := p.ensureSlices(r.Context(), obj, path, 0, length-1)
	if err != nil {
		p.passthrough(w, r)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := p.copySlices(w, r, obj, path, 0, length-1); err != nil {
		return
	}
	p.countHit(length)
	if allHit {
		go p.readAhead(obj, path, length-1)
	}
}

func (p *Proxy) serveRange(w http.ResponseWriter, r *http.Request, obj, path string, length, start, end int64) {
	allHit, err := p.ensureSlices(r.Context(), obj, path, start, end)
	if err != nil {
		p.passthrough(w, r)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, length))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusPartialContent)
	if err := p.copySlices(w, r, obj, path, start, end); err != nil {
		return
	}
	p.countHit(end - start + 1)
	if allHit {
		go p.readAhead(obj, path, end)
	}
}

func (p *Proxy) countHit(n int64) {
	p.mu.Lock()
	p.hits++
	p.hitBytes += uint64(n)
	p.mu.Unlock()
}

// copyBufs pools 1 MiB transfer buffers so serving costs one disk read +
// one socket write per MiB instead of 32 KiB-chunked syscall pairs.
var copyBufs = sync.Pool{New: func() any {
	b := make([]byte, 1<<20)
	return &b
}}

// copySlices streams bytes [start,end] from slice files without
// materializing the range in memory. Slices evicted mid-serve (object
// larger than cache) are refetched on demand, so oversized objects stay
// correct at the cost of re-downloads.
func (p *Proxy) copySlices(w http.ResponseWriter, r *http.Request, obj, path string, start, end int64) error {
	ctx := r.Context()
	sz := p.cfg.SliceBytes
	for i := start / sz; i <= end/sz; i++ {
		f, _, ok := p.get(sliceKey(obj, i))
		if !ok {
			if err := p.fetchSlice(ctx, obj, path, i); err != nil {
				return err
			}
			f, _, ok = p.get(sliceKey(obj, i))
			if !ok {
				return fmt.Errorf("slice unavailable")
			}
		}
		lo, hi := int64(0), sz-1
		if i == start/sz {
			lo = start % sz
		}
		if i == end/sz {
			if fstat, err := f.Stat(); err == nil {
				if fstat.Size() < hi+1 {
					hi = fstat.Size() - 1
				}
			}
			if want := end % sz; want < hi {
				hi = want
			}
		}
		_, err := f.Seek(lo, io.SeekStart)
		if err == nil {
			buf := copyBufs.Get().(*[]byte)
			_, err = io.CopyBuffer(w, io.LimitReader(f, hi-lo+1), *buf)
			copyBufs.Put(buf)
		}
		f.Close()
		if err != nil {
			return err
		}
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
	}
	return nil
}

// passthrough forwards non-cacheable operations (HEAD, LIST) with the
// proxy's own signature; nothing from the client request is forwarded.
func (p *Proxy) passthrough(w http.ResponseWriter, r *http.Request) {
	target := *p.cfg.Upstream
	target.Path = singleJoin(target.Path, r.URL.EscapedPath())
	target.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), nil)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	p.signer.sign(req)
	resp, err := p.cfg.Client.Do(req)
	if err != nil {
		http.Error(w, "origin fetch failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for h, vs := range resp.Header {
		w.Header()[h] = vs
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) serveMetrics(w http.ResponseWriter, _ *http.Request) {
	// Snapshot under the lock, format after it: a slow scraper must
	// never hold the index lock.
	p.mu.Lock()
	hits, hitBytes := p.hits, p.hitBytes
	misses, missBytes, fetchSecs := p.misses, p.missBytes, p.fetchSecs
	evictions, originErr, used := p.evictions, p.originErr, p.used
	p.mu.Unlock()
	fmt.Fprintln(w, "# TYPE s3cache_hits_total counter\n# TYPE s3cache_hit_bytes_total counter\n# TYPE s3cache_origin_fetches_total counter\n# TYPE s3cache_origin_bytes_total counter\n# TYPE s3cache_origin_fetch_seconds_total counter\n# TYPE s3cache_evictions_total counter\n# TYPE s3cache_origin_errors_total counter\n# TYPE s3cache_disk_used_bytes gauge")
	fmt.Fprintf(w, "s3cache_hits_total %d\ns3cache_hit_bytes_total %d\ns3cache_origin_fetches_total %d\ns3cache_origin_bytes_total %d\ns3cache_origin_fetch_seconds_total %f\ns3cache_evictions_total %d\ns3cache_origin_errors_total %d\ns3cache_disk_used_bytes %d\n",
		hits, hitBytes, misses, missBytes, fetchSecs, evictions, originErr, used)
}
