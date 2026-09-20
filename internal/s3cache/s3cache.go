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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config wires the proxy. Upstream is the S3 origin base URL
// (scheme://host[:port], no bucket path).
type Config struct {
	Upstream   *url.URL
	CacheDir   string
	MaxBytes   int64
	SliceBytes int64
	// Fetchers bounds concurrent upstream slice fetches; slices within
	// one request are fetched in parallel up to this limit.
	Fetchers int
	// ReadAhead prefetches this many subsequent slices after a served
	// range, off the serving path. 0 disables.
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
		out.SliceBytes = 1 << 20
	}
	if out.Fetchers <= 0 {
		out.Fetchers = 8
	}
	if out.Timeout <= 0 {
		out.Timeout = 60 * time.Second
	}
	if out.Client == nil {
		out.Client = &http.Client{Timeout: out.Timeout}
	}
	return out
}

// entry tracks one cached slice file.
type entry struct {
	key  string
	path string
	size int64
}

// Proxy serves S3 GETs from disk slices, passing everything else through.
type Proxy struct {
	cfg    Config
	mux    *http.ServeMux
	signer *signer

	mu    sync.Mutex
	lru   *list.List
	index map[string]*list.Element
	used  int64

	flightsMu sync.Mutex
	flights   map[string]*flight
	sem       chan struct{}
	prefetch  chan struct{}

	totalsMu sync.Mutex
	totals   map[string]int64 // object key -> length from Content-Range

	metricsMu sync.Mutex
	hits      uint64
	hitBytes  uint64
	misses    uint64 // upstream slice fetches
	missBytes uint64
	evictions uint64
	originErr uint64
}

type flight struct {
	done chan struct{}
	err  error
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
		cfg:     cfg,
		mux:     http.NewServeMux(),
		signer:  newSigner(cfg.KeyID, cfg.Secret, cfg.Region),
		lru:     list.New(),
		index:   map[string]*list.Element{},
		flights: map[string]*flight{},
		totals:  map[string]int64{},
		sem:     make(chan struct{}, cfg.Fetchers),
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
	if r.Method != http.MethodGet {
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

// parseRange handles bytes=start-end, bytes=start- and bytes=-suffix.
func parseRange(header string, length int64) (int64, int64, error) {
	if header == "" {
		if length < 0 {
			return 0, -1, fmt.Errorf("unknown length")
		}
		return 0, length - 1, nil
	}
	rest, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("unsupported range unit")
	}
	// Only single ranges; multipart falls back to full-body 200.
	if strings.Contains(rest, ",") {
		return 0, 0, fmt.Errorf("multipart ranges unsupported")
	}
	lo, hi, _ := strings.Cut(rest, "-")
	if lo == "" {
		suf, err := strconv.ParseInt(hi, 10, 64)
		if err != nil || suf <= 0 || length < 0 {
			return 0, 0, fmt.Errorf("bad suffix range")
		}
		if suf > length {
			suf = length
		}
		return length - suf, length - 1, nil
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, fmt.Errorf("bad range start")
	}
	if hi == "" {
		if length < 0 {
			return start, -1, nil
		}
		if start >= length {
			return 0, 0, fmt.Errorf("unsatisfiable")
		}
		return start, length - 1, nil
	}
	end, err := strconv.ParseInt(hi, 10, 64)
	if err != nil || end < start {
		return 0, 0, fmt.Errorf("bad range end")
	}
	if length >= 0 {
		if start >= length {
			return 0, 0, fmt.Errorf("unsatisfiable")
		}
		if end >= length {
			end = length - 1
		}
	}
	return start, end, nil
}

// splitRange structurally parses a Range header without validating
// against a length: returns lo, hi (hi=-1 when open-ended/absent),
// multipart flag, and syntax errors.
func splitRange(header string) (int64, int64, bool, error) {
	if header == "" {
		return 0, -1, false, nil
	}
	rest, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, false, fmt.Errorf("unsupported range unit")
	}
	if strings.Contains(rest, ",") {
		return 0, 0, true, nil
	}
	lo, hi, _ := strings.Cut(rest, "-")
	if lo == "" {
		return 0, -1, false, nil // suffix; needs length
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, fmt.Errorf("bad range start")
	}
	if hi == "" {
		return start, -1, false, nil
	}
	end, err := strconv.ParseInt(hi, 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("bad range end")
	}
	return start, end, false, nil
}

// get returns an open file for a cached slice (hit path is TOCTOU-safe:
// the fd pins the bytes even if eviction unlinks the path).
func (p *Proxy) get(key string) (*os.File, int64, bool) {
	p.mu.Lock()
	el, ok := p.index[key]
	if !ok {
		p.mu.Unlock()
		return nil, 0, false
	}
	p.lru.MoveToFront(el)
	ent := el.Value.(*entry)
	f, err := os.Open(ent.path)
	p.mu.Unlock()
	if err != nil {
		return nil, 0, false
	}
	return f, ent.size, true
}

func (p *Proxy) insert(key, path string, size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if el, ok := p.index[key]; ok {
		p.lru.MoveToFront(el)
		return
	}
	p.index[key] = p.lru.PushFront(&entry{key: key, path: path, size: size})
	p.used += size
	for p.used > p.cfg.MaxBytes {
		back := p.lru.Back()
		if back == nil {
			break
		}
		ent := back.Value.(*entry)
		delete(p.index, ent.key)
		p.lru.Remove(back)
		p.used -= ent.size
		p.metricsMu.Lock()
		p.evictions++
		p.metricsMu.Unlock()
		_ = os.Remove(ent.path)
	}
}

// fetchSlice downloads one aligned slice; concurrent fetchers collapse
// onto a single upstream GET via per-slice singleflight.
func (p *Proxy) fetchSlice(ctx context.Context, obj, path string, i int64) error {
	key := sliceKey(obj, i)
	if f, _, ok := p.get(key); ok {
		f.Close()
		return nil
	}
	p.flightsMu.Lock()
	fl, ok := p.flights[key]
	if !ok {
		fl = &flight{done: make(chan struct{})}
		p.flights[key] = fl
		go func() {
			defer close(fl.done)
			fl.err = p.downloadSlice(ctx, obj, path, i)
			p.flightsMu.Lock()
			delete(p.flights, key)
			p.flightsMu.Unlock()
		}()
	}
	p.flightsMu.Unlock()
	<-fl.done
	return fl.err
}

func (p *Proxy) downloadSlice(ctx context.Context, obj, path string, i int64) error {
	key := sliceKey(obj, i)
	if f, _, ok := p.get(key); ok {
		f.Close()
		return nil
	}
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	lo := i * p.cfg.SliceBytes
	hi := lo + p.cfg.SliceBytes - 1
	target := *p.cfg.Upstream
	target.Path = singleJoin(target.Path, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", lo, hi))
	p.signer.sign(req)
	resp, err := p.cfg.Client.Do(req)
	if err != nil {
		p.bumpOriginErr()
		return err
	}
	defer resp.Body.Close()
	var body io.Reader = resp.Body
	var size, total int64 = -1, -1
	switch resp.StatusCode {
	case http.StatusPartialContent:
		size = p.cfg.SliceBytes
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			if start, end, n, err := parseContentRange(cr); err == nil {
				total = n
				if n >= 0 && end-lo+1 < size {
					size = end - lo + 1
				}
				_ = start
			}
		}
	case http.StatusOK:
		// Upstream ignored Range (small object): only usable for slice 0.
		if i != 0 {
			p.bumpOriginErr()
			return fmt.Errorf("upstream 200 for slice %d", i)
		}
		if n, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64); err == nil && n >= 0 {
			total = n
			if n < size {
				size = n
			}
		}
	default:
		p.bumpOriginErr()
		return fmt.Errorf("upstream status %d", resp.StatusCode)
	}
	dest := slicePath(p.cfg.CacheDir, key)
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	written, err := io.Copy(tmp, io.LimitReader(body, p.cfg.SliceBytes))
	tmp.Close()
	if err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if size < 0 || written < size {
		size = written
	}
	if err := os.Rename(tmpName, dest); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	p.insert(key, dest, size)
	if total >= 0 {
		p.rememberTotal(obj, total)
	}
	p.metricsMu.Lock()
	p.misses++
	p.missBytes += uint64(size)
	p.metricsMu.Unlock()
	return nil
}

func singleJoin(base, p string) string {
	return strings.TrimSuffix(base, "/") + "/" + strings.TrimPrefix(p, "/")
}

func parseContentRange(cr string) (int64, int64, int64, error) {
	// bytes lo-hi/total
	rest, ok := strings.CutPrefix(cr, "bytes ")
	if !ok {
		return 0, 0, -1, fmt.Errorf("bad content-range")
	}
	bounds, total, _ := strings.Cut(rest, "/")
	lo, hi, _ := strings.Cut(bounds, "-")
	start, err1 := strconv.ParseInt(lo, 10, 64)
	end, err2 := strconv.ParseInt(hi, 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, -1, fmt.Errorf("bad content-range bounds")
	}
	if total == "*" {
		return start, end, -1, nil
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return 0, 0, -1, fmt.Errorf("bad content-range total")
	}
	return start, end, n, nil
}

func (p *Proxy) bumpOriginErr() {
	p.metricsMu.Lock()
	p.originErr++
	p.metricsMu.Unlock()
}

// rememberTotal caches object length learned from Content-Range. Bounded:
// a full-lake scan through the proxy must not grow it without limit.
// Persisted best-effort so a restarted proxy keeps serving from disk
// without a single origin trip.
func (p *Proxy) rememberTotal(obj string, length int64) {
	p.totalsMu.Lock()
	if p.totals[obj] == length {
		p.totalsMu.Unlock()
		return
	}
	if len(p.totals) >= 1<<16 {
		p.totals = map[string]int64{obj: length}
	} else {
		p.totals[obj] = length
	}
	p.totalsMu.Unlock()
	if f, err := os.OpenFile(p.objectsLogPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		fmt.Fprintf(f, "%s %d\n", obj, length)
		f.Close()
	}
}

func (p *Proxy) objectsLogPath() string { return filepath.Join(p.cfg.CacheDir, "objects.log") }

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

// recover re-admits persisted slices and object lengths after a proxy
// restart. Slice files are content-keyed but the key is an opaque hash,
// so object lengths ride in objects.log; without them serve() cannot
// validate ranges without an origin HEAD/GET.
func (p *Proxy) recover() {
	_ = filepath.Walk(p.cfg.CacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		// Layout: CacheDir/<key[:2]>/<key[2:]>/<index>.slice — slice keys
		// contain a slash (obj/index), which filepath.Join nests.
		idx, ok := strings.CutSuffix(info.Name(), ".slice")
		if !ok || idx == "" {
			return nil // tmp files, logs
		}
		parent := filepath.Base(filepath.Dir(path))
		grand := filepath.Base(filepath.Dir(filepath.Dir(path)))
		if len(grand) != 2 || !isDigits(idx) {
			return nil
		}
		p.insert(grand+parent+"/"+idx, path, info.Size())
		return nil
	})
	data, err := os.ReadFile(p.objectsLogPath())
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > 1<<16 { // keep the most recent entries only
		lines = lines[len(lines)-1<<16:]
	}
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || n < 0 {
			continue
		}
		p.totals[fields[0]] = n
	}
}

func (p *Proxy) cachedTotal(obj string) (int64, bool) {
	p.totalsMu.Lock()
	defer p.totalsMu.Unlock()
	n, ok := p.totals[obj]
	return n, ok
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request) {
	// Read-only cache: writers go direct to S3. Fail closed.
	switch r.Method {
	case http.MethodGet:
	case http.MethodHead:
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
	if err != nil {
		if err.Error() == "multipart ranges unsupported" {
			p.serveFull(w, r, obj, path, length)
			return
		}
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

// ensureSlices fetches [start,end]'s slices in parallel, bounded by the
// global FETCHERS semaphore; the first error cancels the rest.
func (p *Proxy) ensureSlices(ctx context.Context, obj, path string, start, end int64) error {
	sz := p.cfg.SliceBytes
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		work     = make(chan int64)
		errOnce  sync.Once
		firstErr error
	)
	for w := 0; w < p.cfg.Fetchers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				if err := p.fetchSlice(ctx, obj, path, i); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
				}
			}
		}()
	}
	for i := start / sz; i <= end/sz; i++ {
		select {
		case work <- i:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
	return firstErr
}

func (p *Proxy) serveFull(w http.ResponseWriter, r *http.Request, obj, path string, length int64) {
	if length == 0 {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "0")
		w.Header().Set("Content-Type", "application/octet-stream")
		return
	}
	if err := p.ensureSlices(r.Context(), obj, path, 0, length-1); err != nil {
		p.passthrough(w, r)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := p.copySlices(w, r, obj, path, 0, length-1); err != nil {
		return
	}
	p.metricsMu.Lock()
	p.hits++
	p.hitBytes += uint64(length)
	p.metricsMu.Unlock()
	go p.readAhead(obj, path, length-1)
}

func (p *Proxy) serveRange(w http.ResponseWriter, r *http.Request, obj, path string, length, start, end int64) {
	if err := p.ensureSlices(r.Context(), obj, path, start, end); err != nil {
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
	p.metricsMu.Lock()
	p.hits++
	p.hitBytes += uint64(end - start + 1)
	p.metricsMu.Unlock()
	go p.readAhead(obj, path, end)
}

// readAhead prefetches the next slices of an object after a served
// range, off the serving path and bounded by the READAHEAD lane.
// Parquet reads are largely sequential within a file, so this converts
// serial row-group latency into pipelined fetches.
func (p *Proxy) readAhead(obj, path string, end int64) {
	if p.prefetch == nil {
		return
	}
	sz := p.cfg.SliceBytes
	length, ok := p.cachedTotal(obj)
	if !ok {
		return
	}
	for n := int64(1); n <= int64(p.cfg.ReadAhead); n++ {
		i := end/sz + n
		if i*sz >= length {
			return
		}
		select {
		case p.prefetch <- struct{}{}:
			go func(i int64) {
				defer func() { <-p.prefetch }()
				ctx, cancel := context.WithTimeout(context.Background(), p.cfg.Timeout)
				defer cancel()
				_ = p.fetchSlice(ctx, obj, path, i)
			}(i)
		default:
			return // prefetch lane busy; never block serving
		}
	}
}

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
			_, err = io.CopyN(w, f, hi-lo+1)
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
	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()
	p.mu.Lock()
	used := p.used
	p.mu.Unlock()
	fmt.Fprintln(w, "# TYPE s3cache_hits_total counter\n# TYPE s3cache_hit_bytes_total counter\n# TYPE s3cache_origin_fetches_total counter\n# TYPE s3cache_origin_bytes_total counter\n# TYPE s3cache_evictions_total counter\n# TYPE s3cache_origin_errors_total counter\n# TYPE s3cache_disk_used_bytes gauge")
	fmt.Fprintf(w, "s3cache_hits_total %d\ns3cache_hit_bytes_total %d\ns3cache_origin_fetches_total %d\ns3cache_origin_bytes_total %d\ns3cache_evictions_total %d\ns3cache_origin_errors_total %d\ns3cache_disk_used_bytes %d\n",
		p.hits, p.hitBytes, p.misses, p.missBytes, p.evictions, p.originErr, used)
}
