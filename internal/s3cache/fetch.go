package s3cache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// parseContentLength parses a Content-Length header value.
func parseContentLength(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

// maxSpanSlices caps one coalesced upstream GET (span*SliceBytes <=
// maxSpanSlices*SliceBytes): long runs split into several span fetches
// so no single origin response exceeds a bounded size.
const maxSpanSlices = 16

// fetchSlice downloads one aligned slice; concurrent callers for the
// same slice collapse onto a single upstream GET via singleflight.
// The leader's context governs the download (waiters share its result).
func (p *Proxy) fetchSlice(ctx context.Context, obj, path string, i int64) error {
	key := sliceKey(obj, i)
	if f, _, ok := p.get(key); ok {
		f.Close()
		return nil
	}
	_, err, _ := p.fetch.Do(key, func() (any, error) {
		// Re-check under the flight: a prior flight may have just landed.
		if f, _, ok := p.get(key); ok {
			f.Close()
			return nil, nil
		}
		return nil, p.downloadSlice(ctx, obj, path, i)
	})
	return err
}

// fetchRun fetches a contiguous run of missing slices [first,last] as
// one upstream ranged GET, then splits the response into slice files.
// This is the request-count fix: a DuckDB range spanning k slices costs
// one origin GET instead of k (k RTT waves collapse into ~1).
// Concurrent identical runs (16 clients missing the same span) collapse
// onto one origin GET via singleflight, keyed by run bounds.
func (p *Proxy) fetchRun(ctx context.Context, obj, path string, first, last int64) error {
	runKey := sliceKey(obj, first) + "-" + strconv.FormatInt(last, 10)
	_, err, _ := p.fetch.Do(runKey, func() (any, error) {
		// Re-check under the flight: a prior flight may have landed.
		allCached := true
		for i := first; i <= last && allCached; i++ {
			if f, _, ok := p.get(sliceKey(obj, i)); ok {
				f.Close()
			} else {
				allCached = false
			}
		}
		if allCached {
			return nil, nil
		}
		return nil, p.downloadSpan(ctx, obj, path, first, last)
	})
	return err
}

func (p *Proxy) downloadSpan(ctx context.Context, obj, path string, first, last int64) error {
	lo := first * p.cfg.SliceBytes
	hi := last*p.cfg.SliceBytes + p.cfg.SliceBytes - 1
	target := *p.cfg.Upstream
	target.Path = singleJoin(target.Path, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", lo, hi))
	p.signer.sign(req)
	fetchStart := time.Now()
	resp, err := p.cfg.Client.Do(req)
	if err != nil {
		p.bumpOriginErr()
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusPartialContent {
		// Span path only handles well-behaved ranged origins; a 200
		// (origin ignored Range) falls back to per-slice fetches, which
		// already handle the small-object case.
		_ = resp.Body.Close()
		for i := first; i <= last; i++ {
			if err := p.fetchSlice(ctx, obj, path, i); err != nil {
				return err
			}
		}
		return nil
	}
	total := int64(-1)
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if _, _, n, err := parseContentRange(cr); err == nil {
			total = n
		}
	}

	// Stream the span to one temp file, then split by slice.
	spanBytes := (last - first + 1) * p.cfg.SliceBytes
	spanDir := filepath.Dir(slicePath(p.cfg.CacheDir, sliceKey(obj, first)))
	if err := os.MkdirAll(spanDir, 0755); err != nil {
		return err
	}
	span, err := os.CreateTemp(spanDir, ".span-*")
	if err != nil {
		return err
	}
	spanName := span.Name()
	written, err := io.Copy(span, io.LimitReader(resp.Body, spanBytes))
	span.Close()
	if err != nil {
		_ = os.Remove(spanName)
		return err
	}

	for i := first; i <= last; i++ {
		soff := (i - first) * p.cfg.SliceBytes
		size := p.cfg.SliceBytes
		if soff >= written {
			break // span shorter than requested (tail slice / small object)
		}
		if soff+size > written {
			size = written - soff
		}
		key := sliceKey(obj, i)
		dest := slicePath(p.cfg.CacheDir, key)
		tmp, err := os.CreateTemp(spanDir, ".tmp-*")
		if err != nil {
			_ = os.Remove(spanName)
			return err
		}
		tmpName := tmp.Name()
		sf, err := os.Open(spanName)
		if err != nil {
			_ = os.Remove(tmpName)
			_ = os.Remove(spanName)
			return err
		}
		_, copyErr := sf.Seek(soff, io.SeekStart)
		if copyErr == nil {
			_, copyErr = io.CopyN(tmp, sf, size)
		}
		_ = sf.Close()
		_ = tmp.Close()
		if copyErr != nil {
			_ = os.Remove(tmpName)
			_ = os.Remove(spanName)
			return copyErr
		}
		if err := os.Rename(tmpName, dest); err != nil {
			_ = os.Remove(tmpName)
			_ = os.Remove(spanName)
			return err
		}
		p.insert(key, dest, size)
	}
	_ = os.Remove(spanName)
	if total >= 0 {
		p.rememberTotal(obj, total)
	}
	p.mu.Lock()
	p.misses += uint64(last - first + 1)
	p.missBytes += uint64(written)
	p.fetchSecs += time.Since(fetchStart).Seconds()
	p.mu.Unlock()
	return nil
}

func (p *Proxy) downloadSlice(ctx context.Context, obj, path string, i int64) error {
	key := sliceKey(obj, i)
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
	fetchStart := time.Now()
	resp, err := p.cfg.Client.Do(req)
	if err != nil {
		p.bumpOriginErr()
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	var body io.Reader = resp.Body
	var size, total int64
	switch resp.StatusCode {
	case http.StatusPartialContent:
		size = p.cfg.SliceBytes
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			if _, end, n, err := parseContentRange(cr); err == nil {
				total = n
				if n >= 0 && end-lo+1 < size {
					size = end - lo + 1
				}
			}
		}
	case http.StatusOK:
		// Upstream ignored Range (small object): only usable for slice 0.
		if i != 0 {
			p.bumpOriginErr()
			return fmt.Errorf("upstream 200 for slice %d", i)
		}
		if n, err := parseContentLength(resp.Header.Get("Content-Length")); err == nil {
			total = n
			size = n
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
	_ = tmp.Close()
	if err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if written < size {
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
	p.mu.Lock()
	p.misses++
	p.missBytes += uint64(size)
	p.fetchSecs += time.Since(fetchStart).Seconds()
	p.mu.Unlock()
	return nil
}

// runsOf groups consecutive missing slice indices into runs, splitting
// runs longer than maxSpanSlices into bounded spans.
func runsOf(missing []int64) [][2]int64 {
	var runs [][2]int64
	for i := 0; i < len(missing); {
		j := i
		for j+1 < len(missing) && missing[j+1] == missing[j]+1 && missing[j+1]-missing[i] < maxSpanSlices {
			j++
		}
		runs = append(runs, [2]int64{missing[i], missing[j]})
		i = j + 1
	}
	return runs
}

// ensureSlices fetches the slice-index range [first,last], coalescing
// contiguous missing slices into single ranged upstream GETs (bounded by
// FETCHERS in-flight spans; the first error cancels the rest). It
// reports whether every slice was already cached: callers prefetch only
// on full hits, so read-ahead never amplifies eviction churn on cold or
// oversized working sets.
func (p *Proxy) ensureSlices(ctx context.Context, obj, path string, first, last int64) (bool, error) {
	var missing []int64
	for i := first; i <= last; i++ {
		if f, _, ok := p.get(sliceKey(obj, i)); ok {
			f.Close()
		} else {
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		return true, nil
	}
	runs := runsOf(missing)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(p.cfg.Fetchers)
	for _, run := range runs {
		g.Go(func() error {
			if run[0] == run[1] {
				return p.fetchSlice(ctx, obj, path, run[0])
			}
			return p.fetchRun(ctx, obj, path, run[0], run[1])
		})
	}
	return false, g.Wait()
}

// readAhead prefetches the next slices of an object after a fully
// cached range is served, off the serving path and bounded by the
// READAHEAD lane. Parquet reads are largely sequential within a file,
// so this converts serial row-group latency into pipelined fetches.
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

func (p *Proxy) bumpOriginErr() {
	p.mu.Lock()
	p.originErr++
	p.mu.Unlock()
}
