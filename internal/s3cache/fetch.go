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

// ensureSlices fetches [start,end]'s slices in parallel, bounded by
// FETCHERS; the first error cancels the rest (errgroup semantics). It
// reports whether every slice was already cached: callers prefetch only
// on full hits, so read-ahead never amplifies eviction churn on cold or
// oversized working sets.
func (p *Proxy) ensureSlices(ctx context.Context, obj, path string, start, end int64) (bool, error) {
	sz := p.cfg.SliceBytes
	var missing []int64
	for i := start / sz; i <= end/sz; i++ {
		if f, _, ok := p.get(sliceKey(obj, i)); ok {
			f.Close()
		} else {
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		return true, nil
	}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(p.cfg.Fetchers)
	for _, i := range missing {
		g.Go(func() error {
			return p.fetchSlice(ctx, obj, path, i)
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

func parseContentLength(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}
