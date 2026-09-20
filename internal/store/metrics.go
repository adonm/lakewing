package store

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
)

// responseCounts buckets OGC responses by status for Prometheus.
type responseCounts struct {
	ok200, ok204, ok304    atomic.Uint64
	err400, err404, err406 atomic.Uint64
	err429, err500         atomic.Uint64
}

// Metrics mirrors /metrics without consuming a pool connection, in
// Prometheus text exposition (valid numbers only).
func (s *Store) Metrics() string {
	var b strings.Builder
	b.WriteString("# HELP lakewing_http_responses_total OGC responses served by status.\n# TYPE lakewing_http_responses_total counter\n")
	for _, kv := range [][2]string{
		{"200", strconv.FormatUint(s.responses.ok200.Load(), 10)},
		{"204", strconv.FormatUint(s.responses.ok204.Load(), 10)},
		{"304", strconv.FormatUint(s.responses.ok304.Load(), 10)},
		{"400", strconv.FormatUint(s.responses.err400.Load(), 10)},
		{"404", strconv.FormatUint(s.responses.err404.Load(), 10)},
		{"406", strconv.FormatUint(s.responses.err406.Load(), 10)},
		{"429", strconv.FormatUint(s.responses.err429.Load(), 10)},
		{"500", strconv.FormatUint(s.responses.err500.Load(), 10)},
	} {
		fmt.Fprintf(&b, "lakewing_http_responses_total{status=%q} %s\n", kv[0], kv[1])
	}
	b.WriteString("# HELP lakewing_http_requests_total OGC request attempts.\n# TYPE lakewing_http_requests_total counter\n")
	fmt.Fprintf(&b, "lakewing_http_requests_total %d\n", s.httpRequests.Load())
	threads, memory := "", ""
	for _, kv := range s.tuning {
		switch kv[0] {
		case "threads":
			threads = kv[1]
		case "memory_limit":
			memory = kv[1]
		}
	}
	b.WriteString("# HELP lakewing_duckdb_threads Shared DuckDB threads.\n# TYPE lakewing_duckdb_threads gauge\n")
	fmt.Fprintf(&b, "lakewing_duckdb_threads %s\n", numericOrZero(threads))
	b.WriteString("# HELP lakewing_duckdb_memory_limit_bytes Shared DuckDB memory budget.\n# TYPE lakewing_duckdb_memory_limit_bytes gauge\n")
	fmt.Fprintf(&b, "lakewing_duckdb_memory_limit_bytes %d\n", parseBytes(memory))
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	b.WriteString("# HELP lakewing_go_goroutines Live goroutines.\n# TYPE lakewing_go_goroutines gauge\n")
	fmt.Fprintf(&b, "lakewing_go_goroutines %d\n", runtime.NumGoroutine())
	b.WriteString("# HELP lakewing_go_heap_bytes Go heap in use.\n# TYPE lakewing_go_heap_bytes gauge\n")
	fmt.Fprintf(&b, "lakewing_go_heap_bytes %d\n", mem.HeapInuse)
	return b.String()
}

// numericOrZero passes through plain integers, else 0 (Prometheus needs
// bare numbers).
func numericOrZero(s string) string {
	s = strings.TrimSpace(s)
	if _, err := strconv.Atoi(s); err != nil {
		return "0"
	}
	return s
}

// parseBytes parses DuckDB byte quantities ("1.0 GiB", "512MiB", "4096").
func parseBytes(s string) uint64 {
	s = strings.TrimSpace(s)
	mult := 1.0
	// Longest suffixes first so "GiB" wins over "B" (map order is random).
	for _, suf := range []struct {
		suffix string
		mult   float64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1},
	} {
		if v, ok := strings.CutSuffix(s, suf.suffix); ok {
			s, mult = strings.TrimSpace(v), suf.mult
			break
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return uint64(f * mult)
}

func (s *Store) CountRequest() { s.httpRequests.Add(1) }

// CountResponse buckets one served OGC response by status.
func (s *Store) CountResponse(status int) {
	switch status {
	case 200:
		s.responses.ok200.Add(1)
	case 204:
		s.responses.ok204.Add(1)
	case 304:
		s.responses.ok304.Add(1)
	case 400:
		s.responses.err400.Add(1)
	case 404:
		s.responses.err404.Add(1)
	case 406:
		s.responses.err406.Add(1)
	case 429:
		s.responses.err429.Add(1)
	default:
		s.responses.err500.Add(1)
	}
}
