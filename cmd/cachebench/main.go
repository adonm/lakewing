// cachebench runs fixed DuckLake workloads and meters their S3 traffic in kind.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adonm/lakewing/internal/dbutil"
	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/plan"
	_ "github.com/duckdb/duckdb-go/v2"
)

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func reply(w http.ResponseWriter, value any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		value = map[string]string{"error": err.Error()}
	}
	json.NewEncoder(w).Encode(value)
}

type counter struct {
	Requests uint64 `json:"requests"`
	Bytes    uint64 `json:"bytes"`
}

type meter struct {
	mu     sync.Mutex
	active int
	counts map[string]counter
}

type countedWriter struct {
	http.ResponseWriter
	status int
	bytes  uint64
}

func (w *countedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *countedWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *countedWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += uint64(n)
	return n, err
}

func (m *meter) handler(target *url.URL) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	// Keep the incoming Host: it is part of the client's SigV4 signature.
	proxy.Transport = &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 128}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		reply(w, map[string]any{"active": m.active, "counts": m.counts}, nil)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		fmt.Fprintln(w, "# TYPE lakewing_bench_s3_requests_total counter\n# TYPE lakewing_bench_s3_bytes_total counter")
		for key, count := range m.counts {
			parts := strings.Split(key, "/")
			labels := fmt.Sprintf("backend=%q,operation=%q,kind=%q,status=%q", parts[0], parts[1], parts[2], parts[3])
			fmt.Fprintf(w, "lakewing_bench_s3_requests_total{%s} %d\nlakewing_bench_s3_bytes_total{%s} %d\n", labels, count.Requests, labels, count.Bytes)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		backend := strings.Split(r.Host, ".")[0]
		switch backend {
		case "direct-s3", "mountpoint-s3", "rclone-s3", "geesefs-s3", "httpcache-s3":
		default:
			http.Error(w, "unknown benchmark endpoint", http.StatusBadRequest)
			return
		}
		op, kind := r.Method, "other"
		if r.URL.Query().Has("list-type") || r.URL.Query().Has("prefix") {
			op = "LIST"
		}
		if strings.Contains(r.URL.Path, "/catalogs/") {
			kind = "catalog"
		} else if strings.Contains(r.URL.Path, "/data/") {
			kind = "data"
		}
		m.mu.Lock()
		m.active++
		m.mu.Unlock()
		out := &countedWriter{ResponseWriter: w, status: 200}
		defer func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			key := fmt.Sprintf("%s/%s/%s/%d", backend, op, kind, out.status)
			count := m.counts[key]
			count.Requests++
			count.Bytes += out.bytes
			m.counts[key] = count
			m.active--
		}()
		proxy.ServeHTTP(out, r)
	})
	return mux
}

func queries() map[string]string {
	page := func(bounds string, limit int) string {
		where := "layer='buildings' AND source_id=1"
		if bounds != "" {
			b := strings.Split(bounds, ",")
			where += fmt.Sprintf(" AND xmax>=%s AND xmin<=%s AND ymax>=%s AND ymin<=%s AND ST_Intersects(geom,ST_MakeEnvelope(%s))", b[0], b[2], b[1], b[3], bounds)
		}
		return fmt.Sprintf("SELECT id,ST_AsGeoJSON(geom),properties::VARCHAR FROM (SELECT id,geom,properties FROM lake.features WHERE %s ORDER BY id LIMIT %d) p ORDER BY id", where, limit)
	}
	return map[string]string{
		"CITY":  page("4.895,52.365,4.905,52.375", 101),
		"BROAD": page("2,48,4,51", 101),
		"FULL":  page("", 1001),
		"DEEP":  plan.HeavyItemsSQL("lake.features", "layer='buildings' AND source_id=1", "ORDER BY id LIMIT 101 OFFSET 50000"),
		// Force all payloads to be read: count(*) alone can be metadata-only.
		"SCAN": "SELECT count(*)::VARCHAR,sum(octet_length(ST_AsWKB(geom)))::VARCHAR,sum(length(properties::VARCHAR))::VARCHAR FROM lake.features",
	}
}

type worker struct {
	mu      sync.Mutex
	db      *sql.DB
	conn    *sql.Conn
	seq     int
	samples []map[string]any
}

func sendTrace(sample map[string]any, start time.Time) error {
	endpoint := os.Getenv("OTLP_ENDPOINT")
	if endpoint == "" {
		return nil
	}
	trace, span := make([]byte, 16), make([]byte, 8)
	if _, err := rand.Read(trace); err != nil {
		return err
	}
	if _, err := rand.Read(span); err != nil {
		return err
	}
	attrs := []map[string]any{}
	for _, key := range []string{"backend", "pod", "phase", "query", "sha256"} {
		attrs = append(attrs, map[string]any{"key": key, "value": map[string]any{"stringValue": sample[key]}})
	}
	sample["trace_id"] = fmt.Sprintf("%x", trace)
	value := map[string]any{"resourceSpans": []any{map[string]any{
		"resource": map[string]any{"attributes": []any{map[string]any{"key": "service.name", "value": map[string]any{"stringValue": "duckdb-" + os.Getenv("BACKEND")}}}},
		"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "lakewing-cachebench"}, "spans": []any{map[string]any{
			"traceId": sample["trace_id"], "spanId": fmt.Sprintf("%x", span), "name": "DuckLake " + sample["query"].(string), "kind": 1,
			"startTimeUnixNano": strconv.FormatInt(start.UnixNano(), 10), "endTimeUnixNano": strconv.FormatInt(start.Add(time.Duration(sample["ms"].(float64)*float64(time.Millisecond))).UnixNano(), 10), "attributes": attrs,
		}}}},
	}}}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(endpoint+"/v1/traces", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("trace export: %s", response.Status)
	}
	return nil
}

func processIO() map[string]uint64 {
	result := map[string]uint64{}
	data, _ := os.ReadFile("/proc/self/io")
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			result[strings.TrimSuffix(fields[0], ":")] = value
		}
	}
	return result
}

func (s *worker) close() {
	if s.conn != nil {
		s.conn.Close()
		s.db.Close()
		s.conn, s.db = nil, nil
	}
}

func (s *worker) open(ctx context.Context) (map[string]any, error) {
	s.close()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	s.db, s.conn = db, conn
	threads, err := strconv.Atoi(env("THREADS", "4"))
	if err != nil || threads < 1 {
		return nil, fmt.Errorf("invalid THREADS")
	}
	settings := []string{"LOAD ducklake", "LOAD spatial", "LOAD httpfs", "SET threads=" + strconv.Itoa(threads), "SET memory_limit=" + filter.Quote(env("MEMORY_LIMIT", "1GB")), "SET temp_directory='/duckdb-temp'", "SET max_temp_directory_size='4GB'", "SET parquet_metadata_cache=true", "SET enable_http_metadata_cache=true", "SET enable_external_file_cache=true", "SET validate_external_file_cache='NO_VALIDATION'", "SET cache_local_files=false", "SET autoinstall_known_extensions=false", "SET autoload_known_extensions=false"}
	if err := dbutil.ExecAll(ctx, conn, settings...); err != nil {
		return nil, err
	}
	if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE SECRET s3bench (TYPE S3, KEY_ID %s, SECRET %s, ENDPOINT %s, URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')", filter.Quote(os.Getenv("AWS_ACCESS_KEY_ID")), filter.Quote(os.Getenv("AWS_SECRET_ACCESS_KEY")), filter.Quote(endpoint)))
		if err != nil {
			return nil, fmt.Errorf("S3 setup failed")
		}
	}
	start := time.Now()
	snapshot, err := strconv.Atoi(env("SNAPSHOT", "6"))
	if err != nil || snapshot < 0 {
		return nil, fmt.Errorf("invalid SNAPSHOT")
	}
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS lake (READ_ONLY, DATA_PATH %s, OVERRIDE_DATA_PATH true, SNAPSHOT_VERSION %d)", filter.Quote("ducklake:"+os.Getenv("CATALOG")), filter.Quote(os.Getenv("DATA_ROOT")), snapshot))
	if err != nil {
		return nil, err
	}
	attachMS := float64(time.Since(start).Microseconds()) / 1000
	if _, err := conn.ExecContext(ctx, "USE lake"); err != nil {
		return nil, err
	}
	meta, err := dbutil.QueryTable(ctx, conn, "SELECT version(),max(snapshot_id)::VARCHAR FROM snapshots()")
	return map[string]any{"attach_ms": attachMS, "catalog_ms": float64(time.Since(start).Microseconds()) / 1000, "metadata": meta, "snapshot": snapshot, "backend": os.Getenv("BACKEND"), "pod": os.Getenv("HOSTNAME"), "threads": threads, "memory_limit": env("MEMORY_LIMIT", "1GB")}, err
}

func (s *worker) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.Handle("GET /profiles/", http.StripPrefix("/profiles/", http.FileServer(http.Dir("/results"))))
	mux.HandleFunc("POST /open", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		value, err := s.open(r.Context())
		if err != nil {
			s.close()
		}
		reply(w, value, err)
	})
	mux.HandleFunc("POST /close", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.close()
		reply(w, map[string]bool{"closed": true}, nil)
	})
	mux.HandleFunc("POST /query", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		name, phase := r.URL.Query().Get("name"), r.URL.Query().Get("phase")
		query, ok := queries()[name]
		if !ok || s.conn == nil {
			http.Error(w, "open session and choose a fixed query", http.StatusBadRequest)
			return
		}
		s.seq++
		profile := fmt.Sprintf("%06d-%s.json", s.seq, name)
		if err := dbutil.ExecAll(r.Context(), s.conn, "SET enable_profiling='json'", "SET profiling_output="+filter.Quote(filepath.Join("/results", profile))); err != nil {
			reply(w, nil, err)
			return
		}
		beforeIO := processIO()
		start := time.Now()
		rows, err := dbutil.QueryTable(r.Context(), s.conn, query)
		elapsed := float64(time.Since(start).Microseconds()) / 1000
		ioDelta := processIO()
		for key := range ioDelta {
			ioDelta[key] -= beforeIO[key]
		}
		s.conn.ExecContext(r.Context(), "PRAGMA disable_profiling")
		if err == nil && len(rows) == 0 {
			err = fmt.Errorf("empty result for %s", name)
		}
		if err != nil {
			reply(w, nil, err)
			return
		}
		data, err := json.Marshal(rows)
		if err != nil {
			reply(w, nil, err)
			return
		}
		sample := map[string]any{"backend": os.Getenv("BACKEND"), "pod": os.Getenv("HOSTNAME"), "query": name, "phase": phase, "ms": elapsed, "rows": len(rows), "sha256": fmt.Sprintf("%x", sha256.Sum256(data)), "result_bytes": len(data), "profile": profile, "time": start.UTC().Format(time.RFC3339Nano)}
		sample["process_io"] = ioDelta
		if err := sendTrace(sample, start); err != nil {
			reply(w, nil, err)
			return
		}
		if name == "SCAN" {
			sample["values"] = rows
		}
		s.samples = append(s.samples, sample)
		logData, _ := json.Marshal(sample)
		log.Print(string(logData))
		reply(w, sample, nil)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		// Retain each observation until collection, including short query phases.
		for i, sample := range s.samples {
			fmt.Fprintf(w, "lakewing_bench_query_seconds{backend=%q,query=%q,phase=%q,sample=%q} %g\n", sample["backend"], sample["query"], sample["phase"], strconv.Itoa(i), sample["ms"].(float64)/1000)
		}
	})
	return mux
}

func main() {
	var handler http.Handler
	if len(os.Args) > 1 && os.Args[1] == "meter" {
		target, err := url.Parse(env("UPSTREAM", "http://seaweed:8333"))
		if err != nil {
			log.Fatal(err)
		}
		handler = (&meter{counts: map[string]counter{}}).handler(target)
	} else {
		if err := os.MkdirAll("/results", 0755); err != nil {
			log.Fatal(err)
		}
		handler = (&worker{}).handler()
	}
	server := &http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}
