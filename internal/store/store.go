// Package store ports src/store.rs + src/db.rs: the shared DuckDB pool over
// a DuckLake snapshot served over S3_DIRECT (normally through the node-local
// s3cache proxy; see docs/s3cache.md).
//
// Storage model (lakewing): reads resolve to s3:// catalog + data URLs.
// Writes (build/index publish) go direct to S3 over the S3 API.
//
// Build with -tags=duckdb_use_lib against the pinned DuckDB 2.0 library
// (.deps/duckdb, v2.0.0-alpha42069): the preview binding's bundled engine
// is 1.5.x, which cannot open 2.0 catalogs.
package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/lakewing/internal/dbutil"
	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/index"
	"github.com/adonm/lakewing/internal/lance"
)

// StoreError mirrors store::Error.
type StoreError struct {
	Kind    string // invalid|notfound|overloaded|backend
	Message string
}

func (e *StoreError) Error() string { return e.Message }

func Invalid(msg string) *StoreError { return &StoreError{Kind: "invalid", Message: msg} }
func NotFound(msg string) *StoreError {
	return &StoreError{Kind: "notfound", Message: msg}
}
func Overloaded() *StoreError        { return &StoreError{Kind: "overloaded", Message: "server overloaded"} }
func Backend(msg string) *StoreError { return &StoreError{Kind: "backend", Message: msg} }

// META mirrors store::META: DuckLake metadata schema for the serving attach.
const META = "__ducklake_metadata_shard"

// ExpectedFeatureSchema mirrors EXPECTED_FEATURE_SCHEMA: catalogs whose
// features table differs stay on catalog reads.
var ExpectedFeatureSchema = [][2]string{
	{"id", "VARCHAR"}, {"layer", "VARCHAR"}, {"source_id", "BIGINT"},
	{"geom", "GEOMETRY"}, {"properties", "JSON"}, {"sortkey", "BIGINT"},
	{"xmin", "DOUBLE"}, {"ymin", "DOUBLE"}, {"xmax", "DOUBLE"}, {"ymax", "DOUBLE"},
	{"cx", "DOUBLE"}, {"cy", "DOUBLE"}, {"name", "VARCHAR"},
}

// ShardManifest mirrors store::ShardManifest.
type ShardManifest struct {
	Version       uint32      `json:"version"`
	Backend       string      `json:"backend"`
	SchemaVersion uint32      `json:"schema_version"`
	Source        string      `json:"source"`
	BBox          [4]float64  `json:"bbox"`
	Rows          int64       `json:"rows"`
	BuiltAt       string      `json:"built_at"`
	Layout        *LakeLayout `json:"layout,omitempty"`
}

// LakeLayout mirrors store::LakeLayout.
type LakeLayout struct {
	FileMB   uint64 `json:"file_mb"`
	RowGroup uint64 `json:"row_group"`
	Sort     string `json:"sort"`
}

// Config mirrors StoreConfig: s3:// catalog location plus DATA_PATH
// override for the data root.
type Config struct {
	Location     string // s3:// URL of the .ducklake catalog
	DataRoot     string // s3:// data root (DATA_PATH override); empty = stored path
	IndexJSON    *string
	Connections  int
	MaxWaiters   int
	MaxWait      time.Duration
	BulkLimit    int
	Threads      int64
	MemoryMB     uint64
	QueryTimeout time.Duration
	// TempDir for DuckDB spill files (SET temp_directory per connection).
	// Empty leaves the engine default (process TMPDIR). In kind/k8s this
	// points at the ephemeral emptyDir volume.
	TempDir string
	// Lance optionally adds an indexed Lance dataset as the feature source
	// for items/item lookups. The DuckLake catalog still pins the snapshot
	// and serves collections/tiles/flight (hybrid store).
	Lance *lance.Config
}

// ResolvedIndex is a trusted serving index over data file URLs.
type ResolvedIndex struct {
	Index index.ServingIndex
	URLs  []string
}

// Store is the shared read pool: N dedicated connections (session state
// persists per conn), a FIFO-fair semaphore with bounded queue (429s), and
// an optional bulk lane below the pool size.
type Store struct {
	Collections []string
	Snapshot    int64

	fallbackFrom string
	index        *ResolvedIndex
	lance        *lance.Source

	mu     sync.Mutex
	pool   []*sql.Conn
	sem    chan struct{}
	bulk   chan struct{}
	queued atomic.Int64

	maxWaiters   int64
	maxWait      time.Duration
	queryTimeout time.Duration

	db *sql.DB

	httpRequests atomic.Uint64 // total OGC attempts (items/item/tiles)
	responses    responseCounts
	tuning       [][2]string
	manifest     *ShardManifest
}

// CachedBody mirrors store::CachedBody: strong ETag over exact bytes.
type CachedBody struct {
	Bytes []byte
	ETag  string
}

func WithBytes(b []byte) CachedBody {
	h := fnv.New64a()
	h.Write(b)
	return CachedBody{Bytes: b, ETag: fmt.Sprintf(`"%x-%d"`, h.Sum64(), len(b))}
}

// GzipBody compresses on a worker (callers run this off the pool).
func GzipBody(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// CatalogURL mirrors store::catalog_url (exported for build/index).
func CatalogURL(location string) string { return catalogURL(location) }

// ResolvedSnapshot mirrors store::ResolvedFiles: DATA base + ordered
// DATA-relative paths at a pinned snapshot.
type ResolvedSnapshot struct {
	Base     string
	Relpaths []string
}

// SnapshotFiles resolves the live files at a snapshot for index builds,
// failing closed exactly like serving resolution.
func SnapshotFiles(ctx context.Context, c *sql.Conn, snapshot int64) (*ResolvedSnapshot, error) {
	r, err := resolveFiles(ctx, c, snapshot)
	if err != nil {
		return nil, err
	}
	return &ResolvedSnapshot{Base: r.base, Relpaths: r.relpaths}, nil
}

// catalogURL mirrors store::catalog_url.
func catalogURL(location string) string {
	return "ducklake:" + strings.TrimRight(location, "/")
}

func attachOptions(snapshot *int64, dataPathOverride *string) string {
	opts := []string{"READ_ONLY"}
	if dataPathOverride != nil {
		base := *dataPathOverride
		if !strings.HasSuffix(base, "/") {
			base += "/"
		}
		opts = append(opts, "DATA_PATH "+filter.Quote(base), "OVERRIDE_DATA_PATH true")
	}
	if snapshot != nil {
		opts = append(opts, fmt.Sprintf("SNAPSHOT_VERSION %d", *snapshot))
	}
	return strings.Join(opts, ", ")
}

func slash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

// fileURLs mirrors store::file_urls with a single data base: relative
// paths join the DataRoot override (or the stored base for local
// fixtures); absolute paths pass through.
func fileURLs(base string, relpaths []string) []string {
	out := make([]string, len(relpaths))
	for i, rel := range relpaths {
		if strings.Contains(rel, "://") || strings.HasPrefix(rel, "/") {
			out[i] = rel
		} else {
			out[i] = slash(base) + rel
		}
	}
	return out
}

// resolvedFiles mirrors store::ResolvedFiles: DATA base + ordered
// DATA-relative paths at the pinned snapshot, failing closed on schema
// mismatch, deletes, inlined data, or absolute segments.
type resolvedFiles struct {
	base     string
	relpaths []string
}

func resolveFiles(ctx context.Context, c *sql.Conn, snapshot int64) (*resolvedFiles, error) {
	fail := func(format string, args ...any) (*resolvedFiles, error) {
		return nil, fmt.Errorf(format, args...)
	}
	tid, err := dbutil.QueryInt(ctx, c,
		fmt.Sprintf("SELECT table_id FROM %s.ducklake_table WHERE table_name='features'", META))
	if err != nil {
		return fail("features table id: %v", err)
	}
	cols, err := dbutil.QueryTable(ctx, c, "SELECT column_name, column_type FROM (DESCRIBE shard.features)")
	if err != nil {
		return fail("features schema: %v", err)
	}
	if len(cols) != len(ExpectedFeatureSchema) {
		return fail("features schema differs from builder schema")
	}
	for i, want := range ExpectedFeatureSchema {
		got0, got1 := "", ""
		if cols[i][0] != nil {
			got0 = *cols[i][0]
		}
		if cols[i][1] != nil {
			got1 = *cols[i][1]
		}
		if got0 != want[0] || got1 != want[1] {
			return fail("features schema differs from builder schema")
		}
	}
	files, err := dbutil.QueryTable(ctx, c, fmt.Sprintf(
		"SELECT path, path_is_relative::VARCHAR FROM %s.ducklake_data_file WHERE table_id=%d AND begin_snapshot<=%d AND (end_snapshot IS NULL OR end_snapshot>%d) ORDER BY file_order",
		META, tid, snapshot, snapshot))
	if err != nil {
		return fail("file list: %v", err)
	}
	if len(files) == 0 {
		return fail("no live files at snapshot")
	}
	table, err := dbutil.QueryTable(ctx, c, fmt.Sprintf(
		"SELECT schema_id::VARCHAR, path, path_is_relative::VARCHAR FROM %s.ducklake_table WHERE table_id=%d AND begin_snapshot<=%d AND (end_snapshot IS NULL OR end_snapshot>%d)",
		META, tid, snapshot, snapshot))
	if err != nil {
		return fail("table entry: %v", err)
	}
	if len(table) != 1 || len(table[0]) != 3 {
		return fail("table entry is not unique at snapshot")
	}
	str := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	var schemaID int64
	if _, err := fmt.Sscanf(str(table[0][0]), "%d", &schemaID); err != nil {
		return fail("table schema id is not an integer")
	}
	tablePath, tableRel := str(table[0][1]), str(table[0][2])
	schema, err := dbutil.QueryTable(ctx, c, fmt.Sprintf(
		"SELECT path, path_is_relative::VARCHAR FROM %s.ducklake_schema WHERE schema_id=%d AND begin_snapshot<=%d AND (end_snapshot IS NULL OR end_snapshot>%d)",
		META, schemaID, snapshot, snapshot))
	if err != nil {
		return fail("schema entry: %v", err)
	}
	if len(schema) != 1 || len(schema[0]) != 2 {
		return fail("schema entry is not unique at snapshot")
	}
	schemaPath, schemaRel := str(schema[0][0]), str(schema[0][1])
	for _, seg := range []string{schemaPath, tablePath} {
		if strings.Contains(seg, "://") || strings.HasPrefix(seg, "/") {
			return fail("absolute schema/table segment")
		}
	}
	if !strings.EqualFold(tableRel, "true") || !strings.EqualFold(schemaRel, "true") {
		return fail("non-relative schema/table segment")
	}
	prefix := slash(schemaPath) + slash(tablePath)
	deletes, err := dbutil.QueryInt(ctx, c, fmt.Sprintf(
		"SELECT count(*) FROM %s.ducklake_delete_file WHERE table_id=%d AND begin_snapshot<=%d AND (end_snapshot IS NULL OR end_snapshot>%d)",
		META, tid, snapshot, snapshot))
	if err != nil {
		return fail("delete files: %v", err)
	}
	if deletes > 0 {
		return fail("%d live delete files", deletes)
	}
	inlinedDel, err := dbutil.QueryInt(ctx, c, fmt.Sprintf(
		"SELECT count(*) FROM %s.ducklake_inlined_delete_%d WHERE begin_snapshot<=%d",
		META, tid, snapshot))
	if err != nil {
		if !strings.Contains(err.Error(), "does not exist") {
			return fail("inlined deletes: %v", err)
		}
	} else if inlinedDel > 0 {
		return fail("%d inlined deletes", inlinedDel)
	}
	inlined, err := dbutil.QueryInt(ctx, c, fmt.Sprintf(
		"SELECT count(*) FROM %s.ducklake_inlined_data_tables WHERE table_id=%d", META, tid))
	if err != nil {
		return fail("inlined data: %v", err)
	}
	if inlined > 0 {
		return fail("inlined data present")
	}
	stored, err := dbutil.QueryTable(ctx, c, "SELECT data_path FROM ducklake_settings('shard')")
	if err != nil || len(stored) == 0 || stored[0][0] == nil || *stored[0][0] == "" {
		return fail("catalog data path is empty")
	}
	var relpaths []string
	for _, f := range files {
		path := str(f[0])
		rel := f[1] != nil && strings.EqualFold(*f[1], "true")
		if rel {
			relpaths = append(relpaths, prefix+path)
		} else {
			relpaths = append(relpaths, path)
		}
	}
	return &resolvedFiles{base: *stored[0][0], relpaths: relpaths}, nil
}

// Open mirrors Store::open_config: validates config, attaches the catalog
// pinned to max(snapshot_id), freezes the serving file list, validates the
// serving index, and opens the dedicated-connection pool.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Connections < 1 {
		return nil, Invalid("connections must be positive")
	}
	if cfg.Threads <= 0 {
		return nil, Invalid("threads must be positive")
	}
	if cfg.BulkLimit < 1 || cfg.BulkLimit > cfg.Connections {
		return nil, Invalid("bulk limit must be between 1 and connections")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, Backend("open duckdb: " + err.Error())
	}
	db.SetMaxOpenConns(cfg.Connections + 1)
	boot, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, Backend("connect: " + err.Error())
	}
	if _, err := boot.ExecContext(ctx, fmt.Sprintf("SET threads=%d", cfg.Threads)); err != nil {
		boot.Close()
		db.Close()
		return nil, Backend(err.Error())
	}
	if cfg.MemoryMB > 0 {
		if _, err := boot.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMiB'", cfg.MemoryMB)); err != nil {
			boot.Close()
			db.Close()
			return nil, Backend(err.Error())
		}
	}
	if err := setupSession(ctx, boot, cfg.TempDir); err != nil {
		boot.Close()
		db.Close()
		return nil, Backend(err.Error())
	}
	var dataOverride *string
	if cfg.DataRoot != "" {
		dataOverride = &cfg.DataRoot
	}
	catalog := catalogURL(cfg.Location)
	if err := dbutil.ExecAll(ctx, boot,
		fmt.Sprintf("ATTACH %s AS shard (%s)", filter.Quote(catalog), attachOptions(nil, dataOverride)),
		"USE shard",
	); err != nil {
		boot.Close()
		db.Close()
		return nil, Backend("attach: " + err.Error())
	}
	snapshot, err := dbutil.QueryInt(ctx, boot, "SELECT max(snapshot_id) FROM snapshots()")
	if err != nil {
		boot.Close()
		db.Close()
		return nil, Backend("pin snapshot: " + err.Error())
	}
	if err := dbutil.ExecAll(ctx, boot,
		"USE memory",
		"DETACH shard",
		fmt.Sprintf("ATTACH %s AS shard (%s)", filter.Quote(catalog), attachOptions(&snapshot, dataOverride)),
		"USE shard",
	); err != nil {
		boot.Close()
		db.Close()
		return nil, Backend("attach pinned: " + err.Error())
	}

	s := &Store{
		Snapshot:     snapshot,
		db:           db,
		sem:          make(chan struct{}, cfg.Connections),
		maxWaiters:   int64(cfg.MaxWaiters),
		maxWait:      cfg.MaxWait,
		queryTimeout: cfg.QueryTimeout,
	}
	for i := 0; i < cfg.Connections; i++ {
		s.sem <- struct{}{}
	}
	if cfg.BulkLimit < cfg.Connections {
		s.bulk = make(chan struct{}, cfg.BulkLimit)
		for i := 0; i < cfg.BulkLimit; i++ {
			s.bulk <- struct{}{}
		}
	}

	// Frozen serving source at the pinned snapshot; fail closed to the
	// catalog table on any doubt.
	base := ""
	if dataOverride != nil {
		base = strings.TrimRight(*dataOverride, "/")
		if base != "" {
			base += "/"
		}
	}
	resolved, rerr := resolveFiles(ctx, boot, snapshot)
	var allURLs []string
	if rerr != nil {
		s.fallbackFrom = "features"
	} else {
		effBase := resolved.base
		if base != "" {
			effBase = strings.TrimRight(base, "/")
		}
		allURLs = fileURLs(effBase, resolved.relpaths)
		if len(allURLs) == 0 {
			s.fallbackFrom = "features"
		} else {
			quoted := make([]string, len(allURLs))
			for i, u := range allURLs {
				quoted[i] = filter.Quote(u)
			}
			sort.Strings(quoted)
			s.fallbackFrom = "read_parquet([" + strings.Join(quoted, ",") + "])"
		}
	}

	// Serving index sidecar: read-only S3 sidecar object; validated against the
	// pinned snapshot and exact file-set agreement.
	idxJSON := cfg.IndexJSON
	if idxJSON == nil {
		if raw, err := os.ReadFile(index.IndexPathFor(cfg.Location)); err == nil {
			str := string(raw)
			idxJSON = &str
		}
	}
	if idxJSON != nil && rerr == nil {
		var doc index.ServingIndex
		if err := json.Unmarshal([]byte(*idxJSON), &doc); err == nil &&
			doc.Version == index.Version && doc.DuckLakeCommit == snapshot &&
			len(doc.Files) == len(resolved.relpaths) {
			match := true
			for i := range doc.Files {
				if strings.TrimLeft(doc.Files[i].Path, "/") != strings.TrimLeft(resolved.relpaths[i], "/") {
					match = false
					break
				}
			}
			if match {
				urls := make([]string, len(doc.Files))
				for i, f := range doc.Files {
					rel := strings.TrimLeft(f.Path, "/")
					if strings.Contains(rel, "://") || strings.HasPrefix(rel, "/") {
						urls[i] = rel
					} else if base != "" {
						urls[i] = strings.TrimRight(base, "/") + "/" + rel
					} else {
						urls[i] = slash(resolved.base) + rel
					}
				}
				s.index = &ResolvedIndex{Index: doc, URLs: urls}
			}
		}
	}

	pool := make([]*sql.Conn, 0, cfg.Connections)
	for i := 0; i < cfg.Connections; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			boot.Close()
			for _, p := range pool {
				p.Close()
			}
			db.Close()
			return nil, Backend("pool connect: " + err.Error())
		}
		if err := setupSession(ctx, c, cfg.TempDir); err != nil {
			boot.Close()
			c.Close()
			for _, p := range pool {
				p.Close()
			}
			db.Close()
			return nil, Backend(err.Error())
		}
		if err := dbutil.ExecAll(ctx, c, "USE shard"); err != nil {
			boot.Close()
			c.Close()
			for _, p := range pool {
				p.Close()
			}
			db.Close()
			return nil, Backend(err.Error())
		}
		pool = append(pool, c)
	}
	boot.Close()
	probe := pool[0]
	if _, err := dbutil.QueryTable(ctx, probe, "SELECT cx, cy, name FROM features LIMIT 0"); err != nil {
		for _, p := range pool {
			p.Close()
		}
		db.Close()
		return nil, Invalid("shard is missing derived cx/cy/name columns; rebuild")
	}
	collections, err := dbutil.QueryStrings(ctx, probe, "SELECT id FROM collections ORDER BY id")
	if err != nil {
		for _, p := range pool {
			p.Close()
		}
		db.Close()
		return nil, Backend("collections: " + err.Error())
	}
	for _, id := range collections {
		if !filter.CollectionID(id) {
			for _, p := range pool {
				p.Close()
			}
			db.Close()
			return nil, Invalid("shard contains an invalid collection id")
		}
	}
	s.Collections = collections
	s.pool = pool
	if cfg.Lance != nil {
		src, err := lance.Open(ctx, *cfg.Lance)
		if err != nil {
			for _, p := range pool {
				p.Close()
			}
			db.Close()
			return nil, Backend(err.Error())
		}
		s.lance = src
	}
	if raw, err := os.ReadFile(cfg.Location + ".manifest.json"); err == nil {
		var m ShardManifest
		if jerr := json.Unmarshal(raw, &m); jerr == nil {
			s.manifest = &m
		}
	}
	for _, key := range []string{"threads", "memory_limit"} {
		rows, err := dbutil.QueryTable(ctx, probe, "SELECT value FROM duckdb_settings() WHERE name='"+key+"'")
		if err == nil && len(rows) > 0 && rows[0][0] != nil {
			s.tuning = append(s.tuning, [2]string{key, *rows[0][0]})
		}
	}
	return s, nil
}

// ServingFiles reports how many files the serving index covers, or -1 when
// no trusted index attached (reads use the frozen full file list).
func (s *Store) ServingFiles() int {
	if s.index == nil {
		return -1
	}
	return len(s.index.URLs)
}

// Collection validates a collection id.
func (s *Store) Collection(id string) error {
	if !filter.CollectionID(id) {
		return Invalid("unknown collection")
	}
	for _, c := range s.Collections {
		if c == id {
			return nil
		}
	}
	return NotFound("unknown collection")
}

// ReadSource prunes to candidate data file URLs via the serving index.
func (s *Store) ReadSource(bounds *[4]float64) string {
	if s.index == nil {
		return s.fallbackFrom
	}
	if bounds == nil {
		return s.fallbackFrom
	}
	picks := index.PruneFiles(&s.index.Index, bounds)
	if len(picks) == 0 {
		// Footer-only probe of one file: matches nothing, opens nothing
		// else. Wrapped as a subquery so callers keep appending WHERE.
		if len(s.index.URLs) > 0 {
			return "(SELECT * FROM read_parquet([" + filter.Quote(s.index.URLs[0]) + "]) WHERE FALSE) AS _empty"
		}
		return s.fallbackFrom
	}
	if len(picks) == len(s.index.URLs) {
		return s.fallbackFrom
	}
	urls := make([]string, len(picks))
	for i, p := range picks {
		urls[i] = filter.Quote(s.index.URLs[p])
	}
	return "read_parquet([" + strings.Join(urls, ",") + "])"
}

// Predicate builds the WHERE fragment with bbox-range pruning + exact
// ST_Intersects on boundary candidates, mirroring store::predicate.
func Predicate(collection string, bounds *[4]float64, sources []int64) string {
	base := filter.Predicate(collection, bounds, sources)
	if bounds == nil {
		return base
	}
	return fmt.Sprintf("%s AND %s AND ((%s) OR (%s))",
		base, filter.BBoxOverlap(*bounds),
		filter.BBoxContained(*bounds), filter.SpatialPredicate(*bounds))
}

// checkout holds one dedicated connection plus semaphore permits.
type checkout struct {
	conn    *sql.Conn
	release func()
}

func (s *Store) acquire(ctx context.Context, bulk bool) (*checkout, error) {
	if bulk && s.bulk != nil {
		select {
		case <-s.bulk:
		case <-ctx.Done():
			return nil, Overloaded()
		}
	}
	q := s.queued.Add(1)
	ok := false
	defer func() {
		if !ok {
			s.queued.Add(-1)
			if bulk && s.bulk != nil {
				s.bulk <- struct{}{}
			}
		}
	}()
	if q > s.maxWaiters {
		return nil, Overloaded()
	}
	tctx, cancel := context.WithTimeout(ctx, s.maxWait)
	defer cancel()
	select {
	case <-s.sem:
		s.queued.Add(-1)
		s.mu.Lock()
		var c *sql.Conn
		if len(s.pool) > 0 {
			c = s.pool[len(s.pool)-1]
			s.pool = s.pool[:len(s.pool)-1]
		}
		s.mu.Unlock()
		if c == nil {
			if bulk && s.bulk != nil {
				s.bulk <- struct{}{}
			}
			return nil, Backend("pool exhausted")
		}
		ok = true
		return &checkout{conn: c, release: func() {
			s.mu.Lock()
			s.pool = append(s.pool, c)
			s.mu.Unlock()
			s.sem <- struct{}{}
			if bulk && s.bulk != nil {
				s.bulk <- struct{}{}
			}
		}}, nil
	case <-tctx.Done():
		return nil, Overloaded()
	}
}

// Query runs fn with one pooled connection and a deadline. Note: unlike the
// Rust pool's cross-thread interrupt handle, Go cancellation is via context
// (best-effort for in-flight DuckDB execution); the deadline still bounds
// queueing and client-visible latency.
func (s *Store) Query(ctx context.Context, bulk bool, fn func(ctx context.Context, c *sql.Conn) error) error {
	co, err := s.acquire(ctx, bulk)
	if err != nil {
		return err
	}
	defer co.release()
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}
	return fn(ctx, co.conn)
}

// Close drains the pool.
func (s *Store) Close() error {
	if s.lance != nil {
		s.lance.Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.pool {
		c.Close()
	}
	s.pool = nil
	return s.db.Close()
}

// Lance reports the indexed Lance feature source, or nil when the store
// serves from Parquet only.
func (s *Store) Lance() *lance.Source { return s.lance }
