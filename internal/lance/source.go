// Package lance adapts the lance-go bindings (pinned lance =8.0.0, native
// library vendored under .deps/lance-go) into the lakewing serving path.
//
// Role: an indexed feature source alongside the DuckLake/Parquet store.
// Scans push SQL filters into Lance (scalar/RTree index machinery) and
// stream Arrow record readers that DuckDB consumes via Arrow view
// registration — one process, one pool, DuckDB keeps exact predicates,
// ordering, pagination and serialization.
//
// Geometry columns are projected as WKB (ST_AsBinary): the Arrow view
// import surfaces GeoArrow storage as nested structs on this engine, while
// WKB feeds ST_GeomFromWKB exactly like the Parquet path.
package lance

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	lance "github.com/gstamatakis95/lance-go/lance"
)

// Config pins one Lance dataset as a serving source. Tag wins over
// Version; both zero/empty means latest (development only — serving must
// pin exactly like the DuckLake snapshot).
type Config struct {
	URI            string
	Tag            string
	Version        uint64
	StorageOptions map[string]string
}

// Source is a pinned, read-only Lance dataset handle. Safe for concurrent
// Scanner builds; callers own the returned readers.
type Source struct {
	ds  *lance.Dataset
	cfg Config
}

// Open pins the dataset at the configured tag/version and validates that
// the feature columns the serving SQL expects are present.
func Open(ctx context.Context, cfg Config) (*Source, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("lance: uri is required")
	}
	if cfg.Tag == "" && cfg.Version == 0 {
		return nil, fmt.Errorf("lance: pin a tag or version (latest is not a snapshot)")
	}
	opts := []lance.OpenOption{lance.WithStorageOptions(cfg.StorageOptions)}
	if cfg.Tag != "" {
		opts = append(opts, lance.WithTag(cfg.Tag))
	} else {
		opts = append(opts, lance.WithVersion(cfg.Version))
	}
	ds, err := lance.Open(ctx, cfg.URI, opts...)
	if err != nil {
		return nil, fmt.Errorf("lance: open %s: %w", cfg.URI, err)
	}
	return &Source{ds: ds, cfg: cfg}, nil
}

// Close releases the dataset handle.
func (s *Source) Close() { s.ds.Close() }

// Version reports the pinned dataset version.
func (s *Source) Version(ctx context.Context) uint64 {
	info, err := s.ds.Version(ctx)
	if err != nil {
		return 0
	}
	return info.Version
}

// Page describes one serving scan.
type Page struct {
	// Filter is DataFusion SQL pushed into the scan (collection, sources,
	// bbox-column overlap; DuckDB re-checks exact predicates).
	Filter string
	// Narrow restricts the projection to id-like columns for the ids-first
	// phase of deep pagination.
	Narrow bool
	// Limit and Offset apply inside Lance (0 = unset).
	Limit, Offset int64
}

var (
	narrowColumns = []string{"id", "layer", "source_id"}
	// pageProjection: payload columns with geometry as binary plus every
	// column the exact predicate (layer/sources/bbox) references.
	pageProjection = [][2]string{
		{"id", "id"}, {"geom", "ST_AsBinary(geom)"}, {"properties", "properties"},
		{"layer", "layer"}, {"source_id", "source_id"},
		{"xmin", "xmin"}, {"ymin", "ymin"}, {"xmax", "xmax"}, {"ymax", "ymax"},
	}
	idProjection     = [][2]string{{"id", "id"}}
	narrowProjection = [][2]string{{"id", "id"}, {"layer", "layer"}, {"source_id", "source_id"}}
)

// Scan builds the reader for one page. WKB geometry is returned as a binary
// `geom` column; `id` is VARCHAR and `properties` VARCHAR.
func (s *Source) Scan(ctx context.Context, page Page) (array.RecordReader, error) {
	sc := s.ds.Scan().Filter(page.Filter).BatchSize(8192)
	if page.Narrow {
		for _, p := range narrowProjection {
			sc = sc.ProjectExpr(p[0], p[1])
		}
	} else {
		for _, p := range pageProjection {
			sc = sc.ProjectExpr(p[0], p[1])
		}
	}
	if page.Limit > 0 {
		sc = sc.Limit(page.Limit)
	}
	if page.Offset > 0 {
		sc = sc.Offset(page.Offset)
	}
	rdr, err := sc.Reader(ctx)
	if err != nil {
		return nil, fmt.Errorf("lance: scan: %w", err)
	}
	return rdr, nil
}

// IDScan scans only the id column (ids-first payload phase builds an
// id IN (...) filter instead).
func (s *Source) IDScan(ctx context.Context, filter string) (array.RecordReader, error) {
	sc := s.ds.Scan().Filter(filter).BatchSize(8192)
	for _, p := range idProjection {
		sc = sc.ProjectExpr(p[0], p[1])
	}
	rdr, err := sc.Reader(ctx)
	if err != nil {
		return nil, fmt.Errorf("lance: id scan: %w", err)
	}
	return rdr, nil
}

// QuoteIDs renders a SQL id IN (...) list.
func QuoteIDs(ids []string) string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = "'" + strings.ReplaceAll(id, "'", "''") + "'"
	}
	return strings.Join(quoted, ",")
}
