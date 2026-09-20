// Package materialize ports src/materialize.rs: build + index.
//
// Writes go direct to S3 (no mount, no Cachey): build stages locally, then
// publishes data files + catalog + sidecars with additive-only semantics.
package materialize

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/lakewing/internal/dbutil"
	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/index"
	"github.com/adonm/lakewing/internal/store"
)

type SortOrder string

const (
	SortGrid    SortOrder = "grid"
	SortHilbert SortOrder = "hilbert"
	SortNone    SortOrder = "none"
)

// Spec mirrors materialize::Build flags.
type Spec struct {
	From           string
	Collection     string
	BBox           [4]float64
	Out            string
	DataDir        string
	DataURL        string
	Limit          *int64
	FileMB         uint64
	RowGroup       uint64
	Sort           SortOrder
	SourceID       int64
	ContentAddress bool
}

func absDir(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func withSlash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

// dataURL resolves the portable DATA_PATH exactly like Build::run.
func dataURL(spec Spec) string {
	if spec.DataURL != "" {
		if strings.Contains(spec.DataURL, "://") {
			return withSlash(spec.DataURL)
		}
		return withSlash(absDir(spec.DataURL))
	}
	return withSlash(absDir(spec.DataDir))
}

// connect opens an in-memory DuckDB with one dedicated connection.
func connect(ctx context.Context) (*sql.DB, *sql.Conn, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, err
	}
	c, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return db, c, nil
}

func loadExt(ctx context.Context, c *sql.Conn, ext string) error {
	if _, err := c.ExecContext(ctx, "LOAD "+ext); err == nil {
		return nil
	}
	if _, err := c.ExecContext(ctx, "INSTALL "+ext); err != nil {
		return fmt.Errorf("INSTALL %s: %w", ext, err)
	}
	if _, err := c.ExecContext(ctx, "LOAD "+ext); err != nil {
		return fmt.Errorf("LOAD %s: %w", ext, err)
	}
	return nil
}

// Run mirrors Build::run: validate, stage, write, repoint, verify,
// optionally content-address, publish, manifest, serving index.
func Run(ctx context.Context, spec Spec) (int64, error) {
	if !filter.CollectionID(spec.Collection) {
		return 0, fmt.Errorf("invalid collection id")
	}
	switch spec.Sort {
	case SortGrid, SortHilbert, SortNone:
	default:
		return 0, fmt.Errorf("sort must be grid, hilbert, or none")
	}
	if _, err := os.Stat(spec.Out); err == nil {
		return 0, fmt.Errorf("%s exists; build a new shard path", spec.Out)
	}
	parent := filepath.Dir(spec.Out)
	if parent == "" {
		parent = "."
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return 0, err
	}
	staging, err := os.MkdirTemp(parent, ".lakewing-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(staging) }()
	stagingCatalog := filepath.Join(staging, "catalog.ducklake")
	stagingData := filepath.Join(staging, "files")
	if err := os.MkdirAll(stagingData, 0o755); err != nil {
		return 0, err
	}
	dataRoot := dataURL(spec)
	rows, err := write(ctx, spec, stagingCatalog, stagingData, dataRoot)
	if err != nil {
		return 0, err
	}
	if err := repointDataPaths(ctx, stagingCatalog, stagingData, dataRoot); err != nil {
		return 0, err
	}
	if err := verifyNoStagingPaths(ctx, stagingCatalog, stagingData); err != nil {
		return 0, err
	}
	if spec.ContentAddress {
		n, err := contentAddressData(ctx, stagingCatalog, stagingData)
		if err != nil {
			return 0, err
		}
		fmt.Printf("content-addressed %d data files\n", n)
	}
	if err := os.MkdirAll(spec.DataDir, 0o755); err != nil {
		return 0, err
	}
	if err := publishTree(stagingData, spec.DataDir); err != nil {
		return 0, err
	}
	if err := os.Rename(stagingCatalog, spec.Out); err != nil {
		return 0, err
	}
	manifest := store.ShardManifest{
		Version: 1, Backend: "lake", SchemaVersion: 2,
		Source: spec.From, BBox: spec.BBox, Rows: rows,
		BuiltAt: strconv.FormatInt(time.Now().Unix(), 10),
		Layout: &store.LakeLayout{
			FileMB: spec.FileMB, RowGroup: spec.RowGroup, Sort: string(spec.Sort),
		},
	}
	pretty, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(spec.Out+".manifest.json", pretty, 0o644); err != nil {
		return 0, err
	}
	doc, err := GenerateIndex(ctx, spec.Out, absDir(spec.DataDir))
	if err != nil {
		return 0, fmt.Errorf("serving index: %w", err)
	}
	idxPretty, _ := json.MarshalIndent(doc, "", "  ")
	if err := os.WriteFile(spec.Out+".serving.json", idxPretty, 0o644); err != nil {
		return 0, err
	}
	fmt.Printf("materialized %d features into %s (+ %s)\n", rows, spec.Out, spec.DataDir)
	return rows, nil
}

// write mirrors Build::write: schema, clustered INSERT, collections, flush.
func write(ctx context.Context, spec Spec, catalog, stagingData, dataRoot string) (int64, error) {
	db, c, err := connect(ctx)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	defer c.Close()
	for _, ext := range []string{"spatial", "ducklake"} {
		if err := loadExt(ctx, c, ext); err != nil {
			return 0, err
		}
	}
	if strings.HasPrefix(spec.From, "https://") || strings.HasPrefix(spec.From, "http://") ||
		strings.HasPrefix(spec.From, "s3://") {
		if err := loadExt(ctx, c, "httpfs"); err != nil {
			return 0, err
		}
	}
	input := "read_parquet(" + filter.Quote(spec.From) + ")"
	cols, err := dbutil.QueryTable(ctx, c, "DESCRIBE SELECT * FROM "+input)
	if err != nil {
		return 0, fmt.Errorf("describe input: %w", err)
	}
	str := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	geomCol := ""
	for _, col := range cols {
		if str(col[0]) == "geometry" {
			geomCol = str(col[1])
			break
		}
	}
	if geomCol == "" {
		return 0, fmt.Errorf("source is missing geometry")
	}
	geometry := "geometry"
	if !strings.HasPrefix(geomCol, "GEOMETRY") {
		geometry = "ST_GeomFromWKB(geometry)"
	}
	var props []string
	for _, col := range cols {
		name := str(col[0])
		if name == "geometry" || name == "bbox" {
			continue
		}
		id := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		props = append(props, id+" := "+id)
	}
	properties := strings.Join(props, ", ")
	w, s, e, n := spec.BBox[0], spec.BBox[1], spec.BBox[2], spec.BBox[3]
	longitude := fmt.Sprintf("bbox.xmin <= %v AND bbox.xmax >= %v", e, w)
	if w > e {
		longitude = fmt.Sprintf("(bbox.xmax >= %v OR bbox.xmin <= %v)", w, e)
	}
	limit := ""
	if spec.Limit != nil {
		limit = fmt.Sprintf("LIMIT %d", *spec.Limit)
	}
	var sortDDL, sortSelect string
	switch spec.Sort {
	case SortGrid:
		sortDDL = "ALTER TABLE features SET SORTED BY (sortkey ASC, id ASC)"
		sortSelect = fmt.Sprintf("((ST_XMin(%s) - %d) * 100)::BIGINT * 1000 + ((ST_YMin(%s) - %d) * 100)::BIGINT,",
			geometry, int64(math.Floor(w)), geometry, int64(math.Floor(s)))
	case SortHilbert:
		sortDDL = "ALTER TABLE features SET SORTED BY (sortkey ASC, id ASC)"
		sortSelect = fmt.Sprintf("ST_Hilbert(%s, ST_Extent(ST_MakeEnvelope(%v, %v, %v, %v))),",
			geometry, w, s, e, n)
	default:
		sortSelect = "0,"
	}
	catalogSQL := filter.Quote("ducklake:" + catalog)
	stagingSQL := filter.Quote(withSlash(stagingData))
	dataURLSQL := filter.Quote(withSlash(dataRoot))
	setup := []string{
		fmt.Sprintf("ATTACH %s AS lake (DATA_PATH %s)", catalogSQL, dataURLSQL),
		"DETACH lake",
		fmt.Sprintf("ATTACH %s AS lake (DATA_PATH %s, OVERRIDE_DATA_PATH true)", catalogSQL, stagingSQL),
		"USE lake",
		fmt.Sprintf("CALL lake.set_option('target_file_size', '%dMB')", spec.FileMB),
		fmt.Sprintf("CALL lake.set_option('parquet_row_group_size', %d)", spec.RowGroup),
		"CALL lake.set_option('parquet_compression', 'zstd')",
		"CALL lake.set_option('parquet_compression_level', 3)",
		"CREATE TABLE features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON, sortkey BIGINT, xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE, cx DOUBLE, cy DOUBLE, name VARCHAR)",
		"CREATE TABLE collections(id VARCHAR)",
	}
	if sortDDL != "" {
		setup = append(setup, sortDDL)
	}
	if err := dbutil.ExecAll(ctx, c, setup...); err != nil {
		return 0, err
	}
	spatial := strings.ReplaceAll(filter.SpatialPredicate(spec.BBox), "geom,", geometry+",")
	insert := fmt.Sprintf(
		"INSERT INTO features SELECT type || ':' || id::VARCHAR, %s::VARCHAR, %d::BIGINT, %s, "+
			"to_json(struct_pack(%s)), %s "+
			"ST_XMin(%s), ST_YMin(%s), ST_XMax(%s), ST_YMax(%s), "+
			"ST_X(ST_Centroid(%s)), ST_Y(ST_Centroid(%s)), "+
			"coalesce(json_extract_string(to_json(struct_pack(%s)), '$.name'), json_extract_string(to_json(struct_pack(%s)), '$.tags.name')) "+
			"FROM %s WHERE %s AND bbox.ymin <= %v AND bbox.ymax >= %v AND %s %s;",
		filter.Quote(spec.Collection), spec.SourceID, geometry,
		properties, sortSelect,
		geometry, geometry, geometry, geometry,
		geometry, geometry,
		properties, properties,
		input, longitude, n, s, spatial, limit)
	if _, err := c.ExecContext(ctx, insert); err != nil {
		return 0, fmt.Errorf("insert: %w", err)
	}
	if err := dbutil.ExecAll(ctx, c,
		"INSERT INTO collections SELECT DISTINCT layer AS id FROM features ORDER BY id",
		"CALL ducklake_flush_inlined_data('lake')",
	); err != nil {
		return 0, err
	}
	return dbutil.QueryInt(ctx, c, "SELECT count(*) FROM features")
}

// repointDataPaths mirrors materialize::repoint_data_paths.
func repointDataPaths(ctx context.Context, catalog, stagingData, dataRoot string) error {
	stagingRoot := withSlash(stagingData)
	db, c, err := connect(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	defer c.Close()
	if err := loadExt(ctx, c, "ducklake"); err != nil {
		return err
	}
	update := fmt.Sprintf(
		"UPDATE __ducklake_metadata_lake.ducklake_data_file SET path = %s || substr(path, %d + 1) WHERE path LIKE %s",
		filter.Quote(dataRoot), len(stagingRoot), filter.Quote(stagingRoot+"%"))
	return dbutil.ExecAll(ctx, c,
		fmt.Sprintf("ATTACH %s AS lake", filter.Quote("ducklake:"+catalog)),
		"USE lake", update)
}

// verifyNoStagingPaths mirrors materialize::verify_no_staging_paths.
func verifyNoStagingPaths(ctx context.Context, catalog, stagingData string) error {
	stagingRoot := withSlash(stagingData)
	db, c, err := connect(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	defer c.Close()
	if err := loadExt(ctx, c, "ducklake"); err != nil {
		return err
	}
	if err := dbutil.ExecAll(ctx, c,
		fmt.Sprintf("ATTACH %s AS lake (READ_ONLY)", filter.Quote("ducklake:"+catalog)),
		"USE lake",
	); err != nil {
		return err
	}
	rows, err := dbutil.QueryTable(ctx, c, fmt.Sprintf(
		"SELECT path FROM __ducklake_metadata_lake.ducklake_data_file WHERE path LIKE %s LIMIT 1",
		filter.Quote(stagingRoot+"%")))
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return fmt.Errorf("catalog still references staging data paths")
	}
	return nil
}

// contentAddressData mirrors materialize::content_address_data: rename data
// files to <sha256>.parquet and rewrite catalog references to match.
func contentAddressData(ctx context.Context, catalog, stagingData string) (int, error) {
	var files []string
	var walk func(dir string) error
	walk = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, e := range entries {
			p := filepath.Join(dir, e.Name())
			if e.IsDir() {
				if err := walk(p); err != nil {
					return err
				}
			} else {
				files = append(files, p)
			}
		}
		return nil
	}
	if err := walk(stagingData); err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("no data files to content-address")
	}
	db, c, err := connect(ctx)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	defer c.Close()
	if err := loadExt(ctx, c, "ducklake"); err != nil {
		return 0, err
	}
	if err := dbutil.ExecAll(ctx, c,
		fmt.Sprintf("ATTACH %s AS lake", filter.Quote("ducklake:"+catalog)),
		"USE lake",
	); err != nil {
		return 0, err
	}
	rows, err := dbutil.QueryTable(ctx, c,
		"SELECT path FROM __ducklake_metadata_lake.ducklake_data_file")
	if err != nil {
		return 0, err
	}
	unmatched := map[string]bool{}
	for _, r := range rows {
		if r[0] != nil {
			unmatched[*r[0]] = true
		}
	}
	renamed := 0
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			return 0, err
		}
		sum := sha256.Sum256(raw)
		hexstr := hex.EncodeToString(sum[:])
		oldName := filepath.Base(path)
		newName := hexstr + ".parquet"
		var targets []string
		for stored := range unmatched {
			if stored == oldName || strings.HasSuffix(stored, "/"+oldName) {
				targets = append(targets, stored)
			}
		}
		sort.Strings(targets)
		if len(targets) == 0 {
			return 0, fmt.Errorf("catalog has no reference to data file %s", oldName)
		}
		for _, stored := range targets {
			newStored := newName
			if stored != oldName {
				newStored = stored[:len(stored)-len(oldName)-1] + "/" + newName
			}
			if err := dbutil.ExecAll(ctx, c, fmt.Sprintf(
				"UPDATE __ducklake_metadata_lake.ducklake_data_file SET path = %s WHERE path = %s",
				filter.Quote(newStored), filter.Quote(stored))); err != nil {
				return 0, err
			}
			delete(unmatched, stored)
		}
		if oldName != newName {
			dest := filepath.Join(filepath.Dir(path), newName)
			if _, err := os.Stat(dest); err == nil {
				destRaw, err := os.ReadFile(dest)
				if err != nil {
					return 0, err
				}
				destSum := sha256.Sum256(destRaw)
				if hex.EncodeToString(destSum[:]) != hexstr {
					return 0, fmt.Errorf("name collision for %s", newName)
				}
				if path != dest {
					if err := os.Remove(path); err != nil {
						return 0, err
					}
				}
			} else if err := os.Rename(path, dest); err != nil {
				return 0, err
			}
		}
		renamed++
	}
	if len(unmatched) > 0 {
		return 0, fmt.Errorf("catalog references %d files missing from staging", len(unmatched))
	}
	return renamed, nil
}

func publishTree(source, dest string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, e := range entries {
		target := filepath.Join(dest, e.Name())
		src := filepath.Join(source, e.Name())
		if e.IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			if err := publishTree(src, target); err != nil {
				return err
			}
		} else if _, err := os.Stat(target); os.IsNotExist(err) {
			raw, err := os.ReadFile(src)
			if err != nil {
				return err
			}
			if err := os.WriteFile(target, raw, 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// GenerateIndex mirrors materialize::generate_index: resolve the catalog's
// live files at its pinned snapshot, read footer bbox statistics from
// localBase (absolute local dir + catalog-relative paths), and compile the
// serving index document.
func GenerateIndex(ctx context.Context, location, localBase string) (index.ServingIndex, error) {
	var empty index.ServingIndex
	db, c, err := connect(ctx)
	if err != nil {
		return empty, err
	}
	defer db.Close()
	defer c.Close()
	if err := loadExt(ctx, c, "ducklake"); err != nil {
		return empty, err
	}
	if err := loadExt(ctx, c, "spatial"); err != nil {
		return empty, err
	}
	catalog := store.CatalogURL(location)
	if err := dbutil.ExecAll(ctx, c,
		fmt.Sprintf("ATTACH %s AS shard (READ_ONLY)", filter.Quote(catalog)),
		"USE shard",
	); err != nil {
		return empty, err
	}
	snapshot, err := dbutil.QueryInt(ctx, c, "SELECT max(snapshot_id) FROM snapshots()")
	if err != nil {
		return empty, err
	}
	resolved, err := store.SnapshotFiles(ctx, c, snapshot)
	if err != nil {
		return empty, fmt.Errorf("index file resolution: %w", err)
	}
	base := strings.TrimSuffix(localBase, "/")
	absolute := make([]string, len(resolved.Relpaths))
	for i, rel := range resolved.Relpaths {
		absolute[i] = base + "/" + rel
	}
	stats, err := fileBboxes(ctx, c, absolute)
	if err != nil {
		return empty, err
	}
	doc, err := index.BuildIndex(snapshot, resolved.Relpaths, stats)
	if err != nil {
		return empty, fmt.Errorf("index build: %w", err)
	}
	return doc, nil
}

// fileBboxes mirrors index::file_bboxes: footer-only bbox per file.
func fileBboxes(ctx context.Context, c *sql.Conn, urls []string) ([][4]float64, error) {
	out := make([][4]float64, 0, len(urls))
	for _, u := range urls {
		rows, err := dbutil.QueryTable(ctx, c, fmt.Sprintf(
			"SELECT min(xmin)::VARCHAR, max(xmax)::VARCHAR, min(ymin)::VARCHAR, max(ymax)::VARCHAR FROM read_parquet(%s)",
			filter.Quote(u)))
		if err != nil {
			return nil, fmt.Errorf("file stats for %s: %w", u, err)
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("no stats for %s", u)
		}
		vals := [4]float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()}
		for i := 0; i < 4 && i < len(rows[0]); i++ {
			cell := rows[0][i]
			raw := "NaN"
			if cell != nil {
				raw = *cell
			}
			f, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return nil, fmt.Errorf("unparseable stats for %s", u)
			}
			vals[i] = f
		}
		for _, v := range vals {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return nil, fmt.Errorf("non-finite bbox stats for %s", u)
			}
		}
		out = append(out, [4]float64{vals[0], vals[2], vals[1], vals[3]})
	}
	return out, nil
}
