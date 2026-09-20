package ogc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/lance"
	"github.com/adonm/lakewing/internal/store"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/duckdb/duckdb-go/v2"
)

// lanceViewSeq names registered views uniquely: pool connections are
// long-lived and view names must never collide across concurrent pages.
var lanceViewSeq atomic.Uint64

// lanceRegister scans one Lance page and registers it as an Arrow view on
// this connection, returning SQL that surfaces (id, geom GEOMETRY,
// properties) plus the predicate columns. The release func drops the view
// and releases the stream; call it when the page query finishes.
func lanceRegister(ctx context.Context, src *lance.Source, c *sql.Conn, page lance.Page) (string, func(), error) {
	rdr, err := src.Scan(ctx, page)
	if err != nil {
		return "", nil, err
	}
	name := fmt.Sprintf("__lw_lance_%d", lanceViewSeq.Add(1))
	var release func()
	if err := c.Raw(func(driverConn any) error {
		arrow, err := duckdb.NewArrowFromConn(driverConn.(driver.Conn))
		if err != nil {
			return err
		}
		rel, err := arrow.RegisterView(rdr, name)
		if err != nil {
			return err
		}
		release = rel
		return nil
	}); err != nil {
		rdr.Release()
		return "", nil, fmt.Errorf("lance view: %w", err)
	}
	drop := func() {
		release()
		_, _ = c.ExecContext(context.WithoutCancel(ctx), "DROP VIEW IF EXISTS "+name)
	}
	return "(SELECT id, ST_GeomFromWKB(geom) AS geom, properties, layer, source_id, " +
		"xmin, ymin, xmax, ymax FROM " + name + ")", drop, nil
}

// lancePushed builds the candidate-superset filter pushed into the Lance
// scan: the DuckDB exact predicate re-checks everything on top (the same
// contract as Parquet zonemap pruning).
func lancePushed(collection string, bounds *[4]float64, sources []int64) string {
	base := filter.Predicate(collection, bounds, sources)
	if bounds == nil {
		return base
	}
	return base + " AND " + filter.BBoxOverlap(*bounds)
}

// lanceIDs scans the narrow id projection for the pushed filter and returns
// the id-ordered page window (limit+1 ids so callers detect hasNext).
func lanceIDs(ctx context.Context, src *lance.Source, pushed string, limit int, offset uint32) ([]string, error) {
	rdr, err := src.Scan(ctx, lance.Page{Filter: pushed, Narrow: true})
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	var ids []string
	for rdr.Next() {
		rec := rdr.Record()
		idx := rec.Schema().FieldIndices("id")
		if len(idx) != 1 {
			return nil, fmt.Errorf("lance ids: id column missing")
		}
		switch col := rec.Column(idx[0]).(type) {
		case *array.String:
			for i := 0; i < int(rec.NumRows()); i++ {
				ids = append(ids, col.Value(i))
			}
		case *array.LargeString:
			for i := 0; i < int(rec.NumRows()); i++ {
				ids = append(ids, col.Value(i))
			}
		default:
			return nil, fmt.Errorf("lance ids: id column is %s", rec.Column(idx[0]).DataType())
		}
	}
	if err := rdr.Err(); err != nil {
		return nil, err
	}
	sort.Strings(ids)
	start := int(offset)
	if start > len(ids) {
		start = len(ids)
	}
	ids = ids[start:]
	if limit+1 < len(ids) {
		ids = ids[:limit+1]
	}
	return ids, nil
}

// emptyPageQuery returns rows shaped like the items query but matching
// nothing (pages beyond the end of the result set).
const emptyPageQuery = "SELECT '' AS id, '' AS geom, '' AS properties::VARCHAR WHERE FALSE"

// lanceItemsQuery builds the items-page SQL for a Lance-sourced store on
// this connection, registering the views it needs. Single-phase for
// cursor/limit pages; ids-first two-phase for deep offsets, mirroring
// plan.HeavyItemsSQL over the registered views.
func lanceItemsQuery(ctx context.Context, st *store.Store, c *sql.Conn, collection string,
	bounds *[4]float64, sources []int64, pageWhere, pageTail string, deep bool, limit int, offset uint32) (string, func(), error) {
	src := st.Lance()
	pushed := lancePushed(collection, bounds, sources)
	if !deep {
		from, drop, err := lanceRegister(ctx, src, c, lance.Page{Filter: pushed})
		if err != nil {
			return "", nil, err
		}
		query := "SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM " +
			"(SELECT id, geom, properties FROM " + from + " WHERE " + pageWhere + " " + pageTail + ") AS page ORDER BY id"
		return query, drop, nil
	}
	ids, err := lanceIDs(ctx, src, pushed, limit, offset)
	if err != nil {
		return "", nil, err
	}
	if len(ids) == 0 {
		return emptyPageQuery, func() {}, nil
	}
	payload := pushed + " AND id IN (" + lance.QuoteIDs(ids) + ")"
	from, drop, err := lanceRegister(ctx, src, c, lance.Page{Filter: payload})
	if err != nil {
		return "", nil, err
	}
	query := "SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM " +
		"(SELECT id, geom, properties FROM " + from + ") AS page ORDER BY id"
	return query, drop, nil
}

// lanceItemQuery builds the single-feature lookup for a Lance-sourced
// store: the pushed filter includes the id equality so the scalar index
// answers it; DuckDB re-checks layer/sources and renders.
func lanceItemQuery(ctx context.Context, st *store.Store, c *sql.Conn, id, collection string, sources []int64) (string, func(), error) {
	src := st.Lance()
	pushed := lancePushed(collection, nil, sources) + " AND id = " + filter.Quote(id)
	from, drop, err := lanceRegister(ctx, src, c, lance.Page{Filter: pushed, Limit: 2})
	if err != nil {
		return "", nil, err
	}
	query := "SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM " +
		"(SELECT id, geom, properties FROM " + from + ") AS page LIMIT 1"
	return query, drop, nil
}
