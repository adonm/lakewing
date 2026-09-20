// Package plan ports src/plan.rs: normalized requests and query planning.
package plan

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/adonm/lakewing/internal/filter"
)

// Pagination mirrors plan::Pagination.
type Pagination struct {
	Cursor *string
	Offset uint32
}

// ItemsRequest is a validated items page: everything needed to plan it.
type ItemsRequest struct {
	Collection string
	Sources    []int64
	Bounds     *[4]float64
	Limit      uint32
	Pagination Pagination
	Datetime   *string
}

// CanonicalQS mirrors ItemsRequest::canonical_qs so equivalent spellings
// share one cache entry.
func (r ItemsRequest) CanonicalQS() string {
	var offset uint32
	var cursor *string
	if r.Pagination.Cursor != nil {
		cursor = r.Pagination.Cursor
	} else {
		offset = r.Pagination.Offset
	}
	parts := []string{fmt.Sprintf("limit=%d", r.Limit), fmt.Sprintf("offset=%d", offset)}
	if cursor != nil {
		parts = append(parts, "cursor="+url.QueryEscape(*cursor))
	}
	if r.Bounds != nil {
		b := r.Bounds
		raw := fmt.Sprintf("%v,%v,%v,%v", b[0], b[1], b[2], b[3])
		parts = append(parts, "bbox="+encodeBbox(raw))
	}
	if r.Datetime != nil {
		parts = append(parts, "datetime="+*r.Datetime)
	}
	srcs := make([]string, len(r.Sources))
	for i, s := range r.Sources {
		srcs[i] = fmt.Sprintf("%d", s)
	}
	parts = append(parts, "sources="+strings.ReplaceAll(strings.Join(srcs, ","), ",", "%2C"))
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func encodeBbox(raw string) string {
	enc := url.QueryEscape(raw)
	enc = strings.ReplaceAll(enc, "%2C", ",")
	enc = strings.ReplaceAll(enc, "%2E", ".")
	enc = strings.ReplaceAll(enc, "%2D", "-")
	return enc
}

// Href mirrors ItemsRequest::href.
func (r ItemsRequest) Href() string {
	return "/collections/" + r.Collection + "/items?" + r.CanonicalQS()
}

// IsHeavy mirrors plan::is_heavy: heavy pages share the bulk admission lane.
func IsHeavy(limit uint32, p Pagination, bounds *[4]float64) bool {
	if limit > 100 {
		return true
	}
	if p.Cursor == nil && p.Offset >= 1000 {
		return true
	}
	if bounds != nil {
		w, s, e, n := bounds[0], bounds[1], bounds[2], bounds[3]
		width := e - w
		if e < w {
			width = (180.0 - w) + (e + 180.0)
		}
		if width*(n-s) >= 6.0 {
			return true
		}
	} else if limit >= 100 {
		return true
	}
	return false
}

// ItemsSQL mirrors plan::items_sql. from is the store's frozen serving
// source (catalog table or file list).
func ItemsSQL(_ ItemsRequest, pageWhere, from string) string {
	return "SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM " +
		"(SELECT id, geom, properties FROM " + from + " WHERE " + pageWhere + ") AS page " +
		"ORDER BY id"
}

// HeavyItemsSQL is the ids-first two-phase variant for OFFSET-driven total
// sorts: OFFSET defeats DuckDB's top-N heapsort, forcing a full sort, so
// sorting narrow ids then joining back payloads wins big. Cursor/limit
// pages keep top-N heapsort and stay single-phase. ids are unique per
// snapshot (type:id), so the join preserves rows exactly.
func HeavyItemsSQL(from, where, tail string) string {
	return "WITH page_ids AS (SELECT id FROM " + from + " WHERE " + where + " " + tail + ") " +
		"SELECT p.id, ST_AsGeoJSON(f.geom), f.properties::VARCHAR FROM page_ids p " +
		"JOIN " + from + " f ON f.id = p.id ORDER BY p.id"
}

// PageParts mirrors plan::page_parts.
func PageParts(base string, limit uint32, p Pagination) (string, string) {
	if p.Cursor != nil {
		return base + " AND id > " + filter.Quote(*p.Cursor),
			fmt.Sprintf("ORDER BY id LIMIT %d", limit+1)
	}
	return base, fmt.Sprintf("ORDER BY id LIMIT %d OFFSET %d", limit+1, p.Offset)
}
