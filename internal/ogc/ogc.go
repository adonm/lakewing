// Package ogc ports src/api.rs: OGC API Features over the shared shard,
// now via Huma v2 (OpenAPI 3.1 generated from the registered operations).
//
// Byte-heavy routes (items, item, tiles) use raw huma.Context handlers so
// ETags, gzip variants, conditional requests and status codes match the
// historical contract exactly; the operations still appear in the spec.
package ogc

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/plan"
	"github.com/adonm/lakewing/internal/store"
	"github.com/adonm/lakewing/internal/tiles"
	"github.com/danielgtaylor/huma/v2"
)

const GeoJSON = "application/geo+json"
const cacheControl = "public, max-age=60"
const vary = "Accept, Accept-Encoding, X-Source-Ids"

var Conformance = []string{
	"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
	"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
	"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
}

// raw routes a handler with full byte control while also documenting the
// operation in Huma's generated spec (Adapter.Handle alone only routes).
func raw(api huma.API, op huma.Operation, params []*huma.Param, handler func(huma.Context)) {
	api.Adapter().Handle(&op, handler)
	op.Parameters = params
	item := api.OpenAPI().Paths[op.Path]
	if item == nil {
		item = &huma.PathItem{}
		api.OpenAPI().Paths[op.Path] = item
	}
	get := op
	if op.Method == http.MethodGet {
		item.Get = &get
	}
}

func qp(name, desc string) *huma.Param {
	return &huma.Param{Name: name, In: "query", Description: desc}
}

func pp(name, desc string) *huma.Param {
	return &huma.Param{Name: name, In: "path", Required: true, Description: desc}
}

// Register wires all OGC routes onto the Huma API.
func Register(api huma.API, st *store.Store) {
	huma.Register(api, huma.Operation{
		OperationID: "landing", Method: http.MethodGet, Path: "/",
		Summary: "Landing page",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body map[string]any
	}, error) {
		out := &struct{ Body map[string]any }{}
		out.Body = map[string]any{"title": "lakewing",
			"description": "OGC Features and Arrow Flight over a DuckDB shard",
			"links": []any{
				link("/", "self", "application/json"),
				link("/api", "service-desc", "application/vnd.oai.openapi+json;version=3.0"),
				link("/docs", "service-doc", "text/html"),
				link("/conformance", "conformance", "application/json"),
				link("/collections", "data", "application/json"),
			}}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "conformance", Method: http.MethodGet, Path: "/conformance",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body map[string]any
	}, error) {
		out := &struct{ Body map[string]any }{}
		out.Body = map[string]any{"conformsTo": Conformance}
		return out, nil
	})

	raw(api, huma.Operation{
		OperationID: "health", Method: http.MethodGet, Path: "/healthz",
		Summary: "Liveness",
	}, nil, func(ctx huma.Context) {
		writeBody(ctx, 200, []byte("ok"), "text/plain; charset=utf-8", "no-store")
	})

	raw(api, huma.Operation{
		OperationID: "metrics", Method: http.MethodGet, Path: "/metrics",
		Summary: "Point-in-time counters",
	}, nil, func(ctx huma.Context) {
		writeBody(ctx, 200, []byte(st.Metrics()), "text/plain; charset=utf-8", "no-store")
	})

	huma.Register(api, huma.Operation{
		OperationID: "collections", Method: http.MethodGet, Path: "/collections",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body map[string]any
	}, error) {
		out := &struct{ Body map[string]any }{}
		cols := make([]any, 0, len(st.Collections))
		for _, c := range st.Collections {
			cols = append(cols, metadata(c))
		}
		out.Body = map[string]any{"collections": cols,
			"links": []any{link("/collections", "self", "application/json")}}
		return out, nil
	})

	raw(api, huma.Operation{
		OperationID: "collection", Method: http.MethodGet, Path: "/collections/{collection}",
		Summary: "Collection metadata",
	}, []*huma.Param{pp("collection", "Collection id")}, func(ctx huma.Context) {
		id := ctx.Param("collection")
		if err := st.Collection(id); err != nil {
			fail(ctx, st, err)
			return
		}
		body, _ := json.Marshal(metadata(id))
		writeBody(ctx, 200, body, "application/json", cacheControl)
	})

	raw(api, huma.Operation{
		OperationID: "items", Method: http.MethodGet, Path: "/collections/{collection}/items",
		Summary: "Collection features as GeoJSON",
	}, []*huma.Param{
		pp("collection", "Collection id"),
		qp("bbox", "CRS84 bounds w,s,e,n (west > east crosses the antimeridian)"),
		qp("limit", "Page size 1..1000 (default 10)"),
		qp("offset", "Rows to skip (default 0; cursor wins)"),
		qp("cursor", "Exclusive lower bound on feature id (next links)"),
		qp("datetime", "RFC3339 instant or interval (validated only)"),
		qp("sources", "Comma-separated source ids (intersects X-Source-Ids)"),
	}, func(ctx huma.Context) {
		itemsHandler(ctx, st)
	})

	raw(api, huma.Operation{
		OperationID: "item", Method: http.MethodGet, Path: "/collections/{collection}/items/{featureId}",
		Summary: "Single feature",
	}, []*huma.Param{
		pp("collection", "Collection id"),
		pp("featureId", "Feature id"),
		qp("sources", "Comma-separated source ids (intersects X-Source-Ids)"),
	}, func(ctx huma.Context) {
		itemHandler(ctx, st)
	})

	raw(api, huma.Operation{
		OperationID: "tile", Method: http.MethodGet, Path: "/collections/{collection}/tiles/{z}/{x}/{y}",
		Summary: "XYZ vector tile",
	}, []*huma.Param{
		pp("collection", "Collection id"),
		pp("z", "Zoom level"),
		pp("x", "Tile column"),
		pp("y", "Tile row"),
		qp("sources", "Comma-separated source ids (intersects X-Source-Ids)"),
	}, func(ctx huma.Context) {
		tileHandler(ctx, st)
	})

	raw(api, huma.Operation{
		OperationID: "spec", Method: http.MethodGet, Path: "/api",
		Summary: "OpenAPI description (3.0, generated)",
	}, nil, func(ctx huma.Context) {
		// Single source: Huma's registry. /api stays on the 3.0
		// downgrade to match the oas30 conformance claim and the
		// historical versioned content type; /api.json serves 3.1.
		ctx.SetHeader("Location", "/api-3.0.json")
		ctx.SetStatus(302)
	})

	raw(api, huma.Operation{
		OperationID: "apidocs", Method: http.MethodGet, Path: "/api.html",
		Summary: "API documentation",
	}, nil, func(ctx huma.Context) {
		ctx.SetHeader("Location", "/docs")
		ctx.SetStatus(302)
	})
}

func link(href, rel, kind string) map[string]any {
	return map[string]any{"href": href, "rel": rel, "type": kind}
}

func metadata(id string) map[string]any {
	return map[string]any{"id": id, "title": id, "itemType": "feature", "links": []any{
		link("/collections/"+id, "self", "application/json"),
		link("/collections/"+id+"/items", "items", GeoJSON),
	}}
}

func writeBody(ctx huma.Context, status int, body []byte, contentType, cache string) {
	ctx.SetHeader("Content-Type", contentType)
	ctx.SetHeader("Vary", vary)
	ctx.SetHeader("Cache-Control", cache)
	ctx.SetStatus(status)
	ctx.BodyWriter().Write(body)
}

// fail mirrors poem::error::ResponseError for store::Error.
func fail(ctx huma.Context, st *store.Store, err error) {
	status := 500
	desc := "shard query failed"
	if se, ok := err.(*store.StoreError); ok {
		switch se.Kind {
		case "invalid":
			status, desc = 400, se.Message
		case "notfound":
			status, desc = 404, se.Message
		case "overloaded":
			status, desc = 429, se.Message
		}
		if se.Kind == "backend" {
			slog.Error("shard query failed", "err", se.Message)
		}
	} else if err == sql.ErrNoRows {
		status, desc = 404, "not found"
	} else if err != nil {
		slog.Error("shard query failed", "err", err.Error())
	}
	st.CountResponse(status)
	body, _ := json.Marshal(map[string]any{"code": status, "description": desc})
	ctx.SetHeader("Content-Type", "application/json")
	ctx.SetHeader("Vary", vary)
	ctx.SetHeader("Cache-Control", "no-store")
	if status == 429 {
		ctx.SetHeader("Retry-After", "1")
	}
	ctx.SetStatus(status)
	ctx.BodyWriter().Write(body)
}

// success serves a fresh body with ETag + conditional + gzip negotiation,
// mirroring api::body_response. MVT callers pass gz=false (identity only).
func success(ctx huma.Context, st *store.Store, body store.CachedBody, contentType string, gz bool) {
	ifNoneMatch := ctx.Header("If-None-Match")
	if etagMatches(ifNoneMatch, body.ETag) {
		st.CountResponse(304)
		notModified(ctx, body.ETag)
		return
	}
	if gz && wantsGzip(ctx.Header("Accept-Encoding")) {
		gzipped, err := store.GzipBody(body.Bytes)
		if err == nil {
			gb := store.WithBytes(gzipped)
			if etagMatches(ifNoneMatch, gb.ETag) {
				st.CountResponse(304)
				notModified(ctx, gb.ETag)
				return
			}
			ctx.SetHeader("Content-Type", contentType)
			ctx.SetHeader("Content-Encoding", "gzip")
			ctx.SetHeader("Vary", vary)
			ctx.SetHeader("ETag", gb.ETag)
			ctx.SetHeader("Cache-Control", cacheControl)
			st.CountResponse(200)
			ctx.SetStatus(200)
			ctx.BodyWriter().Write(gb.Bytes)
			return
		}
	}
	ctx.SetHeader("Content-Type", contentType)
	ctx.SetHeader("Vary", vary)
	ctx.SetHeader("ETag", body.ETag)
	ctx.SetHeader("Cache-Control", cacheControl)
	st.CountResponse(200)
	ctx.SetStatus(200)
	ctx.BodyWriter().Write(body.Bytes)
}

func notModified(ctx huma.Context, etag string) {
	ctx.SetHeader("Vary", vary)
	ctx.SetHeader("ETag", etag)
	ctx.SetHeader("Cache-Control", cacheControl)
	ctx.SetStatus(304)
}

func etagMatches(header, etag string) bool {
	if header == "" {
		return false
	}
	wanted := strings.Trim(header, `"`)
	_ = wanted
	want := strings.Trim(etag, `"`)
	for _, cand := range strings.Split(header, ",") {
		cand = strings.TrimSpace(cand)
		if cand == "*" {
			return true
		}
		cand = strings.TrimPrefix(cand, "W/")
		if strings.Trim(cand, `"`) == want {
			return true
		}
	}
	return false
}

func wantsGzip(header string) bool {
	if header == "" {
		return false
	}
	for _, r := range strings.Split(header, ",") {
		parts := strings.Split(r, ";")
		if !strings.EqualFold(strings.TrimSpace(parts[0]), "gzip") {
			continue
		}
		q := 1.0
		for _, p := range parts[1:] {
			if v, ok := strings.CutPrefix(strings.TrimSpace(p), "q="); ok {
				if f, err := strconv.ParseFloat(v, 32); err == nil {
					q = f
				} else {
					q = 0
				}
			}
		}
		if q > 0 {
			return true
		}
	}
	return false
}

// geojsonType mirrors api::geojson_type: most-specific Accept range wins.
func geojsonType(ctx huma.Context) (string, bool) {
	accept := ctx.Header("Accept")
	if accept == "" {
		return GeoJSON, true
	}
	bestSpec, bestQ := -1, 0.0
	for _, r := range strings.Split(accept, ",") {
		parts := strings.Split(r, ";")
		media := strings.TrimSpace(parts[0])
		var spec int
		switch media {
		case GeoJSON:
			spec = 2
		case "application/*":
			spec = 1
		case "*/*":
			spec = 0
		default:
			continue
		}
		q := 1.0
		for _, p := range parts[1:] {
			if v, ok := strings.CutPrefix(strings.TrimSpace(p), "q="); ok {
				if f, err := strconv.ParseFloat(v, 32); err == nil {
					q = f
				} else {
					q = 0
				}
			}
		}
		if spec > bestSpec {
			bestSpec, bestQ = spec, q
		}
	}
	if bestQ > 0 && bestQ <= 1 {
		return GeoJSON, true
	}
	return "", false
}

// cellString normalizes a DuckDB value cell to text. The Go binding maps
// the JSON logical type (ST_AsGeoJSON output, properties) to
// map[string]any; VARCHAR arrives as string.
func cellString(v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "", nil
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case map[string]any:
		b, err := json.Marshal(t)
		return string(b), err
	default:
		return fmt.Sprintf("%v", t), nil
	}
}

// rejectUnknown mirrors #[serde(deny_unknown_fields)] on query structs.
func rejectUnknown(ctx huma.Context, known ...string) *store.StoreError {
	allowed := map[string]bool{}
	for _, k := range known {
		allowed[k] = true
	}
	u := ctx.URL()
	for key := range u.Query() {
		if !allowed[key] {
			return store.Invalid("unknown query parameter: " + key)
		}
	}
	return nil
}

func sourceIDs(ctx huma.Context, requested string, hasRequested bool) ([]int64, error) {
	var q, h []int64
	header := ctx.Header("X-Source-Ids")
	hasH := header != ""
	var err error
	if hasRequested {
		if q, err = filter.ParseSources(requested); err != nil {
			return nil, err
		}
	}
	if hasH {
		if h, err = filter.ParseSources(header); err != nil {
			return nil, err
		}
	}
	return filter.Sources(q, h, hasRequested, hasH), nil
}

func featureLinks(id, collection string, sources []int64) []any {
	strs := make([]string, len(sources))
	for i, s := range sources {
		strs[i] = strconv.FormatInt(s, 10)
	}
	href := "/collections/" + collection + "/items/" + url.PathEscape(id) + "?sources=" + strings.Join(strs, ",")
	return []any{
		link(href, "self", GeoJSON),
		link("/collections/"+collection, "collection", "application/json"),
	}
}

func renderFeature(collection, id string, geomJSON, props []byte, sources []int64) []byte {
	var geom, p json.RawMessage
	geom = bytes.TrimSpace(geomJSON)
	if len(geom) == 0 {
		geom = json.RawMessage("null")
	}
	p = bytes.TrimSpace(props)
	if len(p) == 0 {
		p = json.RawMessage("{}")
	}
	feat := map[string]any{
		"type": "Feature", "id": id,
		"geometry": geom, "properties": p,
		"links": featureLinks(id, collection, sources),
	}
	b, _ := json.Marshal(feat)
	return b
}

func itemsHandler(ctx huma.Context, st *store.Store) {
	st.CountRequest()
	collection := ctx.Param("collection")
	if err := st.Collection(collection); err != nil {
		fail(ctx, st, err)
		return
	}
	kind, ok := geojsonType(ctx)
	if !ok {
		ctx.SetHeader("Content-Type", "application/json")
		ctx.SetHeader("Vary", vary)
		ctx.SetHeader("Cache-Control", "no-store")
		ctx.SetStatus(406)
		body, _ := json.Marshal(map[string]any{"code": 406, "description": "not acceptable"})
		ctx.BodyWriter().Write(body)
		return
	}
	if err := rejectUnknown(ctx, "bbox", "limit", "offset", "cursor", "datetime", "sources"); err != nil {
		fail(ctx, st, err)
		return
	}
	u := ctx.URL()
	q := u.Query()
	limit := 10
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fail(ctx, st, store.Invalid("limit must be an integer"))
			return
		}
		limit = n
	}
	if limit < 1 || limit > 1000 {
		fail(ctx, st, store.Invalid("limit must be between 1 and 1000"))
		return
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			fail(ctx, st, store.Invalid("offset must be a non-negative integer"))
			return
		}
		offset = n
	}
	var bounds *[4]float64
	if v := q.Get("bbox"); v != "" {
		b, err := filter.ParseBBox(v)
		if err != nil {
			fail(ctx, st, store.Invalid(err.Error()))
			return
		}
		bounds = &b
	}
	var dt *string
	if v := q.Get("datetime"); v != "" {
		if err := filter.ValidateDatetime(v); err != nil {
			fail(ctx, st, store.Invalid(err.Error()))
			return
		}
		dt = &v
	}
	_, hasSources := q["sources"]
	sources, err := sourceIDs(ctx, q.Get("sources"), hasSources)
	if err != nil {
		fail(ctx, st, store.Invalid(err.Error()))
		return
	}
	var pagination plan.Pagination
	if c := q.Get("cursor"); c != "" {
		pagination = plan.Pagination{Cursor: &c}
	} else {
		pagination = plan.Pagination{Offset: uint32(offset)}
	}
	req := plan.ItemsRequest{
		Collection: collection, Sources: sources, Bounds: bounds,
		Limit: uint32(limit), Pagination: pagination, Datetime: dt,
	}
	href := req.Href()
	heavy := plan.IsHeavy(uint32(limit), pagination, bounds)
	fetch := store.Predicate(collection, bounds, sources)
	pageWhere, pageTail := plan.PageParts(fetch, uint32(limit), pagination)
	// Two-phase only for deep offsets: OFFSET forces a total sort, where
	// narrow-id sort + join-back wins. Cursor/limit pages use top-N
	// heapsort and a second scan would only add probe overhead.
	deep := pagination.Cursor == nil && pagination.Offset >= 1000
	var query string
	if st.Lance() == nil {
		from := st.ReadSource(bounds)
		query = plan.ItemsSQL(req, pageWhere+" "+pageTail, from)
		if deep {
			query = plan.HeavyItemsSQL(from, pageWhere, pageTail)
		}
	}

	type row struct{ id, geom, props string }
	var rows []row
	qerr := st.Query(ctx.Context(), heavy, func(qctx context.Context, c *sql.Conn) error {
		run := query
		if st.Lance() != nil {
			q, drop, err := lanceItemsQuery(qctx, st, c, collection, bounds, sources,
				pageWhere, pageTail, deep, int(limit), pagination.Offset)
			if err != nil {
				return err
			}
			defer drop()
			run = q
		}
		r, err := c.QueryContext(qctx, run)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			var id string
			var geomRaw, propsRaw any
			if err := r.Scan(&id, &geomRaw, &propsRaw); err != nil {
				return err
			}
			geom, err := cellString(geomRaw)
			if err != nil {
				return err
			}
			props, err := cellString(propsRaw)
			if err != nil {
				return err
			}
			if geom == "" {
				geom = "null"
			}
			if props == "" {
				props = "{}"
			}
			rows = append(rows, row{id: id, geom: geom, props: props})
		}
		return r.Err()
	})
	if qerr != nil {
		fail(ctx, st, qerr)
		return
	}
	hasNext := len(rows) > limit
	if hasNext {
		rows = rows[:limit]
	}
	feats := make([]json.RawMessage, 0, len(rows))
	for _, r := range rows {
		feats = append(feats, renderFeature(collection, r.id, []byte(r.geom), []byte(r.props), sources))
	}
	links := []any{
		link(href, "self", GeoJSON),
		link("/collections/"+collection, "collection", "application/json"),
	}
	if hasNext && len(rows) > 0 {
		last := rows[len(rows)-1].id
		next := req
		next.Pagination = plan.Pagination{Cursor: &last}
		links = append(links, link("/collections/"+collection+"/items?"+next.CanonicalQS(), "next", GeoJSON))
	}
	var buf bytes.Buffer
	buf.WriteString(`{"type":"FeatureCollection","numberReturned":`)
	buf.WriteString(strconv.Itoa(len(feats)))
	buf.WriteString(`,"features":[`)
	for i, f := range feats {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(f)
	}
	buf.WriteString(`],"links":`)
	lb, _ := json.Marshal(links)
	buf.Write(lb)
	buf.WriteByte('}')
	success(ctx, st, store.WithBytes(buf.Bytes()), kind, true)
}

func itemHandler(ctx huma.Context, st *store.Store) {
	st.CountRequest()
	collection := ctx.Param("collection")
	id := ctx.Param("featureId")
	if err := st.Collection(collection); err != nil {
		fail(ctx, st, err)
		return
	}
	kind, ok := geojsonType(ctx)
	if !ok {
		ctx.SetHeader("Content-Type", "application/json")
		ctx.SetHeader("Vary", vary)
		ctx.SetHeader("Cache-Control", "no-store")
		ctx.SetStatus(406)
		body, _ := json.Marshal(map[string]any{"code": 406, "description": "not acceptable"})
		ctx.BodyWriter().Write(body)
		return
	}
	if err := rejectUnknown(ctx, "sources"); err != nil {
		fail(ctx, st, err)
		return
	}
	u := ctx.URL()
	q := u.Query()
	_, hasSources := q["sources"]
	sources, err := sourceIDs(ctx, q.Get("sources"), hasSources)
	if err != nil {
		fail(ctx, st, store.Invalid(err.Error()))
		return
	}
	if len(sources) == 0 {
		fail(ctx, st, store.NotFound(id))
		return
	}
	srcs := make([]string, len(sources))
	for i, s := range sources {
		srcs[i] = strconv.FormatInt(s, 10)
	}
	from := st.ReadSource(nil)
	query := fmt.Sprintf("SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM %s WHERE id = %s AND layer = %s AND source_id IN (%s) LIMIT 1",
		from, filter.Quote(id), filter.Quote(collection), strings.Join(srcs, ","))
	var rawID string
	var geomRaw, propsRaw any
	qerr := st.Query(ctx.Context(), false, func(qctx context.Context, c *sql.Conn) error {
		run := query
		if st.Lance() != nil {
			q, drop, err := lanceItemQuery(qctx, st, c, id, collection, sources)
			if err != nil {
				return err
			}
			defer drop()
			run = q
		}
		return c.QueryRowContext(qctx, run).Scan(&rawID, &geomRaw, &propsRaw)
	})
	if qerr != nil {
		if qerr == sql.ErrNoRows {
			fail(ctx, st, store.NotFound(id))
			return
		}
		fail(ctx, st, qerr)
		return
	}
	g, err := cellString(geomRaw)
	if err != nil {
		fail(ctx, st, store.Backend(err.Error()))
		return
	}
	if g == "" {
		g = "null"
	}
	p, err := cellString(propsRaw)
	if err != nil {
		fail(ctx, st, store.Backend(err.Error()))
		return
	}
	if p == "" {
		p = "{}"
	}
	success(ctx, st, store.WithBytes(renderFeature(collection, rawID, []byte(g), []byte(p), sources)), kind, true)
}

func tileHandler(ctx huma.Context, st *store.Store) {
	st.CountRequest()
	collection := ctx.Param("collection")
	if err := st.Collection(collection); err != nil {
		fail(ctx, st, err)
		return
	}
	if err := rejectUnknown(ctx, "sources"); err != nil {
		fail(ctx, st, err)
		return
	}
	z64, err := strconv.ParseUint(ctx.Param("z"), 10, 8)
	if err != nil {
		fail(ctx, st, store.Invalid("tile coordinates outside XYZ matrix"))
		return
	}
	x64, err := strconv.ParseUint(ctx.Param("x"), 10, 32)
	if err != nil {
		fail(ctx, st, store.Invalid("tile coordinates outside XYZ matrix"))
		return
	}
	y64, err := strconv.ParseUint(ctx.Param("y"), 10, 32)
	if err != nil {
		fail(ctx, st, store.Invalid("tile coordinates outside XYZ matrix"))
		return
	}
	z, x, y := uint8(z64), uint32(x64), uint32(y64)
	if err := tiles.ValidateTile(z, x, y); err != nil {
		fail(ctx, st, store.Invalid(err.Error()))
		return
	}
	u := ctx.URL()
	q := u.Query()
	_, hasSources := q["sources"]
	sources, err := sourceIDs(ctx, q.Get("sources"), hasSources)
	if err != nil {
		fail(ctx, st, store.Invalid(err.Error()))
		return
	}
	bbox := tiles.XYZToBBox(z, x, y)
	from := st.ReadSource(&bbox)
	fetch := store.Predicate(collection, &bbox, sources)
	half := 6378137.0 * 3.141592653589793
	span := 2.0 * half / float64(uint64(1)<<z)
	west := -half + float64(x)*span
	north := half - float64(y)*span
	query := tiles.MVTSQL(collection, from, fetch, west, north-span, west+span, north)
	var tile []byte
	qerr := st.Query(ctx.Context(), true, func(qctx context.Context, c *sql.Conn) error {
		var raw []byte
		if err := c.QueryRowContext(qctx, query).Scan(&raw); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		tile = raw
		return nil
	})
	if qerr != nil {
		fail(ctx, st, qerr)
		return
	}
	if len(tile) == 0 {
		ctx.SetHeader("Vary", vary)
		ctx.SetHeader("Cache-Control", cacheControl)
		ctx.SetStatus(204)
		return
	}
	success(ctx, st, store.WithBytes(tile), "application/vnd.mapbox-vector-tile", false)
}
