//go:build duckdb_arrow

package flight

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdbarrow "github.com/duckdb/duckdb-go/v2"
)

var errNotDuckDB = errors.New("not a duckdb driver connection")

// checkSchema fails loudly if the engine's Arrow mapping drifts from the
// Flight contract (names and physical types must agree; nullability may
// differ).
func checkSchema(got, want *arrow.Schema) error {
	if got.NumFields() != want.NumFields() {
		return fmt.Errorf("arrow schema field count %d != %d", got.NumFields(), want.NumFields())
	}
	for i := range want.Fields() {
		g, w := got.Field(i), want.Field(i)
		if g.Name != w.Name || g.Type.ID() != w.Type.ID() {
			return fmt.Errorf("arrow schema field %d: %s/%v != %s/%v",
				i, g.Name, g.Type, w.Name, w.Type)
		}
	}
	return nil
}

// streamBatches runs the query through DuckDB's native Arrow export
// (data-chunk to record-batch conversion inside the driver) and writes
// each batch to w.
func streamBatches(ctx context.Context, c *sql.Conn, query string, cols []string, schema *arrow.Schema, w *flight.Writer, mem memory.Allocator) error {
	var dc driver.Conn
	if err := c.Raw(func(conn any) error {
		var ok bool
		dc, ok = conn.(driver.Conn)
		if !ok {
			return errNotDuckDB
		}
		return nil
	}); err != nil {
		return err
	}
	arr, err := duckdbarrow.NewArrowFromConn(dc)
	if err != nil {
		return err
	}
	reader, err := arr.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer reader.Release()
	if err := checkSchema(reader.Schema(), schema); err != nil {
		return err
	}
	wrote := false
	for reader.Next() {
		wrote = true
		if err := w.Write(reader.Record()); err != nil {
			return err
		}
	}
	if err := reader.Err(); err != nil {
		return err
	}
	if !wrote {
		// Empty streams still carry the schema.
		rb := array.NewRecordBuilder(mem, schema)
		defer rb.Release()
		rec := rb.NewRecord()
		defer rec.Release()
		return w.Write(rec)
	}
	return nil
}
