//go:build !duckdb_arrow

package flight

import (
	"context"
	"database/sql"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// streamBatches runs the query through database/sql rows and converts
// row-at-a-time through array builders. Used when the duckdb_arrow build
// tag is off; prefer the native Arrow export otherwise.
func streamBatches(ctx context.Context, c *sql.Conn, query string, cols []string, schema *arrow.Schema, w *flight.Writer, mem memory.Allocator) error {
	rows, err := c.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	flush := func() error {
		batch := rb.NewRecordBatch()
		defer batch.Release()
		return w.Write(batch)
	}
	names, err := rows.Columns()
	if err != nil {
		return err
	}
	_ = names
	n := 0
	wrote := false
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		if err := appendRow(rb, cols, vals); err != nil {
			return err
		}
		n++
		if n >= batchRows {
			wrote = true
			if err := flush(); err != nil {
				return err
			}
			n = 0
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n > 0 || !wrote {
		if err := flush(); err != nil {
			return err
		}
	}
	return nil
}
