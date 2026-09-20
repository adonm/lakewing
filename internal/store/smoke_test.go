// Smoke probe for the DuckDB 2.0 preview binding: version, extension
// loading (spatial, ducklake) and opening a real fixture catalog.
// Runs only with LAKEWING_TEST_SHARD set to a .ducklake path; skipped in CI.
package store

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestPreviewSmoke(t *testing.T) {
	shard := os.Getenv("LAKEWING_TEST_SHARD")
	if shard == "" {
		t.Skip("LAKEWING_TEST_SHARD unset")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("duckdb version: %s", version)
	for _, ext := range []string{"spatial", "ducklake"} {
		if _, err := db.Exec("INSTALL " + ext); err != nil {
			t.Fatalf("INSTALL %s: %v", ext, err)
		}
		if _, err := db.Exec("LOAD " + ext); err != nil {
			t.Fatalf("LOAD %s: %v", ext, err)
		}
	}
	if _, err := db.Exec("ATTACH 'ducklake:" + shard + "' AS shard (READ_ONLY)"); err != nil {
		t.Fatalf("ATTACH: %v", err)
	}
	if _, err := db.Exec("USE shard"); err != nil {
		t.Fatalf("USE: %v", err)
	}
	var n int64
	if err := db.QueryRow("SELECT count(*) FROM features").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("features: %d", n)
	if n == 0 {
		t.Fatal("empty catalog")
	}

	// Serve boot path: open the pooled store over the same catalog.
	// Local fixtures store an absolute DATA_PATH baked at build time, so
	// point the override at the fixture files dir (sibling <shard>.files).
	dataroot := os.Getenv("LAKEWING_TEST_DATAROOT")
	if dataroot == "" {
		dataroot = strings.TrimSuffix(shard, ".ducklake") + ".files"
	}
	st, err := Open(context.Background(), Config{
		Location: shard, DataRoot: dataroot, Connections: 2, MaxWaiters: 4,
		MaxWait: 5 * time.Second, BulkLimit: 2, Threads: 1,
		MemoryMB: 512, QueryTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer func() { _ = st.Close() }()
	t.Logf("snapshot=%d collections=%v serving_files=%d", st.Snapshot, st.Collections, st.ServingFiles())
	if len(st.Collections) == 0 || st.Snapshot <= 0 {
		t.Fatal("store opened without collections/snapshot")
	}
	if st.fallbackFrom == "features" {
		t.Fatal("serving file list failed closed to catalog table")
	}
	// One bbox-pruned read through the pool, exercising ReadSource + Predicate.
	bounds := [4]float64{4.3, 51.9, 4.5, 52.1}
	from := st.ReadSource(&bounds)
	fetch := Predicate("buildings", &bounds, []int64{1})
	var one string
	qerr := st.Query(context.Background(), false, func(ctx context.Context, c *sql.Conn) error {
		return c.QueryRowContext(ctx,
			"SELECT count(*)::VARCHAR FROM "+from+" WHERE "+fetch).Scan(&one)
	})
	if qerr != nil {
		t.Fatalf("pool query: %v", qerr)
	}
	t.Logf("bbox count: %s", one)
	// Temp spill dir reaches every pooled connection.
	dir := "/tmp/opencode/lw-tempdir"
	st2, err := Open(context.Background(), Config{
		Location: shard, DataRoot: dataroot, Connections: 1, MaxWaiters: 1,
		MaxWait: 5 * time.Second, BulkLimit: 1, Threads: 1,
		MemoryMB: 512, QueryTimeout: 30 * time.Second, TempDir: dir,
	})
	if err != nil {
		t.Fatalf("store.Open tempdir: %v", err)
	}
	defer func() { _ = st2.Close() }()
	var got string
	if err := st2.Query(context.Background(), false, func(ctx context.Context, c *sql.Conn) error {
		return c.QueryRowContext(ctx, "SELECT current_setting('temp_directory')").Scan(&got)
	}); err != nil {
		t.Fatalf("temp_directory: %v", err)
	}
	if got != dir {
		t.Fatalf("temp_directory = %q, want %q", got, dir)
	}
}
