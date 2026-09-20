package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/adonm/lakewing/internal/dbutil"
	"github.com/adonm/lakewing/internal/filter"
)

// setupSession mirrors store::setup_session: extensions (LOAD, falling
// back to INSTALL+LOAD), then the lockdown.
func setupSession(ctx context.Context, c *sql.Conn, tempDir string) error {
	for _, ext := range []string{"ducklake", "spatial", "httpfs"} {
		if _, err := c.ExecContext(ctx, "LOAD "+ext); err != nil {
			if _, err := c.ExecContext(ctx, "INSTALL "+ext); err != nil {
				return fmt.Errorf("INSTALL %s: %w", ext, err)
			}
			if _, err := c.ExecContext(ctx, "LOAD "+ext); err != nil {
				return fmt.Errorf("LOAD %s: %w", ext, err)
			}
		}
	}
	stmts := []string{
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET parquet_metadata_cache=true",
		"SET enable_http_metadata_cache=true",
		"SET validate_external_file_cache='NO_VALIDATION'",
		"SET late_materialization_max_rows=0",
	}
	// S3_DIRECT mode: point DuckDB at S3 (normally the node-local s3cache
	// proxy). Catalog/data locations are s3:// URLs; writers still go
	// direct to the origin. The proxy is
	// the sole signer, so workers need no credentials here: a secret
	// without KEY_ID gives anonymous access to the proxy.
	if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
		// OR REPLACE: setupSession runs on every pooled connection of the
		// same DuckDB instance, and secrets live per instance.
		secretSQL := fmt.Sprintf("CREATE OR REPLACE SECRET s3direct (TYPE S3, ENDPOINT %s, URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')", filter.Quote(endpoint))
		if id := os.Getenv("AWS_ACCESS_KEY_ID"); id != "" {
			secretSQL = fmt.Sprintf("CREATE OR REPLACE SECRET s3direct (TYPE S3, KEY_ID %s, SECRET %s, ENDPOINT %s, URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')",
				filter.Quote(id), filter.Quote(os.Getenv("AWS_SECRET_ACCESS_KEY")), filter.Quote(endpoint))
		}
		stmts = append(stmts, secretSQL)
	}
	if tempDir != "" {
		stmts = append(stmts, "SET temp_directory="+filter.Quote(tempDir))
	}
	return dbutil.ExecAll(ctx, c, stmts...)
}
