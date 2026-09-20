// Command lakewing: build a DuckLake snapshot (direct S3 writes), then
// serve it through Huma OGC REST and Arrow Flight (CSI mount reads).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"

	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/flight"
	"github.com/adonm/lakewing/internal/materialize"
	"github.com/adonm/lakewing/internal/ogc"
	"github.com/adonm/lakewing/internal/store"
)

func main() {
	root := &cobra.Command{Use: "lakewing", Short: "DuckLake snapshots over S3; Huma OGC + Arrow Flight"}
	root.AddCommand(buildCmd(), indexCmd(), serveCmd())
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func buildCmd() *cobra.Command {
	var from, collection, bboxRaw, out, dataDir, dataURL, sort string
	var limit int64
	var fileMB, rowGroup int
	var sourceID int64
	var contentAddress bool
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build a DuckLake snapshot (direct S3 writes)",
		RunE: func(cmd *cobra.Command, args []string) error {
			bbox, err := filter.ParseBBox(bboxRaw)
			if err != nil {
				return err
			}
			spec := materialize.Spec{
				From: from, Collection: collection, BBox: bbox,
				Out: out, DataDir: dataDir, DataURL: dataURL,
				FileMB: uint64(fileMB), RowGroup: uint64(rowGroup),
				Sort: materialize.SortOrder(sort), SourceID: sourceID,
				ContentAddress: contentAddress,
			}
			if limit > 0 {
				spec.Limit = &limit
			}
			_, err = materialize.Run(cmd.Context(), spec)
			return err
		},
	}
	cmd.Flags().StringVar(&from, "from", "https://data.openstreetmap.us/layercake/buildings.parquet", "input GeoParquet")
	cmd.Flags().StringVar(&collection, "collection", "buildings", "collection id")
	cmd.Flags().StringVar(&bboxRaw, "bbox", "", "required CRS84 w,s,e,n")
	cmd.Flags().StringVar(&out, "out", "fixtures/osm.ducklake", "output catalog path")
	cmd.Flags().StringVar(&dataDir, "data-dir", "fixtures/osm.files", "staging data dir")
	cmd.Flags().StringVar(&dataURL, "data-url", "", "portable DATA_PATH (s3:// prefix for lake publishes)")
	cmd.Flags().Int64Var(&limit, "limit", 0, "development row cap (0 = none)")
	cmd.Flags().IntVar(&fileMB, "file-mb", 32, "target parquet file size")
	cmd.Flags().IntVar(&rowGroup, "row-group", 8192, "parquet row group size")
	cmd.Flags().StringVar(&sort, "sort", "grid", "grid|hilbert|none")
	cmd.Flags().Int64Var(&sourceID, "source-id", 1, "source id stamp")
	cmd.Flags().BoolVar(&contentAddress, "content-address", false, "name data files by content hash")
	_ = cmd.MarkFlagRequired("bbox")
	return cmd
}

func indexCmd() *cobra.Command {
	var shard, dataDir, out string
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Compile a serving index sidecar for an existing catalog",
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				out = shard + ".serving.json"
			}
			abs, err := filepath.Abs(dataDir)
			if err != nil {
				return err
			}
			doc, err := materialize.GenerateIndex(cmd.Context(), shard, abs)
			if err != nil {
				return err
			}
			pretty, _ := json.MarshalIndent(doc, "", "  ")
			if dir := filepath.Dir(out); dir != "" {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return err
				}
			}
			if err := os.WriteFile(out, pretty, 0o644); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "indexed %d files in %d partitions -> %s\n",
				len(doc.Files), len(doc.Partitions), out)
			return nil
		},
	}
	cmd.Flags().StringVar(&shard, "shard", "", "catalog path (mount path)")
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "data root (mount path)")
	cmd.Flags().StringVar(&out, "out", "", "output sidecar (default <shard>.serving.json)")
	_ = cmd.MarkFlagRequired("shard")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}

func serveCmd() *cobra.Command {
	var shard, dataRoot string
	var listen, flightListen string
	var connections int
	var maxWaitMS uint64
	var maxWaiters int
	var flightConcurrency int
	var threads int64
	var memoryMB uint64
	var queryTimeoutMS uint64
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve a pinned snapshot: Huma OGC + Arrow Flight (CSI mount reads)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
			defer stop()
			bulkLimit := connections
			if flightConcurrency > 0 {
				bulkLimit = flightConcurrency
			}
			if threads <= 0 {
				// Auto: one DuckDB thread per CPU. Measured faster than 1
				// across the whole battery (3-5x on heavy pages, no
				// throughput loss under concurrency); pass an explicit
				// small value only to cap CPU on shared boxes.
				threads = int64(runtime.NumCPU())
			}
			st, err := store.Open(ctx, store.Config{
				Location: shard, DataRoot: dataRoot,
				Connections: connections, MaxWaiters: maxWaiters,
				MaxWait:   time.Duration(maxWaitMS) * time.Millisecond,
				BulkLimit: bulkLimit, Threads: threads, MemoryMB: memoryMB,
				QueryTimeout: time.Duration(queryTimeoutMS) * time.Millisecond,
			})
			if err != nil {
				return err
			}
			defer st.Close()
			slog.Info("serving shard", "shard", shard, "snapshot", st.Snapshot, "http", listen, "flight", flightListen)

			router := chi.NewMux()
			cfg := huma.DefaultConfig("lakewing", "0.1.0")
			cfg.OpenAPIPath = "/api"
			api := humachi.New(router, cfg)
			ogc.Register(api, st)

			httpSrv := &http.Server{Addr: listen, Handler: router}
			lc, err := net.Listen("tcp", listen)
			if err != nil {
				return err
			}
			// A bind/runtime failure in either listener terminates the service.
			errCh := make(chan error, 2)
			go func() {
				if err := httpSrv.Serve(lc); err != http.ErrServerClosed {
					errCh <- err
				}
			}()
			go func() {
				if err := flight.Serve(ctx, flightListen, st); err != nil {
					errCh <- err
				}
			}()
			select {
			case err := <-errCh:
				return err
			case <-ctx.Done():
				shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = httpSrv.Shutdown(shutCtx)
				return nil
			}
		},
	}
	cmd.Flags().StringVar(&shard, "shard", "", "mount path to the .ducklake catalog (e.g. /mnt/lake/catalogs/<sha>.ducklake)")
	cmd.Flags().StringVar(&dataRoot, "data-root", "", "mount path to the data root (DATA_PATH override)")
	cmd.Flags().StringVar(&listen, "listen", "0.0.0.0:3000", "HTTP listen addr")
	cmd.Flags().StringVar(&flightListen, "flight-listen", "127.0.0.1:50051", "Flight listen addr")
	cmd.Flags().IntVar(&connections, "connections", 8, "pooled DuckDB connections")
	cmd.Flags().Uint64Var(&maxWaitMS, "max-wait-ms", 250, "queue wait before 429")
	cmd.Flags().IntVar(&maxWaiters, "max-waiters", 128, "max queued requests")
	cmd.Flags().IntVar(&flightConcurrency, "flight-concurrency", 0, "bulk lane cap (0 = pool size)")
	cmd.Flags().Int64Var(&threads, "threads", 0, "shared DuckDB threads (0 = NumCPU)")
	cmd.Flags().Uint64Var(&memoryMB, "memory-mb", 4096, "shared DuckDB memory MiB (0 = default)")
	cmd.Flags().Uint64Var(&queryTimeoutMS, "query-timeout-ms", 30000, "query deadline ms (0 = none)")
	_ = cmd.MarkFlagRequired("shard")
	return cmd
}
