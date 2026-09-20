// s3cache is the node-local read-through S3 slice cache for DuckDB
// workers. One DaemonSet pod per node; readers point their S3 endpoint
// at the node-local address so every pod shares one NVMe copy.
//
//	UPSTREAM    S3 origin base URL, e.g. http://seaweed:8333 (required)
//	LISTEN      listen address, default :8080
//	CACHE_DIR   disk dir, default /cache (mount NVMe here)
//	CACHE_BYTES disk budget, default 10737418240 (10 GiB)
//	SLICE_BYTES slice size, default 1048576 (1 MiB)
//	FETCHERS    concurrent upstream slice fetches, default 8
package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/adonm/lakewing/internal/s3cache"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func bytesEnv(key string, fallback int64) int64 {
	v, err := strconv.ParseInt(os.Getenv(key), 10, 64)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func main() {
	upstream, err := url.Parse(os.Getenv("UPSTREAM"))
	if err != nil || upstream == nil || upstream.Host == "" {
		log.Fatal("UPSTREAM must be set to the S3 origin base URL")
	}
	proxy, err := s3cache.New(s3cache.Config{
		Upstream:   upstream,
		CacheDir:   env("CACHE_DIR", "/cache"),
		MaxBytes:   bytesEnv("CACHE_BYTES", 10<<30),
		SliceBytes: bytesEnv("SLICE_BYTES", 1<<20),
		Fetchers:   int(bytesEnv("FETCHERS", 8)),
		Timeout:    60 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	addr := env("LISTEN", ":8080")
	log.Printf("s3cache upstream=%s dir=%s", upstream.Redacted(), os.Getenv("CACHE_DIR"))
	server := &http.Server{Addr: addr, Handler: proxy, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}
