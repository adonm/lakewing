// Flight round-trip against a real fixture catalog. Runs only with
// LAKEWING_TEST_SHARD set (same gate as the store smoke test).
package flight

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/adonm/lakewing/internal/store"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	shard := os.Getenv("LAKEWING_TEST_SHARD")
	if shard == "" {
		t.Skip("LAKEWING_TEST_SHARD unset")
	}
	dataroot := os.Getenv("LAKEWING_TEST_DATAROOT")
	if dataroot == "" {
		dataroot = strings.TrimSuffix(shard, ".ducklake") + ".files"
	}
	st, err := store.Open(context.Background(), store.Config{
		Location: shard, DataRoot: dataroot, Connections: 4, MaxWaiters: 8,
		MaxWait: 5 * time.Second, BulkLimit: 4, Threads: 1,
		MemoryMB: 1024, QueryTimeout: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestFlightRoundTrip(t *testing.T) {
	st := testStore(t)
	addr := "127.0.0.1:50199"
	// LAKEWING_TEST_FLIGHT_ADDR dials a live server instead of spinning one.
	if live := os.Getenv("LAKEWING_TEST_FLIGHT_ADDR"); live != "" {
		addr = live
	} else {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			_ = Serve(ctx, addr, st)
		}()
		time.Sleep(500 * time.Millisecond)
	}

	cl, err := flight.NewClientWithMiddleware(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close() //nolint:errcheck // test client

	// ListFlights: one entry per collection, empty criteria required.
	infos := 0
	stream, err := cl.ListFlights(context.Background(), &flightpb.Criteria{})
	if err != nil {
		t.Fatalf("ListFlights: %v", err)
	}
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
		infos++
	}
	if infos != len(st.Collections) {
		t.Fatalf("ListFlights returned %d infos, want %d", infos, len(st.Collections))
	}
	badStream, err := cl.ListFlights(context.Background(), &flightpb.Criteria{Expression: []byte("x")})
	if err != nil {
		t.Fatalf("ListFlights call: %v", err)
	}
	if _, err := badStream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("criteria error = %v, want InvalidArgument", err)
	}

	// DoGet round trip with a bbox ticket (LAKEWING_TEST_BBOX overrides the
	// default Amsterdam window, e.g. for the Berlin kind seed).
	limit := 100
	bbox := []float64{4.3, 51.9, 4.5, 52.1}
	if raw := os.Getenv("LAKEWING_TEST_BBOX"); raw != "" {
		var parsed []float64
		if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
			t.Fatalf("LAKEWING_TEST_BBOX: %v", err)
		}
		bbox = parsed
	}
	ticket := Ticket{
		Collection: "buildings",
		BBox:       bbox,
		Columns:    []string{"id", "x", "y", "name"},
		Limit:      &limit,
		Sources:    []int64{1},
	}
	raw, _ := json.Marshal(ticket)
	fdata, err := cl.DoGet(context.Background(), &flightpb.Ticket{Ticket: raw})
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	r, err := flight.NewRecordReader(fdata)
	if err != nil {
		t.Fatalf("record reader: %v", err)
	}
	defer r.Release()
	rows := 0
	var firstID string
	for r.Next() {
		rec := r.RecordBatch()
		rows += int(rec.NumRows())
		if firstID == "" && rec.NumRows() > 0 {
			if s, ok := rec.Column(0).(*array.String); ok {
				firstID = s.Value(0)
			}
		}
		rec.Release()
	}
	if err := r.Err(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	t.Logf("flight rows=%d first=%s schema=%v", rows, firstID, r.Schema())
	if rows == 0 || rows > 100 {
		t.Fatalf("unexpected row count %d", rows)
	}
	if firstID == "" {
		t.Fatal("empty first id")
	}

	// Unknown collection → NotFound; bad column → InvalidArgument. For
	// server-streaming RPCs the status surfaces on receive, not on call.
	expectCode := func(ticket []byte, want codes.Code) {
		t.Helper()
		fdata, err := cl.DoGet(context.Background(), &flightpb.Ticket{Ticket: ticket})
		if err != nil {
			if status.Code(err) != want {
				t.Fatalf("DoGet call err = %v, want %v", err, want)
			}
			return
		}
		r, err := flight.NewRecordReader(fdata)
		if err != nil {
			if status.Code(err) != want {
				t.Fatalf("reader err = %v, want %v", err, want)
			}
			return
		}
		defer r.Release()
		for r.Next() {
			r.RecordBatch().Release()
		}
		if got := status.Code(r.Err()); got != want {
			t.Fatalf("stream err = %v, want %v", r.Err(), want)
		}
	}
	bad, _ := json.Marshal(Ticket{Collection: "nope", Sources: []int64{1}})
	expectCode(bad, codes.NotFound)
	badCol, _ := json.Marshal(Ticket{Collection: "buildings", Columns: []string{"nope"}, Sources: []int64{1}})
	expectCode(badCol, codes.InvalidArgument)
}
