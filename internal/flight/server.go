// Flight serving: read-only Arrow Flight over the shard (ListFlights,
// GetFlightInfo, GetSchema, DoGet). Ticket + validation shapes live in
// flight.go; this file bridges DuckDB rows to Arrow record batches.
package flight

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	flightpb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/store"
)

const batchRows = 1024

// SchemaForColumns mirrors the Flight projection contract.
func SchemaForColumns(cols []string) (*arrow.Schema, error) {
	// All fields nullable: the native Arrow export marks every field
	// nullable, and the IPC writer requires exact schema agreement with
	// the streamed batches.
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		switch c {
		case "id":
			fields[i] = arrow.Field{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true}
		case "geometry":
			fields[i] = arrow.Field{Name: "geometry", Type: arrow.BinaryTypes.Binary, Nullable: true}
		case "properties":
			fields[i] = arrow.Field{Name: "properties", Type: arrow.BinaryTypes.String, Nullable: true}
		case "source_id":
			fields[i] = arrow.Field{Name: "source_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
		case "x":
			fields[i] = arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
		case "y":
			fields[i] = arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
		case "name":
			fields[i] = arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unknown column %q", c)
		}
	}
	return arrow.NewSchema(fields, nil), nil
}

// Server implements flightpb.FlightServiceServer (read-only).
type Server struct {
	flightpb.UnimplementedFlightServiceServer
	store *store.Store
	mem   memory.Allocator
}

// NewServer builds the Flight service over a store.
func NewServer(st *store.Store) *Server {
	return &Server{store: st, mem: memory.NewGoAllocator()}
}

func invalid(err error) error { return status.Errorf(codes.InvalidArgument, "%v", err) }

// descriptorToTicket mirrors flight::descriptor: one-part collection path
// (defaults) or a JSON command ticket.
func descriptorToTicket(desc *flightpb.FlightDescriptor) (Ticket, error) {
	switch desc.Type {
	case flightpb.FlightDescriptor_PATH:
		if len(desc.Path) == 1 {
			return Ticket{Collection: desc.Path[0]}, nil
		}
	case flightpb.FlightDescriptor_CMD:
		var t Ticket
		if err := json.Unmarshal(desc.Cmd, &t); err != nil {
			return t, err
		}
		return t, nil
	}
	return Ticket{}, fmt.Errorf("descriptor must be a one-part collection path or a JSON command")
}

// plannedTicket validates collection + columns + bbox + limit, returning
// the normalized ticket, bounds and sources (ticket ∩ header).
func (s *Server) plannedTicket(t Ticket, header []int64, hasHeader bool) (Ticket, *[4]float64, []int64, error) {
	if err := s.store.Collection(t.Collection); err != nil {
		return t, nil, nil, toStatus(err)
	}
	cols := t.Columns
	if cols == nil {
		cols = []string{"id", "geometry", "properties", "source_id"}
	}
	t.Columns = cols
	validated, _, _, err := t.Validate()
	if err != nil {
		return t, nil, nil, invalid(err)
	}
	t.Columns = validated
	var bounds *[4]float64
	if t.BBox != nil {
		b, err := filter.BBox(t.BBox)
		if err != nil {
			return t, nil, nil, invalid(err)
		}
		bounds = &b
	}
	hasRequested := t.Sources != nil
	sources := filter.Sources(t.Sources, header, hasRequested, hasHeader)
	return t, bounds, sources, nil
}

func toStatus(err error) error {
	if se, ok := err.(*store.StoreError); ok {
		switch se.Kind {
		case "invalid":
			return status.Errorf(codes.InvalidArgument, "%s", se.Message)
		case "notfound":
			return status.Errorf(codes.NotFound, "%s", se.Message)
		case "overloaded":
			return status.Errorf(codes.ResourceExhausted, "%s", se.Message)
		}
		return status.Errorf(codes.Internal, "shard query failed")
	}
	return status.Errorf(codes.Internal, "shard query failed")
}

func headerSources(streamCtx context.Context) ([]int64, bool) {
	md, ok := metadata.FromIncomingContext(streamCtx)
	if !ok {
		return nil, false
	}
	vals := md.Get("x-source-ids")
	if len(vals) == 0 {
		return nil, false
	}
	ids, err := filter.ParseSources(vals[0])
	if err != nil {
		return nil, false
	}
	return ids, true
}

func (s *Server) infoFor(t Ticket) (*flightpb.FlightInfo, error) {
	cols := t.Columns
	if cols == nil {
		cols = []string{"id", "geometry", "properties", "source_id"}
	}
	schema, err := SchemaForColumns(cols)
	if err != nil {
		return nil, err
	}
	ticketBytes, _ := json.Marshal(t)
	return &flightpb.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.mem),
		FlightDescriptor: nil,
		Endpoint: []*flightpb.FlightEndpoint{{
			Ticket:   &flightpb.Ticket{Ticket: ticketBytes},
			Location: []*flightpb.Location{{Uri: flight.LocationReuseConnection}},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// ListFlights rejects non-empty criteria; one FlightInfo per collection.
func (s *Server) ListFlights(crit *flightpb.Criteria, stream flightpb.FlightService_ListFlightsServer) error {
	if len(crit.GetExpression()) > 0 {
		return status.Errorf(codes.InvalidArgument, "criteria must be empty")
	}
	for _, collection := range s.store.Collections {
		t := Ticket{Collection: collection}
		info, err := s.infoFor(t)
		if err != nil {
			return err
		}
		info.FlightDescriptor = &flightpb.FlightDescriptor{
			Type: flightpb.FlightDescriptor_PATH, Path: []string{collection},
		}
		if err := stream.Send(info); err != nil {
			return err
		}
	}
	return nil
}

// GetFlightInfo resolves a descriptor to endpoints + schema.
func (s *Server) GetFlightInfo(ctx context.Context, desc *flightpb.FlightDescriptor) (*flightpb.FlightInfo, error) {
	t, err := descriptorToTicket(desc)
	if err != nil {
		return nil, invalid(err)
	}
	if _, _, _, err := s.plannedTicket(t, nil, false); err != nil {
		return nil, err
	}
	info, err := s.infoFor(t)
	if err != nil {
		return nil, err
	}
	info.FlightDescriptor = desc
	return info, nil
}

// GetSchema resolves a descriptor to its Arrow schema.
func (s *Server) GetSchema(ctx context.Context, desc *flightpb.FlightDescriptor) (*flightpb.SchemaResult, error) {
	t, err := descriptorToTicket(desc)
	if err != nil {
		return nil, invalid(err)
	}
	nt, _, _, err := s.plannedTicket(t, nil, false)
	if err != nil {
		return nil, err
	}
	schema, err := SchemaForColumns(nt.Columns)
	if err != nil {
		return nil, err
	}
	return &flightpb.SchemaResult{Schema: flight.SerializeSchema(schema, s.mem)}, nil
}

// DoGet streams the ticket query as Arrow record batches (schema always
// sent, even for empty streams).
func (s *Server) DoGet(tick *flightpb.Ticket, stream flightpb.FlightService_DoGetServer) error {
	var t Ticket
	if err := json.Unmarshal(tick.Ticket, &t); err != nil {
		return invalid(err)
	}
	header, hasHeader := headerSources(stream.Context())
	nt, _, sources, err := s.plannedTicket(t, header, hasHeader)
	if err != nil {
		return err
	}
	schema, err := SchemaForColumns(nt.Columns)
	if err != nil {
		return err
	}
	query, err := nt.SQL(s.store, sources)
	if err != nil {
		return invalid(err)
	}
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	qerr := s.store.Query(stream.Context(), true, func(ctx context.Context, c *sql.Conn) error {
		return streamBatches(ctx, c, query, nt.Columns, schema, w, s.mem)
	})
	if qerr != nil {
		return toStatus(qerr)
	}
	return nil
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int32:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case int:
		return int64(t), nil
	default:
		return 0, fmt.Errorf("not an integer: %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	default:
		return 0, fmt.Errorf("not a float: %T", v)
	}
}

func toBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	default:
		return nil, fmt.Errorf("not bytes: %T", v)
	}
}

func toJSONString(v any) (string, bool, error) {
	switch t := v.(type) {
	case nil:
		return "", false, nil
	case string:
		return t, true, nil
	case []byte:
		return string(t), true, nil
	default:
		b, err := json.Marshal(t)
		return string(b), true, err
	}
}

// appendRow appends one scanned row to the record builder.
func appendRow(rb *array.RecordBuilder, cols []string, vals []any) error {
	for i, c := range cols {
		v := vals[i]
		switch c {
		case "id":
			s, _ := v.(string)
			if v == nil {
				rb.Field(i).(*array.StringBuilder).AppendNull()
			} else {
				rb.Field(i).(*array.StringBuilder).Append(s)
			}
		case "geometry":
			b, err := toBytes(v)
			if err != nil {
				return err
			}
			if b == nil {
				rb.Field(i).(*array.BinaryBuilder).AppendNull()
			} else {
				rb.Field(i).(*array.BinaryBuilder).Append(b)
			}
		case "properties":
			s, ok, err := toJSONString(v)
			if err != nil {
				return err
			}
			if !ok {
				rb.Field(i).(*array.StringBuilder).AppendNull()
			} else {
				rb.Field(i).(*array.StringBuilder).Append(s)
			}
		case "source_id":
			if v == nil {
				rb.Field(i).(*array.Int64Builder).AppendNull()
			} else {
				n, err := toInt64(v)
				if err != nil {
					return err
				}
				rb.Field(i).(*array.Int64Builder).Append(n)
			}
		case "x", "y":
			if v == nil {
				rb.Field(i).(*array.Float64Builder).AppendNull()
			} else {
				f, err := toFloat64(v)
				if err != nil {
					return err
				}
				rb.Field(i).(*array.Float64Builder).Append(f)
			}
		case "name":
			s, _ := v.(string)
			if v == nil {
				rb.Field(i).(*array.StringBuilder).AppendNull()
			} else {
				rb.Field(i).(*array.StringBuilder).Append(s)
			}
		}
	}
	return nil
}

// Serve runs the Flight service until ctx ends.
func Serve(ctx context.Context, addr string, st *store.Store) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	flightpb.RegisterFlightServiceServer(srv, NewServer(st))
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	return srv.Serve(lis)
}
