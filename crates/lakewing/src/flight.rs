//! Read-only Arrow Flight over the same ids-first pages (port of the Go
//! serve's flight contract). Tickets mirror ShardTicket
//! {collection, bbox, columns, limit, offset, sources}; columns: id,
//! geometry (WKB), properties, source_id, x, y, name.
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};

use crate::app::App;
use crate::duck::{exact_predicate, pushed_filter};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardTicket {
    pub collection: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Vec<f64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<i64>>,
}

fn column_type(name: &str) -> Option<DataType> {
    match name {
        "id" | "properties" | "name" => Some(DataType::Utf8),
        "geometry" => Some(DataType::Binary),
        "source_id" => Some(DataType::Int64),
        "x" | "y" => Some(DataType::Float64),
        _ => None,
    }
}

fn validate_columns(ticket: &ShardTicket) -> Result<Vec<String>, Status> {
    let cols = match &ticket.columns {
        None => vec![
            "id".into(),
            "geometry".into(),
            "properties".into(),
            "source_id".into(),
        ],
        Some(cols) => cols.clone(),
    };
    if cols.is_empty() {
        return Err(Status::invalid_argument("columns must not be empty"));
    }
    let mut seen = std::collections::HashSet::new();
    for c in &cols {
        if column_type(c).is_none() {
            return Err(Status::invalid_argument(format!("unknown column {c:?}")));
        }
        if !seen.insert(c.clone()) {
            return Err(Status::invalid_argument(format!("duplicate column {c:?}")));
        }
    }
    Ok(cols)
}

fn ticket_schema(cols: &[String]) -> SchemaRef {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(c.clone(), column_type(c).unwrap(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn ticket_bounds(ticket: &ShardTicket) -> Result<Option<[f64; 4]>, Status> {
    match &ticket.bbox {
        None => Ok(None),
        Some(bbox)
            if bbox.len() == 4
                && bbox.iter().all(|v| v.is_finite())
                && bbox[0] < bbox[2]
                && bbox[1] < bbox[3] =>
        {
            Ok(Some([bbox[0], bbox[1], bbox[2], bbox[3]]))
        }
        Some(_) => Err(Status::invalid_argument("bbox must be 4 finite numbers")),
    }
}

fn validate_ticket(ticket: &ShardTicket) -> Result<(Vec<String>, usize, usize), Status> {
    let cols = validate_columns(ticket)?;
    let limit = ticket.limit.unwrap_or(10_000);
    if !(0..=100_000).contains(&limit) {
        return Err(Status::invalid_argument("limit must be 0..100000"));
    }
    let offset = ticket.offset.unwrap_or(0);
    if offset < 0 {
        return Err(Status::invalid_argument("offset must be >= 0"));
    }
    ticket_bounds(ticket)?;
    Ok((cols, limit as usize, offset as usize))
}

fn default_sources(ticket: &ShardTicket) -> Vec<i64> {
    ticket.sources.clone().unwrap_or_else(|| vec![1])
}

fn ticket_bytes(ticket: &ShardTicket) -> Result<bytes::Bytes, Status> {
    serde_json::to_vec(ticket)
        .map(bytes::Bytes::from)
        .map_err(|e| Status::internal(format!("{e}")))
}

fn descriptor_to_ticket(desc: &FlightDescriptor) -> Result<ShardTicket, Status> {
    use arrow_flight::flight_descriptor::DescriptorType;
    match DescriptorType::try_from(desc.r#type) {
        Ok(DescriptorType::Path) if desc.path.len() == 1 => Ok(ShardTicket {
            collection: desc.path[0].clone(),
            ..Default::default()
        }),
        Ok(DescriptorType::Cmd) => serde_json::from_slice(&desc.cmd)
            .map_err(|e| Status::invalid_argument(format!("bad ticket JSON: {e}"))),
        _ => Err(Status::invalid_argument(
            "descriptor must be a one-part collection path or a JSON command",
        )),
    }
}

fn endpoint_for(ticket: &ShardTicket) -> Result<FlightEndpoint, Status> {
    Ok(FlightEndpoint {
        ticket: Some(Ticket {
            ticket: ticket_bytes(ticket)?,
        }),
        ..Default::default()
    })
}

pub struct FlightServer {
    app: Arc<App>,
}

impl FlightServer {
    pub fn new(app: Arc<App>) -> Self {
        Self { app }
    }

    fn info_for(&self, ticket: &ShardTicket) -> Result<FlightInfo, Status> {
        let (cols, ..) = validate_ticket(ticket)?;
        let schema = ticket_schema(&cols);
        Ok(FlightInfo {
            schema: encode_schema(&schema)?,
            flight_descriptor: Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
                cmd: ticket_bytes(ticket)?,
                ..Default::default()
            }),
            endpoint: vec![endpoint_for(ticket)?],
            ..Default::default()
        })
    }

    /// Run the ticket as an ids-first page and return its batches in the
    /// ticket's projected schema.
    async fn batches(&self, ticket: &ShardTicket) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        let (cols, limit, offset) = validate_ticket(ticket)?;
        if self.app.collections.iter().all(|c| c != &ticket.collection) {
            return Err(Status::invalid_argument("unknown collection"));
        }
        let bounds = ticket_bounds(ticket)?;
        let sources = default_sources(ticket);
        let exact = exact_predicate(&ticket.collection, bounds, &sources);
        let pushed = pushed_filter(
            &ticket.collection,
            bounds,
            &sources,
            self.app.lance.geo_geom,
        );

        let fetch = limit + offset;
        let ids = self
            .app
            .lance
            .scan_ids_topk(&pushed, fetch)
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;
        let ids: Vec<String> = ids.into_iter().skip(offset).take(limit).collect();
        if ids.is_empty() {
            return Ok((ticket_schema(&cols), Vec::new()));
        }
        let payload = format!("{pushed} AND id IN ({})", crate::duck::id_in_list(&ids));
        let batches = self
            .app
            .lance
            .scan_flight(&payload, &cols)
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| ticket_schema(&cols));
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let _ = rows;
        let _ = &exact;
        Ok((schema, batches))
    }
}

fn encode_schema(schema: &SchemaRef) -> Result<bytes::Bytes, Status> {
    // arrow-flight 58 pins the same arrow-ipc as our arrow workspace dep,
    // so arrow::ipc::writer::IpcWriteOptions is the concrete type behind
    // SchemaAsIpc.
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let result: arrow_flight::SchemaResult = arrow_flight::SchemaAsIpc {
        pair: (schema, &options),
    }
    .try_into()
    .map_err(|e| Status::internal(format!("schema ipc: {e:?}")))?;
    Ok(result.schema)
}

#[tonic::async_trait]
impl FlightService for FlightServer {
    type HandshakeStream = ResponseStream<HandshakeResponse>;
    type ListFlightsStream = ResponseStream<FlightInfo>;
    type DoGetStream = ResponseStream<FlightData>;
    type DoPutStream = ResponseStream<PutResult>;
    type DoExchangeStream = ResponseStream<FlightData>;
    type DoActionStream = ResponseStream<arrow_flight::Result>;
    type ListActionsStream = ResponseStream<ActionType>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("anonymous only"))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        if !request.into_inner().expression.is_empty() {
            return Err(Status::invalid_argument("criteria must be empty"));
        }
        let infos: Result<Vec<FlightInfo>, Status> = self
            .app
            .collections
            .iter()
            .map(|c| {
                self.info_for(&ShardTicket {
                    collection: c.clone(),
                    ..Default::default()
                })
            })
            .collect();
        let infos = infos?;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for info in infos {
                if tx.send(Ok(info)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = descriptor_to_ticket(&request.into_inner())?;
        Ok(Response::new(self.info_for(&ticket)?))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        let info = self.get_flight_info(request).await?;
        Ok(Response::new(arrow_flight::PollInfo {
            info: Some(info.into_inner()),
            ..Default::default()
        }))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let ticket = descriptor_to_ticket(&request.into_inner())?;
        let (cols, ..) = validate_ticket(&ticket)?;
        Ok(Response::new(SchemaResult {
            schema: encode_schema(&ticket_schema(&cols))?,
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket: ShardTicket = serde_json::from_slice(&request.into_inner().ticket)
            .map_err(|e| Status::invalid_argument(format!("bad ticket JSON: {e}")))?;
        let (cols, ..) = validate_ticket(&ticket)?;
        let (_schema, batches) = self.batches(&ticket).await?;
        let schema = ticket_schema(&cols);
        let (batch_tx, batch_rx) =
            mpsc::channel::<Result<RecordBatch, arrow_flight::error::FlightError>>(4);
        tokio::spawn(async move {
            for batch in batches {
                if batch_tx.send(Ok(batch)).await.is_err() {
                    break;
                }
            }
        });
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(tokio_stream::wrappers::ReceiverStream::new(batch_rx));
        Ok(Response::new(Box::pin(encoder.map(|item| {
            item.map_err(|e| Status::internal(format!("{e}")))
        }))))
    }

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::permission_denied("read-only"))
    }

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::permission_denied("read-only"))
    }

    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("no actions"))
    }

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("no actions"))
    }
}

pub type ResponseStream<T> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Serve Flight on `addr` until the process ends.
pub async fn serve(app: Arc<App>, addr: &str) -> anyhow::Result<()> {
    let listener = tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(FlightServer::new(app)))
        .serve(
            addr.parse()
                .map_err(|e| anyhow::anyhow!("flight listen {addr}: {e:?}"))?,
        );
    listener.await?;
    Ok(())
}
