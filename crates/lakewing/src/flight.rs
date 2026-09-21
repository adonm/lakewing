//! Read-only Flight: shared exact selection, canonical schemas and bounded streaming.
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;

use crate::app::App;
use crate::query::{check_snapshot, parse_sources, QueryError, Selection, MAX_OFFSET};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ShardTicket {
    pub collection: String,
    pub bbox: Option<[f64; 4]>,
    pub columns: Option<Vec<String>>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub sources: Option<Vec<i64>>,
    pub cursor: Option<String>,
    pub snapshot: Option<u64>,
}

pub struct FlightServer {
    app: Arc<App>,
}

impl FlightServer {
    pub fn new(app: Arc<App>) -> Self {
        Self { app }
    }

    fn validate(
        &self,
        ticket: &ShardTicket,
    ) -> Result<(Vec<String>, SchemaRef, Selection), Status> {
        self.app
            .validate_collection(&ticket.collection)
            .map_err(status)?;
        check_snapshot(ticket.snapshot, self.app.lance.version).map_err(status)?;
        let columns = ticket.columns.clone().unwrap_or_else(|| {
            vec![
                "id".into(),
                "geometry".into(),
                "properties".into(),
                "source_id".into(),
            ]
        });
        if columns.is_empty() || columns.len() > 256 {
            return Err(Status::invalid_argument(
                "columns must contain 1..256 fields",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        if columns.iter().any(|c| !seen.insert(c)) {
            return Err(Status::invalid_argument("duplicate columns"));
        }
        let schema = self.app.lance.flight_schema(&columns).map_err(status)?;
        let limit = ticket.limit.unwrap_or(10_000);
        let offset = ticket.offset.unwrap_or(0);
        if !(0..=100_000).contains(&limit) || !(0..=MAX_OFFSET as i64).contains(&offset) {
            return Err(Status::invalid_argument(
                "limit and offset must be 0..100000",
            ));
        }
        let sources = ticket.sources.clone().unwrap_or_else(|| vec![1]);
        if sources.len() > 256 {
            return Err(Status::invalid_argument(
                "at most 256 sources may be selected",
            ));
        }
        let selection = Selection::new(
            ticket.collection.clone(),
            ticket.bbox,
            sources,
            limit as usize,
            offset as usize,
            ticket.cursor.as_deref(),
            self.app.lance.version,
        )
        .map_err(status)?;
        // Validate even empty selections, for consistent metadata and DoGet errors.
        selection
            .filter(self.app.lance.geo_geom, self.app.lance.spatial)
            .map_err(status)?;
        Ok((columns, schema, selection))
    }

    fn info_for(&self, mut ticket: ShardTicket) -> Result<FlightInfo, Status> {
        let (_, schema, _) = self.validate(&ticket)?;
        ticket.snapshot = Some(self.app.lance.version);
        let bytes = ticket_bytes(&ticket)?;
        Ok(FlightInfo {
            schema: encode_schema(&schema)?,
            flight_descriptor: Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
                cmd: bytes.clone(),
                ..Default::default()
            }),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket { ticket: bytes }),
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: true,
            ..Default::default()
        })
    }
}

fn status(error: anyhow::Error) -> Status {
    if let Some(error) = error.downcast_ref::<QueryError>() {
        match error.status {
            400 => Status::invalid_argument(&error.message),
            404 => Status::not_found(&error.message),
            409 => Status::failed_precondition(&error.message),
            413 | 429 => Status::resource_exhausted(&error.message),
            504 => Status::deadline_exceeded(&error.message),
            _ => Status::internal(&error.message),
        }
    } else {
        tracing::error!(%error, "Flight query failed");
        Status::internal("internal query error")
    }
}

fn apply_sources(
    ticket: &mut ShardTicket,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(), Status> {
    if let Some(raw) = metadata.get("x-source-ids") {
        let raw = raw
            .to_str()
            .map_err(|_| Status::invalid_argument("invalid x-source-ids"))?;
        let selected = parse_sources(Some(raw)).map_err(status)?;
        ticket.sources = Some(match ticket.sources.take() {
            None => selected,
            Some(sources) => sources
                .into_iter()
                .filter(|s| selected.contains(s))
                .collect(),
        });
    }
    Ok(())
}

fn ticket_bytes(ticket: &ShardTicket) -> Result<bytes::Bytes, Status> {
    serde_json::to_vec(ticket)
        .map(bytes::Bytes::from)
        .map_err(|e| Status::internal(e.to_string()))
}

fn descriptor_to_ticket(desc: FlightDescriptor) -> Result<ShardTicket, Status> {
    use arrow_flight::flight_descriptor::DescriptorType;
    match DescriptorType::try_from(desc.r#type) {
        Ok(DescriptorType::Path) if desc.path.len() == 1 => Ok(ShardTicket {
            collection: desc.path[0].clone(),
            ..Default::default()
        }),
        Ok(DescriptorType::Cmd) => serde_json::from_slice(&desc.cmd)
            .map_err(|e| Status::invalid_argument(format!("bad ticket JSON: {e}"))),
        _ => Err(Status::invalid_argument(
            "descriptor must be a collection path or JSON command",
        )),
    }
}

fn encode_schema(schema: &SchemaRef) -> Result<bytes::Bytes, Status> {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let result: SchemaResult = arrow_flight::SchemaAsIpc {
        pair: (schema, &options),
    }
    .try_into()
    .map_err(|e| Status::internal(format!("schema IPC: {e}")))?;
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
        if !request.get_ref().expression.is_empty() {
            return Err(Status::invalid_argument("criteria must be empty"));
        }
        let infos = self
            .app
            .collections
            .iter()
            .map(|c| {
                self.info_for(ShardTicket {
                    collection: c.clone(),
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(Box::pin(futures::stream::iter(
            infos.into_iter().map(Ok),
        ))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (metadata, _, desc) = request.into_parts();
        let mut ticket = descriptor_to_ticket(desc)?;
        apply_sources(&mut ticket, &metadata)?;
        Ok(Response::new(self.info_for(ticket)?))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Ok(Response::new(arrow_flight::PollInfo {
            info: Some(self.get_flight_info(request).await?.into_inner()),
            ..Default::default()
        }))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let (metadata, _, desc) = request.into_parts();
        let mut ticket = descriptor_to_ticket(desc)?;
        apply_sources(&mut ticket, &metadata)?;
        let (_, schema, _) = self.validate(&ticket)?;
        Ok(Response::new(SchemaResult {
            schema: encode_schema(&schema)?,
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let (metadata, _, raw) = request.into_parts();
        if raw.ticket.len() > 64 * 1024 {
            return Err(Status::invalid_argument("ticket exceeds 64 KiB"));
        }
        let mut ticket: ShardTicket = serde_json::from_slice(&raw.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad ticket JSON: {e}")))?;
        apply_sources(&mut ticket, &metadata)?;
        let (columns, schema, selection) = self.validate(&ticket)?;
        let permit = self.app.admit(true).map_err(|e| status(e.into()))?;
        let span = tracing::info_span!(
            "flight.do_get",
            collection = ticket.collection,
            dataset_version = self.app.lance.version
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let prepare = async {
            let keys = self
                .app
                .selected_keys(&selection, 0)
                .await
                .map_err(status)?;
            self.app
                .lance
                .scan_flight(&App::payload_filter(&ticket.collection, &keys), &columns)
                .await
                .map_err(status)
        };
        let batches = tokio::time::timeout_at(deadline, prepare.instrument(span.clone()))
            .await
            .map_err(|_| Status::deadline_exceeded("query deadline exceeded"))??;
        let batches =
            futures::stream::try_unfold((batches, permit), move |(mut batches, permit)| {
                let span = span.clone();
                async move {
                    let next = tokio::time::timeout_at(deadline, batches.try_next())
                        .await
                        .map_err(|_| {
                            arrow_flight::error::FlightError::from(Status::deadline_exceeded(
                                "query deadline exceeded",
                            ))
                        })?
                        .map_err(|e| arrow_flight::error::FlightError::from(status(e)))?;
                    Ok(next.map(|batch| (batch, (batches, permit))))
                }
                .instrument(span)
            });
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches);
        Ok(Response::new(Box::pin(
            encoder.map(|item| item.map_err(Status::from)),
        )))
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

pub async fn serve(app: Arc<App>, addr: &str) -> anyhow::Result<()> {
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(FlightServer::new(app)))
        .serve(addr.parse()?)
        .await?;
    Ok(())
}
