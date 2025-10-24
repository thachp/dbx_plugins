use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use dbx_plugin_api::{ActorClaimsView, AggregateStateView, EventMetadataView, StoredEventRecord};
use dbx_plugin_api::{
    AppendEventRequest as ControlAppendEventRequest, ControlClient, ControlClientError,
};
use prost_types::Timestamp;
use serde_json::Value;
use tokio::signal;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

pub mod proto {
    tonic::include_proto!("eventdbx.api");
}

use proto::event_service_server::{EventService, EventServiceServer};
use proto::{
    ActorClaims, Aggregate, AppendEventRequest, AppendEventResponse, Event, EventMetadata,
    GetAggregateRequest, GetAggregateResponse, HealthRequest, HealthResponse,
    ListAggregatesRequest, ListAggregatesResponse, ListEventsRequest, ListEventsResponse,
    VerifyAggregateRequest, VerifyAggregateResponse,
};

#[derive(Clone, Debug)]
pub struct Options {
    pub bind: String,
    pub control_addr: String,
    pub page_size: usize,
    pub page_limit: usize,
}

pub async fn run(options: Options) -> anyhow::Result<()> {
    let addr = options
        .bind
        .parse()
        .with_context(|| format!("invalid gRPC bind address {}", options.bind))?;
    let service = GrpcService::new(options.control_addr, options.page_size, options.page_limit);

    info!("gRPC plugin listening on {}", addr);
    Server::builder()
        .add_service(EventServiceServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("gRPC server terminated unexpectedly")?;

    Ok(())
}

#[derive(Clone)]
struct GrpcService {
    control_addr: Arc<String>,
    page_size: usize,
    page_limit: usize,
}

impl GrpcService {
    fn new(control_addr: String, page_size: usize, page_limit: usize) -> Self {
        Self {
            control_addr: Arc::new(control_addr),
            page_size,
            page_limit,
        }
    }

    async fn connect(&self) -> Result<ControlClient, Status> {
        ControlClient::connect(self.control_addr.as_ref())
            .await
            .map_err(map_control_error)
    }

    fn clamp_take(&self, take: u64, default: usize) -> usize {
        let mut value = if take == 0 { default } else { take as usize };
        if value == 0 {
            return 0;
        }
        if value > self.page_limit {
            value = self.page_limit;
        }
        value
    }
}

#[tonic::async_trait]
impl EventService for GrpcService {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".to_string(),
        }))
    }

    async fn list_aggregates(
        &self,
        request: Request<ListAggregatesRequest>,
    ) -> Result<Response<ListAggregatesResponse>, Status> {
        let ListAggregatesRequest { skip, take } = request.into_inner();
        let skip = skip as usize;
        let take = self.clamp_take(take, self.page_size);
        if take == 0 {
            return Ok(Response::new(ListAggregatesResponse {
                aggregates: Vec::new(),
            }));
        }

        let mut client = self.connect().await?;
        let aggregates = client
            .list_aggregates(skip, Some(take))
            .await
            .map_err(map_control_error)?;

        let response = ListAggregatesResponse {
            aggregates: aggregates.into_iter().map(convert_aggregate).collect(),
        };
        Ok(Response::new(response))
    }

    async fn get_aggregate(
        &self,
        request: Request<GetAggregateRequest>,
    ) -> Result<Response<GetAggregateResponse>, Status> {
        let GetAggregateRequest {
            aggregate_type,
            aggregate_id,
        } = request.into_inner();
        let mut client = self.connect().await?;
        let aggregate = client
            .get_aggregate(&aggregate_type, &aggregate_id)
            .await
            .map_err(map_control_error)?;

        match aggregate {
            Some(aggregate) => Ok(Response::new(GetAggregateResponse {
                aggregate: Some(convert_aggregate(aggregate)),
            })),
            None => Err(Status::not_found("aggregate not found")),
        }
    }

    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsResponse>, Status> {
        let ListEventsRequest {
            aggregate_type,
            aggregate_id,
            skip,
            take,
        } = request.into_inner();
        let skip = skip as usize;
        let take = self.clamp_take(take, self.page_limit);
        if take == 0 {
            return Ok(Response::new(ListEventsResponse { events: Vec::new() }));
        }

        let mut client = self.connect().await?;
        let events = client
            .list_events(&aggregate_type, &aggregate_id, skip, Some(take))
            .await
            .map_err(map_control_error)?;

        let mut converted = Vec::with_capacity(events.len());
        for record in events {
            converted.push(convert_event(record)?);
        }

        Ok(Response::new(ListEventsResponse { events: converted }))
    }

    async fn append_event(
        &self,
        request: Request<AppendEventRequest>,
    ) -> Result<Response<AppendEventResponse>, Status> {
        let token = extract_token(request.metadata())?;
        let proto::AppendEventRequest {
            aggregate_type,
            aggregate_id,
            event_type,
            payload_json,
            patch_json,
            note,
            metadata_json,
        } = request.into_inner();

        let has_payload = !payload_json.trim().is_empty();
        let has_patch = patch_json
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);

        if has_payload && has_patch {
            return Err(Status::invalid_argument(
                "payload_json must be empty when patch_json is provided",
            ));
        }
        if !has_payload && !has_patch {
            return Err(Status::invalid_argument(
                "payload_json must be provided when patch_json is absent",
            ));
        }

        let payload_value = if has_payload {
            Some(
                serde_json::from_str::<Value>(&payload_json)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            )
        } else {
            None
        };
        let patch_value = if let Some(ref patch) = patch_json {
            if patch.trim().is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<Value>(patch)
                        .map_err(|err| Status::invalid_argument(err.to_string()))?,
                )
            }
        } else {
            None
        };
        let metadata_value = metadata_json
            .as_ref()
            .and_then(|metadata| {
                let trimmed = metadata.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            })
            .map(|trimmed| {
                serde_json::from_str::<Value>(trimmed)
                    .map_err(|err| Status::invalid_argument(err.to_string()))
            })
            .transpose()?;
        let note_value = note.and_then(|note| {
            let trimmed = note.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let request = ControlAppendEventRequest {
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload: payload_value,
            patch: patch_value,
            metadata: metadata_value,
            note: note_value,
        };

        let mut client = self.connect().await?;
        let record = client
            .append_event(request)
            .await
            .map_err(map_control_error)?;

        let event = convert_event(record)?;
        Ok(Response::new(AppendEventResponse { event: Some(event) }))
    }

    async fn verify_aggregate(
        &self,
        request: Request<VerifyAggregateRequest>,
    ) -> Result<Response<VerifyAggregateResponse>, Status> {
        let VerifyAggregateRequest {
            aggregate_type,
            aggregate_id,
        } = request.into_inner();
        let mut client = self.connect().await?;
        let merkle_root = client
            .verify_aggregate(&aggregate_type, &aggregate_id)
            .await
            .map_err(map_control_error)?;

        Ok(Response::new(VerifyAggregateResponse { merkle_root }))
    }
}

fn extract_token(metadata: &tonic::metadata::MetadataMap) -> Result<String, Status> {
    let value = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?;
    let value = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid authorization metadata"))?;
    value
        .strip_prefix("Bearer ")
        .map(|token| token.trim().to_string())
        .ok_or_else(|| Status::unauthenticated("authorization metadata must use Bearer token"))
}

fn map_control_error(err: ControlClientError) -> Status {
    match err {
        ControlClientError::Server { code, message } => match code.as_str() {
            "invalid_token" | "unauthorized" => Status::unauthenticated(message),
            "token_expired" | "token_limit_reached" => Status::permission_denied(message),
            "aggregate_not_found" | "schema_not_found" => Status::not_found(message),
            "schema_violation" | "invalid_schema" | "config" => Status::invalid_argument(message),
            "aggregate_archived" => Status::failed_precondition(message),
            _ => Status::internal(message),
        },
        ControlClientError::Protocol(message) => Status::internal(message),
        ControlClientError::Json(err) => Status::internal(err.to_string()),
        ControlClientError::Capnp(err) => Status::internal(err.to_string()),
        ControlClientError::Io(err) => Status::unavailable(err.to_string()),
    }
}

fn convert_aggregate(value: AggregateStateView) -> Aggregate {
    Aggregate {
        aggregate_type: value.aggregate_type,
        aggregate_id: value.aggregate_id,
        version: value.version,
        state: value.state.into_iter().collect::<HashMap<_, _>>(),
        merkle_root: value.merkle_root,
        archived: value.archived,
    }
}

fn convert_event(value: StoredEventRecord) -> Result<Event, Status> {
    let StoredEventRecord {
        aggregate_type,
        aggregate_id,
        event_type,
        version,
        payload,
        extensions,
        metadata,
        hash,
        merkle_root,
    } = value;

    let payload_json =
        serde_json::to_string(&payload).map_err(|err| Status::internal(err.to_string()))?;
    let extensions_json = extensions
        .map(|ext| {
            serde_json::to_string(&ext).map_err(|err| Status::internal(err.to_string()))
        })
        .transpose()?;

    Ok(Event {
        aggregate_type,
        aggregate_id,
        event_type,
        version,
        payload_json,
        metadata: Some(convert_metadata(metadata)),
        hash,
        merkle_root,
        extensions_json,
    })
}

fn convert_metadata(value: EventMetadataView) -> EventMetadata {
    EventMetadata {
        event_id: value.event_id,
        created_at: Some(Timestamp {
            seconds: value.created_at.timestamp(),
            nanos: value.created_at.timestamp_subsec_nanos() as i32,
        }),
        issued_by: value.issued_by.map(convert_actor_claims),
        note: value.note.unwrap_or_default(),
    }
}

fn convert_actor_claims(value: ActorClaimsView) -> ActorClaims {
    ActorClaims {
        group: value.group,
        user: value.user,
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
