use std::{collections::BTreeMap, future::Future, sync::Arc};

use anyhow::Context;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use dbx_plugin_api::{ActorClaimsView, AggregateStateView, EventMetadataView, StoredEventRecord};
use dbx_plugin_api::{
    AppendEventRequest as ControlAppendEventRequest, ControlClient, ControlClientError,
    ControlResult,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::info;

#[derive(Clone, Debug)]
pub struct Options {
    pub bind: String,
    pub control_addr: String,
    pub page_size: usize,
    pub page_limit: usize,
}

pub async fn run(options: Options) -> anyhow::Result<()> {
    let state = RestState::new(options.control_addr, options.page_size, options.page_limit);
    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/aggregates", get(list_aggregates))
        .route(
            "/v1/aggregates/{aggregate_type}/{aggregate_id}",
            get(get_aggregate),
        )
        .route(
            "/v1/aggregates/{aggregate_type}/{aggregate_id}/events",
            get(list_events),
        )
        .route("/v1/events", post(append_event_global))
        .route(
            "/v1/aggregates/{aggregate_type}/{aggregate_id}/verify",
            get(verify_aggregate),
        )
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind(&options.bind)
        .await
        .with_context(|| format!("failed to bind REST listener on {}", options.bind))?;

    info!("REST plugin listening on {}", options.bind);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("REST server terminated unexpectedly")?;
    Ok(())
}

#[derive(Clone)]
struct RestState {
    control_addr: Arc<String>,
    page_size: usize,
    page_limit: usize,
}

impl RestState {
    fn new(control_addr: String, page_size: usize, page_limit: usize) -> Self {
        Self {
            control_addr: Arc::new(control_addr),
            page_size,
            page_limit,
        }
    }

    async fn with_client<F, Fut, T>(&self, f: F) -> ControlResult<T>
    where
        F: FnOnce(ControlClient) -> Fut,
        Fut: Future<Output = ControlResult<T>>,
    {
        let client = ControlClient::connect(self.control_addr.as_ref()).await?;
        f(client).await
    }

    fn clamp_take(&self, take: Option<usize>, default: usize) -> usize {
        let mut value = take.unwrap_or(default);
        if value == 0 {
            return 0;
        }
        if value > self.page_limit {
            value = self.page_limit;
        }
        value
    }
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
}

#[derive(Deserialize, Default)]
struct AggregatesQuery {
    #[serde(default)]
    skip: Option<usize>,
    #[serde(default)]
    take: Option<usize>,
}

async fn list_aggregates(
    State(state): State<RestState>,
    Query(params): Query<AggregatesQuery>,
) -> Result<Json<Vec<AggregateResponse>>, AppError> {
    let skip = params.skip.unwrap_or(0);
    let take = state.clamp_take(params.take, state.page_size);
    if take == 0 {
        return Ok(Json(Vec::new()));
    }

    let aggregates = state
        .with_client(|client| async move {
            let mut client = client;
            client.list_aggregates(skip, Some(take)).await
        })
        .await
        .map_err(AppError::from)?;

    Ok(Json(
        aggregates.into_iter().map(Into::into).collect::<Vec<_>>(),
    ))
}

#[derive(Deserialize, Default)]
struct EventsQuery {
    #[serde(default)]
    skip: Option<usize>,
    #[serde(default)]
    take: Option<usize>,
}

async fn get_aggregate(
    State(state): State<RestState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<AggregateResponse>, AppError> {
    let aggregate_type_clone = aggregate_type.clone();
    let aggregate_id_clone = aggregate_id.clone();
    let aggregate = state
        .with_client(|mut client| async move {
            client
                .get_aggregate(&aggregate_type_clone, &aggregate_id_clone)
                .await
        })
        .await
        .map_err(AppError::from)?;

    match aggregate {
        Some(aggregate) => Ok(Json(aggregate.into())),
        None => Err(AppError::not_found("aggregate not found")),
    }
}

async fn list_events(
    State(state): State<RestState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
    Query(params): Query<EventsQuery>,
) -> Result<Json<Vec<EventResponse>>, AppError> {
    let skip = params.skip.unwrap_or(0);
    let take = state.clamp_take(params.take, state.page_limit);
    if take == 0 {
        return Ok(Json(Vec::new()));
    }

    let aggregate_type_clone = aggregate_type.clone();
    let aggregate_id_clone = aggregate_id.clone();
    let events = state
        .with_client(|mut client| async move {
            client
                .list_events(&aggregate_type_clone, &aggregate_id_clone, skip, Some(take))
                .await
        })
        .await
        .map_err(AppError::from)?;

    Ok(Json(events.into_iter().map(Into::into).collect()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AppendEventRequest {
    #[serde(alias = "aggregate_type")]
    aggregate_type: String,
    #[serde(alias = "aggregate_id")]
    aggregate_id: String,
    #[serde(alias = "event_type")]
    event_type: String,
    #[serde(default, alias = "payload")]
    payload: Option<Value>,
    #[serde(default, alias = "patch")]
    patch: Option<Value>,
    #[serde(default, alias = "metadata")]
    metadata: Option<Value>,
    #[serde(default, alias = "note")]
    note: Option<String>,
}

async fn append_event_global(
    State(state): State<RestState>,
    headers: HeaderMap,
    Json(request): Json<AppendEventRequest>,
) -> Result<Json<EventResponse>, AppError> {
    let token =
        extract_bearer_token(&headers).ok_or_else(|| AppError::unauthorized("missing token"))?;

    if request.payload.is_some() && request.patch.is_some() {
        return Err(AppError::bad_request(
            "payload and patch cannot both be provided",
        ));
    }
    if request.payload.is_none() && request.patch.is_none() {
        return Err(AppError::bad_request(
            "either payload or patch must be provided",
        ));
    }

    let control_request = ControlAppendEventRequest {
        token,
        aggregate_type: request.aggregate_type,
        aggregate_id: request.aggregate_id,
        event_type: request.event_type,
        payload: request.payload,
        patch: request.patch,
        metadata: request.metadata,
        note: request.note,
    };

    let record = state
        .with_client(|mut client| async move { client.append_event(control_request).await })
        .await
        .map_err(AppError::from)?;

    Ok(Json(record.into()))
}

async fn verify_aggregate(
    State(state): State<RestState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<VerifyResponse>, AppError> {
    let aggregate_type_clone = aggregate_type.clone();
    let aggregate_id_clone = aggregate_id.clone();
    let merkle_root = state
        .with_client(|mut client| async move {
            client
                .verify_aggregate(&aggregate_type_clone, &aggregate_id_clone)
                .await
        })
        .await
        .map_err(AppError::from)?;

    Ok(Json(VerifyResponse { merkle_root }))
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?;
    let value = value.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .map(|token| token.trim().to_string())
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'a str>,
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
    code: Option<String>,
}

impl AppError {
    fn new(status: StatusCode, message: impl Into<String>, code: Option<String>) -> Self {
        Self {
            status,
            message: message.into(),
            code,
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message, None)
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            message,
            Some("unauthorized".into()),
        )
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message, Some("not_found".into()))
    }
}

impl From<ControlClientError> for AppError {
    fn from(err: ControlClientError) -> Self {
        match err {
            ControlClientError::Server { code, message } => {
                let status = match code.as_str() {
                    "invalid_token" | "unauthorized" => StatusCode::UNAUTHORIZED,
                    "token_expired" | "token_limit_reached" | "aggregate_archived" => {
                        StatusCode::FORBIDDEN
                    }
                    "schema_violation" | "invalid_schema" => StatusCode::BAD_REQUEST,
                    "aggregate_not_found" | "schema_not_found" => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                AppError::new(status, message, Some(code))
            }
            ControlClientError::Protocol(message) => AppError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                message,
                Some("protocol".into()),
            ),
            ControlClientError::Json(err) => AppError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
                Some("serialization".into()),
            ),
            ControlClientError::Capnp(err) => AppError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
                Some("capnp".into()),
            ),
            ControlClientError::Io(err) => {
                AppError::new(StatusCode::BAD_GATEWAY, err.to_string(), Some("io".into()))
            }
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ErrorBody {
            message: &self.message,
            code: self.code.as_deref(),
        });
        (self.status, body).into_response()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AggregateResponse {
    aggregate_type: String,
    aggregate_id: String,
    version: u64,
    state: BTreeMap<String, String>,
    merkle_root: String,
    archived: bool,
}

impl From<AggregateStateView> for AggregateResponse {
    fn from(value: AggregateStateView) -> Self {
        Self {
            aggregate_type: value.aggregate_type,
            aggregate_id: value.aggregate_id,
            version: value.version,
            state: value.state,
            merkle_root: value.merkle_root,
            archived: value.archived,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventResponse {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    version: u64,
    payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    extensions: Option<Value>,
    metadata: EventMetadataResponse,
    hash: String,
    merkle_root: String,
}

impl From<StoredEventRecord> for EventResponse {
    fn from(value: StoredEventRecord) -> Self {
        Self {
            aggregate_type: value.aggregate_type,
            aggregate_id: value.aggregate_id,
            event_type: value.event_type,
            version: value.version,
            payload: value.payload,
            extensions: value.extensions,
            metadata: value.metadata.into(),
            hash: value.hash,
            merkle_root: value.merkle_root,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventMetadataResponse {
    event_id: String,
    created_at: String,
    issued_by: Option<ActorClaimsResponse>,
    note: Option<String>,
}

impl From<EventMetadataView> for EventMetadataResponse {
    fn from(value: EventMetadataView) -> Self {
        EventMetadataResponse {
            event_id: value.event_id,
            created_at: value.created_at.to_rfc3339(),
            issued_by: value.issued_by.map(Into::into),
            note: value.note,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorClaimsResponse {
    group: String,
    user: String,
}

impl From<ActorClaimsView> for ActorClaimsResponse {
    fn from(value: ActorClaimsView) -> Self {
        Self {
            group: value.group,
            user: value.user,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VerifyResponse {
    merkle_root: String,
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
