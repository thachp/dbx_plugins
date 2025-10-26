use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use async_graphql::http::GraphQLPlaygroundConfig;
use async_graphql::Json as GqlJson;
use async_graphql::Result as GqlResult;
use async_graphql::{EmptySubscription, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Json;
use axum::{Extension, Router};
use dbx_plugin_api::{ActorClaimsView, AggregateStateView, EventMetadataView, StoredEventRecord};
use dbx_plugin_api::{
    AppendEventRequest as ControlAppendEventRequest, ControlClient, ControlClientError,
};
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::info;

#[derive(Clone, Debug)]
pub struct Options {
    pub bind: String,
    pub control_addr: String,
    pub page_size: usize,
    pub page_limit: usize,
}

pub async fn run(options: Options) -> anyhow::Result<()> {
    let state = GraphqlState::new(
        options.control_addr.clone(),
        options.page_size,
        options.page_limit,
    );
    let schema = build_schema(state);

    let app = Router::new()
        .route("/health", get(health))
        .route("/graphql", get(graphql_playground).post(graphql_handler))
        .route("/graphql/playground", get(graphql_playground))
        .layer(Extension(schema));

    let listener = TcpListener::bind(&options.bind)
        .await
        .with_context(|| format!("failed to bind GraphQL listener on {}", options.bind))?;

    info!("GraphQL plugin listening on {}", options.bind);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("GraphQL server terminated unexpectedly")?;

    Ok(())
}

type EventSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

fn build_schema(state: GraphqlState) -> EventSchema {
    Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        EmptySubscription,
    )
    .data(state)
    .finish()
}

#[derive(Clone)]
struct GraphqlState {
    control_addr: Arc<String>,
    page_size: usize,
    page_limit: usize,
}

impl GraphqlState {
    fn new(control_addr: String, page_size: usize, page_limit: usize) -> Self {
        Self {
            control_addr: Arc::new(control_addr),
            page_size,
            page_limit,
        }
    }

    async fn connect(&self) -> Result<ControlClient, async_graphql::Error> {
        ControlClient::connect(self.control_addr.as_ref())
            .await
            .map_err(map_control_error)
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

#[derive(Default)]
struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn aggregates(
        &self,
        ctx: &async_graphql::Context<'_>,
        skip: Option<usize>,
        take: Option<usize>,
    ) -> GqlResult<Vec<Aggregate>> {
        let state = ctx.data::<GraphqlState>()?;
        let skip = skip.unwrap_or(0);
        let take = state.clamp_take(take, state.page_size);
        if take == 0 {
            return Ok(Vec::new());
        }

        let mut client = state.connect().await?;
        let aggregates = client
            .list_aggregates(skip, Some(take))
            .await
            .map_err(map_control_error)?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(aggregates)
    }

    async fn aggregate(
        &self,
        ctx: &async_graphql::Context<'_>,
        aggregate_type: String,
        aggregate_id: String,
    ) -> GqlResult<Option<Aggregate>> {
        let state = ctx.data::<GraphqlState>()?;
        let mut client = state.connect().await?;
        let aggregate = client
            .get_aggregate(&aggregate_type, &aggregate_id)
            .await
            .map_err(map_control_error)?;

        Ok(aggregate.map(Into::into))
    }

    async fn aggregate_events(
        &self,
        ctx: &async_graphql::Context<'_>,
        aggregate_type: String,
        aggregate_id: String,
        skip: Option<usize>,
        take: Option<usize>,
    ) -> GqlResult<Vec<Event>> {
        let state = ctx.data::<GraphqlState>()?;
        let skip = skip.unwrap_or(0);
        let take = state.clamp_take(take, state.page_limit);
        if take == 0 {
            return Ok(Vec::new());
        }

        let mut client = state.connect().await?;
        let events = client
            .list_events(&aggregate_type, &aggregate_id, skip, Some(take))
            .await
            .map_err(map_control_error)?;

        Ok(events.into_iter().map(Into::into).collect())
    }

    async fn verify_aggregate(
        &self,
        ctx: &async_graphql::Context<'_>,
        aggregate_type: String,
        aggregate_id: String,
    ) -> GqlResult<VerifyResult> {
        let state = ctx.data::<GraphqlState>()?;
        let mut client = state.connect().await?;
        let merkle_root = client
            .verify_aggregate(&aggregate_type, &aggregate_id)
            .await
            .map_err(map_control_error)?;

        Ok(VerifyResult { merkle_root })
    }
}

#[derive(Default)]
struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn append_event(
        &self,
        ctx: &async_graphql::Context<'_>,
        input: AppendEventInput,
    ) -> GqlResult<Event> {
        let state = ctx.data::<GraphqlState>()?;
        let headers = ctx
            .data::<HeaderMap>()
            .map_err(|_| async_graphql::Error::new("missing request headers"))?;
        let token = extract_bearer_token(headers)
            .ok_or_else(|| async_graphql::Error::new("unauthorized: missing bearer token"))?;

        if input.payload.is_some() && input.patch.is_some() {
            return Err(async_graphql::Error::new(
                "payload and patch cannot both be provided",
            ));
        }
        if input.payload.is_none() && input.patch.is_none() {
            return Err(async_graphql::Error::new(
                "either payload or patch must be provided",
            ));
        }

        let request = ControlAppendEventRequest {
            token,
            aggregate_type: input.aggregate_type.clone(),
            aggregate_id: input.aggregate_id.clone(),
            event_type: input.event_type.clone(),
            payload: input.payload.clone(),
            patch: input.patch.clone(),
            metadata: input.metadata.clone(),
            note: input.note.clone(),
        };

        let mut client = state.connect().await?;
        let record = client
            .append_event(request)
            .await
            .map_err(map_control_error)?;

        Ok(record.into())
    }
}

#[derive(async_graphql::InputObject)]
struct AppendEventInput {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Option<Value>,
    patch: Option<Value>,
    metadata: Option<Value>,
    note: Option<String>,
}

#[derive(SimpleObject)]
struct VerifyResult {
    merkle_root: String,
}

#[derive(SimpleObject)]
struct Aggregate {
    aggregate_type: String,
    aggregate_id: String,
    version: u64,
    state: GqlJson<BTreeMap<String, String>>,
    merkle_root: String,
    archived: bool,
}

impl From<AggregateStateView> for Aggregate {
    fn from(value: AggregateStateView) -> Self {
        Self {
            aggregate_type: value.aggregate_type,
            aggregate_id: value.aggregate_id,
            version: value.version,
            state: GqlJson(value.state),
            merkle_root: value.merkle_root,
            archived: value.archived,
        }
    }
}

#[derive(SimpleObject)]
struct Event {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    version: u64,
    payload: GqlJson<Value>,
    extensions: Option<GqlJson<Value>>,
    metadata: EventMetadataObject,
    hash: String,
    merkle_root: String,
}

impl From<StoredEventRecord> for Event {
    fn from(value: StoredEventRecord) -> Self {
        Self {
            aggregate_type: value.aggregate_type,
            aggregate_id: value.aggregate_id,
            event_type: value.event_type,
            version: value.version,
            payload: GqlJson(value.payload),
            extensions: value.extensions.map(GqlJson),
            metadata: value.metadata.into(),
            hash: value.hash,
            merkle_root: value.merkle_root,
        }
    }
}

#[derive(SimpleObject)]
struct EventMetadataObject {
    event_id: String,
    created_at: String,
    issued_by: Option<ActorClaimsObject>,
    note: Option<String>,
}

impl From<EventMetadataView> for EventMetadataObject {
    fn from(value: EventMetadataView) -> Self {
        Self {
            event_id: value.event_id,
            created_at: value.created_at.to_rfc3339(),
            issued_by: value.issued_by.map(Into::into),
            note: value.note,
        }
    }
}

#[derive(SimpleObject)]
struct ActorClaimsObject {
    group: String,
    user: String,
}

impl From<ActorClaimsView> for ActorClaimsObject {
    fn from(value: ActorClaimsView) -> Self {
        Self {
            group: value.group,
            user: value.user,
        }
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?;
    let value = value.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .map(|token| token.trim().to_string())
}

async fn graphql_handler(
    Extension(schema): Extension<EventSchema>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> GraphQLResponse {
    let request = request.into_inner().data(headers);
    schema.execute(request).await.into()
}

async fn graphql_playground() -> impl IntoResponse {
    Html(async_graphql::http::playground_source(
        GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
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

fn map_control_error(err: ControlClientError) -> async_graphql::Error {
    use async_graphql::ErrorExtensions;
    match err {
        ControlClientError::Server { code, message } => {
            async_graphql::Error::new(message).extend_with(|_, e| e.set("code", code))
        }
        other => async_graphql::Error::new(other.to_string()),
    }
}
