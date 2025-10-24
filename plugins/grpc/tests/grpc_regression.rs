use std::{io, net::TcpListener, path::PathBuf, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine};
use dbx_plugin_api::ControlClient;
use eventdbx::{
    config::Config,
    restrict::RestrictMode,
    server,
    token::{IssueTokenInput, JwtLimits, TokenManager},
};
use grpc_api::{
    proto::{
        event_service_client::EventServiceClient, AppendEventRequest, GetAggregateRequest,
        ListAggregatesRequest, ListEventsRequest, VerifyAggregateRequest,
    },
    run as run_grpc, Options as GrpcOptions,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};
use tonic::{Code, Request, Status};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_and_query_flow() -> TestResult<()> {
    let temp = TempDir::new()?;
    let mut config = Config::default();
    config.data_dir = temp.path().join("data");
    let http_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping grpc regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let socket_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping grpc regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let grpc_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping grpc regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    config.port = http_port;
    config.restrict = RestrictMode::Off;
    config.data_encryption_key = Some(STANDARD.encode([7u8; 32]));
    config.socket.bind_addr = format!("127.0.0.1:{socket_port}");
    config.api.rest = false;
    config.api.graphql = false;
    config.api.grpc = false;
    config.ensure_data_dir()?;
    let config_path = temp.path().join("config.toml");
    config.save(&config_path)?;

    let encryptor = config
        .encryption_key()?
        .expect("encryption key should be configured");
    let jwt_config = config.jwt_manager_config()?;
    let token_manager = TokenManager::load(
        jwt_config,
        config.tokens_path(),
        config.jwt_revocations_path(),
        Some(encryptor.clone()),
    )?;
    let token = token_manager
        .issue(IssueTokenInput {
            subject: "testers:grpc-regression".into(),
            group: "testers".into(),
            user: "grpc-regression".into(),
            root: true,
            actions: Vec::new(),
            resources: Vec::new(),
            ttl_secs: Some(3600),
            not_before: None,
            issued_by: "tests".into(),
            limits: JwtLimits {
                write_events: None,
                keep_alive: true,
            },
        })?
        .token;
    drop(token_manager);

    let control_addr = config.socket.bind_addr.clone();
    let server_handle = spawn_server(config.clone(), config_path.clone())?;
    wait_for_control(&control_addr).await?;

    let grpc_bind = format!("127.0.0.1:{grpc_port}");
    let plugin_handle = spawn_grpc_plugin(GrpcOptions {
        bind: grpc_bind.clone(),
        control_addr,
        page_size: config.list_page_size,
        page_limit: config.page_limit,
    })?;

    let endpoint = format!("http://{grpc_bind}");
    let mut client = wait_for_grpc(&endpoint).await?;

    let aggregate_type = "grpc-person";
    let aggregate_id = "gp-001";
    let event_note = "gRPC regression bootstrap";
    let payload_json = json!({
        "status": "active",
        "notes": "Created via gRPC regression test"
    })
    .to_string();

    let mut append_request = Request::new(AppendEventRequest {
        aggregate_type: aggregate_type.to_string(),
        aggregate_id: aggregate_id.to_string(),
        event_type: "person-upserted".to_string(),
        payload_json: payload_json.clone(),
        note: Some(event_note.to_string()),
        patch_json: None,
        metadata_json: None,
    });
    set_bearer(&mut append_request, &token)?;
    let append_response = client.append_event(append_request).await?;
    let event = append_response
        .into_inner()
        .event
        .expect("appendEvent should return an event");
    assert_eq!(event.aggregate_type, aggregate_type);
    assert_eq!(event.aggregate_id, aggregate_id);
    assert_eq!(event.event_type, "person-upserted");
    assert_eq!(event.version, 1);
    let metadata = event
        .metadata
        .as_ref()
        .expect("event metadata should be present");
    assert_eq!(metadata.note.as_deref(), Some(event_note));
    let event_payload: serde_json::Value = serde_json::from_str(&event.payload_json)?;
    assert_eq!(event_payload["notes"], "Created via gRPC regression test");
    let first_merkle_root = event.merkle_root.clone();

    let aggregates = client
        .list_aggregates(ListAggregatesRequest { skip: 0, take: 10 })
        .await?
        .into_inner();
    let aggregate_entry = aggregates
        .aggregates
        .iter()
        .find(|agg| agg.aggregate_type == aggregate_type && agg.aggregate_id == aggregate_id)
        .expect("aggregate should appear in aggregates list");
    assert_eq!(aggregate_entry.version, 1);
    assert_eq!(
        aggregate_entry.state.get("status").map(String::as_str),
        Some("active")
    );

    let aggregate_response = client
        .get_aggregate(GetAggregateRequest {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
        })
        .await?
        .into_inner()
        .aggregate
        .expect("aggregate query should return a record");
    assert_eq!(aggregate_response.version, 1);
    assert_eq!(
        aggregate_response.state.get("notes").map(String::as_str),
        Some("Created via gRPC regression test")
    );

    let patch_document = json!([
        { "op": "replace", "path": "/status", "value": "inactive" },
        { "op": "add", "path": "/contact", "value": { "address": { "city": "Spokane" } } }
    ])
    .to_string();
    let patch_note = "gRPC regression patch";

    let mut patch_request = Request::new(AppendEventRequest {
        aggregate_type: aggregate_type.to_string(),
        aggregate_id: aggregate_id.to_string(),
        event_type: "person-patched".to_string(),
        payload_json: String::new(),
        note: Some(patch_note.to_string()),
        patch_json: Some(patch_document),
        metadata_json: None,
    });
    set_bearer(&mut patch_request, &token)?;
    let patch_response = client.append_event(patch_request).await?;
    let patch_event = patch_response
        .into_inner()
        .event
        .expect("patch append should return an event");
    let merkle_root = patch_event.merkle_root.clone();
    let patch_metadata = patch_event
        .metadata
        .as_ref()
        .expect("patch metadata should be present");
    assert_eq!(patch_metadata.note.as_deref(), Some(patch_note));

    let events_response = client
        .list_events(ListEventsRequest {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
            skip: 0,
            take: 10,
        })
        .await?
        .into_inner();
    assert_eq!(events_response.events.len(), 2);
    let first_event = &events_response.events[0];
    assert_eq!(first_event.event_type, "person-upserted");
    assert_eq!(first_event.version, 1);
    assert_eq!(first_event.merkle_root, first_merkle_root);
    assert_eq!(
        first_event
            .metadata
            .as_ref()
            .and_then(|meta| meta.note.as_deref()),
        Some(event_note)
    );
    let second_event = &events_response.events[1];
    assert_eq!(second_event.event_type, "person-patched");
    assert_eq!(second_event.version, 2);
    assert_eq!(second_event.merkle_root, merkle_root);

    let verify_response = client
        .verify_aggregate(VerifyAggregateRequest {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
        })
        .await?
        .into_inner();
    assert_eq!(verify_response.merkle_root, merkle_root);

    let mut unauthorized = Request::new(AppendEventRequest {
        aggregate_type: aggregate_type.to_string(),
        aggregate_id: "gp-unauthorized".to_string(),
        event_type: "person-upserted".to_string(),
        payload_json,
        note: None,
        patch_json: None,
        metadata_json: None,
    });
    let status = client
        .append_event(unauthorized)
        .await
        .map(|_| ())
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);

    server_handle.abort();
    let _ = server_handle.await;

    plugin_handle.abort();
    let _ = plugin_handle.await;

    Ok(())
}

fn allocate_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn spawn_server(
    config: Config,
    config_path: PathBuf,
) -> TestResult<JoinHandle<eventdbx::error::Result<()>>> {
    Ok(tokio::spawn(async move {
        server::run(config, config_path).await
    }))
}

fn spawn_grpc_plugin(options: GrpcOptions) -> TestResult<JoinHandle<anyhow::Result<()>>> {
    Ok(tokio::spawn(async move { run_grpc(options).await }))
}

async fn wait_for_control(control_addr: &str) -> TestResult<()> {
    for _ in 0..40 {
        match ControlClient::connect(control_addr).await {
            Ok(_) => return Ok(()),
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    Err("control socket did not become ready in time".into())
}

async fn wait_for_grpc(
    endpoint: &str,
) -> TestResult<EventServiceClient<tonic::transport::Channel>> {
    for _ in 0..40 {
        match EventServiceClient::connect(endpoint.to_string()).await {
            Ok(client) => return Ok(client),
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    Err("gRPC endpoint did not become ready in time".into())
}

fn set_bearer<T>(request: &mut Request<T>, token: &str) -> TestResult<()> {
    let header_value = format!("Bearer {token}")
        .parse()
        .map_err(|_| "invalid token header")?;
    request.metadata_mut().insert("authorization", header_value);
    Ok(())
}
