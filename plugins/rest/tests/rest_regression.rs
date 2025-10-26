use std::{net::TcpListener, path::PathBuf, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine};
use dbx_plugin_api::ControlClient;
use eventdbx::{
    config::Config,
    restrict::RestrictMode,
    server,
    token::{IssueTokenInput, JwtLimits, TokenManager},
};
use fake::{
    faker::{
        address::en::{CityName, StateAbbr},
        company::en::{Buzzword, CompanyName},
        internet::en::{DomainSuffix, SafeEmail},
        lorem::en::{Paragraph, Words},
        name::en::{FirstName, LastName},
        number::raw::NumberWithFormat,
    },
    locales::EN,
    Fake,
};
use rand::{seq::SliceRandom, thread_rng};
use reqwest::Client;
use dbx_rest::{run as run_rest, Options as RestOptions};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default, Copy, Clone)]
struct SeedCounts {
    people: usize,
    companies: usize,
    animals: usize,
}

impl SeedCounts {
    fn max_take(self) -> usize {
        self.people + self.companies + self.animals + 10
    }
}

fn parse_seed_counts() -> SeedCounts {
    let mut counts = SeedCounts {
        people: 32,
        companies: 16,
        animals: 24,
    };

    for arg in std::env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--people=") {
            counts.people = value.parse().expect("--people must be numeric");
        } else if let Some(value) = arg.strip_prefix("--companies=") {
            counts.companies = value.parse().expect("--companies must be numeric");
        } else if let Some(value) = arg.strip_prefix("--animals=") {
            counts.animals = value.parse().expect("--animals must be numeric");
        }
    }

    counts
}

fn random_species() -> &'static str {
    const OPTIONS: &[&str] = &[
        "Canis lupus",
        "Panthera leo",
        "Elephas maximus",
        "Gorilla gorilla",
        "Delphinus delphis",
        "Loxodonta africana",
        "Cervus elaphus",
        "Equus zebra",
    ];
    OPTIONS
        .choose(&mut thread_rng())
        .copied()
        .unwrap_or("Canis lupus")
}

fn random_language() -> &'static str {
    const LANGS: &[&str] = &["en", "es", "fr", "de", "ja", "zh", "pt", "vi"];
    LANGS.choose(&mut thread_rng()).copied().unwrap_or("en")
}

fn random_diet() -> &'static str {
    const DIETS: &[&str] = &["herbivore", "carnivore", "omnivore", "insectivore", "piscivore"];
    DIETS.choose(&mut thread_rng()).copied().unwrap_or("omnivore")
}

fn random_habitat() -> &'static str {
    const HABITATS: &[&str] = &[
        "savanna enclosure",
        "rainforest dome",
        "grassland range",
        "mountain ridge",
        "wetlands habitat",
        "coastal lagoon",
    ];
    HABITATS
        .choose(&mut thread_rng())
        .copied()
        .unwrap_or("grassland range")
}

fn random_status() -> &'static str {
    const STATUSES: &[&str] = &[
        "healthy",
        "under observation",
        "in rehabilitation",
        "quarantined",
        "released",
    ];
    STATUSES
        .choose(&mut thread_rng())
        .copied()
        .unwrap_or("healthy")
}

fn allocate_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

#[tokio::test(flavor = "multi_thread")]
async fn rest_person_company_flow() -> TestResult<()> {
    let counts = parse_seed_counts();
    let temp = TempDir::new()?;
    let mut config = Config::default();
    config.data_dir = temp.path().join("data");
    let http_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping rest regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let socket_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping rest regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let plugin_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping rest regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    config.port = http_port;
    config.restrict = RestrictMode::Off;
    config.data_encryption_key = Some(STANDARD.encode([1u8; 32]));
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
            subject: "testers:regression".into(),
            group: "testers".into(),
            user: "regression".into(),
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

    let rest_bind = format!("127.0.0.1:{plugin_port}");
    let rest_handle = spawn_rest_plugin(RestOptions {
        bind: rest_bind.clone(),
        control_addr,
        page_size: config.list_page_size,
        page_limit: config.page_limit,
    })?;

    let base_url = format!("http://{}", rest_bind);
    wait_for_health(&base_url).await?;

    let client = Client::new();

    seed_people(&client, &base_url, &token, counts.people).await?;
    seed_companies(&client, &base_url, &token, counts.companies).await?;
    seed_animals(&client, &base_url, &token, counts.animals).await?;

    let aggregates: Value = client
        .get(format!(
            "{base_url}/v1/aggregates?take={}",
            counts.max_take()
        ))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let entries = aggregates
        .as_array()
        .expect("aggregates response should be array");
    assert!(
        entries
            .iter()
            .any(|entry| entry["aggregateType"] == "person"),
        "person aggregate should be present"
    );
    if counts.companies > 0 {
        assert!(
            entries
                .iter()
                .any(|entry| entry["aggregateType"] == "company"),
            "company aggregate should be present"
        );
    }
    if counts.animals > 0 {
        assert!(
            entries
                .iter()
                .any(|entry| entry["aggregateType"] == "animal"),
            "animal aggregate should be present"
        );
    }

    let aggregate: Value = client
        .get(format!("{base_url}/v1/aggregates/person/person-00010"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(aggregate["aggregateType"], "person");

    let verify: Value = client
        .get(format!(
            "{base_url}/v1/aggregates/person/person-00010/verify"
        ))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(verify["merkleRoot"].as_str().is_some());

    let target_animal = format!(
        "animal-{index:05}",
        index = counts.animals.saturating_sub(1)
    );
    let animal_state: Value = client
        .get(format!("{base_url}/v1/aggregates/animal/{target_animal}"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    assert_eq!(
        animal_state["aggregateId"], target_animal,
        "animal aggregate id should match"
    );
    let animal_fields = animal_state["state"]
        .as_object()
        .expect("animal state should be an object");
    assert!(
        animal_fields.len() >= 9,
        "animal aggregate should have a rich attribute set"
    );
    assert!(
        animal_fields.contains_key("species"),
        "animal state should include species"
    );

    server_handle.abort();
    let _ = server_handle.await;

    rest_handle.abort();
    let _ = rest_handle.await;

    Ok(())
}

fn spawn_server(
    config: Config,
    config_path: PathBuf,
) -> TestResult<JoinHandle<eventdbx::error::Result<()>>> {
    Ok(tokio::spawn(async move {
        server::run(config, config_path).await
    }))
}

fn spawn_rest_plugin(options: RestOptions) -> TestResult<JoinHandle<anyhow::Result<()>>> {
    Ok(tokio::spawn(async move { run_rest(options).await }))
}

async fn wait_for_health(base_url: &str) -> TestResult<()> {
    let client = Client::new();
    for _ in 0..40 {
        if let Ok(resp) = client.get(format!("{base_url}/health")).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err("plugin did not become healthy in time".into())
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

async fn seed_people(client: &Client, base_url: &str, token: &str, count: usize) -> TestResult<()> {
    for index in 0..count {
        let aggregate_id = format!("person-{index:05}");
        let payload = json!({
            "first_name": FirstName().fake::<String>(),
            "last_name": LastName().fake::<String>(),
            "email": SafeEmail().fake::<String>(),
            "phone": format!("+1-{}", NumberWithFormat(EN, "###-###-####").fake::<String>()),
            "street": format!(
                "{} {}",
                NumberWithFormat(EN, "#####").fake::<String>(),
                Words(1..3).fake::<Vec<String>>().join(" ")
            ),
            "city": CityName().fake::<String>(),
            "state": StateAbbr().fake::<String>(),
            "postal_code": NumberWithFormat(EN, "#####").fake::<String>(),
            "locale": random_language()
        });
        send_event(
            client,
            base_url,
            token,
            "person",
            &aggregate_id,
            "person_created",
            payload,
        )
        .await?;
    }
    Ok(())
}

async fn seed_companies(
    client: &Client,
    base_url: &str,
    token: &str,
    count: usize,
) -> TestResult<()> {
    for index in 0..count {
        let aggregate_id = format!("company-{index:05}");
        let payload = json!({
            "name": CompanyName().fake::<String>(),
            "email": SafeEmail().fake::<String>(),
            "domain": format!(
                "{}.{}",
                Buzzword().fake::<String>().replace(' ', "-"),
                DomainSuffix().fake::<String>()
            ),
            "incorporated_state": StateAbbr().fake::<String>(),
            "locale": random_language()
        });
        send_event(
            client,
            base_url,
            token,
            "company",
            &aggregate_id,
            "company_created",
            payload,
        )
        .await?;
    }
    Ok(())
}

async fn seed_animals(
    client: &Client,
    base_url: &str,
    token: &str,
    count: usize,
) -> TestResult<()> {
    for index in 0..count {
        let aggregate_id = format!("animal-{index:05}");
        let payload = json!({
            "species": random_species(),
            "tracking_tag": format!("TAG-{}", NumberWithFormat(EN, "#####").fake::<String>()),
            "notes": Paragraph(2..3).fake::<String>(),
            "name": Words(1..3).fake::<Vec<String>>().join(" "),
            "caretaker": format!("{} {}", FirstName().fake::<String>(), LastName().fake::<String>()),
            "diet": random_diet(),
            "habitat": random_habitat(),
            "status": random_status(),
            "home_region": CityName().fake::<String>(),
            "weight_kg": NumberWithFormat(EN, "##").fake::<String>(),
            "favorite_food": Words(1..2).fake::<Vec<String>>().join(" "),
        });
        send_event(
            client,
            base_url,
            token,
            "animal",
            &aggregate_id,
            "animal_created",
            payload,
        )
        .await?;
    }
    Ok(())
}

async fn send_event(
    client: &Client,
    base_url: &str,
    token: &str,
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    payload: Value,
) -> TestResult<()> {
    client
        .post(format!("{base_url}/v1/events"))
        .bearer_auth(token)
        .json(&json!({
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "payload": payload
        }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
