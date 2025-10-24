use clap::Parser;
use dbx_plugin_logging::init_default;
use dbx_rest::{run, Options};

const DEFAULT_FILTER: &str = "info,tower_http=info,dbx_rest=info";

#[derive(Parser, Debug)]
#[command(
    name = "dbx_rest",
    version,
    author,
    about = "EventDBX REST surface plugin"
)]
struct Cli {
    /// HTTP bind address for the REST server.
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,

    /// Control socket address exposed by the EventDBX daemon.
    #[arg(long = "control", default_value = "127.0.0.1:6363")]
    control_addr: String,

    /// Default page size for list endpoints.
    #[arg(long, default_value_t = 50)]
    page_size: usize,

    /// Maximum page size allowed from clients.
    #[arg(long, default_value_t = 100)]
    page_limit: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    init_default("dbx_rest", DEFAULT_FILTER)?;

    run(Options {
        bind: cli.bind,
        control_addr: cli.control_addr,
        page_size: cli.page_size,
        page_limit: cli.page_limit,
    })
    .await
}
