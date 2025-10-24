use clap::Parser;
use dbx_graphql::{run, Options};
use dbx_plugin_logging::init_default;

const DEFAULT_FILTER: &str = "info,tower_http=info,dbx_graphql=info";

#[derive(Parser, Debug)]
#[command(
    name = "dbx_graphql",
    version,
    author,
    about = "EventDBX GraphQL surface plugin"
)]
struct Cli {
    /// HTTP bind address for the GraphQL server.
    #[arg(long, default_value = "0.0.0.0:8081")]
    bind: String,

    /// Control socket address exposed by the EventDBX daemon.
    #[arg(long = "control", default_value = "127.0.0.1:6363")]
    control_addr: String,

    /// Default page size for list operations.
    #[arg(long, default_value_t = 50)]
    page_size: usize,

    /// Maximum page size allowed from clients.
    #[arg(long, default_value_t = 100)]
    page_limit: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    init_default("dbx_graphql", DEFAULT_FILTER)?;

    run(Options {
        bind: cli.bind,
        control_addr: cli.control_addr,
        page_size: cli.page_size,
        page_limit: cli.page_limit,
    })
    .await
}
