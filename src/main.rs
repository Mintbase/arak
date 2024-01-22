use database::Database;
use dotenv::dotenv;
mod config;
mod database;
mod indexer;

use {
    self::{config::Config, indexer::Indexer},
    anyhow::{Context, Result},
    clap::Parser,
    std::{env, path::PathBuf},
};

#[derive(Parser)]
struct Arguments {
    #[clap(short, long, env = "ARAKCONFIG", default_value = "arak.toml")]
    config: PathBuf,
    #[clap(short, long, env = "DB_STRING")]
    db_string: Option<String>,
    #[clap(short, long, env = "NODE_URL")]
    node_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok(); // Couldn't load multiple Env Vars without this!
    let args = Arguments::parse();
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(env::var("RUST_LOGS").unwrap_or("info,arak=debug".to_string()))
        .with_ansi(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let (config, root) = Config::load(&args.config, args.node_url, args.db_string)
        .context("failed to load configuration")?;
    env::set_current_dir(root)?;
    match &config.database {
        config::Database::Sqlite { connection } => {
            run_indexer(&config, database::Sqlite::open(connection)?).await?;
        }
        config::Database::Postgres { connection, schema } => {
            run_indexer(
                &config,
                database::Postgres::connect(connection, schema).await?,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_indexer(config: &Config, db: impl Database) -> Result<()> {
    let eth = ethrpc::http::Client::new(config.ethrpc.clone());

    Indexer::create(eth, db, config.events.clone())?
        .run(indexer::Run {
            page_size: config.indexer.page_size,
            poll_interval: config.indexer.poll_interval,
        })
        .await?;

    Ok(())
}
