mod db;
mod ingest;
mod serve;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "zplc", about = "Integer representation of did:plc identifiers")]
struct Args {
    #[arg(long, env = "ZPLC_DB", default_value = "./data/ids.db", global = true)]
    db: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, env = "ZPLC_LISTEN", default_value = "0.0.0.0:2485")]
        listen: SocketAddr,
    },
    Ingest {
        #[command(subcommand)]
        source: IngestSource,
    },
}

#[derive(Subcommand)]
enum IngestSource {
    /// Ingest and optionally tail plc.directory exports.
    Plc {
        #[arg(long, default_value = "https://plc.directory")]
        url: String,
        #[arg(long)]
        once: bool,
        #[arg(long, default_value_t = 5)]
        poll_interval: u64,
    },
    /// Bootstrap from Plox bundles stored in an atproto repository.
    Plox {
        #[arg(default_value = "at://cerulea.blue/blue.cerulea.plox.bundle")]
        repo: String,
        #[arg(long)]
        pds_url: Option<String>,
        #[arg(long, default_value = "https://plc.directory")]
        plc_url: String,
        #[arg(long, default_value_t = 2)]
        download_concurrency: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "zplc=info".into()),
        )
        .init();

    let args = Args::parse();
    match args.command {
        Command::Serve { listen } => serve::run(&args.db, listen).await,
        Command::Ingest {
            source:
                IngestSource::Plc {
                    url,
                    once,
                    poll_interval,
                },
        } => ingest::plc::run(&args.db, &url, once, poll_interval).await,
        Command::Ingest {
            source:
                IngestSource::Plox {
                    repo,
                    pds_url,
                    plc_url,
                    download_concurrency,
                },
        } => {
            ingest::plox::run(
                &args.db,
                &repo,
                pds_url.as_deref(),
                &plc_url,
                download_concurrency,
            )
            .await
        }
    }
}
