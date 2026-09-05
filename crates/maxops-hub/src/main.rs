use clap::Parser;
use maxops_proto::transport;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Read-only fleet hub")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = serde_json::from_slice(&std::fs::read(Args::parse().config)?)?;
    let (listen, router) = maxops_hub::build(config)?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "hub listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(transport::shutdown())
        .await?;
    Ok(())
}
