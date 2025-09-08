mod time;
mod transport;

use async_nats::service::ServiceExt;
use std::env;
use thiserror::Error;
use tracing::info;
use tracing_subscriber;

type NatsGenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Error, Debug)]
pub enum LoroServerError {
    #[error("nats connection failed")]
    NatsConnectionFailed(#[from] async_nats::ConnectError),
    #[error("nats service start failed: {0}")]
    NatsServiceStartFailed(NatsGenericError),
    #[error("nats service init failed")]
    NatsServiceInitFailed(#[from] transport::LoroServerInitError),
    #[error("tokio signal error")]
    TokioSignalError(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), LoroServerError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "info,loro_internal=warn",
        ))
        .init();
    // console_subscriber::init();

    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let nats_client = async_nats::connect(nats_url).await?;
    let jetstream_context = async_nats::jetstream::new(nats_client.clone());

    let loro_service = nats_client
        .service_builder()
        .description("Sync and persistence server for Loro")
        .stats_handler(|endpoint, _stats| serde_json::json!({"endpoint": endpoint}))
        .start("loro-service", "1.0.0")
        .await
        .map_err(LoroServerError::NatsServiceStartFailed)?;

    let nc = std::sync::Arc::new(nats_client.clone());
    let js = std::sync::Arc::new(jetstream_context.clone());

    transport::init_loro_server(nc.clone(), js.clone(), &loro_service).await?;

    info!("Loro server started");

    tokio::signal::ctrl_c().await?;

    Ok(())
}
