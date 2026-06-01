use clap::Parser;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use llama_capture::config::CaptureConfig;
use llama_capture::sse;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("llama_capture=info".parse().unwrap()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cfg = CaptureConfig::parse();
    if !cfg.is_stdout() && !std::fs::metadata(&cfg.output).is_ok_and(|m| m.is_dir()) {
        error!("Error: output path '{}' is not a directory", cfg.output);
        std::process::exit(1);
    }

    run(cfg).await;
}

fn setup_signal_handler(cancel: CancellationToken) {
    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("Received Ctrl+C, shutting down..."),
            _ = terminate => info!("Received SIGTERM, shutting down..."),
        }
        cancel.cancel();
    });
}

async fn run(cfg: CaptureConfig) {
    let cancel = CancellationToken::new();
    setup_signal_handler(cancel.clone());

    let mut backoff_seconds = 2;

    info!(server = %cfg.url, output = %cfg.output, "starting llama-capture");

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let last_ok = match sse::connect_and_capture(&cfg, &mut backoff_seconds, &cancel).await {
            Ok(()) => {
                info!("SSE connection closed, reconnecting");
                true
            }
            Err(e) => {
                error!(error = %e, "connection lost, reconnecting");
                false
            }
        };

        if cancel.is_cancelled() {
            break;
        }

        let delay = if last_ok {
            std::time::Duration::from_secs(2)
        } else {
            sse::backoff_delay(&mut backoff_seconds)
        };

        tokio::select! {
            _ = cancel.cancelled() => {
                break;
            }
            _ = tokio::time::sleep(delay) => {}
        }
    }

    info!("shutting down");
}
