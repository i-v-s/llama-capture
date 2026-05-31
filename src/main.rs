use clap::Parser;
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
        .init();

    let cfg = CaptureConfig::parse();

    run(cfg).await;
}

async fn run(cfg: CaptureConfig) {
    let ctx = ShutdownCtx::new();

    let mut backoff_seconds = 2;

    info!(server = %cfg.url, output = %cfg.output, "starting llama-capture");

    loop {
        if ctx.is_shutdown() {
            break;
        }

        let last_ok = match sse::connect_and_capture(&cfg, &mut backoff_seconds).await {
            Ok(()) => {
                info!("SSE connection closed, reconnecting");
                true
            }
            Err(e) => {
                error!(error = %e, "connection lost, reconnecting");
                false
            }
        };

        if ctx.is_shutdown() {
            break;
        }

        let delay = if last_ok {
            std::time::Duration::from_secs(2)
        } else {
            sse::backoff_delay(&mut backoff_seconds)
        };

        tokio::select! {
            _ = ctx.wait() => {
                break;
            }
            _ = tokio::time::sleep(delay) => {}
        }
    }

    info!("shutting down");
}

struct ShutdownCtx {
    #[allow(dead_code)]
    signal: tokio::sync::watch::Sender<bool>,
    receiver: tokio::sync::watch::Receiver<bool>,
}

impl ShutdownCtx {
    fn new() -> Self {
        let (signal, receiver) = tokio::sync::watch::channel(false);
        {
            let sig = signal.clone();
            tokio::spawn(async move {
                let mut sigint =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                        .unwrap();
                let mut sigterm =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .unwrap();
                tokio::select! {
                    _ = sigint.recv() => {}
                    _ = sigterm.recv() => {}
                }
                let _ = sig.send(true);
            });
        }
        Self { signal, receiver }
    }

    fn is_shutdown(&self) -> bool {
        *self.receiver.borrow()
    }

    async fn wait(&self) {
        let mut rx = self.receiver.clone();
        let _ = rx.changed().await;
    }
}
