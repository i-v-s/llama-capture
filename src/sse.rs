use std::mem;

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use reqwest::Client;
use tracing::{error, info};

use crate::config::CaptureConfig;
use crate::models::{ActivityLogEntry, SSEEnvelope};

pub async fn fetch_capture(
    server_url: &str,
    api_key: &str,
    id: i64,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let url_str = format!("{server_url}/api/captures/{id}");
    let mut req = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .get(&url_str);

    if !api_key.is_empty() {
        req = req.header("Authorization", format!("Bearer {api_key}"));
    }

    let resp = req.send().await?;
    let status = resp.status();
    if status != 200 {
        return Err(format!("capture {id} returned status {status}").into());
    }

    let data = resp.bytes().await?;
    Ok(data.to_vec())
}

pub async fn process_metrics_event(cfg: &CaptureConfig, data: &str) -> Result<()> {
    let entries: Vec<ActivityLogEntry> = serde_json::from_str(data)?;

    for entry in entries.into_iter() {
        if !entry.has_capture {
            continue;
        }

        match fetch_capture(&cfg.url, &cfg.api_key, entry.id).await {
            Ok(capture_data) => {
                let id = entry.id;
                let model = entry.model.clone();
                match crate::capture::write_capture(entry, &capture_data, cfg).await {
                    Ok(filename) => {
                        info!(id, model, file = %filename, "captured");
                    }
                    Err(e) => {
                        error!(id, error = %e, "write capture");
                    }
                }
            }
            Err(e) => {
                error!(id = entry.id, error = %e, "fetch capture");
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SSEState {
    Idle,
    Collecting,
    Collected(Vec<u8>),
    Skipping,
}

async fn process_sse_envelope(cfg: &CaptureConfig, envelope: SSEEnvelope) -> Result<()> {
    if envelope.type_ != "metrics" {
        return Ok(());
    }

    process_metrics_event(cfg, &envelope.data)
        .await
        .with_context(|| "processing metrics event")
}

struct SSEBuffer {
    state: SSEState,
    line: Vec<u8>,
}

impl Default for SSEBuffer {
    fn default() -> Self {
        Self {
            state: SSEState::Idle,
            line: Vec::new(),
        }
    }
}

impl SSEBuffer {
    fn parse_line(state: &mut SSEState, line: Vec<u8>) -> Result<Option<SSEEnvelope>> {
        if line.is_empty() {
            let result = match state {
                SSEState::Idle | SSEState::Collecting => Err(anyhow!(
                    "SSE loop: Unexpected empty line in mode {:?}",
                    state
                )),
                SSEState::Collected(data) => Ok(Some(
                    serde_json::from_slice(data).with_context(|| "Error parsing SSEEnvelope")?,
                )),
                SSEState::Skipping => Ok(None),
            };
            *state = SSEState::Idle;
            result
        } else if *state == SSEState::Skipping {
            Ok(None)
        } else {
            let (key, value) = if let Some(pos) = line.iter().position(|c| *c == b':') {
                (&line[..pos], &line[pos + 1..].trim_ascii_start())
            } else {
                return Err(anyhow!("SSE loop: Unable to parse line {:?}", line));
            };
            let key = str::from_utf8(key).with_context(|| "from_utf8(key)")?;

            if key == "event" {
                if *state == SSEState::Idle {
                    *state = if value == b"message" {
                        SSEState::Collecting
                    } else {
                        SSEState::Skipping
                    };
                    Ok(None)
                } else {
                    Err(anyhow!("Unexpected 'event'"))
                }
            } else if key == "data" {
                match *state {
                    SSEState::Idle | SSEState::Collecting => {
                        *state = SSEState::Collected(value.to_vec());
                        Ok(None)
                    }
                    _ => Err(anyhow!("Unexpected 'data' in state {:?}", *state)),
                }
            } else {
                Err(anyhow!("Unexpected key {key}"))
            }
        }
    }

    pub async fn process_chunk(&mut self, chunk: &[u8], cfg: &CaptureConfig) -> () {
        let mut start = 0;
        for (i, c) in chunk.iter().enumerate() {
            if *c == b'\n' {
                if i > start {
                    self.line.extend_from_slice(&chunk[start..i]);
                }
                start = i + 1;
                match Self::parse_line(&mut self.state, mem::take(&mut self.line)) {
                    Err(e) => error!("SSE loop error: {e}"),
                    Ok(Some(envelope)) => {
                        if let Err(e) = process_sse_envelope(cfg, envelope).await {
                            error!("process_sse_envelope() error: {e}");
                        }
                    }
                    Ok(None) => {}
                }
            }
        }
        if start < chunk.len() {
            self.line.extend_from_slice(&chunk[start..]);
        }
    }
}

pub async fn run_sse_loop<S, E>(
    cfg: &CaptureConfig,
    stream: S,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: futures::Stream<Item = Result<Bytes, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    use futures::StreamExt;
    let mut stream = std::pin::pin!(stream);
    let mut buffer = SSEBuffer::default();

    while let Some(chunk) = stream.next().await {
        buffer.process_chunk(&chunk?, cfg).await;
    }

    Ok(())
}

pub async fn connect_and_capture(
    cfg: &CaptureConfig,
    backoff_seconds: &mut i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sse_url = format!("{}/api/events", cfg.url);

    let mut req = Client::builder()
        .build()?
        .get(&sse_url)
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive");

    if !cfg.api_key.is_empty() {
        req = req.header("Authorization", format!("Bearer {}", cfg.api_key));
    }

    let resp = req.send().await?;
    let status = resp.status();
    if status != 200 {
        return Err(format!("SSE endpoint returned status {status}").into());
    }

    *backoff_seconds = 2;

    let stream = resp.bytes_stream();
    run_sse_loop(cfg, stream).await
}

pub fn backoff_delay(backoff_seconds: &mut i64) -> std::time::Duration {
    *backoff_seconds *= 2;
    if *backoff_seconds > 60 {
        *backoff_seconds = 60;
    }
    std::time::Duration::from_secs(*backoff_seconds as u64)
}
