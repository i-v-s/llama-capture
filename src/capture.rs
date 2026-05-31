use std::path::Path;

use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::DateTime;
use serde_json::{Map, Value, map::Entry};

use crate::config::CaptureConfig;
use crate::models::ActivityLogEntry;
use crate::sse::SSEBuffer;

pub fn capture_file_name(ts: &DateTime<chrono::Utc>) -> String {
    let utc = *ts;
    utc.format("%Y-%m-%dT%H-%M-%S").to_string()
}

fn decode_entry(capture: &mut Map<String, Value>, key: &str, as_sse: bool) -> Result<()> {
    match capture.entry(key) {
        Entry::Vacant(_) => Err(anyhow!("Capture does not have {key} field")),
        Entry::Occupied(mut o) => {
            let data = o
                .get()
                .as_str()
                .ok_or_else(|| anyhow!("Unable to convert {key} into string"))?;

            let bytes = BASE64_STANDARD.decode(data)?;

            if as_sse {
                let bytes = bytes
                    .trim_ascii_end()
                    .strip_suffix(b"[DONE]")
                    .unwrap_or(&bytes);

                let mut buffer = SSEBuffer::default();

                let values: Vec<Value> = buffer
                    .process_chunk::<Value>(bytes)
                    .into_iter()
                    .collect::<Result<_>>()?;
                o.insert(Value::Array(values));
            } else {
                o.insert(serde_json::from_slice(&bytes).context("decode_entry error")?);
            }
            Ok(())
        }
    }
}

pub async fn write_capture(
    mut entry: ActivityLogEntry,
    data: &[u8],
    config: &CaptureConfig,
) -> Result<String> {
    let mut capture: Map<String, Value> = serde_json::from_slice(data)?;
    if config.decode {
        decode_entry(&mut capture, "req_body", false)?;
        decode_entry(&mut capture, "resp_body", true)?;
    }
    entry.capture = Some(capture);

    let dir = Path::new(&config.output);
    tokio::fs::create_dir_all(dir).await?;

    let name = capture_file_name(&entry.timestamp) + ".json";
    let filename = dir.join(&name);
    let filename_str = filename.to_string_lossy().to_string();

    let data = if config.pretty {
        serde_json::to_string_pretty(&entry)?
    } else {
        serde_json::to_string(&entry)?
    };
    tokio::fs::write(&filename, data).await?;
    Ok(filename_str)
}
