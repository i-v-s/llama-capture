# llama-capture

Capture request and response bodies from a running
[llama-swap](https://github.com/mostlygeek/llama-swap) proxy.

`llama-capture` connects to llama-swap's live event stream, watches activity
metrics, downloads every request/response capture that is still available, and
writes it as JSON. It is useful when you want an append-only audit/debug stream
outside the llama-swap UI.

## What It Captures

Each saved record contains the llama-swap activity metadata plus the downloaded
capture payload:

```json
{
  "id": 1,
  "timestamp": "2026-06-01T16:15:21.561011634Z",
  "model": "Qwen-3.6-27B-fast-mtp",
  "req_path": "/v1/chat/completions",
  "resp_content_type": "text/event-stream",
  "resp_status_code": 200,
  "tokens": {
    "cache_tokens": 0,
    "input_tokens": 861,
    "output_tokens": 901,
    "prompt_per_second": 833.226,
    "tokens_per_second": 45.680
  },
  "duration_ms": 20975,
  "has_capture": true,
  "capture": {
    "req_body": "...",
    "resp_body": "..."
  }
}
```

With `--decode`, `req_body` is decoded from base64 into JSON and `resp_body` is
decoded from base64 Server-Sent Events into an array of streamed JSON chunks.

## Requirements

- Rust toolchain with Cargo.
- A running llama-swap server.
- llama-swap request/response captures enabled. In llama-swap config,
  `captureBuffer` controls how many MB are kept in memory for captures; setting
  it to `0` disables captures. The default is nonzero.
- If llama-swap has `apiKeys` configured, pass the same key with `--api-key`.

## Install

Build from this checkout:

```sh
cargo build --release
```

The binary will be available at:

```sh
target/release/llama-capture
```

You can also run it directly during development:

```sh
cargo run -- --url http://localhost:8080
```

## Quick Start

Write compact JSON records to stdout:

```sh
llama-capture --url http://localhost:8080
```

Write decoded, pretty-printed JSON files to a directory:

```sh
mkdir -p captures
llama-capture \
  --url http://localhost:8080 \
  --output captures \
  --decode \
  --pretty
```

Write decoded captures into the JSONL with `xz` compression:

```sh
llama-capture \
  -u http://localhost:8080 -d \
  | ( trap '' INT; xz -ze - > test.jsonl.xz )
```

Use an API key:

```sh
llama-capture \
  --url http://localhost:8080 \
  --api-key "$LLAMA_SWAP_API_KEY" \
  --output captures \
  --decode
```

The URL must include a scheme, for example `http://localhost:8080` or
`https://llama-swap.example.com`.

## CLI Options

```text
Usage: llama-capture [OPTIONS] --url <URL>

Options:
  -u, --url <URL>          Llama-swap server URL (e.g. http://localhost:8080)
  -o, --output <OUTPUT>    Output folder to write capture files (use '-' for stdout) [default: -]
  -k, --api-key <API_KEY>  API key for authentication (optional)
  -d, --decode             Decode conversation from base64
  -p, --pretty             Pretty print output
  -h, --help               Print help
```

## Output Files

When `--output` is a directory, each capture is saved as:

```text
YYYY-MM-DDTHH-MM_0001.json
```

The timestamp comes from the llama-swap activity entry and the numeric suffix is
the llama-swap capture id. Existing files are skipped, so restarting
`llama-capture` against the same output directory will not overwrite earlier
captures.

When `--output -` is used, every capture is written as one JSON line to stdout.
Logs are written to stderr.

## How It Works With llama-swap

`llama-capture` uses two llama-swap endpoints:

- `GET /api/events` for the live Server-Sent Events stream.
- `GET /api/captures/{id}` for each captured request/response body.

Only `metrics` events with `has_capture: true` are downloaded. If a capture is no
longer in llama-swap's in-memory capture buffer by the time it is requested,
llama-swap may return an error and the capture will be skipped.

If the connection drops, `llama-capture` reconnects automatically with
exponential backoff capped at 60 seconds. Press `Ctrl+C` or send `SIGTERM` for a
clean shutdown.

## llama-swap Example

A minimal llama-swap config with captures available:

```yaml
captureBuffer: 15

models:
  model1:
    cmd: llama-server --port ${PORT} --model /path/to/model.gguf
```

Run llama-swap:

```sh
llama-swap --config config.yaml --listen localhost:8080
```

Then start capture:

```sh
llama-capture --url http://localhost:8080 --output captures --decode --pretty
```

## Troubleshooting

- `output path '...' is not a directory`: create the directory first or use
  `--output -`.
- `SSE endpoint returned status 401` or `403`: pass `--api-key` when llama-swap
  uses `apiKeys`.
- No files are written: make a request through llama-swap, check that
  `captureBuffer` is not `0`, and confirm the activity entry has
  `has_capture: true`.
- Capture fetch errors during high traffic: increase llama-swap `captureBuffer`
  so captures stay in memory longer.
- Behind nginx or another reverse proxy: make sure buffering is disabled for
  `/api/events`, because the event stream must remain live.

## Development

Run tests:

```sh
cargo test
```

Format the code:

```sh
cargo fmt
```

Run clippy:

```sh
cargo clippy --all-targets --all-features
```
