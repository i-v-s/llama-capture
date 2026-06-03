pub mod capture;
pub mod config;
pub mod models;
pub mod sse;

#[cfg(test)]
mod tests {
    use crate::capture::{capture_file_name, write_capture};
    use crate::config::{CaptureConfig, resolve_server_url};
    use crate::models::{ActivityLogEntry, SSEEnvelope};
    use crate::sse::{
        SSEBuffer, backoff_delay, fetch_capture, process_metrics_event, run_sse_loop,
    };
    use chrono::TimeZone;
    use std::path::PathBuf;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;

    // --- capture_file_name tests ---

    #[test]
    fn test_capture_file_name_standard() {
        let ts = chrono::Utc
            .with_ymd_and_hms(2025, 1, 15, 10, 30, 0)
            .unwrap();
        let name = capture_file_name(&ts, 42);
        assert_eq!(name, "2025-01-15T10-30_0042.json");
    }

    #[test]
    fn test_capture_file_name_zero_id() {
        let ts = chrono::Utc
            .with_ymd_and_hms(2025, 6, 1, 23, 59, 59)
            .unwrap();
        let name = capture_file_name(&ts, 0);
        assert_eq!(name, "2025-06-01T23-59_0000.json");
    }

    #[test]
    fn test_capture_file_name_large_id() {
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let name = capture_file_name(&ts, 99999);
        assert_eq!(name, "2025-01-01T00-00_99999.json");
    }

    // --- write_capture tests ---

    fn make_entry(ts: chrono::DateTime<chrono::Utc>, id: i64) -> ActivityLogEntry {
        ActivityLogEntry {
            id,
            timestamp: ts,
            model: "test".to_string(),
            req_path: "/v1/chat".to_string(),
            resp_content_type: "application/json".to_string(),
            resp_status_code: 200,
            tokens: Default::default(),
            duration_ms: 0,
            has_capture: true,
            capture: None,
        }
    }

    fn make_config(output: &str, decode: bool, pretty: bool) -> CaptureConfig {
        CaptureConfig {
            url: "http://localhost:8080".to_string(),
            output: output.to_string(),
            api_key: "".to_string(),
            decode,
            pretty,
        }
    }

    #[tokio::test]
    async fn test_write_capture_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc
            .with_ymd_and_hms(2025, 3, 10, 14, 20, 30)
            .unwrap();
        let entry = make_entry(ts, 7);
        let data = br#"{"key":"val"}"#;

        let cfg = make_config(dir.path().to_str().unwrap(), false, false);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-03-10T14-20_0007.json"));

        write_capture(entry, data, &cfg, &filename).await.unwrap();

        let got = tokio::fs::read(filename.as_ref().unwrap()).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&got).unwrap();
        assert_eq!(json["capture"]["key"], "val");
        assert_eq!(json["id"], 7);
    }

    #[tokio::test]
    async fn test_write_capture_pretty() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);
        let data = br#"{}"#;

        let cfg = make_config(dir.path().to_str().unwrap(), false, true);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-01-01T00-00_0001.json"));

        write_capture(entry, data, &cfg, &filename).await.unwrap();

        let content = tokio::fs::read_to_string(filename.as_ref().unwrap())
            .await
            .unwrap();
        assert!(content.contains('\n'));
    }

    #[tokio::test]
    async fn test_write_capture_stdout() {
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);
        let data = br#"{}"#;

        let cfg = make_config("-", false, false);
        let filename: Option<PathBuf> = None;

        write_capture(entry, data, &cfg, &filename).await.unwrap();
    }

    // --- fetch_capture tests ---

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];

        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }

            request.extend_from_slice(&buf[..n]);
            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        request
    }

    async fn start_mock_server(response: &'static [u8]) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _ = read_http_request(&mut stream).await;
                    let _ = stream.write_all(response).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn test_fetch_capture_returns_json() {
        let url = start_mock_server(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"id\":99,\"path\":\"/\"}",
        )
        .await;

        let data = fetch_capture(&url, "", 99).await.unwrap();
        assert_eq!(data, br#"{"id":99,"path":"/"}"#);
    }

    #[tokio::test]
    async fn test_fetch_capture_error_on_non_200() {
        let url = start_mock_server(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;

        let result = fetch_capture(&url, "", 1).await;
        assert!(result.is_err());
    }

    // --- SSEBuffer tests ---

    fn sse_data_line(payload: &str) -> String {
        format!("event: message\ndata: {}\n\n", payload)
    }

    #[test]
    fn test_sse_buffer_parses_envelope() {
        let mut buf = SSEBuffer::default();
        let payload = r#"{"type":"metrics","data":"[{\"id\":1}]"}"#;
        let lines = sse_data_line(payload);
        let results: Vec<Result<SSEEnvelope, anyhow::Error>> =
            buf.process_chunk::<SSEEnvelope>(lines.as_bytes());
        assert_eq!(results.len(), 1);
        let env = results[0].as_ref().unwrap();
        assert_eq!(env.type_, "metrics");
        assert_eq!(env.data, r#"[{"id":1}]"#);
    }

    #[test]
    fn test_sse_buffer_splits_across_chunks() {
        let mut buf = SSEBuffer::default();
        let payload = r#"{"type":"metrics","data":"[]"}"#;
        let lines = sse_data_line(payload);
        let mid = lines.len() / 2;
        let chunk1 = &lines[..mid];
        let chunk2 = &lines[mid..];

        let r1: Vec<Result<SSEEnvelope, anyhow::Error>> =
            buf.process_chunk::<SSEEnvelope>(chunk1.as_bytes());
        assert_eq!(r1.len(), 0);
        let r2: Vec<Result<SSEEnvelope, anyhow::Error>> =
            buf.process_chunk::<SSEEnvelope>(chunk2.as_bytes());
        assert_eq!(r2.len(), 1);
        let env = r2[0].as_ref().unwrap();
        assert_eq!(env.type_, "metrics");
    }

    #[test]
    fn test_sse_buffer_skips_non_message_event() {
        let mut buf = SSEBuffer::default();
        let lines = "event: other\ndata: ignored\n\n";
        let results: Vec<Result<SSEEnvelope, anyhow::Error>> =
            buf.process_chunk::<SSEEnvelope>(lines.as_bytes());
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_sse_buffer_multiple_events() {
        let mut buf = SSEBuffer::default();
        let lines = "event: message\ndata: {\"type\":\"a\",\"data\":\"1\"}\n\nevent: message\ndata: {\"type\":\"b\",\"data\":\"2\"}\n\n";
        let results: Vec<Result<SSEEnvelope, anyhow::Error>> =
            buf.process_chunk::<SSEEnvelope>(lines.as_bytes());
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().type_, "a");
        assert_eq!(results[1].as_ref().unwrap().type_, "b");
    }

    // --- process_metrics_event tests ---

    #[tokio::test]
    async fn test_process_metrics_event_fetches_capture() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        )
        .await;

        let cfg = CaptureConfig {
            url,
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };
        let payload = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let cancel = CancellationToken::new();
        process_metrics_event(&cfg, payload, &cancel).await.unwrap();
        assert!(dir.path().join("2025-01-01T00-00_0001.json").exists());
    }

    #[tokio::test]
    async fn test_process_metrics_event_skips_no_capture() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };
        let payload = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":false}]"#;
        let cancel = CancellationToken::new();
        process_metrics_event(&cfg, payload, &cancel).await.unwrap();
        assert_eq!(dir.path().read_dir().unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_process_metrics_event_empty_array() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };
        let cancel = CancellationToken::new();
        process_metrics_event(&cfg, "[]", &cancel).await.unwrap();
        assert_eq!(dir.path().read_dir().unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_process_metrics_event_skips_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        )
        .await;

        let cfg = CaptureConfig {
            url,
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };
        let payload = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let cancel = CancellationToken::new();

        process_metrics_event(&cfg, payload, &cancel).await.unwrap();
        let file = dir.path().join("2025-01-01T00-00_0001.json");
        assert!(file.exists());
        let content1 = tokio::fs::read_to_string(&file).await.unwrap();

        process_metrics_event(&cfg, payload, &cancel).await.unwrap();
        let content2 = tokio::fs::read_to_string(&file).await.unwrap();
        assert_eq!(content1, content2);
    }

    #[tokio::test]
    async fn test_process_metrics_event_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };
        let payload = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        process_metrics_event(&cfg, payload, &cancel).await.unwrap();
        assert_eq!(dir.path().read_dir().unwrap().count(), 0);
    }

    // --- run_sse_loop tests ---

    #[tokio::test]
    async fn test_run_sse_loop_processes_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        )
        .await;

        let cfg = CaptureConfig {
            url,
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };

        let metrics = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let env = serde_json::json!({"type": "metrics", "data": metrics});
        let sse_lines = format!("event: message\ndata: {}\n\n", env);
        let reader = std::io::Cursor::new(sse_lines.as_bytes());
        let stream = tokio_util::io::ReaderStream::new(reader);

        let cancel = CancellationToken::new();
        run_sse_loop(&cfg, stream, &cancel).await.unwrap();
        assert!(dir.path().join("2025-01-01T00-00_0001.json").exists());
    }

    #[tokio::test]
    async fn test_run_sse_loop_multiple_metrics_in_one_event() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        )
        .await;

        let cfg = CaptureConfig {
            url,
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };

        let multi_metrics = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true},{"id":2,"timestamp":"2025-01-01T00:01:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let env = serde_json::json!({"type": "metrics", "data": multi_metrics});
        let sse_lines = format!("event: message\ndata: {}\n\n", env);
        let reader = std::io::Cursor::new(sse_lines.as_bytes());
        let stream = tokio_util::io::ReaderStream::new(reader);

        let cancel = CancellationToken::new();
        run_sse_loop(&cfg, stream, &cancel).await.unwrap();
        assert!(dir.path().join("2025-01-01T00-00_0001.json").exists());
        assert!(dir.path().join("2025-01-01T00-01_0002.json").exists());
    }

    #[tokio::test]
    async fn test_run_sse_loop_buffers_split_json_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        )
        .await;

        let cfg = CaptureConfig {
            url,
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };

        let metrics = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let env = serde_json::json!({"type": "metrics", "data": metrics});
        let sse_lines = format!("event: message\ndata: {}\n\n", env);
        let split = sse_lines.find(r#""data""#).unwrap();
        let bytes = sse_lines.as_bytes();
        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = vec![
            Ok(bytes::Bytes::copy_from_slice(&bytes[..split])),
            Ok(bytes::Bytes::copy_from_slice(&bytes[split..])),
        ];
        let stream = futures::stream::iter(chunks);

        let cancel = CancellationToken::new();
        run_sse_loop(&cfg, stream, &cancel).await.unwrap();
        assert!(dir.path().join("2025-01-01T00-00_0001.json").exists());
    }

    #[tokio::test]
    async fn test_run_sse_loop_cancelled() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            api_key: "".to_string(),
            output: "-".to_string(),
            decode: false,
            pretty: false,
        };

        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = vec![];
        let stream = futures::stream::iter(chunks);
        run_sse_loop(&cfg, stream, &cancel).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_sse_loop_skips_non_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
            pretty: false,
        };

        let sse_lines = "event: message\ndata: {\"type\":\"modelStatus\",\"data\":\"[]\"}\n\n";
        let reader = std::io::Cursor::new(sse_lines.as_bytes());
        let stream = tokio_util::io::ReaderStream::new(reader);

        let cancel = CancellationToken::new();
        run_sse_loop(&cfg, stream, &cancel).await.unwrap();
        assert_eq!(dir.path().read_dir().unwrap().count(), 0);
    }

    // --- CaptureConfig helper tests ---

    #[test]
    fn test_config_is_stdout() {
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            output: "-".to_string(),
            api_key: "".to_string(),
            decode: false,
            pretty: false,
        };
        assert!(cfg.is_stdout());
    }

    #[test]
    fn test_config_is_not_stdout() {
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            output: "/tmp/out".to_string(),
            api_key: "".to_string(),
            decode: false,
            pretty: false,
        };
        assert!(!cfg.is_stdout());
    }

    #[test]
    fn test_config_output_folder_stdout() {
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            output: "-".to_string(),
            api_key: "".to_string(),
            decode: false,
            pretty: false,
        };
        assert!(cfg.output_folder().is_none());
    }

    #[test]
    fn test_config_output_folder_path() {
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            output: "/tmp/out".to_string(),
            api_key: "".to_string(),
            decode: false,
            pretty: false,
        };
        assert_eq!(cfg.output_folder(), Some(std::path::Path::new("/tmp/out")));
    }

    // --- resolve_server_url tests ---

    #[test]
    fn test_resolve_server_url_adds_http() {
        assert_eq!(
            resolve_server_url("localhost:8080"),
            "http://localhost:8080"
        );
    }

    #[test]
    fn test_resolve_server_url_preserves_https() {
        assert_eq!(
            resolve_server_url("https://example.com"),
            "https://example.com"
        );
    }

    #[test]
    fn test_resolve_server_url_preserves_http() {
        assert_eq!(
            resolve_server_url("http://localhost:8080"),
            "http://localhost:8080"
        );
    }

    // --- backoff_delay tests ---

    #[test]
    fn test_backoff_delay_doubles() {
        let mut bs = 2;
        let d = backoff_delay(&mut bs);
        assert_eq!(d, std::time::Duration::from_secs(4));
        assert_eq!(bs, 4);
    }

    #[test]
    fn test_backoff_delay_caps_at_60() {
        let mut bs = 60;
        let d = backoff_delay(&mut bs);
        assert_eq!(d, std::time::Duration::from_secs(60));
        assert_eq!(bs, 60);
    }

    #[test]
    fn test_backoff_delay_from_32() {
        let mut bs = 32;
        let d = backoff_delay(&mut bs);
        assert_eq!(d, std::time::Duration::from_secs(60));
        assert_eq!(bs, 60);
    }

    #[test]
    fn test_backoff_delay_from_1() {
        let mut bs = 1;
        let d = backoff_delay(&mut bs);
        assert_eq!(d, std::time::Duration::from_secs(2));
        assert_eq!(bs, 2);
    }

    // --- write_capture decode tests ---

    #[tokio::test]
    async fn test_write_capture_decode_base64() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);

        let req_body =
            base64::Engine::encode(&base64::prelude::BASE64_STANDARD, r#"{"prompt":"hello"}"#);
        let resp_body = base64::Engine::encode(
            &base64::prelude::BASE64_STANDARD,
            "event: message\ndata: {\"content\":\"hi\"}\n\n",
        );
        let data = format!(
            r#"{{"req_body":"{}","resp_body":"{}"}}"#,
            req_body, resp_body
        );

        let cfg = make_config(dir.path().to_str().unwrap(), true, false);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-01-01T00-00_0001.json"));
        write_capture(entry, data.as_bytes(), &cfg, &filename)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(filename.as_ref().unwrap())
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(json["capture"]["req_body"]["prompt"], "hello");
        assert_eq!(json["capture"]["resp_body"][0]["content"], "hi");
    }

    #[tokio::test]
    async fn test_write_capture_decode_strips_done() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);

        let resp_body = base64::Engine::encode(
            &base64::prelude::BASE64_STANDARD,
            "event: message\ndata: {\"content\":\"hi\"}\n\n[DONE]",
        );
        let data = format!(r#"{{"req_body":"e30=","resp_body":"{}"}}"#, resp_body);

        let cfg = make_config(dir.path().to_str().unwrap(), true, false);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-01-01T00-00_0001.json"));
        write_capture(entry, data.as_bytes(), &cfg, &filename)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(filename.as_ref().unwrap())
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(json["capture"]["resp_body"][0]["content"], "hi");
    }

    #[tokio::test]
    async fn test_write_capture_decode_missing_field() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);

        let data = r#"{"other":"value"}"#;
        let cfg = make_config(dir.path().to_str().unwrap(), true, false);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-01-01T00-00_0001.json"));
        let res = write_capture(entry, data.as_bytes(), &cfg, &filename).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_write_capture_decode_invalid_base64() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);

        let data = r#"{"req_body":"!!!invalid","resp_body":"e30="}"#;
        let cfg = make_config(dir.path().to_str().unwrap(), true, false);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-01-01T00-00_0001.json"));
        let res = write_capture(entry, data.as_bytes(), &cfg, &filename).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_write_capture_decode_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);

        let bad_json = base64::Engine::encode(&base64::prelude::BASE64_STANDARD, "not json at all");
        let data = format!(r#"{{"req_body":"{}","resp_body":"e30="}}"#, bad_json);
        let cfg = make_config(dir.path().to_str().unwrap(), true, false);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-01-01T00-00_0001.json"));
        let res = write_capture(entry, data.as_bytes(), &cfg, &filename).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_write_capture_decode_not_string() {
        let dir = tempfile::tempdir().unwrap();
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = make_entry(ts, 1);

        let data = r#"{"req_body":123,"resp_body":"e30="}"#;
        let cfg = make_config(dir.path().to_str().unwrap(), true, false);
        let filename: Option<PathBuf> = Some(dir.path().join("2025-01-01T00-00_0001.json"));
        let res = write_capture(entry, data.as_bytes(), &cfg, &filename).await;
        assert!(res.is_err());
    }

    // --- SSEBuffer error case tests ---

    #[test]
    fn test_sse_buffer_unexpected_empty_line() {
        let mut buf = SSEBuffer::default();
        let res = buf.process_chunk::<serde_json::Value>(b"\n");
        assert!(res.len() == 1 && res[0].is_err());
    }

    #[test]
    fn test_sse_buffer_unparseable_line() {
        let mut buf = SSEBuffer::default();
        let res = buf.process_chunk::<serde_json::Value>(b"event: message\ndata: {}\ngarbage\n");
        let err = res.iter().find(|r| r.is_err());
        assert!(err.is_some());
    }

    #[test]
    fn test_sse_buffer_unexpected_event() {
        let mut buf = SSEBuffer::default();
        let res = buf.process_chunk::<serde_json::Value>(b"event: message\nevent: other\n");
        let err = res.iter().find(|r| r.is_err());
        assert!(err.is_some());
    }

    #[test]
    fn test_sse_buffer_unexpected_data_state() {
        let mut buf = SSEBuffer::default();
        let res = buf.process_chunk::<serde_json::Value>(b"event: message\ndata: {}\ndata: more\n");
        let err = res.iter().find(|r| r.is_err());
        assert!(err.is_some());
    }

    #[test]
    fn test_sse_buffer_unknown_key() {
        let mut buf = SSEBuffer::default();
        let res = buf.process_chunk::<serde_json::Value>(b"event: message\nfoo: bar\n");
        let err = res.iter().find(|r| r.is_err());
        assert!(err.is_some());
    }

    // --- fetch_capture with API key ---

    #[tokio::test]
    async fn test_fetch_capture_with_api_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());

        let (auth_tx, mut auth_rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let request = read_http_request(&mut stream).await;
                let req = std::str::from_utf8(&request).unwrap();
                let has_auth = req
                    .to_lowercase()
                    .contains("authorization: bearer secret-key");
                let _ = auth_tx.send(has_auth).await;

                let resp_body = r#"{"req_body":"e30=","resp_body":"e30="}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let data = fetch_capture(&base_url, "secret-key", 99).await.unwrap();
        let has_auth = auth_rx.recv().await.unwrap();
        assert!(has_auth, "Request should contain Authorization header");
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["req_body"], "e30=");
    }

    #[tokio::test]
    async fn test_fetch_capture_without_api_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());

        let (auth_tx, mut auth_rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let request = read_http_request(&mut stream).await;
                let req = std::str::from_utf8(&request).unwrap();
                let has_auth = req.to_lowercase().contains("authorization");
                let _ = auth_tx.send(has_auth).await;

                let resp_body = r#"{"req_body":"e30="}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let data = fetch_capture(&base_url, "", 99).await.unwrap();
        let has_auth = auth_rx.recv().await.unwrap();
        assert!(!has_auth, "Request should NOT contain Authorization header");
        let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(json["req_body"], "e30=");
    }
}
