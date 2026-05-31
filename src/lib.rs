pub mod capture;
pub mod config;
pub mod models;
pub mod sse;

#[cfg(test)]
mod tests {
    use crate::capture::{capture_file_name, write_capture};
    use crate::config::{CaptureConfig, resolve_server_url};
    use crate::models::ActivityLogEntry;
    use crate::sse::{
        backoff_delay, fetch_capture, parse_metrics_event, parse_sse_envelope,
        process_metrics_event, run_sse_loop,
    };
    use chrono::TimeZone;
    use std::path::Path;
    use tokio::io::AsyncWriteExt;

    // --- parse_metrics_event tests ---

    #[test]
    fn test_parse_metrics_event_single_entry_with_capture() {
        let payload = r#"[{"id":42,"timestamp":"2025-01-15T10:30:00Z","model":"llama3","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{"cache_tokens":0,"input_tokens":10,"output_tokens":20,"prompt_per_second":100,"tokens_per_second":50},"duration_ms":500,"has_capture":true}]"#;
        let entries = parse_metrics_event(payload).expect("unexpected error");
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.id, 42);
        assert!(e.has_capture);
        assert_eq!(e.model, "llama3");
    }

    #[test]
    fn test_parse_metrics_event_multiple_entries() {
        let payload = r#"[{"id":1,"has_capture":true,"timestamp":"2025-01-15T10:30:00Z","model":"m1","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":100},{"id":2,"has_capture":false,"timestamp":"2025-01-15T10:31:00Z","model":"m2","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":200}]"#;
        let entries = parse_metrics_event(payload).expect("unexpected error");
        assert_eq!(entries.len(), 2);
        assert!(entries[0].has_capture);
        assert!(!entries[1].has_capture);
    }

    #[test]
    fn test_parse_metrics_event_empty_array() {
        let entries = parse_metrics_event("[]").expect("unexpected error");
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_parse_metrics_event_invalid_json() {
        let result = parse_metrics_event("not json");
        assert!(result.is_err());
    }

    // --- capture_file_name tests ---

    #[test]
    fn test_capture_file_name_standard() {
        let ts = chrono::Utc
            .with_ymd_and_hms(2025, 1, 15, 10, 30, 0)
            .unwrap();
        let name = capture_file_name(&ts);
        assert_eq!(name, "2025-01-15T10-30-00");
    }

    #[test]
    fn test_capture_file_name_zero_time() {
        let ts = chrono::Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap();
        let name = capture_file_name(&ts);
        assert_eq!(name, "0001-01-01T00-00-00");
    }

    #[test]
    fn test_capture_file_name_no_timezone_suffix() {
        let ts = chrono::Utc
            .with_ymd_and_hms(2025, 6, 1, 23, 59, 59)
            .unwrap();
        let name = capture_file_name(&ts);
        assert!(!name.ends_with('Z'));
        assert!(!name.contains("+"));
    }

    // --- write_capture tests ---

    #[tokio::test]
    async fn test_write_capture_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let ts = chrono::Utc
            .with_ymd_and_hms(2025, 3, 10, 14, 20, 30)
            .unwrap();
        let entry = ActivityLogEntry {
            id: 7,
            timestamp: ts,
            model: "test-model".to_string(),
            req_path: "/v1/chat/completions".to_string(),
            resp_content_type: "application/json".to_string(),
            resp_status_code: 200,
            tokens: Default::default(),
            duration_ms: 0,
            has_capture: true,
            capture: None,
        };
        let data = br#"{"id":7}"#;

        let filename = write_capture(path.to_str().unwrap(), &entry, data)
            .await
            .unwrap();
        let expected = path.join("2025-03-10T14-20-30.log");
        assert_eq!(Path::new(&filename), expected);

        let got = tokio::fs::read(&filename).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn test_write_capture_creates_output_dir() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("sub").join("dir");
        let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let entry = ActivityLogEntry {
            id: 0,
            timestamp: ts,
            model: "".to_string(),
            req_path: "".to_string(),
            resp_content_type: "".to_string(),
            resp_status_code: 0,
            tokens: Default::default(),
            duration_ms: 0,
            has_capture: false,
            capture: None,
        };
        let data = br#"{}"#;

        let _ = write_capture(dir.to_str().unwrap(), &entry, data)
            .await
            .unwrap();
        assert!(dir.exists());
    }

    // --- fetch_capture tests ---

    async fn start_mock_server(handler: fn(tokio::net::TcpStream)) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let handler = handler;
                tokio::spawn(async move {
                    handler(stream);
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn test_fetch_capture_returns_json() {
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n{\"id\":99,\"path\":\"/\"}";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let data = fetch_capture(&url, "", 99).await.unwrap();
        assert_eq!(data, br#"{"id":99,"path":"/"}"#);
    }

    #[tokio::test]
    async fn test_fetch_capture_sends_api_key() {
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let _ = fetch_capture(&url, "test-key-123", 1).await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_capture_skips_empty_api_key() {
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let _ = fetch_capture(&url, "", 1).await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_capture_error_on_non_200() {
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let result = fetch_capture(&url, "", 1).await;
        assert!(result.is_err());
    }

    // --- parse_sse_envelope tests ---

    #[test]
    fn test_parse_sse_envelope_metrics() {
        let envelope = r#"{"type":"metrics","data":"[{\"id\":1}]"}"#;
        let env = parse_sse_envelope(envelope).expect("should parse");
        assert_eq!(env.type_, "metrics");
        assert_eq!(env.data, r#"[{"id":1}]"#);
    }

    #[test]
    fn test_parse_sse_envelope_non_metrics() {
        let envelope = r#"{"type":"modelStatus","data":"[]"}"#;
        let env = parse_sse_envelope(envelope).expect("should parse");
        assert_eq!(env.type_, "modelStatus");
    }

    #[test]
    fn test_parse_sse_envelope_invalid_json() {
        let result = parse_sse_envelope("not json");
        assert!(result.is_none());
    }

    // --- process_metrics_event tests ---

    #[tokio::test]
    async fn test_process_metrics_event_fetches_capture() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let cfg = CaptureConfig {
            url,
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
        };
        let payload = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        process_metrics_event(&cfg, payload).await.unwrap();
        assert!(dir.path().join("2025-01-01T00-00-00.log").exists());
    }

    #[tokio::test]
    async fn test_process_metrics_event_skips_no_capture() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
            decode: false,
        };
        let payload = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":false}]"#;
        process_metrics_event(&cfg, payload).await.unwrap();
        assert_eq!(dir.path().read_dir().unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_process_metrics_event_empty_array() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = CaptureConfig {
            url: "http://localhost:8080".to_string(),
            api_key: "".to_string(),
            output: dir.path().to_str().unwrap().to_string(),
        };
        process_metrics_event(&cfg, "[]").await.unwrap();
        assert_eq!(dir.path().read_dir().unwrap().count(), 0);
    }

    // --- run_sse_loop tests ---

    #[tokio::test]
    async fn test_run_sse_loop_skips_initial_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let cfg = CaptureConfig {
            server_url: url,
            api_key: "".to_string(),
            output_dir: dir.path().to_str().unwrap().to_string(),
        };

        let initial_metrics = r#"[{"id":0,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let initial_env = serde_json::json!({"type": "metrics", "data": initial_metrics});
        let new_metrics = r#"[{"id":1,"timestamp":"2025-01-01T00:01:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let new_env = serde_json::json!({"type": "metrics", "data": new_metrics});

        let sse_lines = format!("data: {}\n\ndata: {}\n\n", initial_env, new_env);
        let reader = std::io::Cursor::new(sse_lines.into_bytes());
        let stream = tokio_util::io::ReaderStream::new(reader);

        let mut skipped_initial = false;
        run_sse_loop(&cfg, stream, &mut skipped_initial)
            .await
            .unwrap();
        assert!(!dir.path().join("2025-01-01T00-00-00.log").exists());
        assert!(dir.path().join("2025-01-01T00-01-00.log").exists());
    }

    #[tokio::test]
    async fn test_run_sse_loop_multiple_metrics_in_one_event() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let cfg = CaptureConfig {
            server_url: url,
            api_key: "".to_string(),
            output_dir: dir.path().to_str().unwrap().to_string(),
            decode: false,
        };

        let multi_metrics = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true},{"id":2,"timestamp":"2025-01-01T00:01:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let env = serde_json::json!({"type": "metrics", "data": multi_metrics});

        let sse_lines = format!("data: {}\n\n", env);
        let reader = std::io::Cursor::new(sse_lines.into_bytes());
        let stream = tokio_util::io::ReaderStream::new(reader);

        let mut skipped_initial = true;
        run_sse_loop(&cfg, stream, &mut skipped_initial)
            .await
            .unwrap();
        assert!(dir.path().join("2025-01-01T00-00-00.log").exists());
        assert!(dir.path().join("2025-01-01T00-01-00.log").exists());
    }

    #[tokio::test]
    async fn test_run_sse_loop_buffers_split_json_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let url = start_mock_server(|mut stream| {
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
            tokio::spawn(async move {
                // write directly
                let _ = stream.write_all(resp).await;
            });
        })
        .await;

        let cfg = CaptureConfig {
            server_url: url,
            api_key: "".to_string(),
            output_dir: dir.path().to_str().unwrap().to_string(),
            decode: false,
        };

        let metrics = r#"[{"id":1,"timestamp":"2025-01-01T00:00:00Z","model":"m","req_path":"/v1/chat/completions","resp_content_type":"application/json","resp_status_code":200,"tokens":{},"duration_ms":0,"has_capture":true}]"#;
        let env = serde_json::json!({"type": "metrics", "data": metrics});
        let sse_lines = format!("data: {}\n\n", env);
        let split = sse_lines.find(r#""data""#).unwrap();
        let bytes = sse_lines.as_bytes();
        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = vec![
            Ok(bytes::Bytes::copy_from_slice(&bytes[..split])),
            Ok(bytes::Bytes::copy_from_slice(&bytes[split..])),
        ];
        let stream = futures::stream::iter(chunks);

        let mut skipped_initial = true;
        run_sse_loop(&cfg, stream, &mut skipped_initial)
            .await
            .unwrap();
        assert!(dir.path().join("2025-01-01T00-00-00.log").exists());
    }

    // --- validate_flags tests ---

    #[test]
    fn test_validate_flags_missing_server() {
        let cli = CliArgs {
            server: "".to_string(),
            output: "/tmp/out".to_string(),
            api_key: "".to_string(),
        };
        let result = CaptureConfig::new(cli);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_flags_missing_output() {
        let cli = CliArgs {
            server: "http://localhost:8080".to_string(),
            output: "".to_string(),
            api_key: "".to_string(),
        };
        let result = CaptureConfig::new(cli);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_flags_valid_with_api_key() {
        let cli = CliArgs {
            server: "http://localhost:8080".to_string(),
            output: "/tmp/out".to_string(),
            api_key: "secret".to_string(),
        };
        let result = CaptureConfig::new(cli);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_flags_valid_without_api_key() {
        let cli = CliArgs {
            server: "http://localhost:8080".to_string(),
            output: "/tmp/out".to_string(),
            api_key: "".to_string(),
        };
        let result = CaptureConfig::new(cli);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_flags_invalid_server_url() {
        let cli = CliArgs {
            server: "/".to_string(),
            output: "/tmp/out".to_string(),
            api_key: "".to_string(),
        };
        let result = CaptureConfig::new(cli);
        assert!(result.is_err());
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
}
