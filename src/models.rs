use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ActivityLogEntry {
    pub id: i64,

    pub timestamp: chrono::DateTime<chrono::Utc>,

    pub model: String,

    pub req_path: String,

    pub resp_content_type: String,

    pub resp_status_code: i64,

    pub tokens: TokenMetrics,

    pub duration_ms: i64,

    pub has_capture: bool,

    pub capture: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct TokenMetrics {
    pub cache_tokens: i64,

    pub input_tokens: i64,

    pub output_tokens: i64,

    pub prompt_per_second: f64,

    pub tokens_per_second: f64,
}

#[derive(Debug, Deserialize)]
pub struct SSEEnvelope {
    #[serde(rename = "type")]
    pub type_: String,

    pub data: String,
}
