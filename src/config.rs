use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "llama-capture", about = "Capture CLI for llama-swap proxy")]
pub struct CaptureConfig {
    /// Llama-swap server URL (e.g. http://localhost:8080)
    #[arg(short, long)]
    pub url: String,

    /// Output folder for capture log files
    #[arg(short, long)]
    pub output: String,

    /// API key for authentication (optional)
    #[arg(short = 'k', long, default_value_t = String::new())]
    pub api_key: String,

    /// Decode conversation from base64
    #[arg(short, long)]
    pub decode: bool,
}

pub fn resolve_server_url(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}
