use std::path::Path;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "llama-capture", about = "Capture CLI for llama-swap proxy")]
pub struct CaptureConfig {
    /// Llama-swap server URL (e.g. http://localhost:8080)
    #[arg(short, long)]
    pub url: String,

    /// Output folder to write capture files (use '-' for stdout)
    #[arg(short, long, default_value = "-")]
    pub output: String,

    /// API key for authentication (optional)
    #[arg(short = 'k', long, default_value_t = String::new())]
    pub api_key: String,

    /// Decode conversation from base64
    #[arg(short, long)]
    pub decode: bool,

    /// Pretty print output
    #[arg(short, long)]
    pub pretty: bool,
}

impl CaptureConfig {
    pub fn is_stdout(&self) -> bool {
        self.output == "-"
    }

    pub fn output_folder(&self) -> Option<&Path> {
        if self.is_stdout() {
            None
        } else {
            Some(Path::new(&self.output))
        }
    }
}

pub fn resolve_server_url(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}
