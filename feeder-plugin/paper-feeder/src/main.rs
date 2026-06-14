//! `paper-feeder` — arXiv + PubMed + Sci-Hub feeder sidecar.
//!
//! Hosts three [`meta_feeder_sdk::FeederPlugin`]s over the feeder HTTP
//! contract. arXiv and PubMed need no config. Sci-Hub is opt-in: it loads only
//! when `SCIHUB_MIRRORS` (comma-separated mirror URLs) is set — otherwise its
//! `configure()` returns `MissingConfig` and the harness soft-skips it, so the
//! feeder still serves arXiv + PubMed.
//!
//! Env:
//! - `META_FEEDER_HTTP_LISTEN` — listen addr (default `0.0.0.0:8080`)
//! - `META_FEEDER_STATE_DIR`   — per-plugin cache root (default `/data/meta-feeder`)
//! - `SCIHUB_MIRRORS`          — comma-separated Sci-Hub mirror base URLs (opt-in)
//! - `RUST_LOG`                — tracing filter (default `info`)

use std::net::SocketAddr;

use meta_feeder_sdk::{serve_feeders, FeederPlugin};
use paper_feeder::{arxiv::ArxivPlugin, pubmed::PubmedPlugin, scihub::ScihubPlugin};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let listen: SocketAddr = std::env::var("META_FEEDER_HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
    let state_dir =
        std::env::var("META_FEEDER_STATE_DIR").unwrap_or_else(|_| "/data/meta-feeder".to_string());

    let mut scihub = ScihubPlugin::new();
    if let Ok(raw) = std::env::var("SCIHUB_MIRRORS") {
        let mirrors: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !mirrors.is_empty() {
            scihub.set_mirrors(mirrors);
        }
    }

    let plugins: Vec<Box<dyn FeederPlugin>> = vec![
        Box::new(ArxivPlugin::new()),
        Box::new(PubmedPlugin::new()),
        Box::new(scihub),
    ];
    serve_feeders(plugins, state_dir, listen).await
}
