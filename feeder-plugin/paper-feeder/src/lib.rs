//! `paper-feeder` library surface — the three paper-source plugins. The binary
//! (`main.rs`) wraps them in [`meta_feeder_sdk::serve_feeders`].

pub mod arxiv;
pub mod pubmed;
pub mod scihub;
