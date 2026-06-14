//! arXiv bridge via the public [arXiv API](https://info.arxiv.org/help/api/index.html).
//!
//! - Search: `GET /api/query?search_query=all:<q>&max_results=N` → Atom feed.
//! - Fetch one: `GET /api/query?id_list=<record_id>` → Atom feed (single entry).
//! - Canonical file format for midhash: the entry's PDF link
//!   (`<link title="pdf" href="…">`). arXiv always provides one; fall back
//!   to the derived URL pattern `<base>/pdf/<record_id>.pdf` if the link
//!   is missing for some reason.
//!
//! No auth, no API key. arXiv asks for a 3-second courtesy delay between
//! search requests — we don't poll automatically (only on user search +
//! on-demand midhash compute), so we satisfy that without explicit rate
//! limiting.
//!
//! **Versioned record_ids.** arXiv search returns ids like
//! `2106.07447v1` — the version suffix matters because v2 of the same
//! paper is a different file with a different midhash. The plugin
//! preserves whatever record_id the search returned and uses it
//! verbatim for the PDF fetch, so the (record_id → midhash) mapping is
//! stable across re-computes.
//!
//! See `docs/gateway-feature.md` §4 for the v0 plugin role.

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use quick_xml::events::Event;
use quick_xml::Reader;

use meta_feeder_sdk::common;
use meta_feeder_sdk::cache::MidhashCache;
#[cfg(test)]
use meta_feeder_sdk::plugin::HashKind;
use meta_feeder_sdk::plugin::{upstream_id_field, ConfigError, FeederPlugin, GatewayQuery, HashOutcome};
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, PluginHealth};

/// Public arXiv API base. `export.arxiv.org` is the recommended host
/// per arXiv's API docs (it's the rate-limit-friendly mirror; also
/// serves `/pdf/` so we get both API and PDF off the same host —
/// simpler test setup).
const DEFAULT_BASE_URL: &str = "https://export.arxiv.org";

/// Generous HTTP timeout. arXiv responds in <1 s usually but PDF
/// downloads of larger papers can hit 10–20 s on cold caches.
const HTTP_TIMEOUT_SECS: u64 = 30;

/// Polite identification per the arXiv API guidelines.
const USER_AGENT: &str = concat!("meta-share/", env!("CARGO_PKG_VERSION"), " (gateway:arxiv)");

/// arXiv gateway plugin. Same structural shape as gutenberg: cheap to
/// construct; `configure()` opens the per-plugin redb cache. The cache
/// stores `(record_id → midhash)` so we don't re-download a multi-MB
/// PDF on every "add" click.
pub struct ArxivPlugin {
    http: reqwest::Client,
    base_url: String,
    cache: Option<MidhashCache>,
}

impl Default for ArxivPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl ArxivPlugin {
    pub fn new() -> Self {
        Self::with_base_url(DEFAULT_BASE_URL.to_string())
    }

    /// Construct the plugin pointing at a non-default base URL — used by
    /// tests (against a `wiremock::MockServer`) and by deployments that
    /// want to ride a private mirror. The base must serve both
    /// `/api/query` (Atom feed) and `/pdf/<id>.pdf` paths.
    pub fn with_base_url(base_url: String) -> Self {
        let http = common::build_http_client(HTTP_TIMEOUT_SECS, USER_AGENT, None);
        Self {
            http,
            base_url,
            cache: None,
        }
    }

    fn cache(&self) -> Result<&MidhashCache, GatewayError> {
        common::require_cache(self.cache.as_ref(), "arxiv")
    }

    /// Search the arXiv Atom API. `query` is passed verbatim as the
    /// `all:` field, so callers get the full-text behaviour arXiv users
    /// expect (free-text + boolean syntax both work upstream).
    async fn search_atom(&self, query: &str, max_results: usize) -> Result<Vec<u8>, GatewayError> {
        let url = format!(
            "{}/api/query?search_query=all:{}&start=0&max_results={}",
            self.base_url.trim_end_matches('/'),
            common::urlencode(query),
            max_results
        );
        common::fetch_bytes(&self.http, &url)
            .await
            .map(|b| b.to_vec())
    }

    /// Single-entry lookup by id. Used by `compute_outcomes` to refresh
    /// the record metadata + locate the PDF link. Same Atom shape as
    /// search; the `id_list=` form returns at most one entry.
    async fn fetch_entry(&self, record_id: &str) -> Result<ArxivEntry, GatewayError> {
        let url = format!(
            "{}/api/query?id_list={}",
            self.base_url.trim_end_matches('/'),
            common::urlencode(record_id)
        );
        let body = common::fetch_bytes(&self.http, &url).await?;
        let mut entries = parse_arxiv_feed(&body).map_err(|e| {
            GatewayError::Permanent(format!("parse arxiv atom for {record_id}: {e}"))
        })?;
        if entries.is_empty() {
            return Err(GatewayError::NotFound);
        }
        Ok(entries.remove(0))
    }

    /// Decide which URL to fetch for `compute_outcomes`. The entry's
    /// `<link title="pdf">` is preferred (canonical, version-stable);
    /// falls back to a derived `/pdf/<id>.pdf` pattern if absent. The
    /// fallback also rewrites scheme/host to the configured base so a
    /// test wiremock can intercept (real arXiv emits absolute URLs
    /// pointing at `arxiv.org`, which would bypass a wiremock pointed
    /// at a different host).
    fn pdf_url_for(&self, entry: &ArxivEntry, record_id: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        if let Some(link) = entry.pdf_link.as_deref() {
            // For real arXiv: the link absolute-URL points at
            // `arxiv.org`. For private mirrors / wiremock: the link
            // already points at our base. Rewrite the host iff our base
            // host differs from the link's, so the same code works in
            // both cases.
            if let Some(rewritten) = rewrite_host(link, base) {
                return rewritten;
            }
        }
        format!("{base}/pdf/{record_id}.pdf")
    }
}

#[async_trait]
impl FeederPlugin for ArxivPlugin {
    fn upstream_id(&self) -> &'static str {
        "arxiv"
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        self.cache = Some(common::open_midhash_cache(cache_dir, "arxiv")?);
        Ok(())
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // Layer A early-return: arXiv only serves `document` /
        // `paper`. A `fileType:image` or `contentKind:movie` filter
        // can never match — skip the upstream call entirely.
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }
        let q = query.free_text_or_star();
        let body = self.search_atom(q, max_results).await?;
        let entries = parse_arxiv_feed(&body).map_err(|e| {
            GatewayError::Permanent(format!("parse arxiv atom search response: {e}"))
        })?;
        // arXiv's `max_results=` is a soft cap; truncate defensively so
        // we never return more than the caller asked for.
        Ok(entries
            .into_iter()
            .take(max_results)
            .map(into_discovery_record)
            .collect())
    }

    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let cache = self.cache()?;
        if let Some(hit) = common::cached_outcome(cache, record_id, "arxiv")? {
            return Ok(hit);
        }

        let entry = self.fetch_entry(record_id).await?;
        let pdf_url = self.pdf_url_for(&entry, record_id);
        let bytes = common::fetch_bytes(&self.http, &pdf_url).await?;
        let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);

        common::store_midhash(cache, record_id, "arxiv", &cid);

        let record = into_discovery_record(entry);
        Ok(common::single_outcome(
            cid,
            bytes,
            record,
            Some("pdf".to_string()),
        ))
    }

    fn health(&self) -> PluginHealth {
        if self.cache.is_some() {
            PluginHealth::Ok
        } else {
            PluginHealth::Degraded {
                reason: "configure() not yet called".to_string(),
            }
        }
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        &["document"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["paper"]
    }
}

// -- Atom feed parsing -------------------------------------------------------
//
// arXiv's API returns an Atom feed with one `<entry>` per result. We
// only need a handful of fields; the parser walks events and accumulates
// per-entry state rather than building a full DOM. quick-xml's
// streaming API is ~10x lighter than serde-xml here.

/// Parsed shape of one Atom `<entry>` element. Fields the gateway
/// surfaces as `DiscoveryRecord.fields` after the
/// `into_discovery_record` conversion.
#[derive(Debug, Default, PartialEq)]
struct ArxivEntry {
    /// Canonical record_id (the `2106.07447v1`-style suffix of the
    /// `<id>http://arxiv.org/abs/...</id>` element). Versioned when the
    /// search returned a versioned id; unversioned otherwise. Stable
    /// across re-computes.
    record_id: String,
    title: Option<String>,
    summary: Option<String>,
    /// ISO 8601 `published` timestamp, kept verbatim — used for
    /// the `year` derived field and as a sortable string in meta-core.
    published: Option<String>,
    authors: Vec<String>,
    /// All `<category term="…">` values. arXiv uses the `cs.AI`,
    /// `math.GT`, etc. taxonomy. Stored as a Vec; collapsed to a CSV
    /// when emitting the DiscoveryRecord.
    categories: Vec<String>,
    /// `<arxiv:primary_category term="…">`. Always present in arXiv's
    /// feed; may equal `categories[0]` or not, depending on tagging.
    primary_category: Option<String>,
    /// Absolute URL of the PDF, extracted from
    /// `<link title="pdf" href="…">`. Used directly by
    /// `compute_outcomes` (the version suffix in the URL is what makes
    /// the midhash stable across re-computes).
    pdf_link: Option<String>,
}

/// Parse an arXiv Atom feed into entries. Tolerant of namespace
/// prefixes (`arxiv:primary_category`) and self-closing `<link/>` tags.
fn parse_arxiv_feed(xml: &[u8]) -> Result<Vec<ArxivEntry>, quick_xml::Error> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut entries: Vec<ArxivEntry> = Vec::new();
    let mut in_entry = false;
    let mut current: ArxivEntry = ArxivEntry::default();
    // Track which simple text field we're collecting between open + close.
    // Only one is open at a time within an entry.
    let mut field: TextField = TextField::None;
    // Author name extraction: we enter `<author>`, then `<name>`, then
    // capture text. `in_author_name` is true only inside `<name>` inside
    // `<author>`.
    let mut in_author = false;
    let mut in_author_name = false;
    let mut text_buf = String::new();
    let mut event_buf = Vec::with_capacity(1024);

    loop {
        match reader.read_event_into(&mut event_buf)? {
            Event::Start(e) => {
                let name = local_name(e.name().as_ref());
                if name == "entry" {
                    in_entry = true;
                    current = ArxivEntry::default();
                } else if in_entry {
                    match name.as_str() {
                        "id" => field = TextField::Id,
                        "title" => field = TextField::Title,
                        "summary" => field = TextField::Summary,
                        "published" => field = TextField::Published,
                        "author" => in_author = true,
                        "name" if in_author => {
                            in_author_name = true;
                            field = TextField::Author;
                        }
                        _ => {}
                    }
                }
            }
            Event::Empty(e) => {
                // Self-closing tags: link, category, arxiv:primary_category.
                if !in_entry {
                    continue;
                }
                let name = local_name(e.name().as_ref());
                match name.as_str() {
                    "link" => {
                        let attrs = collect_attrs(&e);
                        // arXiv emits two links per entry: rel=alternate
                        // (HTML abs page) and rel=related title=pdf. We
                        // only want the PDF.
                        if attrs.get("title").map(String::as_str) == Some("pdf") {
                            if let Some(href) = attrs.get("href") {
                                current.pdf_link = Some(href.clone());
                            }
                        }
                    }
                    "category" => {
                        let attrs = collect_attrs(&e);
                        if let Some(term) = attrs.get("term") {
                            current.categories.push(term.clone());
                        }
                    }
                    "primary_category" => {
                        let attrs = collect_attrs(&e);
                        if let Some(term) = attrs.get("term") {
                            current.primary_category = Some(term.clone());
                        }
                    }
                    _ => {}
                }
            }
            Event::Text(t) => {
                if !in_entry {
                    continue;
                }
                match field {
                    TextField::None => {}
                    _ => {
                        // Atom feeds may split text across events when
                        // entity references are involved; accumulate.
                        if let Ok(s) = t.unescape() {
                            text_buf.push_str(&s);
                        }
                    }
                }
            }
            Event::End(e) => {
                let name = local_name(e.name().as_ref());
                if !in_entry && name != "entry" {
                    continue;
                }
                match name.as_str() {
                    "id" => {
                        current.record_id = strip_abs_prefix(&text_buf);
                        finish_field(&mut field, &mut text_buf);
                    }
                    "title" => {
                        current.title = Some(normalize_whitespace(&text_buf));
                        finish_field(&mut field, &mut text_buf);
                    }
                    "summary" => {
                        current.summary = Some(normalize_whitespace(&text_buf));
                        finish_field(&mut field, &mut text_buf);
                    }
                    "published" => {
                        current.published = Some(text_buf.trim().to_string());
                        finish_field(&mut field, &mut text_buf);
                    }
                    "name" if in_author_name => {
                        let name = text_buf.trim().to_string();
                        if !name.is_empty() {
                            current.authors.push(name);
                        }
                        in_author_name = false;
                        finish_field(&mut field, &mut text_buf);
                    }
                    "author" => {
                        in_author = false;
                    }
                    "entry" => {
                        in_entry = false;
                        entries.push(std::mem::take(&mut current));
                    }
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
        event_buf.clear();
    }
    Ok(entries)
}

/// Which simple text-bearing field the parser is currently inside.
/// Author names live inside `<author><name>...</name></author>`, so the
/// state machine flips `Author` only when both flags are set.
#[derive(Debug)]
enum TextField {
    None,
    Id,
    Title,
    Summary,
    Published,
    Author,
}

fn finish_field(field: &mut TextField, buf: &mut String) {
    *field = TextField::None;
    buf.clear();
}

/// quick-xml hands element names with any XML-namespace prefix attached
/// (e.g. `arxiv:primary_category`). We treat both prefixed and bare
/// forms the same — arXiv only uses one namespace beyond Atom, and we
/// match against the local-name suffix only.
fn local_name(raw: &[u8]) -> String {
    let s = std::str::from_utf8(raw).unwrap_or_default();
    match s.rfind(':') {
        Some(i) => s[i + 1..].to_string(),
        None => s.to_string(),
    }
}

/// Walk a `Start`/`Empty` event's attributes into a `key → value` map.
/// Small allocations only; called once per `<link>` / `<category>`.
fn collect_attrs(e: &quick_xml::events::BytesStart<'_>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for attr in e.attributes().flatten() {
        let key = local_name(attr.key.as_ref());
        let val = std::str::from_utf8(&attr.value)
            .unwrap_or_default()
            .to_string();
        out.insert(key, val);
    }
    out
}

/// `<id>http://arxiv.org/abs/2106.07447v1</id>` → `2106.07447v1`.
/// Tolerates `https://` and missing trailing version. Returns the
/// input verbatim if it doesn't look like an arXiv abs URL.
fn strip_abs_prefix(raw: &str) -> String {
    let trimmed = raw.trim();
    for prefix in [
        "http://arxiv.org/abs/",
        "https://arxiv.org/abs/",
        "http://export.arxiv.org/abs/",
        "https://export.arxiv.org/abs/",
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            return rest.to_string();
        }
    }
    trimmed.to_string()
}

/// Collapse runs of whitespace into single spaces and trim. arXiv
/// titles and summaries come with line-wrapping baked in; the UI
/// renders them in a single line so we normalize at ingest.
fn normalize_whitespace(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Rewrite the host of an absolute URL so a test wiremock can intercept
/// it. Returns `None` if the input isn't a valid absolute http(s) URL.
/// Real arXiv emits PDF links pointing at `arxiv.org` even when the
/// API is hit through `export.arxiv.org`; this lets us always fetch
/// the PDF through the same host we already configured for search.
fn rewrite_host(url: &str, new_base: &str) -> Option<String> {
    let url = url.trim();
    let (_scheme, rest_with_path) = if let Some(s) = url.strip_prefix("http://") {
        ("http", s)
    } else if let Some(s) = url.strip_prefix("https://") {
        ("https", s)
    } else {
        return None;
    };
    let path_start = rest_with_path.find('/')?;
    let path = &rest_with_path[path_start..];
    Some(format!("{}{path}", new_base.trim_end_matches('/')))
}

/// Percent-encode a query-string value. arXiv accepts a small alphabet
/// in `search_query=` and `id_list=`; the safe-set covers everything
/// the gateway emits (free-text queries, dotted arXiv ids, version
/// suffixes).
/// Convert a parsed arXiv entry into the wire-shape `DiscoveryRecord`.
/// Field naming follows `docs/gateway-feature.md` §6.4 (`title`, `type`,
/// `sourceUrl`, `fileName`) with arXiv-specific keys (`arxivid`,
/// `author`, `categories`, `primaryCategory`, `summary`, `published`).
/// The canonical `arxivid` field is required (§6.4 — `format!("{upstream_id}id")`),
/// so typed search filters like `arxivid:2106.07447` work.
fn into_discovery_record(entry: ArxivEntry) -> DiscoveryRecord {
    let record_id = entry.record_id.clone();
    let mut fields: BTreeMap<String, String> = BTreeMap::new();

    if let Some(t) = entry.title {
        fields.insert("title".to_string(), t);
    }
    fields.insert("fileType".to_string(), "document".to_string());
    fields.insert("contentKind".to_string(), "paper".to_string());
    fields.insert(
        "sourceUrl".to_string(),
        format!("https://arxiv.org/abs/{record_id}"),
    );
    fields.insert("fileName".to_string(), format!("arxiv-{record_id}.pdf"));
    fields.insert(upstream_id_field("arxiv"), record_id.clone());
    fields.insert("format".to_string(), "pdf".to_string());
    if !entry.authors.is_empty() {
        fields.insert("author".to_string(), entry.authors.join(", "));
    }
    if !entry.categories.is_empty() {
        fields.insert("categories".to_string(), entry.categories.join(", "));
    }
    if let Some(pc) = entry.primary_category {
        fields.insert("primaryCategory".to_string(), pc);
    }
    if let Some(p) = entry.published.as_deref() {
        fields.insert("published".to_string(), p.to_string());
        // arXiv's `published` is ISO 8601 like `2021-06-14T17:46:43Z` —
        // first 4 chars are the year. Useful for the UI's year filter
        // and meta-core's structured tokenization.
        if p.len() >= 4 && p[..4].chars().all(|c| c.is_ascii_digit()) {
            fields.insert("year".to_string(), p[..4].to_string());
        }
    }
    if let Some(s) = entry.summary {
        fields.insert("summary".to_string(), s);
    }

    DiscoveryRecord {
        upstream_id: "arxiv".to_string(),
        record_id,
        fields,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query: search_query=all:test</title>
  <updated>2026-01-01T00:00:00Z</updated>
  <entry>
    <id>http://arxiv.org/abs/2106.07447v1</id>
    <updated>2021-06-15T18:00:00Z</updated>
    <published>2021-06-14T17:46:43Z</published>
    <title>HuBERT: Self-Supervised Speech Representation Learning</title>
    <summary>We propose a self-supervised approach. The model learns from data.</summary>
    <author><name>Wei-Ning Hsu</name></author>
    <author><name>Benjamin Bolte</name></author>
    <arxiv:primary_category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.SD" scheme="http://arxiv.org/schemas/atom"/>
    <link href="http://arxiv.org/abs/2106.07447v1" rel="alternate" type="text/html"/>
    <link title="pdf" href="http://arxiv.org/pdf/2106.07447v1.pdf" rel="related" type="application/pdf"/>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/1706.03762v5</id>
    <title>Attention Is All You Need</title>
    <summary>The dominant sequence transduction models are based on RNNs.</summary>
    <author><name>Ashish Vaswani</name></author>
    <author><name>Noam Shazeer</name></author>
    <published>2017-06-12T17:57:34Z</published>
    <arxiv:primary_category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <link href="http://arxiv.org/abs/1706.03762v5" rel="alternate" type="text/html"/>
    <link title="pdf" href="http://arxiv.org/pdf/1706.03762v5.pdf" rel="related" type="application/pdf"/>
  </entry>
</feed>"#;

    #[test]
    fn parses_atom_feed_into_entries() {
        let entries = parse_arxiv_feed(SAMPLE_FEED.as_bytes()).expect("parse");
        assert_eq!(entries.len(), 2);

        let e0 = &entries[0];
        assert_eq!(e0.record_id, "2106.07447v1");
        assert_eq!(
            e0.title.as_deref(),
            Some("HuBERT: Self-Supervised Speech Representation Learning")
        );
        assert_eq!(e0.authors, vec!["Wei-Ning Hsu", "Benjamin Bolte"]);
        assert_eq!(e0.categories, vec!["cs.CL", "cs.SD"]);
        assert_eq!(e0.primary_category.as_deref(), Some("cs.CL"));
        assert_eq!(e0.published.as_deref(), Some("2021-06-14T17:46:43Z"));
        assert_eq!(
            e0.pdf_link.as_deref(),
            Some("http://arxiv.org/pdf/2106.07447v1.pdf")
        );

        let e1 = &entries[1];
        assert_eq!(e1.record_id, "1706.03762v5");
        assert_eq!(e1.authors, vec!["Ashish Vaswani", "Noam Shazeer"]);
    }

    #[test]
    fn into_discovery_record_emits_required_fields() {
        let entries = parse_arxiv_feed(SAMPLE_FEED.as_bytes()).expect("parse");
        let rec = into_discovery_record(entries.into_iter().next().unwrap());
        assert_eq!(rec.upstream_id, "arxiv");
        assert_eq!(rec.record_id, "2106.07447v1");
        // §6.4 conventions.
        assert_eq!(
            rec.fields.get("fileType").map(String::as_str),
            Some("document")
        );
        assert_eq!(
            rec.fields.get("contentKind").map(String::as_str),
            Some("paper")
        );
        assert_eq!(rec.fields.get("format").map(String::as_str), Some("pdf"));
        assert_eq!(
            rec.fields.get("sourceUrl").map(String::as_str),
            Some("https://arxiv.org/abs/2106.07447v1")
        );
        assert_eq!(
            rec.fields.get("fileName").map(String::as_str),
            Some("arxiv-2106.07447v1.pdf")
        );
        // Canonical `<upstream_id>id` field — required by §6.4 for
        // typed search filters.
        assert_eq!(
            rec.fields.get("arxivid").map(String::as_str),
            Some("2106.07447v1")
        );
        // Year derived from published timestamp.
        assert_eq!(rec.fields.get("year").map(String::as_str), Some("2021"));
        // Author list flattened.
        assert_eq!(
            rec.fields.get("author").map(String::as_str),
            Some("Wei-Ning Hsu, Benjamin Bolte")
        );
        // Categories flattened.
        assert_eq!(
            rec.fields.get("categories").map(String::as_str),
            Some("cs.CL, cs.SD")
        );
    }

    #[test]
    fn strip_abs_prefix_accepts_both_schemes() {
        assert_eq!(
            strip_abs_prefix("http://arxiv.org/abs/2106.07447v1"),
            "2106.07447v1"
        );
        assert_eq!(
            strip_abs_prefix("https://arxiv.org/abs/2106.07447"),
            "2106.07447"
        );
        assert_eq!(strip_abs_prefix("https://export.arxiv.org/abs/foo"), "foo");
        assert_eq!(strip_abs_prefix("not-an-arxiv-url"), "not-an-arxiv-url");
    }

    #[test]
    fn rewrite_host_swaps_authority() {
        assert_eq!(
            rewrite_host(
                "http://arxiv.org/pdf/2106.07447v1.pdf",
                "https://export.arxiv.org"
            ),
            Some("https://export.arxiv.org/pdf/2106.07447v1.pdf".to_string())
        );
        assert_eq!(rewrite_host("not-a-url", "https://example.com"), None);
    }

    #[test]
    fn normalize_whitespace_collapses_runs() {
        assert_eq!(
            normalize_whitespace("  Hello\n  world\t!  "),
            "Hello world !"
        );
    }

    // -- HTTP integration tests against a wiremock server ----------------

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn configured_plugin_against(server: &MockServer) -> (ArxivPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin = ArxivPlugin::with_base_url(server.uri());
        plugin.configure(dir.path()).expect("configure");
        (plugin, dir)
    }

    #[tokio::test]
    async fn handle_query_maps_atom_response_to_discovery_records() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/query"))
            .and(query_param("search_query", "all:test"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(SAMPLE_FEED)
                    .insert_header("content-type", "application/atom+xml"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("test"), 10)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record_id, "2106.07447v1");
        assert_eq!(
            records[0].fields.get("arxivid").map(String::as_str),
            Some("2106.07447v1")
        );
    }

    #[tokio::test]
    async fn handle_query_truncates_to_max_results() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(SAMPLE_FEED))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("test"), 1)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn compute_outcomes_fetches_pdf_and_caches() {
        let server = MockServer::start().await;
        // Single-entry id_list response — same shape as a search feed
        // but with one entry. The plugin's `fetch_entry` calls this.
        let single = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/2106.07447v1</id>
    <title>HuBERT</title>
    <published>2021-06-14T17:46:43Z</published>
    <author><name>Author One</name></author>
    <arxiv:primary_category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <link href="http://arxiv.org/abs/2106.07447v1" rel="alternate" type="text/html"/>
    <link title="pdf" href="http://arxiv.org/pdf/2106.07447v1.pdf" rel="related" type="application/pdf"/>
  </entry>
</feed>"#;
        Mock::given(method("GET"))
            .and(path("/api/query"))
            .and(query_param("id_list", "2106.07447v1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(single)
                    .insert_header("content-type", "application/atom+xml"),
            )
            .mount(&server)
            .await;
        // PDF response — bytes get hashed; the test asserts the
        // resulting CID is the canonical sha256/multihash of these bytes.
        let pdf_bytes = b"%PDF-1.4 fake pdf body for hashing\n".to_vec();
        let expected_cid = meta_feeder_sdk::hash::compute_ipfs_cid(&pdf_bytes);
        Mock::given(method("GET"))
            .and(path("/pdf/2106.07447v1.pdf"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(pdf_bytes.clone())
                    .insert_header("content-type", "application/pdf"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let outcomes = plugin
            .compute_outcomes("2106.07447v1")
            .await
            .expect("compute");
        assert_eq!(outcomes.len(), 1, "arxiv is single-outcome");
        let outcome = &outcomes[0];
        assert_eq!(outcome.hash.as_str(), expected_cid);
        assert_eq!(outcome.hash_kind, HashKind::Sha2_256);
        // Auto-store inputs: bytes + record + file_extension.
        assert_eq!(outcome.bytes.as_deref(), Some(pdf_bytes.as_slice()));
        assert_eq!(outcome.file_extension.as_deref(), Some("pdf"));
        let rec = outcome
            .record
            .as_ref()
            .expect("record present on fresh compute");
        assert_eq!(rec.record_id, "2106.07447v1");
        assert_eq!(
            rec.fields.get("primaryCategory").map(String::as_str),
            Some("cs.CL")
        );
    }

    #[tokio::test]
    async fn compute_outcomes_cache_hit_skips_http() {
        let server = MockServer::start().await;
        let (plugin, _dir) = configured_plugin_against(&server);
        // Seed the cache directly so the plugin won't issue any HTTP.
        plugin
            .cache
            .as_ref()
            .unwrap()
            .put_midhash("2106.07447v1", "bafyCACHED")
            .unwrap();
        let outcomes = plugin
            .compute_outcomes("2106.07447v1")
            .await
            .expect("cache hit");
        assert_eq!(outcomes.len(), 1);
        let outcome = &outcomes[0];
        assert_eq!(outcome.hash.as_str(), "bafyCACHED");
        // Cache hits return sparse outcomes — the auto-store side
        // effect has already happened on the original miss.
        assert!(outcome.bytes.is_none());
        assert!(outcome.record.is_none());
    }

    /// Partial-success contract guard (§1.5): a hard upstream failure
    /// must still return `Err`, NOT `Ok(vec![])`. `Ok(vec![])` means
    /// "tried, confirmed empty" — distinct semantics. Drift here would
    /// silently turn errors into empty bundles.
    #[tokio::test]
    async fn upstream_404_maps_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/query"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("does-not-exist")
            .await
            .expect_err("expected NotFound (Err, not Ok(vec![]))");
        assert!(matches!(err, GatewayError::NotFound), "got {err:?}");
    }
}
