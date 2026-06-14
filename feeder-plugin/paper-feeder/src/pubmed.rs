//! PubMed Central bridge via the [Europe PMC REST API](https://europepmc.org/RestfulWebService).
//!
//! - Search: `GET <search-base>/search?query=<q>&format=json&pageSize=N&resultType=lite`
//!   → JSON `resultList.result[]`; filter to entries with a `pmcid`
//!   (full-text open-access subset).
//! - PDF fetch: `GET <pdf-base>/articles/<pmcid>?pdf=render` → binary PDF.
//!
//! No auth, no API key. Europe PMC is a public mirror of NIH's PubMed
//! Central with developer-friendly REST endpoints. NIH's own `efetch.fcgi`
//! returns JATS XML by default and would need a separate URL-discovery
//! step to land on the PDF; Europe PMC's `?pdf=render` is the single
//! direct fetch this plugin is built around.
//!
//! **Open-access only.** Records without a `pmcid` field have no full
//! text in Europe PMC and are filtered out at search time — there's
//! nothing for `compute_outcomes` to hash. The PMC OA subset is large
//! (~5 M articles) so the filter still surfaces plenty.
//!
//! See `docs/gateway-feature.md` §4 for the v0 plugin role.

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use meta_feeder_sdk::common;
use meta_feeder_sdk::cache::MidhashCache;
#[cfg(test)]
use meta_feeder_sdk::plugin::HashKind;
use meta_feeder_sdk::plugin::{upstream_id_field, ConfigError, FeederPlugin, GatewayQuery, HashOutcome};
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, PluginHealth};

/// Default Europe PMC REST API root. Used for `/search` and the
/// `?query=PMCID:<id>` single-record lookup that powers `compute_outcomes`.
const DEFAULT_SEARCH_BASE: &str = "https://www.ebi.ac.uk/europepmc/webservices/rest";

/// Default Europe PMC web host. Hosts the `/articles/<pmcid>?pdf=render`
/// endpoint that streams the rendered PDF directly. Different host from
/// the REST API root (Europe PMC splits API and content hosting).
const DEFAULT_PDF_BASE: &str = "https://europepmc.org";

/// HTTP timeout per upstream call. Generous: Europe PMC's PDF render can
/// take 5–10 s on cold articles (it composes the PDF on demand from
/// JATS XML for some entries).
const HTTP_TIMEOUT_SECS: u64 = 30;

/// Polite identification per the Europe PMC API guidelines.
const USER_AGENT: &str = concat!(
    "meta-share/",
    env!("CARGO_PKG_VERSION"),
    " (gateway:pubmed)"
);

/// PubMed Central gateway plugin. Same structural shape as the other
/// fetch-capable v0 plugins: cheap to construct; `configure()` opens the
/// per-plugin redb cache; per-record fetches are memoized so repeated
/// "add" clicks for the same article don't re-download the PDF.
pub struct PubmedPlugin {
    http: reqwest::Client,
    search_base: String,
    pdf_base: String,
    cache: Option<MidhashCache>,
}

impl Default for PubmedPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl PubmedPlugin {
    pub fn new() -> Self {
        Self::with_bases(
            DEFAULT_SEARCH_BASE.to_string(),
            DEFAULT_PDF_BASE.to_string(),
        )
    }

    /// Single-base constructor used by tests against a `wiremock::MockServer`:
    /// the mock serves both `/search` (JSON) and `/articles/<id>` (PDF) under
    /// one host. Production uses [`with_bases`] / [`new`] with two distinct
    /// hosts.
    pub fn with_base_url(base: String) -> Self {
        Self::with_bases(base.clone(), base)
    }

    pub fn with_bases(search_base: String, pdf_base: String) -> Self {
        let http = common::build_http_client(HTTP_TIMEOUT_SECS, USER_AGENT, None);
        Self {
            http,
            search_base,
            pdf_base,
            cache: None,
        }
    }

    fn cache(&self) -> Result<&MidhashCache, GatewayError> {
        common::require_cache(self.cache.as_ref(), "pubmed")
    }

    /// Run a free-text query against Europe PMC's `/search`. Returns the
    /// raw JSON parsed into our internal shape; downstream filters drop
    /// any record without a `pmcid` (the OA filter).
    async fn search_json(
        &self,
        query: &str,
        max_results: usize,
    ) -> Result<EuropePmcSearchResponse, GatewayError> {
        let url = format!(
            "{}/search?query={}&format=json&pageSize={}&resultType=lite",
            self.search_base.trim_end_matches('/'),
            common::urlencode(query),
            max_results
        );
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("GET {url}: {e}")))?;
        common::map_status(&resp)?;
        resp.json::<EuropePmcSearchResponse>()
            .await
            .map_err(|e| GatewayError::Permanent(format!("decode europe-pmc search response: {e}")))
    }

    /// Single-record lookup by PMC id. Used by `compute_outcomes` to
    /// refresh the record metadata before the PDF pull. Europe PMC's
    /// search supports `query=PMCID:<id>` to find exactly one article
    /// (the indexed pmcid → pmid mapping is unambiguous).
    async fn fetch_entry(&self, pmcid: &str) -> Result<EuropePmcResult, GatewayError> {
        let body = self.search_json(&format!("PMCID:{pmcid}"), 1).await?;
        body.result_list
            .result
            .into_iter()
            .next()
            .ok_or(GatewayError::NotFound)
    }

    /// GET the rendered PDF from Europe PMC. The `?pdf=render` query
    /// triggers an on-demand render for articles that aren't pre-baked
    /// (most modern OA papers ARE pre-baked; render is sub-second).
    async fn fetch_pdf(&self, pmcid: &str) -> Result<bytes::Bytes, GatewayError> {
        let url = format!(
            "{}/articles/{}?pdf=render",
            self.pdf_base.trim_end_matches('/'),
            pmcid
        );
        let bytes = common::fetch_bytes(&self.http, &url).await?;
        // Europe PMC occasionally returns a 200 HTML page ("PDF not
        // available") for articles in the OA index whose render failed.
        // Cheapest guard: insist on the PDF magic bytes; reject anything
        // else as a permanent failure so the cache doesn't store junk
        // and the caller gets a clear error.
        if !bytes.starts_with(b"%PDF") {
            return Err(GatewayError::Permanent(format!(
                "europe-pmc returned non-PDF body for {pmcid} (article may have no rendered PDF)"
            )));
        }
        Ok(bytes)
    }
}

#[async_trait]
impl FeederPlugin for PubmedPlugin {
    fn upstream_id(&self) -> &'static str {
        "pubmed"
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        self.cache = Some(common::open_midhash_cache(cache_dir, "pubmed")?);
        Ok(())
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // Layer A early-return: PubMed/Europe PMC only serves
        // `document` / `paper`. A non-paper filter can never match —
        // skip the upstream call entirely.
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }
        let q = query.free_text_or_star();
        // Europe PMC returns mixed PubMed + PMC + PPR records; only those
        // with a `pmcid` field have a fetchable full text. Filter out
        // metadata-only entries early so a later `compute_outcomes` for
        // a returned record_id always has a chance of succeeding.
        let body = self.search_json(q, max_results).await?;
        let records: Vec<DiscoveryRecord> = body
            .result_list
            .result
            .into_iter()
            .filter_map(|r| r.pmcid.clone().map(|pmcid| into_discovery_record(r, pmcid)))
            .take(max_results)
            .collect();
        Ok(records)
    }

    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let cache = self.cache()?;
        if let Some(hit) = common::cached_outcome(cache, record_id, "pubmed")? {
            return Ok(hit);
        }

        let entry = self.fetch_entry(record_id).await?;
        let pmcid = entry.pmcid.clone().ok_or_else(|| {
            // Defensive: `fetch_entry` queried by PMCID, so a missing
            // pmcid in the response means the upstream gave us back the
            // wrong record. Treat as NotFound so the caller can move on.
            GatewayError::NotFound
        })?;
        let bytes = self.fetch_pdf(&pmcid).await?;
        let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);

        common::store_midhash(cache, record_id, "pubmed", &cid);

        let record = into_discovery_record(entry, pmcid);
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

// -- Europe PMC JSON shapes (subset we use) ----------------------------------

#[derive(Debug, Deserialize)]
struct EuropePmcSearchResponse {
    #[serde(rename = "resultList", default)]
    result_list: EuropePmcResultList,
}

#[derive(Debug, Default, Deserialize)]
struct EuropePmcResultList {
    #[serde(default)]
    result: Vec<EuropePmcResult>,
}

/// One record from Europe PMC search. Field names match the API's
/// camelCase JSON keys via `rename_all`; missing fields default to
/// `None`. Everything past `pmcid` is surfaced into the DiscoveryRecord
/// when present.
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct EuropePmcResult {
    /// PubMed numeric id (e.g. "12345678"). Always present for MED-source
    /// records; absent for preprints (`PPR…`).
    pmid: Option<String>,
    /// PMC id (e.g. "PMC2868305"). **Present only when the article has
    /// full text in PMC's OA subset** — this is the discriminator we
    /// filter on at search time.
    pmcid: Option<String>,
    /// Article DOI when registered with CrossRef.
    doi: Option<String>,
    title: Option<String>,
    /// Comma-separated author list, e.g. "Smith J, Doe J".
    author_string: Option<String>,
    journal_title: Option<String>,
    pub_year: Option<String>,
    /// Full abstract text — multi-paragraph. We pass through verbatim;
    /// the UI trims for display.
    abstract_text: Option<String>,
    is_open_access: Option<String>,
}

/// Convert a Europe PMC search result into a `DiscoveryRecord`. The
/// caller is responsible for confirming `pmcid` is `Some` (handle_query
/// filters first) — we accept it as a separate arg so the conversion
/// can't accidentally emit a recordless of full-text availability.
fn into_discovery_record(r: EuropePmcResult, pmcid: String) -> DiscoveryRecord {
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    if let Some(t) = r.title {
        fields.insert("title".to_string(), t);
    }
    fields.insert("fileType".to_string(), "document".to_string());
    fields.insert("contentKind".to_string(), "paper".to_string());
    fields.insert(
        "sourceUrl".to_string(),
        format!("https://europepmc.org/article/PMC/{pmcid}"),
    );
    fields.insert("fileName".to_string(), format!("pubmed-{pmcid}.pdf"));
    fields.insert(upstream_id_field("pubmed"), pmcid.clone());
    fields.insert("format".to_string(), "pdf".to_string());
    if let Some(p) = r.pmid {
        fields.insert("pmid".to_string(), p);
    }
    if let Some(d) = r.doi {
        fields.insert("doi".to_string(), d);
    }
    if let Some(a) = r.author_string {
        fields.insert("author".to_string(), a);
    }
    if let Some(j) = r.journal_title {
        fields.insert("journal".to_string(), j);
    }
    if let Some(y) = r.pub_year {
        fields.insert("year".to_string(), y);
    }
    if let Some(s) = r.abstract_text {
        fields.insert("summary".to_string(), s);
    }
    if let Some(o) = r.is_open_access {
        fields.insert("openAccess".to_string(), o);
    }

    DiscoveryRecord {
        upstream_id: "pubmed".to_string(),
        record_id: pmcid,
        fields,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal search response with one OA record + one non-OA record
    /// (no pmcid). `handle_query` should keep only the OA one.
    const SAMPLE_SEARCH: &str = r#"{
      "hitCount": 2,
      "request": {},
      "resultList": {
        "result": [
          {
            "id": "12345678",
            "source": "MED",
            "pmid": "12345678",
            "pmcid": "PMC2868305",
            "doi": "10.1371/journal.pone.0010068",
            "title": "Discovery of a new species",
            "authorString": "Smith J, Doe J",
            "journalTitle": "PLoS One",
            "pubYear": "2010",
            "abstractText": "We describe a new species discovered in...",
            "isOpenAccess": "Y"
          },
          {
            "id": "98765432",
            "source": "MED",
            "pmid": "98765432",
            "title": "Closed-access paper",
            "authorString": "Other A",
            "journalTitle": "Some Journal",
            "pubYear": "2018",
            "isOpenAccess": "N"
          }
        ]
      }
    }"#;

    #[test]
    fn parses_europe_pmc_search_response() {
        let body: EuropePmcSearchResponse = serde_json::from_str(SAMPLE_SEARCH).expect("parse");
        assert_eq!(body.result_list.result.len(), 2);
        assert_eq!(
            body.result_list.result[0].pmcid.as_deref(),
            Some("PMC2868305")
        );
        assert!(body.result_list.result[1].pmcid.is_none());
    }

    #[test]
    fn into_discovery_record_emits_required_fields() {
        let body: EuropePmcSearchResponse = serde_json::from_str(SAMPLE_SEARCH).unwrap();
        let r0 = body.result_list.result.into_iter().next().unwrap();
        let pmcid = r0.pmcid.clone().unwrap();
        let rec = into_discovery_record(r0, pmcid);
        assert_eq!(rec.upstream_id, "pubmed");
        assert_eq!(rec.record_id, "PMC2868305");
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
            Some("https://europepmc.org/article/PMC/PMC2868305")
        );
        assert_eq!(
            rec.fields.get("fileName").map(String::as_str),
            Some("pubmed-PMC2868305.pdf")
        );
        // Canonical `<upstream_id>id` field — `format!("{upstream_id}id")`
        // gives "pubmedid"; value is the PMC id.
        assert_eq!(
            rec.fields.get("pubmedid").map(String::as_str),
            Some("PMC2868305")
        );
        // Bibliographic surface.
        assert_eq!(rec.fields.get("year").map(String::as_str), Some("2010"));
        assert_eq!(
            rec.fields.get("author").map(String::as_str),
            Some("Smith J, Doe J")
        );
        assert_eq!(
            rec.fields.get("journal").map(String::as_str),
            Some("PLoS One")
        );
        assert_eq!(
            rec.fields.get("doi").map(String::as_str),
            Some("10.1371/journal.pone.0010068")
        );
    }

    // -- HTTP integration tests against a wiremock server ----------------

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn configured_plugin_against(server: &MockServer) -> (PubmedPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin = PubmedPlugin::with_base_url(server.uri());
        plugin.configure(dir.path()).expect("configure");
        (plugin, dir)
    }

    #[tokio::test]
    async fn handle_query_drops_non_oa_records() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .and(query_param("query", "species"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(SAMPLE_SEARCH)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("species"), 10)
            .await
            .expect("handle_query");
        // Two records in the mock; only the one with `pmcid` survives.
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_id, "PMC2868305");
    }

    #[tokio::test]
    async fn handle_query_truncates_to_max_results() {
        let server = MockServer::start().await;
        // Build a synthetic response with several OA records.
        let mut results = String::new();
        for i in 0..5 {
            if i > 0 {
                results.push(',');
            }
            results.push_str(&format!(
                r#"{{ "pmcid": "PMC{i}", "title": "Paper {i}", "pubYear": "2020" }}"#,
                i = i + 100
            ));
        }
        let body = format!(r#"{{ "hitCount": 5, "resultList": {{ "result": [{results}] }} }}"#);
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("anything"), 2)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn compute_outcomes_fetches_pdf_and_caches() {
        let server = MockServer::start().await;
        // PMCID lookup: search?query=PMCID:PMC2868305
        Mock::given(method("GET"))
            .and(path("/search"))
            .and(query_param("query", "PMCID:PMC2868305"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(SAMPLE_SEARCH)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        // PDF response — bytes get hashed.
        let pdf_bytes = b"%PDF-1.4 mock pubmed pdf\n".to_vec();
        let expected_cid = meta_feeder_sdk::hash::compute_ipfs_cid(&pdf_bytes);
        Mock::given(method("GET"))
            .and(path("/articles/PMC2868305"))
            .and(query_param("pdf", "render"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(pdf_bytes.clone())
                    .insert_header("content-type", "application/pdf"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let mut outcomes = plugin
            .compute_outcomes("PMC2868305")
            .await
            .expect("compute");
        assert_eq!(outcomes.len(), 1, "pubmed is single-outcome");
        let outcome = outcomes.remove(0);
        assert_eq!(outcome.hash.as_str(), expected_cid);
        assert_eq!(outcome.hash_kind, HashKind::Sha2_256);
        // Auto-store inputs.
        assert_eq!(outcome.bytes.as_deref(), Some(pdf_bytes.as_slice()));
        assert_eq!(outcome.file_extension.as_deref(), Some("pdf"));
        let rec = outcome.record.expect("record present on fresh compute");
        assert_eq!(rec.record_id, "PMC2868305");
        assert_eq!(
            rec.fields.get("doi").map(String::as_str),
            Some("10.1371/journal.pone.0010068")
        );
    }

    #[tokio::test]
    async fn compute_outcomes_cache_hit_skips_http() {
        let server = MockServer::start().await;
        let (plugin, _dir) = configured_plugin_against(&server);
        plugin
            .cache
            .as_ref()
            .unwrap()
            .put_midhash("PMC2868305", "bafyCACHED")
            .unwrap();
        let mut outcomes = plugin
            .compute_outcomes("PMC2868305")
            .await
            .expect("cache hit");
        assert_eq!(outcomes.len(), 1);
        let outcome = outcomes.remove(0);
        assert_eq!(outcome.hash.as_str(), "bafyCACHED");
        assert!(outcome.bytes.is_none());
        assert!(outcome.record.is_none());
    }

    #[tokio::test]
    async fn compute_outcomes_rejects_non_pdf_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(SAMPLE_SEARCH)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        // Upstream returns an HTML "PDF not available" page with 200 OK
        // — Europe PMC actually does this for articles in the OA index
        // whose render failed. The plugin must reject rather than store
        // junk bytes.
        Mock::given(method("GET"))
            .and(path("/articles/PMC2868305"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("<html>PDF not available</html>")
                    .insert_header("content-type", "text/html"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("PMC2868305")
            .await
            .expect_err("non-PDF");
        assert!(matches!(err, GatewayError::Permanent(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn upstream_404_maps_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("PMC-does-not-exist")
            .await
            .expect_err("expected NotFound");
        assert!(matches!(err, GatewayError::NotFound), "got {err:?}");
    }
}
