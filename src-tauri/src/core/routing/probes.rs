use anyhow::{Context as _, Result};
use reqwest::{Client, Proxy};
use serde::{Deserialize, Serialize};
use std::{
    sync::LazyLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

use crate::core::handle::Handle;

static REQUESTS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(32));
static CANDIDATES: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(24));

pub const GENERAL: [&str; 2] = [
    "https://www.gstatic.com/generate_204",
    "https://cp.cloudflare.com/generate_204",
];
pub const ENDPOINTS: [[(&str, &str); 3]; 3] = [
    [
        ("API", "https://api.openai.com/v1/models"),
        ("ChatGPT", "https://chatgpt.com/"),
        ("Codex", "https://chatgpt.com/backend-api/wham/usage"),
    ],
    [
        ("API", "https://api.anthropic.com/v1/models"),
        ("Claude", "https://claude.ai/login"),
        ("Claude Code", "https://platform.claude.com/login"),
    ],
    [
        ("API", "https://generativelanguage.googleapis.com/v1beta/models"),
        ("Gemini", "https://gemini.google.com/app"),
        ("AI Studio", "https://aistudio.google.com/"),
    ],
];

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Endpoint {
    pub name: String,
    pub state: String,
    pub code: u16,
    pub millis: u64,
    pub checked_at: u64,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct AiProof {
    pub state: String,
    #[serde(default)]
    pub challenged: bool,
    pub checked_at: u64,
    pub endpoints: Vec<Endpoint>,
}

impl AiProof {
    pub fn usable(&self) -> bool {
        matches!(self.state.as_str(), "available" | "pending")
    }
    /// The critical API endpoint answered with its normal unauthenticated code,
    /// which means the service itself accepts this exit; only a web-front
    /// challenge remains and browsers usually clear those.
    pub fn critical_available(&self) -> bool {
        self.endpoints
            .iter()
            .any(|endpoint| endpoint.name == "API" && endpoint.state == "available")
    }
    pub fn confirmed(&self) -> bool {
        self.state == "available"
    }
    pub fn failed(&self) -> bool {
        self.state == "failed"
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Quality {
    pub available: bool,
    pub millis: u64,
    pub checked_at: u64,
    pub timed_out: bool,
}

pub async fn client(port: u16) -> Result<Client> {
    let runtime = crate::config::Config::runtime().await.data_arc();
    let mut proxy = Proxy::all(format!("http://127.0.0.1:{port}"))?;
    if let Some((username, password)) = runtime
        .config
        .as_ref()
        .and_then(|config| config.get("authentication"))
        .and_then(serde_yaml_ng::Value::as_sequence)
        .and_then(|entries| entries.first())
        .and_then(serde_yaml_ng::Value::as_str)
        .and_then(|entry| entry.split_once(':'))
    {
        proxy = proxy.basic_auth(username, password);
    }
    Ok(Client::builder()
        .no_proxy()
        .proxy(proxy)
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(4))
        .user_agent("Mozilla/5.0 ClashVergeRev/AdaptiveRouting")
        .build()?)
}

pub async fn select(group: &str, route: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let _permit = REQUESTS.acquire().await?;
        Handle::mihomo()
            .select_node_for_group(group, route)
            .await
            .map_err(Into::into)
    })
    .await
    .context("Core selection timed out")?
}

/// Classify one endpoint result, mirroring the methodology of community IP
/// checkers: graded evidence, never a single absolute verdict.
///
/// - Hard signals (region block, explicit block page, `cf-mitigated: block`,
///   a bare 403 on a web front with no challenge flavor) fail immediately.
/// - Challenge flavors (`cf-mitigated: challenge`, "just a moment", the
///   "enable javascript" interstitial) mark the endpoint `challenged`: the
///   line stays usable-pending and accumulates evidence at the monitor level,
///   so a browser-passable challenge is tolerated while a persistent one is
///   eventually demoted.
/// - API-style endpoints stay lenient about unauthenticated 401/403/400.
fn classify(name: &str, code: u16, mitigated: Option<&str>, text: &str) -> &'static str {
    let region_block = [
        "unsupported_country",
        "country, region, or territory not supported",
        "not available in your country",
        "not supported in your location",
        "unsupported region",
        "user location is not supported",
    ]
    .iter()
    .any(|value| text.contains(value));
    let hard_block = ["you have been blocked", "access denied", "error 1020"]
        .iter()
        .any(|value| text.contains(value));
    let challenge = [
        "cf-chl-",
        "challenge-platform",
        "just a moment",
        "verify you are human",
        "captcha",
        "unusual traffic",
        "enable javascript and cookies",
    ]
    .iter()
    .any(|value| text.contains(value));
    let web_front = !matches!(name, "API" | "Codex" | "Claude Code" | "transport");
    if region_block || hard_block || mitigated == Some("block") {
        "failed"
    } else if name == "transport" {
        if code == 204 { "available" } else { "failed" }
    } else if web_front && code == 403 {
        if challenge || mitigated == Some("challenge") { "challenged" } else { "failed" }
    } else if challenge || code == 429 || code >= 500 {
        "pending"
    } else if (200..400).contains(&code) || code == 401 || (name == "API" && code == 400) {
        "available"
    } else {
        "pending"
    }
}

async fn endpoint(client: &Client, name: &str, url: &str, monitor: bool, timeout_ms: u64) -> Endpoint {
    let started = Instant::now();
    let mut proof = Endpoint {
        name: name.to_owned(),
        state: "failed".to_owned(),
        checked_at: now(),
        ..Endpoint::default()
    };
    let fetched = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let _candidate = if monitor {
            None
        } else {
            Some(CANDIDATES.acquire().await?)
        };
        let _permit = REQUESTS.acquire().await?;
        let mut response = client.get(url).send().await?;
        let code = response.status().as_u16();
        let mitigated = response
            .headers()
            .get("cf-mitigated")
            .and_then(|value| value.to_str().ok())
            .map(str::to_lowercase);
        if response.url().scheme() != "https" {
            anyhow::bail!("Unexpected HTTP redirect");
        }
        let mut bytes = Vec::new();
        while bytes.len() < 65536 {
            let Some(chunk) = response.chunk().await? else {
                break;
            };
            let remaining = 65536 - bytes.len();
            bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        }
        Ok::<_, anyhow::Error>((
            code,
            mitigated,
            String::from_utf8_lossy(&bytes).to_lowercase(),
        ))
    })
    .await;
    if let Ok(Ok((code, mitigated, text))) = fetched {
        proof.code = code;
        proof.state = classify(name, code, mitigated.as_deref(), &text).to_owned();
    }
    proof.millis = started.elapsed().as_millis() as u64;
    proof.checked_at = now();
    proof
}

pub async fn general(client: &Client, deadline: Duration, monitor: bool) -> Quality {
    // Candidate screening passes on either transport endpoint; the final public-port
    // verification still requires both, so one flaky mirror cannot hide live routes
    // behind a full timeout.
    transport(client, deadline, monitor, false).await
}

async fn transport(client: &Client, deadline: Duration, monitor: bool, require_all: bool) -> Quality {
    let started = Instant::now();
    let timeout_ms = deadline.as_millis() as u64;
    let response = tokio::time::timeout(deadline, async {
        tokio::join!(
            endpoint(client, "transport", GENERAL[0], monitor, timeout_ms),
            endpoint(client, "transport", GENERAL[1], monitor, timeout_ms)
        )
    })
    .await;
    let (available, millis) = match response {
        Ok((a, b)) => (
            if !require_all {
                a.state == "available" || b.state == "available"
            } else {
                a.state == "available" && b.state == "available"
            },
            if !require_all {
                [&a, &b]
                    .into_iter()
                    .filter(|endpoint| endpoint.state == "available")
                    .map(|endpoint| endpoint.millis)
                    .min()
                    .unwrap_or_else(|| a.millis.max(b.millis))
            } else {
                a.millis.max(b.millis)
            },
        ),
        Err(_) => (false, started.elapsed().as_millis() as u64),
    };
    Quality {
        available,
        millis,
        checked_at: now(),
        timed_out: !available && started.elapsed() + Duration::from_millis(100) >= deadline,
    }
}

pub async fn ai(
    client: &Client,
    service: usize,
    previous: Option<&AiProof>,
    cycle: u64,
    monitor: bool,
    timeout_ms: u64,
) -> AiProof {
    let mut endpoints = previous.map_or_else(Vec::new, |proof| proof.endpoints.clone());
    let (name, url) = ENDPOINTS[service][0];
    let critical = endpoint(client, name, url, monitor, timeout_ms).await;
    let critical_failed = critical.state == "failed";
    upsert(&mut endpoints, critical);
    if !critical_failed {
        let full = !monitor || cycle.is_multiple_of(5) || previous.is_none_or(|proof| !proof.confirmed());
        if full {
            let ((a_name, a_url), (b_name, b_url)) = (ENDPOINTS[service][1], ENDPOINTS[service][2]);
            let (a, b) = tokio::join!(
                endpoint(client, a_name, a_url, monitor, timeout_ms),
                endpoint(client, b_name, b_url, monitor, timeout_ms)
            );
            upsert(&mut endpoints, a);
            upsert(&mut endpoints, b);
        } else {
            let index = 1 + cycle as usize % 2;
            let (name, url) = ENDPOINTS[service][index];
            upsert(&mut endpoints, endpoint(client, name, url, monitor, timeout_ms).await);
        }
    }
    let state = if critical_failed || endpoints.iter().any(|proof| proof.state == "failed") {
        "failed"
    } else if endpoints.len() == 3 && endpoints.iter().all(|proof| proof.state == "available") {
        "available"
    } else {
        "pending"
    };
    let challenged = endpoints.iter().any(|proof| proof.state == "challenged");
    AiProof {
        state: state.to_owned(),
        challenged,
        checked_at: now(),
        endpoints,
    }
}

fn upsert(endpoints: &mut Vec<Endpoint>, proof: Endpoint) {
    if let Some(index) = endpoints.iter().position(|endpoint| endpoint.name == proof.name) {
        endpoints[index] = proof;
    } else {
        endpoints.push(proof);
    }
}

pub async fn public_transport(client: &Client, timeout_ms: u64) -> bool {
    transport(client, Duration::from_millis(timeout_ms), true, true)
        .await
        .available
}

/// Resolve the exit IP and country that destinations actually see, through the
/// client's bound proxy. `None` when the trace cannot be read — callers must
/// treat that as unknown, never as evidence of a clean exit.
pub async fn exit_trace(client: &Client, timeout_ms: u64) -> Option<ExitTrace> {
    let text = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let _permit = REQUESTS.acquire().await?;
        Ok::<_, anyhow::Error>(
            client
                .get("https://www.cloudflare.com/cdn-cgi/trace")
                .send()
                .await?
                .text()
                .await?,
        )
    })
    .await
    .ok()?
    .ok()?;
    let mut trace = ExitTrace::default();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("ip=") {
            trace.ip = value.trim().to_owned();
        }
        if let Some(value) = line.strip_prefix("loc=") {
            trace.country = value.trim().to_ascii_uppercase();
        }
    }
    if trace.ip.is_empty() || trace.country.is_empty() {
        return None;
    }
    trace.china = trace.country == "CN";
    Some(trace)
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct ExitTrace {
    pub ip: String,
    pub country: String,
    /// Runtime ground truth for the China-exit guard, independent of node naming.
    pub china: bool,
}

pub async fn public_ai(client: &Client, service: usize, timeout_ms: u64) -> AiProof {
    ai(client, service, None, 0, true, timeout_ms).await
}

#[cfg(test)]
mod tests {
    use super::classify;

    #[test]
    fn hard_blocks_fail_regardless_of_flavor() {
        // The exact case seen in the field: chatgpt.com served a bare Cloudflare
        // 403 with no interstitial text — a hard block on the exit IP.
        assert_eq!(classify("ChatGPT", 403, None, ""), "failed");
        assert_eq!(classify("Claude", 403, None, "sorry, you have been blocked"), "failed");
        assert_eq!(classify("Gemini", 403, Some("block"), ""), "failed");
        assert_eq!(classify("ChatGPT", 200, None, "unsupported_country_region_territory"), "failed");
    }

    #[test]
    fn challenges_grade_as_challenged_not_failed() {
        // Interstitial flavors a real browser may pass stay usable-pending; the
        // monitor accumulates them as evidence instead of a one-shot verdict.
        assert_eq!(
            classify("ChatGPT", 403, None, "enable javascript and cookies to continue"),
            "challenged"
        );
        assert_eq!(classify("Claude", 403, Some("challenge"), ""), "challenged");
        assert_eq!(classify("ChatGPT", 403, None, "just a moment..."), "challenged");
        assert_eq!(classify("AI Studio", 403, None, "challenge-platform"), "challenged");
    }

    #[test]
    fn api_style_endpoints_stay_lenient_about_authorization_codes() {
        assert_eq!(classify("API", 401, None, "{}"), "available");
        assert_eq!(classify("API", 400, None, "api key not valid"), "available");
        assert_eq!(classify("Codex", 403, None, ""), "pending");
        assert_eq!(classify("Claude Code", 403, None, ""), "pending");
        assert_eq!(classify("ChatGPT", 401, None, "{}"), "available");
    }

    #[test]
    fn transient_conditions_remain_pending_and_transport_requires_204() {
        assert_eq!(classify("ChatGPT", 429, None, ""), "pending");
        assert_eq!(classify("Claude", 503, None, ""), "pending");
        assert_eq!(classify("API", 500, None, ""), "pending");
        assert_eq!(classify("transport", 204, None, ""), "available");
        assert_eq!(classify("transport", 200, None, ""), "failed");
    }

    #[test]
    fn trace_parser_extracts_ip_and_country() {
        let text = "fl=x\nh=example\nip=203.0.113.7\nts=1\nloc=jp\ncolo=NRT\n";
        let mut trace = super::ExitTrace::default();
        for line in text.lines() {
            if let Some(value) = line.strip_prefix("ip=") {
                trace.ip = value.trim().to_owned();
            }
            if let Some(value) = line.strip_prefix("loc=") {
                trace.country = value.trim().to_ascii_uppercase();
            }
        }
        trace.china = trace.country == "CN";
        assert_eq!(trace.ip, "203.0.113.7");
        assert_eq!(trace.country, "JP");
        assert!(!trace.china);
    }
}
