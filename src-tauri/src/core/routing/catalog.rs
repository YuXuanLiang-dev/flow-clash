use anyhow::{Context as _, Result, bail};
use serde::Serialize;
use serde_yaml_ng::{Mapping, Value};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
};

use crate::{
    config::{IProfiles, routing::RoutingConfig},
    utils::{dirs, help},
};

pub const GROUPS: [&str; 4] = ["CVR-Auto", "CVR-OpenAI", "CVR-Claude", "CVR-Gemini"];
pub const LANES: usize = 16;

#[derive(Clone, Debug, Serialize)]
pub struct Node {
    pub id: String,
    pub subscription_id: String,
    pub subscription: String,
    pub name: String,
    pub country: String,
    pub residential: bool,
    pub role: String,
    pub proxy_type: String,
    pub udp: bool,
    #[serde(skip)]
    pub proxy: Mapping,
}

#[derive(Clone, Debug, Serialize)]
pub struct Route {
    pub id: String,
    pub ingress: String,
    pub egress: String,
    pub ingress_name: String,
    pub egress_name: String,
    pub ingress_country: String,
    pub country: String,
    pub residential: bool,
    pub rear_subscription: bool,
    pub hops: u8,
    /// Eligible for the general (non-AI) group: every generated single-hop line.
    /// Chains are reserved for the AI groups; most traffic never needs them.
    pub general: bool,
    #[serde(skip)]
    pub proxy: Mapping,
}

impl Route {
    pub fn ai_allowed(&self, settings: &RoutingConfig) -> bool {
        !settings
            .ai_excluded_countries
            .iter()
            .any(|country| *country == self.country || *country == self.ingress_country)
    }
    pub fn same_country(&self) -> bool {
        !self.country.is_empty() && self.country == self.ingress_country
    }
}

#[derive(Clone, Default)]
pub struct Catalog {
    pub nodes: Vec<Node>,
    pub routes: Vec<Arc<Route>>,
}

pub async fn directory(profiles: Arc<Box<IProfiles>>, settings: Arc<RoutingConfig>) -> Result<Arc<Vec<Node>>> {
    type DirectoryCache = Option<(Arc<Box<IProfiles>>, String, Arc<Vec<Node>>)>;
    static CACHE: LazyLock<tokio::sync::Mutex<DirectoryCache>> = LazyLock::new(|| tokio::sync::Mutex::new(None));
    let mut cache = CACHE.lock().await;
    let signature = digest(
        serde_json::to_string(&(
            &settings.node_metadata,
            &settings.profile_roles,
            &settings.residential_profiles,
        ))?
        .as_bytes(),
    );
    if let Some((cached_profiles, cached_signature, nodes)) = cache.as_ref()
        && Arc::ptr_eq(cached_profiles, &profiles)
        && *cached_signature == signature
    {
        return Ok(Arc::clone(nodes));
    }
    let mut metadata = settings.as_ref().clone();
    metadata.enabled = false;
    metadata.chain = false;
    let nodes = Arc::new(load(&profiles, &metadata).await?.nodes);
    *cache = Some((profiles, signature, Arc::clone(&nodes)));
    drop(cache);
    Ok(nodes)
}

pub fn digest(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(24);
    for byte in Sha256::digest(value).iter().take(12) {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn country(name: &str) -> String {
    let name = name.to_lowercase();
    let labels: &[(&str, &[&str])] = &[
        ("HK", &["香港", "🇭🇰", "hong kong", "hongkong"]),
        ("TW", &["台湾", "臺灣", "🇹🇼", "taiwan"]),
        ("JP", &["日本", "东京", "東京", "大阪", "🇯🇵", "japan", "tokyo"]),
        ("SG", &["新加坡", "狮城", "🇸🇬", "singapore"]),
        (
            "US",
            &[
                "美国",
                "美國",
                "洛杉矶",
                "纽约",
                "西雅图",
                "🇺🇸",
                "united states",
                "los angeles",
                "seattle",
            ],
        ),
        ("KR", &["韩国", "韓國", "首尔", "🇰🇷", "korea"]),
        ("GB", &["英国", "英國", "伦敦", "🇬🇧", "united kingdom", "london"]),
        ("DE", &["德国", "德國", "法兰克福", "🇩🇪", "germany", "frankfurt"]),
        ("CA", &["加拿大", "多伦多", "温哥华", "🇨🇦", "canada"]),
        ("AU", &["澳大利亚", "澳洲", "悉尼", "🇦🇺", "australia"]),
        ("FR", &["法国", "法國", "巴黎", "🇫🇷", "france"]),
        ("NL", &["荷兰", "荷蘭", "🇳🇱", "netherlands"]),
        ("IN", &["印度", "🇮🇳", "india"]),
        ("TH", &["泰国", "泰國", "曼谷", "🇹🇭", "thailand"]),
        ("MY", &["马来西亚", "馬來西亞", "🇲🇾", "malaysia"]),
        ("PH", &["菲律宾", "菲律賓", "🇵🇭", "philippines"]),
        ("VN", &["越南", "🇻🇳", "vietnam"]),
        ("ID", &["印尼", "印度尼西亚", "🇮🇩", "indonesia"]),
        ("RU", &["俄罗斯", "俄羅斯", "🇷🇺", "russia"]),
        ("BR", &["巴西", "🇧🇷", "brazil"]),
        ("CH", &["瑞士", "🇨🇭", "switzerland"]),
        ("SE", &["瑞典", "🇸🇪", "sweden"]),
        ("FI", &["芬兰", "芬蘭", "🇫🇮", "finland"]),
        ("IT", &["意大利", "義大利", "🇮🇹", "italy"]),
        ("ES", &["西班牙", "🇪🇸", "spain"]),
        ("AE", &["阿联酋", "迪拜", "🇦🇪", "dubai"]),
        ("TR", &["土耳其", "🇹🇷", "turkey"]),
        ("CN", &["中国大陆", "中國大陸", "🇨🇳", "mainland china"]),
    ];
    for (code, markers) in labels {
        if markers.iter().any(|marker| name.contains(marker)) {
            return (*code).to_owned();
        }
    }
    for token in name.split(|c: char| !c.is_ascii_alphabetic()) {
        let code = token.to_uppercase();
        let code = match code.as_str() {
            "USA" => "US",
            "HKG" => "HK",
            "JPN" => "JP",
            "SGP" => "SG",
            "UK" | "GBR" => "GB",
            "TWN" => "TW",
            _ => &code,
        };
        if labels.iter().any(|(known, _)| *known == code) {
            return code.to_owned();
        }
    }
    String::new()
}

pub async fn load(profiles: &IProfiles, settings: &RoutingConfig) -> Result<Catalog> {
    let mut nodes = Vec::new();
    let mut unique = HashSet::new();
    for item in profiles
        .items
        .iter()
        .flatten()
        .filter(|item| matches!(item.itype.as_deref(), Some("local" | "remote")))
    {
        let (Some(uid), Some(file)) = (&item.uid, &item.file) else {
            continue;
        };
        let path = dirs::app_profiles_dir()?.join(file.as_str());
        let data = help::read_mapping(&path)
            .await
            .with_context(|| format!("Cannot read subscription {uid}"))?;
        let role = settings
            .profile_roles
            .get(uid.as_str())
            .cloned()
            .unwrap_or_else(|| "normal".to_owned());
        for proxy in data.get("proxies").and_then(Value::as_sequence).into_iter().flatten() {
            let Some(proxy) = proxy.as_mapping() else {
                continue;
            };
            let name = proxy.get("name").and_then(Value::as_str).unwrap_or_default().to_owned();
            let proxy_type = proxy.get("type").and_then(Value::as_str).unwrap_or_default().to_owned();
            if name.is_empty() || matches!(proxy_type.as_str(), "direct" | "reject" | "compatible" | "pass") {
                continue;
            }
            let mut identity = proxy.clone();
            identity.remove("name");
            let id = digest(format!("{uid}:{}", serde_yaml_ng::to_string(&identity)?).as_bytes());
            if !unique.insert(id.clone()) {
                continue;
            }
            let metadata = settings.node_metadata.get(&id);
            let country = metadata
                .filter(|meta| !meta.country.is_empty())
                .map_or_else(|| country(&name), |meta| meta.country.to_uppercase());
            let residential = metadata.and_then(|meta| meta.residential).unwrap_or_else(|| {
                settings
                    .residential_profiles
                    .iter()
                    .any(|profile| profile == uid.as_str())
                    || ["家宽", "住宅", "家庭", "residential", " isp"]
                        .iter()
                        .any(|label| name.to_lowercase().contains(label))
            });
            let udp = proxy.get("udp").and_then(Value::as_bool).unwrap_or(matches!(
                proxy_type.as_str(),
                "hysteria" | "hysteria2" | "tuic" | "wireguard"
            ));
            let mut raw = proxy.clone();
            raw.remove("dialer-proxy");
            raw.insert("name".into(), format!("CVR-node-{id}").into());
            raw.insert("tfo".into(), false.into());
            nodes.push(Node {
                id,
                subscription_id: uid.to_string(),
                subscription: item.name.clone().unwrap_or_default().to_string(),
                name,
                country,
                residential,
                role: role.clone(),
                proxy_type,
                udp,
                proxy: raw,
            });
        }
        if settings.enabled
            && settings.profile_roles.contains_key(uid.as_str())
            && data.get("proxy-providers").is_some()
            && data.get("proxies").is_none()
        {
            bail!(
                "Subscription {uid} only contains proxy providers. Import its expanded Mihomo node configuration before assigning a routing role."
            );
        }
    }
    let mut active_settings = settings.clone();
    active_settings.profile_roles.retain(|uid, _| {
        profiles.items.iter().flatten().any(|profile| {
            profile.uid.as_deref() == Some(uid) && matches!(profile.itype.as_deref(), Some("local" | "remote"))
        })
    });
    active_settings.chain = settings.enabled && active_settings.has_chain_roles();
    let routes = build_routes(&nodes, &active_settings)?;
    Ok(Catalog { nodes, routes })
}

fn build_routes(nodes: &[Node], settings: &RoutingConfig) -> Result<Vec<Arc<Route>>> {
    let mut routes = Vec::new();
    let assigned = nodes
        .iter()
        .any(|node| matches!(node.role.as_str(), "ingress" | "egress"));
    // A China-mainland exit would expose a Chinese IP to every destination and
    // is never a valid exit for adaptive routing; the front hop may still be in
    // China. Node naming and manual marks are the static line of defense — the
    // engine's runtime exit trace is the second.
    let exit = |node: &Node| node.country != "CN";
    // Residential nodes of the egress subscription are scarce and exist to serve
    // AI traffic as chain rears ("residential IP routes ChatGPT only"); they are
    // never generated as single-hop lines for the general group.
    let general_exit = |node: &Node| exit(node) && !(node.residential && node.role == "egress");
    for node in nodes
        .iter()
        .filter(|node| settings.chain || !assigned || node.role != "normal")
        .filter(|node| general_exit(node))
    {
        routes.push(Arc::new(route(node, node, 1)));
    }
    if settings.chain {
        let ingress = nodes.iter().filter(|node| node.role == "ingress").collect::<Vec<_>>();
        let egress = nodes
            .iter()
            .filter(|node| node.role == "egress" && exit(node))
            .collect::<Vec<_>>();
        if settings.enabled && (ingress.is_empty() || egress.is_empty()) {
            bail!("Select subscriptions with nodes for both the front and rear roles");
        }
        if ingress.len().saturating_mul(egress.len()) > 20000 {
            bail!("More than 20,000 chain combinations; narrow the subscription roles before enabling");
        }
        for front in &ingress {
            for rear in &egress {
                if matches!(
                    rear.proxy_type.as_str(),
                    "hysteria" | "hysteria2" | "tuic" | "wireguard"
                ) && !front.udp
                {
                    continue;
                }
                routes.push(Arc::new(route(front, rear, 2)));
            }
        }
    }
    Ok(routes)
}

fn route(front: &Node, rear: &Node, hops: u8) -> Route {
    let id = format!(
        "CVR-route-{}",
        digest(format!("{}:{}:{hops}", front.id, rear.id).as_bytes())
    );
    let mut proxy = rear.proxy.clone();
    proxy.insert("name".into(), id.clone().into());
    if hops == 2 {
        proxy.insert("dialer-proxy".into(), format!("CVR-node-{}", front.id).into());
    }
    Route {
        id,
        ingress: front.id.clone(),
        egress: rear.id.clone(),
        ingress_name: front.name.clone(),
        egress_name: rear.name.clone(),
        ingress_country: front.country.clone(),
        country: rear.country.clone(),
        residential: rear.residential,
        rear_subscription: rear.role == "egress",
        hops,
        general: hops == 1,
        proxy,
    }
}

fn sequence<'a>(config: &'a mut Mapping, key: &str) -> &'a mut Vec<Value> {
    let value = config
        .entry(Value::from(key))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    if !value.is_sequence() {
        *value = Value::Sequence(Vec::new());
    }
    match value {
        Value::Sequence(values) => values,
        _ => unreachable!(),
    }
}

pub async fn overlay(config: Mapping, profiles: &IProfiles) -> Result<Mapping> {
    let settings = crate::config::routing::routing_config().await?;
    if !settings.enabled {
        return Ok(config);
    }
    let catalog = load(profiles, &settings).await?;
    overlay_catalog(config, &settings, &catalog, &super::status())
}

fn overlay_catalog(
    mut config: Mapping,
    settings: &RoutingConfig,
    catalog: &Catalog,
    current: &super::RoutingStatus,
) -> Result<Mapping> {
    if catalog.routes.is_empty() {
        if catalog.nodes.is_empty() {
            return Ok(config);
        }
        bail!("No compatible routes were generated");
    }
    for key in ["proxies", "proxy-groups", "listeners"] {
        if sequence(&mut config, key).iter().any(|value| {
            value
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| name.starts_with("CVR-"))
        }) {
            bail!("Subscription uses reserved CVR- names");
        }
    }
    let proxies = sequence(&mut config, "proxies");
    proxies.extend(catalog.nodes.iter().map(|node| Value::Mapping(node.proxy.clone())));
    proxies.extend(catalog.routes.iter().map(|route| Value::Mapping(route.proxy.clone())));
    // The general group serves everyday traffic with single-hop lines from both
    // subscriptions; two-hop chains stay reserved for the AI groups.
    let mut names = vec![Value::from("REJECT")];
    names.extend(
        catalog
            .routes
            .iter()
            .filter(|route| route.general)
            .map(|route| Value::from(route.id.clone())),
    );
    let groups = sequence(&mut config, "proxy-groups");
    for (index, name) in GROUPS.iter().enumerate() {
        let mut candidates = if index == 0 {
            names.clone()
        } else {
            std::iter::once(Value::from("REJECT"))
                .chain(
                    catalog
                        .routes
                        .iter()
                        .filter(|route| route.hops == 2 && route.ai_allowed(settings))
                        .map(|route| Value::from(route.id.clone())),
                )
                // Single-hop general lines stay selectable in AI groups so the
                // "ride the general line until a verified chain exists" mirror
                // has a legal target in the core selector.
                .chain(
                    catalog
                        .routes
                        .iter()
                        .filter(|route| route.general)
                        .map(|route| Value::from(route.id.clone())),
                )
                .collect::<Vec<_>>()
        };
        if let Some(current) = current.current[index].as_ref()
            && let Some(position) = candidates
                .iter()
                .position(|candidate| candidate.as_str() == Some(&current.route.id))
        {
            let selected = candidates.remove(position);
            candidates.insert(0, selected);
        }
        groups.insert(
            index,
            serde_yaml_ng::to_value(
                serde_json::json!({"name":name,"type":"select","proxies":candidates,"hidden":false}),
            )?,
        );
    }
    for lane in 0..LANES {
        let name = format!("CVR-Probe-{lane}");
        sequence(&mut config, "proxy-groups").push(serde_yaml_ng::to_value(
            serde_json::json!({"name":name,"type":"select","proxies":names,"hidden":true}),
        )?);
        let port = settings.probe_base_port + lane as u16;
        if ["mixed-port", "port", "socks-port", "redir-port", "tproxy-port"]
            .iter()
            .any(|key| config.get(*key).and_then(Value::as_u64) == Some(u64::from(port)))
            || sequence(&mut config, "listeners")
                .iter()
                .any(|listener| listener.get("port").and_then(Value::as_u64) == Some(u64::from(port)))
        {
            bail!("Routing probe port {port} conflicts with a proxy listener");
        }
        sequence(&mut config, "listeners").push(serde_yaml_ng::to_value(
            serde_json::json!({"name":name,"type":"mixed","listen":"127.0.0.1","port":port,"proxy":name}),
        )?);
    }
    config.insert("mode".into(), "rule".into());
    // Connection-level performance tuning for the generated configuration:
    // concurrent dialing shortens handshakes, unified delay keeps measurements honest.
    config.insert("tcp-concurrent".into(), true.into());
    config.insert("unified-delay".into(), true.into());
    let mut rules = vec![
        Value::from(format!("DOMAIN,www.gstatic.com,{}", GROUPS[0])),
        Value::from(format!("DOMAIN,cp.cloudflare.com,{}", GROUPS[0])),
    ];
    for (index, (group, domains)) in [
        (
            GROUPS[1],
            vec![
                "openai.com",
                "chatgpt.com",
                "oaistatic.com",
                "oaiusercontent.com",
                "openaimerge.com",
                "openai.auth0.com",
                "openaicomproduction.auth0.com",
                "workos.com",
                "workoscdn.com",
                "featuregates.org",
                "featureassets.org",
                "statsig.com",
                "statsigapi.net",
            ],
        ),
        (
            GROUPS[2],
            vec![
                "anthropic.com",
                "claude.ai",
                "claude.com",
                "claudeusercontent.com",
                "claudemcpcontent.com",
            ],
        ),
        (
            GROUPS[3],
            vec![
                "gemini.google.com",
                "aistudio.google.com",
                "generativelanguage.googleapis.com",
                "aiplatform.googleapis.com",
                "cloudcode-pa.googleapis.com",
                "cloudaicompanion.googleapis.com",
                "gemini.google",
                "googleapis.com",
                "gstatic.com",
                "googleusercontent.com",
                "accounts.google.com",
            ],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        if settings.ai_enabled[index] {
            for domain in domains {
                rules.push(Value::from(format!("DOMAIN-SUFFIX,{domain},{group}")));
            }
        }
    }
    for rule in sequence(&mut config, "rules").iter() {
        let Some(rule) = rule.as_str() else {
            continue;
        };
        let mut parts = rule.split(',').map(str::to_owned).collect::<Vec<_>>();
        let index = if parts.last().is_some_and(|part| part == "no-resolve") {
            parts.len().saturating_sub(2)
        } else {
            parts.len().saturating_sub(1)
        };
        if parts.len() >= 2 && !matches!(parts[index].as_str(), "DIRECT" | "REJECT" | "REJECT-DROP" | "PASS") {
            parts[index] = GROUPS[0].to_owned();
        }
        rules.push(Value::from(parts.join(",")));
    }
    if !rules
        .iter()
        .any(|rule| rule.as_str().is_some_and(|rule| rule.starts_with("MATCH,")))
    {
        rules.push(Value::from(format!("MATCH,{}", GROUPS[0])));
    }
    config.insert("rules".into(), Value::Sequence(rules));
    let mut profile = config
        .get("profile")
        .and_then(Value::as_mapping)
        .cloned()
        .unwrap_or_default();
    profile.insert("store-selected".into(), false.into());
    config.insert("profile".into(), Value::Mapping(profile));
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, role: &str) -> Node {
        Node {
            id: id.to_owned(),
            subscription_id: role.to_owned(),
            subscription: role.to_owned(),
            name: id.to_owned(),
            country: "JP".to_owned(),
            residential: false,
            role: role.to_owned(),
            proxy_type: "ss".to_owned(),
            udp: true,
            proxy: Mapping::new(),
        }
    }

    fn residential_node(id: &str, role: &str) -> Node {
        let mut node = node(id, role);
        node.residential = true;
        node
    }

    #[test]
    fn general_traffic_rides_single_hops_and_chains_stay_ai_only() {
        let settings = RoutingConfig {
            enabled: true,
            chain: true,
            ..RoutingConfig::default()
        };
        // Non-AI traffic never needs the chain: the general group always gets
        // single-hop lines from both subscriptions, while chains are generated
        // only for the AI groups.
        let nodes = [node("front", "ingress"), node("rear", "egress")];
        let routes = build_routes(&nodes, &settings).unwrap();
        assert!(routes.iter().any(|route| route.general && route.hops == 1));
        assert!(routes.iter().any(|route| !route.general && route.hops == 2));
    }

    #[test]
    fn residential_rear_nodes_are_reserved_for_ai_chains() {
        let settings = RoutingConfig {
            enabled: true,
            chain: true,
            ..RoutingConfig::default()
        };
        let nodes = [
            node("front", "ingress"),
            residential_node("precious", "egress"),
            node("other-front", "ingress"),
        ];
        let routes = build_routes(&nodes, &settings).unwrap();
        // The scarce residential node never becomes a single-hop general line...
        assert!(!routes.iter().any(|route| route.hops == 1 && route.egress == "precious"));
        // ...but it still serves AI traffic as the rear of a chain.
        assert!(routes
            .iter()
            .any(|route| route.hops == 2 && route.egress == "precious" && !route.general));
        // Residential front-subscription nodes remain daily drivers.
        let nodes = [residential_node("daily", "normal")];
        assert!(build_routes(&nodes, &RoutingConfig::default())
            .unwrap()
            .iter()
            .any(|route| route.hops == 1 && route.egress == "daily" && route.general));
    }

    #[test]
    fn udp_exit_does_not_use_an_entry_without_udp_support() {
        let settings = RoutingConfig {
            enabled: true,
            chain: true,
            ..RoutingConfig::default()
        };
        let mut front = node("front", "ingress");
        front.udp = false;
        let mut rear = node("rear", "egress");
        rear.proxy_type = "hysteria2".to_owned();
        // The UDP-incompatible chain is dropped; general single-hops remain.
        let routes = build_routes(&[front, rear], &settings).unwrap();
        assert!(!routes.iter().any(|route| route.hops == 2));
        assert!(routes.iter().all(|route| route.general));
    }

    #[test]
    fn ai_exclusions_apply_to_both_hops() {
        let settings = RoutingConfig::default();
        let front = node("front", "ingress");
        let mut rear = node("rear", "egress");
        rear.country = "HK".to_owned();
        assert!(!route(&front, &rear, 2).ai_allowed(&settings));
        assert!(!route(&rear, &front, 2).ai_allowed(&settings));
    }

    #[test]
    fn single_hop_uses_all_nodes_or_the_only_assigned_subscription() {
        let settings = RoutingConfig::default();
        let nodes = [node("one", "normal"), node("two", "normal")];
        assert_eq!(build_routes(&nodes, &settings).unwrap().len(), 2);
        let nodes = [node("one", "ingress"), node("two", "normal")];
        let routes = build_routes(&nodes, &settings).unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].egress, "one");
        assert_eq!(routes[0].hops, 1);
    }

    #[test]
    fn china_mainland_exits_are_never_generated() {
        let settings = RoutingConfig::default();
        let mut cn = node("大陆中转", "normal");
        cn.country = "CN".to_owned();
        let routes = build_routes(&[cn, node("日本01", "normal")], &settings).unwrap();
        assert!(routes.iter().all(|route| route.country != "CN"));

        let mut cn_rear = node("rear", "egress");
        cn_rear.country = "CN".to_owned();
        let chain_settings = RoutingConfig {
            enabled: true,
            chain: true,
            ..RoutingConfig::default()
        };
        let routes = build_routes(
            &[node("front", "ingress"), cn_rear.clone(), node("rear-ok", "egress")],
            &chain_settings,
        )
        .unwrap();
        assert!(routes.iter().all(|route| route.country != "CN"));
        // A chain whose only rear is China-mainland is a configuration error.
        assert!(build_routes(&[node("front", "ingress"), cn_rear], &chain_settings).is_err());
    }

    #[test]
    fn digest_is_stable_compact_hex() {
        let first = digest(b"sample");
        assert_eq!(first.len(), 24);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(first, digest(b"sample"));
        assert_ne!(first, digest(b"other"));
    }

    #[test]
    fn empty_subscription_directory_does_not_block_startup() {
        let config: Mapping = serde_yaml_ng::from_str("mixed-port: 17970\n").unwrap();
        let enhanced = overlay_catalog(
            config.clone(),
            &RoutingConfig::default(),
            &Catalog::default(),
            &super::super::RoutingStatus::default(),
        )
        .unwrap();
        assert_eq!(enhanced, config);
    }
    #[test]
    fn public_transport_rules_precede_ai_rules_and_use_rule_mode() {
        let settings = RoutingConfig::default();
        let nodes = vec![node("front", "ingress"), node("rear", "egress")];
        let catalog = Catalog {
            routes: build_routes(&nodes, &settings).unwrap(),
            nodes,
        };
        let config: Mapping =
            serde_yaml_ng::from_str("mode: global\nmixed-port: 7897\nrules: [MATCH,DIRECT]\n").unwrap();
        let enhanced = overlay_catalog(config, &settings, &catalog, &super::super::RoutingStatus::default()).unwrap();
        assert_eq!(enhanced.get("mode").unwrap().as_str(), Some("rule"));
        assert_eq!(
            enhanced.get("proxy-groups").unwrap().as_sequence().unwrap()[0]
                .get("name")
                .unwrap()
                .as_str(),
            Some(GROUPS[0])
        );
        assert_eq!(
            enhanced.get("rules").unwrap().as_sequence().unwrap()[0].as_str(),
            Some("DOMAIN,www.gstatic.com,CVR-Auto")
        );
    }

    #[test]
    fn ai_groups_keep_general_single_hops_selectable_for_the_mirror() {
        // Regression: the mirror selects the general single-hop line into AI
        // groups; if those names were absent the core would reject the selector
        // update with "proxy not exist" and startup would fail.
        let settings = RoutingConfig {
            enabled: true,
            chain: true,
            ..RoutingConfig::default()
        };
        let nodes = vec![node("front", "ingress"), node("rear", "egress")];
        let catalog = Catalog {
            routes: build_routes(&nodes, &settings).unwrap(),
            nodes,
        };
        let config: Mapping = serde_yaml_ng::from_str("mixed-port: 17970\n").unwrap();
        let enhanced =
            overlay_catalog(config, &settings, &catalog, &super::super::RoutingStatus::default()).unwrap();
        for index in 1..4 {
            let candidates = enhanced
                .get("proxy-groups")
                .and_then(Value::as_sequence)
                .and_then(|groups| groups.get(index))
                .and_then(|group| group.get("proxies"))
                .and_then(Value::as_sequence)
                .unwrap();
            let names = candidates
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>();
            let chain = catalog
                .routes
                .iter()
                .find(|route| route.hops == 2)
                .unwrap();
            let single = catalog
                .routes
                .iter()
                .find(|route| route.general)
                .unwrap();
            assert!(names.contains(&chain.id.as_str()));
            assert!(names.contains(&single.id.as_str()));
        }
    }

    #[test]
    fn probe_port_conflict_rejects_the_candidate_configuration() {
        let settings = RoutingConfig::default();
        let nodes = vec![node("front", "ingress"), node("rear", "egress")];
        let catalog = Catalog {
            routes: build_routes(&nodes, &settings).unwrap(),
            nodes,
        };
        let config: Mapping = serde_yaml_ng::from_str("socks-port: 17923\n").unwrap();
        assert!(overlay_catalog(config, &settings, &catalog, &super::super::RoutingStatus::default()).is_err());
    }
}
