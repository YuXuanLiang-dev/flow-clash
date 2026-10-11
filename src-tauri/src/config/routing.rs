use anyhow::{Result, bail};
use arc_swap::{ArcSwap, ArcSwapOption};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::OnceCell;

use crate::{
    config::Config,
    core::CoreManager,
    utils::{dirs, help},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    pub enabled: bool,
    pub chain: bool,
    pub allow_direct_fallback: bool,
    pub ai_enabled: [bool; 3],
    pub ai_excluded_countries: Vec<String>,
    pub mechanisms: RoutingMechanisms,
    pub profile_roles: BTreeMap<String, String>,
    pub residential_profiles: Vec<String>,
    pub preferred_ingress: Vec<String>,
    pub preferred_egress: Vec<String>,
    pub ai_priorities: BTreeMap<String, Vec<String>>,
    pub prefer_residential: bool,
    pub health_interval: u64,
    pub ai_health_interval: u64,
    pub probe_base_port: u16,
    pub node_metadata: BTreeMap<String, NodeMetadata>,
    pub folders: Vec<ProfileFolder>,
    pub profile_folders: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeMetadata {
    pub country: String,
    pub residential: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileFolder {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingMechanisms {
    pub fast_concurrency: usize,
    pub retry_concurrency: usize,
    pub ai_concurrency: usize,
    pub fast_timeout_ms: u64,
    pub retry_timeout_ms: u64,
    pub initial_batch: usize,
    pub general_budget: usize,
    pub recovery_budget: usize,
    pub general_failures: u8,
    pub ai_failures: u8,
    pub general_retry_seconds: u64,
    pub ai_retry_seconds: u64,
    pub recovery_retry_seconds: u64,
    pub recovery_max_retry_seconds: u64,
    pub failure_cooldown_seconds: u64,
    pub switch_min_seconds: u64,
    pub improvement_percent: u64,
    pub improvement_ms: u64,
    pub network_debounce_seconds: u64,
    pub cache_ttl_hours: u64,
    pub cache_network_limit: usize,
}

impl Default for RoutingMechanisms {
    fn default() -> Self {
        Self {
            fast_concurrency: 8,
            retry_concurrency: 2,
            ai_concurrency: 2,
            fast_timeout_ms: 4000,
            retry_timeout_ms: 8000,
            // A doubled first batch keeps all fast lanes refilling without waiting
            // for the slowest probe of a smaller batch to time out.
            initial_batch: 16,
            general_budget: 32,
            recovery_budget: 256,
            general_failures: 2,
            ai_failures: 3,
            general_retry_seconds: 5,
            ai_retry_seconds: 15,
            recovery_retry_seconds: 15,
            recovery_max_retry_seconds: 120,
            failure_cooldown_seconds: 300,
            switch_min_seconds: 60,
            improvement_percent: 20,
            improvement_ms: 50,
            network_debounce_seconds: 3,
            cache_ttl_hours: 168,
            cache_network_limit: 8,
        }
    }
}

impl RoutingMechanisms {
    fn validate(&self) -> Result<()> {
        let bounds = [
            ("fast concurrency", self.fast_concurrency as u64, 1, 8),
            ("retry concurrency", self.retry_concurrency as u64, 1, 2),
            ("AI concurrency", self.ai_concurrency as u64, 1, 2),
            ("fast timeout", self.fast_timeout_ms, 1000, 8000),
            ("retry timeout", self.retry_timeout_ms, self.fast_timeout_ms, 20000),
            ("initial batch", self.initial_batch as u64, 1, 64),
            (
                "general budget",
                self.general_budget as u64,
                self.initial_batch as u64,
                512,
            ),
            (
                "recovery budget",
                self.recovery_budget as u64,
                self.general_budget as u64,
                20000,
            ),
            ("general failure threshold", u64::from(self.general_failures), 1, 10),
            ("AI failure threshold", u64::from(self.ai_failures), 1, 10),
            ("general retry interval", self.general_retry_seconds, 1, 60),
            ("AI retry interval", self.ai_retry_seconds, 5, 120),
            ("recovery retry", self.recovery_retry_seconds, 5, 300),
            (
                "maximum recovery retry",
                self.recovery_max_retry_seconds,
                self.recovery_retry_seconds,
                900,
            ),
            ("failure cooldown", self.failure_cooldown_seconds, 10, 1800),
            ("minimum switch interval", self.switch_min_seconds, 10, 600),
            ("switch improvement percentage", self.improvement_percent, 5, 80),
            ("minimum switch improvement", self.improvement_ms, 0, 1000),
            ("network debounce", self.network_debounce_seconds, 1, 30),
            ("cache lifetime", self.cache_ttl_hours, 1, 720),
            ("cached networks", self.cache_network_limit as u64, 1, 32),
        ];
        for (name, value, minimum, maximum) in bounds {
            if !(minimum..=maximum).contains(&value) {
                bail!("{name} must be {minimum}–{maximum}");
            }
        }
        Ok(())
    }
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            chain: false,
            allow_direct_fallback: false,
            ai_enabled: [true; 3],
            ai_excluded_countries: vec!["CN".to_owned(), "HK".to_owned()],
            mechanisms: RoutingMechanisms::default(),
            profile_roles: BTreeMap::new(),
            residential_profiles: Vec::new(),
            preferred_ingress: Vec::new(),
            preferred_egress: Vec::new(),
            ai_priorities: BTreeMap::new(),
            prefer_residential: true,
            health_interval: 15,
            ai_health_interval: 60,
            probe_base_port: 17920,
            node_metadata: BTreeMap::new(),
            folders: Vec::new(),
            profile_folders: BTreeMap::new(),
        }
    }
}

impl RoutingConfig {
    pub fn has_chain_roles(&self) -> bool {
        self.profile_roles.values().any(|role| role == "ingress")
            && self.profile_roles.values().any(|role| role == "egress")
    }

    fn assign_role(&mut self, uid: &str, role: &str) {
        self.profile_roles
            .retain(|profile, assigned| profile != uid && (role == "normal" || assigned != role));
        if role != "normal" {
            self.profile_roles.insert(uid.to_owned(), role.to_owned());
        }
        self.enabled = true;
    }

    pub fn validate(&mut self) -> Result<()> {
        self.mechanisms.validate()?;
        if !(5..=60).contains(&self.health_interval) || !(30..=300).contains(&self.ai_health_interval) {
            bail!("Health intervals must be 5–60 and 30–300 seconds");
        }
        if !(1024..=65515).contains(&self.probe_base_port) {
            bail!("Probe base port must be 1024–65515");
        }
        for country in self
            .ai_excluded_countries
            .iter_mut()
            .chain(self.node_metadata.values_mut().map(|meta| &mut meta.country))
        {
            *country = country.trim().to_ascii_uppercase();
            if !country.is_empty() && (country.len() != 2 || !country.bytes().all(|byte| byte.is_ascii_alphabetic())) {
                bail!("Country labels must be two-letter codes");
            }
        }
        self.ai_excluded_countries.retain(|country| !country.is_empty());
        // Hong Kong and mainland China can never serve AI traffic: services block
        // them and a Chinese exit would expose the user's location.
        self.ai_excluded_countries.push("HK".to_owned());
        self.ai_excluded_countries.push("CN".to_owned());
        self.ai_excluded_countries.sort();
        self.ai_excluded_countries.dedup();
        if self
            .profile_roles
            .values()
            .any(|role| !matches!(role.as_str(), "ingress" | "egress" | "normal"))
        {
            bail!("Unsupported subscription role");
        }
        self.chain = self.has_chain_roles();
        if self
            .ai_priorities
            .keys()
            .any(|service| !crate::core::routing::SERVICES.contains(&service.as_str()))
        {
            bail!("Unsupported AI service");
        }
        for priorities in [&mut self.preferred_ingress, &mut self.preferred_egress]
            .into_iter()
            .chain(self.ai_priorities.values_mut())
        {
            let mut seen = std::collections::HashSet::new();
            priorities.retain(|id| !id.is_empty() && seen.insert(id.clone()));
            if priorities.len() > 64 {
                bail!("Each priority list supports up to 64 nodes");
            }
        }
        let mut ids = std::collections::HashSet::new();
        for folder in &mut self.folders {
            folder.name = folder.name.trim().to_owned();
            if folder.id.is_empty() || folder.name.is_empty() || !ids.insert(folder.id.clone()) {
                bail!("Folder IDs must be unique and folder names cannot be empty");
            }
        }
        self.profile_folders.retain(|_, folder| ids.contains(folder));
        Ok(())
    }
}

pub struct RoutingStore {
    pub committed: ArcSwap<RoutingConfig>,
    pub pending: ArcSwapOption<RoutingConfig>,
}

pub async fn routing_store() -> Result<&'static RoutingStore> {
    static STORE: OnceCell<RoutingStore> = OnceCell::const_new();
    STORE
        .get_or_try_init(|| async {
            let path = dirs::app_home_dir()?.join("routing.yaml");
            let mut config = if path.exists() {
                help::read_yaml::<RoutingConfig>(&path).await?
            } else {
                RoutingConfig::default()
            };
            config.validate()?;
            Ok(RoutingStore {
                committed: ArcSwap::from_pointee(config),
                pending: ArcSwapOption::empty(),
            })
        })
        .await
}

pub async fn routing_config() -> Result<Arc<RoutingConfig>> {
    let store = routing_store().await?;
    Ok(store.pending.load_full().unwrap_or_else(|| store.committed.load_full()))
}

pub async fn save_routing_config(mut config: RoutingConfig) -> Result<()> {
    config.validate()?;
    let _write = Config::lock_config_write().await;
    apply_routing_config(config).await
}

pub async fn set_subscription_role(uid: &str, role: &str) -> Result<()> {
    if !matches!(role, "ingress" | "egress" | "normal") {
        bail!("Unsupported subscription role");
    }
    let _write = Config::lock_config_write().await;
    let profiles = Config::profiles().await.data_arc();
    if !profiles.items.iter().flatten().any(|profile| {
        profile.uid.as_deref() == Some(uid) && matches!(profile.itype.as_deref(), Some("local" | "remote"))
    }) {
        bail!("Subscription no longer exists");
    }
    let mut config = routing_store().await?.committed.load_full().as_ref().clone();
    config.assign_role(uid, role);
    config.validate()?;
    apply_routing_config(config).await
}

async fn apply_routing_config(config: RoutingConfig) -> Result<()> {
    let store = routing_store().await?;
    let previous = store.committed.load_full();
    let path = dirs::app_home_dir()?.join("routing.yaml");
    help::save_yaml(&path, &config, Some("# Adaptive routing for Clash Verge Rev")).await?;
    let mut previous_policy = previous.as_ref().clone();
    previous_policy.folders = config.folders.clone();
    previous_policy.profile_folders = config.profile_folders.clone();
    if (!previous.enabled && !config.enabled)
        || serde_json::to_value(&previous_policy)? == serde_json::to_value(&config)?
    {
        store.committed.store(Arc::new(config));
        return Ok(());
    }
    let country_metadata = |settings: &RoutingConfig| {
        settings
            .node_metadata
            .iter()
            .map(|(id, metadata)| (id.clone(), metadata.country.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    let reload = previous.enabled != config.enabled
        || previous.chain != config.chain
        || previous.allow_direct_fallback != config.allow_direct_fallback
        || previous.profile_roles != config.profile_roles
        || previous.ai_enabled != config.ai_enabled
        || previous.ai_excluded_countries != config.ai_excluded_countries
        || previous.probe_base_port != config.probe_base_port
        || country_metadata(&previous) != country_metadata(&config);
    let reselect = previous.preferred_ingress != config.preferred_ingress
        || previous.preferred_egress != config.preferred_egress
        || previous.ai_priorities != config.ai_priorities
        || previous.chain != config.chain
        || previous.profile_roles != config.profile_roles;
    if !reload {
        store.committed.store(Arc::new(config));
        if reselect {
            crate::core::routing::reevaluate();
        } else {
            crate::core::routing::restart();
        }
        return Ok(());
    }
    store.pending.store(Some(Arc::new(config.clone())));
    let applied = CoreManager::global().update_config_forced().await;
    match applied {
        Ok(outcome) if outcome.is_valid() => {
            store.committed.store(Arc::new(config));
            store.pending.store(None);
            crate::core::handle::Handle::refresh_clash();
            if reselect {
                crate::core::routing::reevaluate();
            } else {
                crate::core::routing::restart();
            }
            Ok(())
        }
        result => {
            store.pending.store(None);
            help::save_yaml(&path, previous.as_ref(), Some("# Adaptive routing for Clash Verge Rev")).await?;
            match result {
                Ok(outcome) => bail!("Routing configuration was not applied: {outcome}"),
                Err(error) => Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_is_automatic_and_roles_determine_hop_count() {
        let mut config = RoutingConfig::default();
        config.validate().unwrap();
        assert!(config.enabled);
        assert!(!config.chain);
        config.assign_role("first", "ingress");
        config.validate().unwrap();
        assert!(!config.chain);
        config.assign_role("second", "egress");
        config.validate().unwrap();
        assert!(config.chain);
        config.assign_role("replacement", "ingress");
        config.validate().unwrap();
        assert!(!config.profile_roles.contains_key("first"));
        assert!(config.chain);
        config.assign_role("second", "normal");
        config.validate().unwrap();
        assert!(!config.chain);
        assert_eq!(config.ai_enabled, [true; 3]);
    }

    #[test]
    fn older_configuration_receives_mechanism_defaults() {
        let mut config: RoutingConfig =
            serde_json::from_str(r#"{"chain":false,"health_interval":20,"mechanisms":{"ai_failures":4}}"#).unwrap();
        config.validate().unwrap();
        assert_eq!(config.mechanisms.ai_failures, 4);
        assert_eq!(config.mechanisms.fast_concurrency, 8);
        assert!(!config.allow_direct_fallback);
    }

    #[test]
    fn invalid_timeouts_and_expansion_budgets_are_rejected() {
        let mut config = RoutingConfig::default();
        config.mechanisms.retry_timeout_ms = 1000;
        assert!(config.validate().is_err());
        config.mechanisms = RoutingMechanisms::default();
        config.mechanisms.recovery_budget = 16;
        assert!(config.validate().is_err());
        config.mechanisms = RoutingMechanisms::default();
        config.mechanisms.recovery_max_retry_seconds = 10;
        assert!(config.validate().is_err());
    }

    #[test]
    fn excluded_countries_are_normalized_and_blocked_homelands_remain_excluded() {
        let mut config = RoutingConfig::default();
        config.ai_excluded_countries = vec![" jp ".to_owned(), "JP".to_owned()];
        config.validate().unwrap();
        assert_eq!(config.ai_excluded_countries, ["CN", "HK", "JP"]);
        // Removing them outright is not honored.
        config.ai_excluded_countries = Vec::new();
        config.validate().unwrap();
        assert!(config.ai_excluded_countries.contains(&"CN".to_owned()));
        assert!(config.ai_excluded_countries.contains(&"HK".to_owned()));
    }
}
