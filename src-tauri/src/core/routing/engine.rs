use anyhow::Result;
use arc_swap::ArcSwap;
use network_interface::{NetworkInterface, NetworkInterfaceConfig as _};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use super::{
    catalog::{self, Catalog, GROUPS, Route},
    probes::{self, AiProof, Quality},
};
use crate::{
    config::{
        Config,
        routing::{RoutingConfig, routing_config},
    },
    core::{CoreManager, handle::Handle, manager::RunningMode},
    process::AsyncHandler,
    utils::{dirs, help},
};

static SNAPSHOT: LazyLock<ArcSwap<RoutingStatus>> = LazyLock::new(|| ArcSwap::from_pointee(RoutingStatus::default()));
static GENERATION: AtomicU64 = AtomicU64::new(0);
static VERIFIED: AtomicBool = AtomicBool::new(false);
static RESELECT: AtomicBool = AtomicBool::new(false);
static TASK: parking_lot::Mutex<Option<tauri::async_runtime::JoinHandle<()>>> = parking_lot::Mutex::new(None);
static ACTIVATION_LOCKS: LazyLock<[tokio::sync::Mutex<()>; 4]> =
    LazyLock::new(|| std::array::from_fn(|_| tokio::sync::Mutex::new(())));
static CACHE_REVISION: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Default, Serialize)]
pub struct RoutingStatus {
    pub enabled: bool,
    pub phase: String,
    pub reason: String,
    pub last_error: String,
    pub network: String,
    pub network_pending: bool,
    pub elapsed_ms: u64,
    pub first_route_ms: Option<u64>,
    pub candidate_total: usize,
    pub fast_done: usize,
    pub fast_target: usize,
    pub retry_done: usize,
    pub retry_total: usize,
    pub ai_done: usize,
    pub ai_target: usize,
    pub active_probes: usize,
    pub enabled_groups: [bool; 4],
    pub eligible_candidates: [usize; 4],
    pub checking: [bool; 4],
    pub recovering: [bool; 4],
    pub next_checks: [u64; 4],
    pub next_retry_at: Option<u64>,
    pub next_optimization_at: Option<u64>,
    pub current: [Option<Selection>; 4],
}

#[derive(Clone, Serialize)]
pub struct Selection {
    pub route: Arc<Route>,
    pub quality: Quality,
    pub ai: Option<AiProof>,
    pub failures: u8,
    pub exit_since: u64,
    pub switched_at: u64,
    pub last_checked: u64,
    /// This group currently rides the general line instead of a verified route of
    /// its own; AI traffic follows the general selection until a two-hop chain
    /// passes AI verification.
    #[serde(default)]
    pub mirrored: bool,
    /// What destinations actually see on this line: exit IP and country.
    #[serde(default)]
    pub exit: probes::ExitTrace,
}

/// A verified candidate waiting for activation in one of the four groups.
type PendingCandidate = (Arc<Route>, Quality, Option<AiProof>);

pub fn status() -> Arc<RoutingStatus> {
    SNAPSHOT.load_full()
}
pub fn verified() -> bool {
    VERIFIED.load(Ordering::Acquire)
}

pub fn start() {
    let mut task = TASK.lock();
    if task.as_ref().is_some_and(|task| !task.inner().is_finished()) {
        return;
    }
    let generation = GENERATION.fetch_add(1, Ordering::AcqRel) + 1;
    *task = Some(AsyncHandler::spawn(move || async move {
        if let Err(error) = run(generation).await
            && generation == GENERATION.load(Ordering::Acquire)
        {
            VERIFIED.store(false, Ordering::Release);
            let mut state = status().as_ref().clone();
            state.phase = "error".to_owned();
            state.last_error = error.to_string();
            SNAPSHOT.store(Arc::new(state));
        }
    }));
}

pub fn restart() {
    stop();
    start();
}

pub fn reevaluate() {
    RESELECT.store(true, Ordering::Release);
    restart();
}

pub fn stop() {
    GENERATION.fetch_add(1, Ordering::AcqRel);
    let task = TASK.lock().take();
    if let Some(task) = task {
        task.abort();
    }
    VERIFIED.store(false, Ordering::Release);
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct CacheEntry {
    at: u64,
    selected: Vec<String>,
    successes: Vec<String>,
}

#[derive(Default, Serialize, Deserialize)]
struct Cache {
    networks: BTreeMap<String, CacheEntry>,
}

enum Event {
    ProxyError {
        epoch: u64,
        reason: String,
    },
    Activated {
        epoch: u64,
        group: usize,
        route: Arc<Route>,
        quality: Quality,
        outcome: std::result::Result<Option<AiProof>, String>,
        exit: Option<probes::ExitTrace>,
        restored: bool,
        acceptance: Option<oneshot::Sender<bool>>,
    },
    ActivationFinished {
        group: usize,
        restored: bool,
        reason: String,
    },
    Refined {
        epoch: u64,
        anchor: String,
        route: Arc<Route>,
        quality: Quality,
    },
    General {
        round: u64,
        route: Arc<Route>,
        quality: Quality,
        retry: bool,
        lane: usize,
    },
    Ai {
        round: u64,
        group: usize,
        route: Arc<Route>,
        quality: Quality,
        proof: AiProof,
        trace: Option<probes::ExitTrace>,
        lane: usize,
    },
    Monitor {
        epoch: u64,
        group: usize,
        route: String,
        quality: Option<Quality>,
        ai: Option<AiProof>,
    },
    Mirrored {
        epoch: u64,
        group: usize,
        route: Option<Arc<Route>>,
        quality: Option<Quality>,
    },
}

struct Search {
    round: u64,
    order: VecDeque<Arc<Route>>,
    ai_order: [VecDeque<Arc<Route>>; 3],
    retries: VecDeque<Arc<Route>>,
    fast_busy: [Option<String>; 8],
    retry_busy: [Option<String>; 2],
    ai_busy: [Option<(usize, String)>; 2],
    tried: usize,
    stage: usize,
    normal_target: usize,
    limit: usize,
    ai_cursor: usize,
    started: Instant,
}

struct Engine {
    generation: u64,
    epoch: Arc<AtomicU64>,
    settings: Arc<RoutingConfig>,
    catalog: Catalog,
    state: RoutingStatus,
    cache: Cache,
    cache_key: String,
    jobs: JoinSet<()>,
    activations: JoinSet<()>,
    tx: mpsc::Sender<Event>,
    search: Option<Search>,
    monitor_busy: [bool; 4],
    mirroring: [bool; 4],
    next_monitor: [Instant; 4],
    cycles: [u64; 4],
    recovery: [bool; 4],
    validated: [bool; 4],
    failed_at: [u64; 4],
    quality: HashMap<String, Quality>,
    successes: Vec<String>,
    rotation: [usize; 4],
    round: u64,
    began: Instant,
    next_search: Instant,
    retry_attempt: u32,
    runtime_identity: usize,
    switching: [bool; 4],
    pending: [Vec<PendingCandidate>; 4],
    deferred: Option<(Arc<Route>, Quality, String)>,
    refining: bool,
    network_available: bool,
    network_candidate: Option<(Option<String>, Instant)>,
    reselect: bool,
    // Per route id, an expiry timestamp per group; borrowing lookups avoid an
    // allocation for every candidate while ordering a search round.
    cooldown: HashMap<String, [u64; 4]>,
    /// Route id → last observed exit ground truth (IP, country, China flag).
    exit_intel: HashMap<String, probes::ExitTrace>,
}

async fn wait_for_core(generation: u64) {
    while CoreManager::global().current_core_readiness_generation().is_none() {
        if generation != GENERATION.load(Ordering::Acquire) || Handle::global().is_exiting() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Park until profiles produce routes; `true` means the caller should keep waiting
/// in a fresh engine (the runtime configuration changed underneath us).
async fn wait_for_profiles(generation: u64, runtime_identity: usize) -> bool {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if generation != GENERATION.load(Ordering::Acquire)
            || Handle::global().is_exiting()
            || matches!(*CoreManager::global().get_running_mode(), RunningMode::NotRunning)
        {
            return false;
        }
        if Arc::as_ptr(&Config::runtime().await.data_arc()) as usize != runtime_identity {
            restart();
            return false;
        }
    }
}

/// Re-pin previously selected routes that still exist, so a canceled verification
/// cannot leave a group pointing at an unpublished candidate. AI groups without a
/// retained line of their own mirror the general line instead of pointing at REJECT.
async fn restore_selections(
    state: &mut RoutingStatus,
    catalog: &Catalog,
    settings: &RoutingConfig,
    previous: &RoutingStatus,
) -> Result<()> {
    for (group, enabled) in state.enabled_groups.iter().enumerate() {
        let _selection = ACTIVATION_LOCKS[group].lock().await;
        let retained = previous.current[group]
            .as_ref()
            .filter(|_| *enabled)
            .and_then(|selection| {
                catalog
                    .routes
                    .iter()
                    .find(|route| {
                        route.id == selection.route.id
                            && ((group == 0 && route.general)
                                || (group > 0
                                    && settings.chain
                                    && !selection.mirrored
                                    && route.hops == 2
                                    && route.ai_allowed(settings)))
                    })
                    .map(|route| (selection, route))
            });
        // An AI group with no line of its own follows whatever the general group
        // holds once the loop reaches it (group 0 is processed first).
        let mirrored = (group > 0 && retained.is_none())
            .then(|| {
                state.current[0].as_ref().filter(|_| *enabled).and_then(|general| {
                    catalog
                        .routes
                        .iter()
                        .find(|route| route.id == general.route.id && route.general)
                })
            })
            .flatten();
        let target = retained
            .map(|(_, route)| route.id.as_str())
            .or_else(|| mirrored.map(|route| route.id.as_str()))
            .unwrap_or("REJECT");
        probes::select(GROUPS[group], target).await?;
        if let Some((selection, route)) = retained {
            let mut selection = selection.clone();
            selection.route = Arc::clone(route);
            selection.failures = 0;
            state.current[group] = Some(selection);
        } else if let Some(route) = mirrored {
            state.current[group] = Some(Selection {
                route: Arc::clone(route),
                quality: Quality::default(),
                ai: None,
                failures: 0,
                exit_since: 0,
                switched_at: 0,
                last_checked: 0,
                mirrored: true,
                exit: probes::ExitTrace::default(),
            });
        }
    }
    Ok(())
}

/// Optimistic start: pin the cached last-good general line before any probing, so
/// traffic can flow immediately while the search verifies it in the background.
/// AI groups do not restore their own cached line — they mirror the general line
/// until a two-hop chain passes AI verification.
async fn restore_cached_routes(
    state: &mut RoutingStatus,
    cache: &Cache,
    cache_key: &str,
    catalog: &Catalog,
    settings: &RoutingConfig,
) -> Result<()> {
    let Some(route) = optimistic_routes(cache, cache_key, settings, catalog).filter(|_| state.current[0].is_none())
    else {
        return Ok(());
    };
    probes::select(GROUPS[0], &route.id).await?;
    state.current[0] = Some(Selection {
        route,
        quality: Quality::default(),
        ai: None,
        failures: 0,
        exit_since: 0,
        // A zero switch timestamp keeps every later verification newer.
        switched_at: 0,
        last_checked: 0,
        mirrored: false,
        exit: probes::ExitTrace::default(),
    });
    Ok(())
}

fn optimistic_routes(
    cache: &Cache,
    cache_key: &str,
    settings: &RoutingConfig,
    catalog: &Catalog,
) -> Option<Arc<Route>> {
    let fresh = cache
        .networks
        .get(cache_key)
        .filter(|entry| probes::now().saturating_sub(entry.at) < settings.mechanisms.cache_ttl_hours * 3600000);
    fresh?
        .selected
        .iter()
        .find_map(|id| {
            catalog
                .routes
                .iter()
                .find(|route| route.id == *id && route.general)
        })
        .cloned()
}

fn initial_status(
    catalog: &Catalog,
    settings: &RoutingConfig,
    enabled_groups: [bool; 4],
    network: &str,
) -> RoutingStatus {
    let eligible_candidates = std::array::from_fn(|group| {
        catalog
            .routes
            .iter()
            .filter(|route| {
                group == 0
                    || (enabled_groups[group]
                        && settings.chain
                        && route.hops == 2
                        && route.ai_allowed(settings))
            })
            .count()
    });
    RoutingStatus {
        enabled: true,
        network: network.to_owned(),
        candidate_total: catalog.routes.len(),
        enabled_groups,
        eligible_candidates,
        ..RoutingStatus::default()
    }
}

async fn run(generation: u64) -> Result<()> {
    let settings = routing_config().await?;
    if !settings.enabled {
        SNAPSHOT.store(Arc::new(RoutingStatus {
            phase: "disabled".to_owned(),
            ..RoutingStatus::default()
        }));
        return Ok(());
    }
    SNAPSHOT.store(Arc::new(RoutingStatus {
        enabled: true,
        phase: "waiting_core".to_owned(),
        current: status().current.clone(),
        ..RoutingStatus::default()
    }));
    wait_for_core(generation).await;
    if generation != GENERATION.load(Ordering::Acquire) || Handle::global().is_exiting() {
        return Ok(());
    }
    let profiles = Config::profiles().await.data_arc();
    let catalog = catalog::load(&profiles, &settings).await?;
    let runtime = Config::runtime().await.data_arc();
    let runtime_identity = Arc::as_ptr(&runtime) as usize;
    if catalog.routes.is_empty() {
        VERIFIED.store(false, Ordering::Release);
        SNAPSHOT.store(Arc::new(RoutingStatus {
            enabled: true,
            phase: "waiting_profiles".to_owned(),
            ..RoutingStatus::default()
        }));
        wait_for_profiles(generation, runtime_identity).await;
        return Ok(());
    }
    let cache_path = dirs::app_home_dir()?.join("routing-cache.yaml");
    let cache = if cache_path.exists() {
        help::read_yaml::<Cache>(&cache_path).await.unwrap_or_default()
    } else {
        Cache::default()
    };
    let signature = network().await?;
    let (tx, rx) = mpsc::channel(64);
    let enabled_groups = [
        true,
        settings.ai_enabled[0],
        settings.ai_enabled[1],
        settings.ai_enabled[2],
    ];
    let mut state = initial_status(&catalog, &settings, enabled_groups, signature.as_deref().unwrap_or_default());
    restore_selections(&mut state, &catalog, &settings, &status()).await?;
    let key = cache_key(&state.network, &settings);
    restore_cached_routes(&mut state, &cache, &key, &catalog, &settings).await?;
    let recovery = std::array::from_fn(|group| enabled_groups[group] && (group == 0 || settings.chain));
    let mut engine = Engine {
        generation,
        epoch: Arc::new(AtomicU64::new(1)),
        settings,
        catalog,
        state,
        cache,
        cache_key: key,
        jobs: JoinSet::new(),
        activations: JoinSet::new(),
        tx,
        search: None,
        monitor_busy: [false; 4],
        mirroring: [false; 4],
        next_monitor: [Instant::now(); 4],
        cycles: [0; 4],
        recovery,
        validated: [false; 4],
        failed_at: [0; 4],
        cooldown: HashMap::new(),
        exit_intel: HashMap::new(),
        quality: HashMap::new(),
        successes: Vec::new(),
        rotation: [0; 4],
        round: 0,
        began: Instant::now(),
        next_search: Instant::now(),
        retry_attempt: 0,
        runtime_identity,
        switching: [false; 4],
        pending: std::array::from_fn(|_| Vec::new()),
        deferred: None,
        refining: false,
        network_available: signature.is_some(),
        network_candidate: None,
        reselect: RESELECT.swap(false, Ordering::AcqRel),
    };
    if engine.network_available {
        engine.begin_search(true);
    }
    event_loop(generation, engine, rx).await
}

/// Steady state: drive probes, monitors and network observation until the engine
/// generation is superseded or the app exits.
async fn event_loop(generation: u64, mut engine: Engine, mut rx: mpsc::Receiver<Event>) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut network_tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    network_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if generation != GENERATION.load(Ordering::Acquire)
            || Handle::global().is_exiting()
            || matches!(*CoreManager::global().get_running_mode(), RunningMode::NotRunning)
        {
            return Ok(());
        }
        tokio::select! {
            _ = tick.tick() => {
                if engine.network_available {
                    engine.sync_mirrors();
                    engine.refine();
                    engine.monitors();
                    if engine.search.is_none() && engine.recovery.iter().any(|value| *value) && Instant::now() >= engine.next_search { engine.begin_search(false); }
                    engine.dispatch();
                    engine.dispatch_activations();
                }
                while engine.jobs.try_join_next().is_some() {}
                while engine.activations.try_join_next().is_some() {}
                engine.publish();
            }
            Some(event) = rx.recv() => { engine.receive(event); }
            _ = network_tick.tick() => {
                let runtime = Config::runtime().await.data_arc();
                if Arc::as_ptr(&runtime) as usize != engine.runtime_identity { restart(); return Ok(()); }
                match network().await {
                    Ok(next) => engine.observe_network(next),
                    Err(error) => engine.state.last_error = error.to_string(),
                }
            }
        }
    }
}

fn rank(
    route: &Route,
    settings: &RoutingConfig,
    current: Option<&Selection>,
    group: usize,
    keep_exit: bool,
) -> [usize; 8] {
    let preference = |list: &[String], id: &str| list.iter().position(|node| node == id).unwrap_or(usize::MAX);
    let ai = group.checked_sub(1);
    let custom = ai
        .and_then(|index| settings.ai_priorities.get(super::SERVICES[index]))
        .map_or(usize::MAX, |list| preference(list, &route.egress));
    [
        usize::from(settings.chain && route.hops < 2),
        usize::from(keep_exit && current.is_some_and(|selected| selected.route.egress != route.egress)),
        custom,
        preference(&settings.preferred_egress, &route.egress),
        preference(&settings.preferred_ingress, &route.ingress),
        if ai.is_some() && settings.prefer_residential {
            if route.residential && route.rear_subscription {
                0
            } else if route.residential {
                1
            } else {
                2
            }
        } else {
            0
        },
        usize::from(
            current
                .is_some_and(|selected| !selected.route.country.is_empty() && selected.route.country != route.country),
        ),
        usize::from(!route.same_country()),
    ]
}

/// 0 for a fully verified AI proof (or the general group), 1 for one that is
/// only usable-pending. Sorting key only — never a demotion signal.
fn candidate_penalty(group: usize, proof: Option<&AiProof>) -> usize {
    usize::from(group > 0 && !proof.is_some_and(|proof| proof.confirmed()))
}

fn backoff(settings: &RoutingConfig, attempt: u32) -> u64 {
    settings
        .mechanisms
        .recovery_retry_seconds
        .saturating_mul(1u64 << attempt.min(10))
        .min(settings.mechanisms.recovery_max_retry_seconds)
}

impl Engine {
    fn valid(&self) -> bool {
        self.generation == GENERATION.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn sync_mirrors_no_park_for_test(&mut self) {
        // Mirror of the park decision in sync_mirrors, without job spawning:
        // an AI-excluded general exit leaves followers without a line.
        let excluded = self
            .state
            .current[0]
            .as_ref()
            .is_some_and(|general| !general.route.ai_allowed(&self.settings));
        if excluded {
            for group in 1..4 {
                if self.state.current[group]
                    .as_ref()
                    .is_some_and(|selection| selection.mirrored)
                {
                    self.state.current[group] = None;
                }
            }
        }
    }
    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// An AI group rides the general line while it has no verified route of its own;
    /// in single-hop configurations it simply stays there for good.
    fn sync_mirrors(&mut self) {
        let Some((route, quality)) = self
            .state
            .current[0]
            .as_ref()
            .map(|general| (Arc::clone(&general.route), general.quality.clone()))
        else {
            return;
        };
        for (group, group_name) in GROUPS.iter().enumerate().skip(1) {
            if !self.state.enabled_groups[group] || self.mirroring[group] {
                continue;
            }
            let needs_mirror = match self.state.current[group].as_ref() {
                None => true,
                Some(selection) => selection.mirrored && selection.route.id != route.id,
            };
            if !needs_mirror {
                continue;
            }
            self.mirroring[group] = true;
            let tx = self.tx.clone();
            let epoch = self.epoch();
            let route = Arc::clone(&route);
            let followed = quality.clone();
            self.jobs.spawn(async move {
                let quality = probes::select(group_name, &route.id).await.ok().map(|()| followed);
                let _ = tx
                    .send(Event::Mirrored {
                        epoch,
                        group,
                        route: Some(route),
                        quality,
                    })
                    .await;
            });
        }
    }

    fn on_mirrored(
        &mut self,
        epoch: u64,
        group: usize,
        route: Option<Arc<Route>>,
        quality: Option<Quality>,
    ) {
        self.mirroring[group] = false;
        if epoch != self.epoch() {
            return;
        }
        let (Some(route), Some(quality)) = (route, quality) else {
            return;
        };
        let general_holds = self
            .state
            .current[0]
            .as_ref()
            .is_some_and(|general| general.route.id == route.id);
        let still_follower = self.state.current[group]
            .as_ref()
            .is_none_or(|selection| selection.mirrored);
        if !general_holds || !still_follower || !self.state.enabled_groups[group] {
            return;
        }
        let exit = self
            .state
            .current[0]
            .as_ref()
            .map(|general| general.exit.clone())
            .unwrap_or_default();
        self.state.current[group] = Some(Selection {
            route,
            quality,
            ai: None,
            failures: 0,
            exit_since: 0,
            switched_at: probes::now(),
            last_checked: 0,
            mirrored: true,
            exit,
        });
        self.next_monitor[group] =
            Instant::now() + Duration::from_secs(self.settings.ai_health_interval);
        self.publish();
        Handle::refresh_clash();
    }
    fn cooling(&self, group: usize, route: &Route) -> bool {
        self.cooldown
            .get(route.id.as_str())
            .is_some_and(|until| until[group] > probes::now())
    }

    fn ordered(&self, group: usize) -> Vec<Arc<Route>> {
        let current = self.state.current[group].as_ref();
        let cached = self.cache.networks.get(&self.cache_key).filter(|entry| {
            probes::now().saturating_sub(entry.at) < self.settings.mechanisms.cache_ttl_hours * 3600000
        });
        let mut routes = self
            .catalog
            .routes
            .iter()
            .filter(|route| {
                (if group == 0 { route.general } else { route.ai_allowed(&self.settings) && route.hops == 2 })
                    && !self.cooling(group, route)
                    && !self.exit_intel.get(&route.id).is_some_and(|trace| trace.china)
            })
            .cloned()
            .collect::<Vec<_>>();
        let key = |route: &Route| {
            rank(
                route,
                &self.settings,
                current,
                group,
                self.recovery[group] && !self.reselect,
            )
        };
        routes.sort_by_key(|route| {
            (
                usize::from(!self.reselect && current.is_some_and(|selected| selected.route.id != route.id)),
                key(route),
                cached.map_or(usize::MAX, |entry| {
                    entry
                        .selected
                        .iter()
                        .chain(&entry.successes)
                        .position(|id| *id == route.id)
                        .unwrap_or(usize::MAX)
                }),
            )
        });
        let mut balanced = Vec::with_capacity(routes.len());
        // Diversify both hops within a preference tier; failure has no monotonic ordering.
        let mut offset = 0;
        while offset < routes.len() {
            let tier = key(&routes[offset]);
            let end = (offset + 1..routes.len())
                .find(|index| key(&routes[*index]) != tier)
                .unwrap_or(routes.len());
            let mut fronts = HashSet::new();
            let mut rears = HashSet::new();
            let mut remaining = Vec::new();
            for route in &routes[offset..end] {
                if current.is_some_and(|selected| selected.route.id == route.id)
                    || (!fronts.contains(&route.ingress) && !rears.contains(&route.egress))
                {
                    fronts.insert(route.ingress.clone());
                    rears.insert(route.egress.clone());
                    balanced.push(Arc::clone(route));
                } else {
                    remaining.push(Arc::clone(route));
                }
            }
            balanced.extend(remaining);
            offset = end;
        }
        let prefix = self.settings.mechanisms.initial_batch.min(balanced.len());
        if balanced.len() > prefix {
            let tail_len = balanced.len() - prefix;
            balanced[prefix..].rotate_left(self.rotation[group] % tail_len);
        }
        balanced
    }

    fn begin_search(&mut self, optimize_general: bool) {
        self.round += 1;
        let mut order = if optimize_general || self.recovery[0] {
            self.ordered(0)
        } else {
            Vec::new()
        };
        let normal_target = self.settings.mechanisms.general_budget.min(order.len());
        let limit = if self.recovery[0] {
            self.settings.mechanisms.recovery_budget.min(order.len())
        } else {
            normal_target
        };
        order.truncate(limit);
        let ai_order: [VecDeque<Arc<Route>>; 3] = std::array::from_fn(|index| {
            if !self.settings.chain || !self.recovery[index + 1] {
                return VecDeque::new();
            }
            let mut routes = self.ordered(index + 1);
            routes.truncate(self.settings.mechanisms.recovery_budget);
            routes.into()
        });
        let stage = self.settings.mechanisms.initial_batch.min(order.len());
        self.state.fast_done = 0;
        self.state.fast_target = stage;
        self.state.retry_done = 0;
        self.state.retry_total = 0;
        self.state.ai_done = 0;
        self.state.ai_target = ai_order.iter().map(VecDeque::len).sum();
        self.state.next_retry_at = None;
        self.state.phase = "searching".to_owned();
        self.state.reason = if self.reselect {
            "manual_search"
        } else {
            "verify_candidates"
        }
        .to_owned();
        self.search = Some(Search {
            round: self.round,
            order: order.into(),
            ai_order,
            retries: VecDeque::new(),
            fast_busy: std::array::from_fn(|_| None),
            retry_busy: std::array::from_fn(|_| None),
            ai_busy: std::array::from_fn(|_| None),
            tried: 0,
            stage,
            normal_target,
            limit,
            ai_cursor: 0,
            started: Instant::now(),
        });
    }

    fn route_busy(&self, id: &str) -> bool {
        self.search.as_ref().is_some_and(|search| {
            search
                .fast_busy
                .iter()
                .chain(&search.retry_busy)
                .flatten()
                .any(|route| route == id)
                || search.ai_busy.iter().flatten().any(|(_, route)| route == id)
        }) || self.state.current.iter().enumerate().any(|(group, selected)| {
            self.monitor_busy[group] && selected.as_ref().is_some_and(|selected| selected.route.id == id)
        })
    }

    fn dispatch(&mut self) {
        let Some(mut search) = self.search.take() else {
            return;
        };
        if self.state.fast_done >= search.stage && search.tried >= search.stage {
            let target = if self.recovery[0] {
                search.limit
            } else {
                search.normal_target
            };
            search.stage = search
                .stage
                .saturating_mul(2)
                .max(self.settings.mechanisms.initial_batch)
                .min(target);
            self.state.fast_target = search.stage;
        }
        for lane in 0..self.settings.mechanisms.fast_concurrency {
            if search.fast_busy[lane].is_some() || search.tried >= search.stage {
                continue;
            }
            let Some(route) = search.order.pop_front() else {
                break;
            };
            search.tried += 1;
            search.fast_busy[lane] = Some(route.id.clone());
            spawn_general(
                &mut self.jobs,
                self.tx.clone(),
                search.round,
                route,
                Arc::clone(&self.settings),
                lane,
                false,
            );
        }
        for slot in 0..self.settings.mechanisms.retry_concurrency {
            if search.retry_busy[slot].is_some() {
                continue;
            }
            let Some(route) = search.retries.pop_front() else {
                break;
            };
            search.retry_busy[slot] = Some(route.id.clone());
            spawn_general(
                &mut self.jobs,
                self.tx.clone(),
                search.round,
                route,
                Arc::clone(&self.settings),
                8 + slot,
                true,
            );
        }
        for index in 0..3 {
            if !self.recovery[index + 1] {
                search.ai_order[index].clear();
            }
        }
        for slot in 0..self.settings.mechanisms.ai_concurrency {
            if search.ai_busy[slot].is_some() {
                continue;
            }
            let mut candidate = None;
            for _ in 0..3 {
                let index = search.ai_cursor % 3;
                search.ai_cursor += 1;
                if !self.recovery[index + 1] {
                    continue;
                }
                let waiting = search.ai_order[index].len();
                for _ in 0..waiting {
                    let Some(route) = search.ai_order[index].pop_front() else {
                        break;
                    };
                    if self.cooling(index + 1, &route) {
                        continue;
                    }
                    let busy = search
                        .fast_busy
                        .iter()
                        .chain(&search.retry_busy)
                        .flatten()
                        .any(|id| *id == route.id)
                        || search.ai_busy.iter().flatten().any(|(_, id)| *id == route.id)
                        || self.state.current.iter().enumerate().any(|(group, selection)| {
                            self.monitor_busy[group]
                                && selection
                                    .as_ref()
                                    .is_some_and(|selection| selection.route.id == route.id)
                        });
                    if busy {
                        search.ai_order[index].push_back(route);
                        continue;
                    }
                    candidate = Some((index + 1, route));
                    break;
                }
                if candidate.is_some() {
                    break;
                }
            }
            let Some((group, route)) = candidate else {
                continue;
            };
            let lane = 10 + slot;
            search.ai_busy[slot] = Some((group, route.id.clone()));
            let tx = self.tx.clone();
            let round = search.round;
            let settings = Arc::clone(&self.settings);
            let reuse = self
                .quality
                .get(&route.id)
                .filter(|quality| {
                    fresh(
                        quality.checked_at,
                        self.failed_at[group],
                        settings.mechanisms.retry_timeout_ms * 4,
                    )
                })
                .cloned();
            self.jobs.spawn(async move {
                let result = async {
                    probes::select(&format!("CVR-Probe-{lane}"), &route.id).await?;
                    let client = probes::client(settings.probe_base_port + lane as u16).await?;
                    let quality = match reuse {
                        Some(quality) => quality,
                        None => {
                            probes::general(
                                &client,
                                Duration::from_millis(settings.mechanisms.retry_timeout_ms),
                                false,
                            )
                            .await
                        }
                    };
                    let proof = if quality.available {
                        probes::ai(&client, group - 1, None, 0, false, settings.mechanisms.retry_timeout_ms).await
                    } else {
                        AiProof::default()
                    };
                    let trace = if quality.available {
                        probes::exit_trace(&client, settings.mechanisms.retry_timeout_ms).await
                    } else {
                        None
                    };
                    Ok::<_, anyhow::Error>((quality, proof, trace))
                };
                let (quality, mut proof, trace) = tokio::time::timeout(
                    Duration::from_millis(settings.mechanisms.retry_timeout_ms * 4 + 3000),
                    result,
                )
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or_default();
                // A China-mainland exit is a hard fail regardless of endpoint verdicts.
                if trace.as_ref().is_some_and(|trace| trace.china) {
                    proof.state = "failed".to_owned();
                    proof.challenged = false;
                }
                let _ = tx
                    .send(Event::Ai {
                        round,
                        group,
                        route,
                        quality,
                        proof,
                        trace,
                        lane,
                    })
                    .await;
            });
        }
        self.state.ai_target = self.state.ai_done
            + search.ai_order.iter().map(VecDeque::len).sum::<usize>()
            + search.ai_busy.iter().flatten().count();
        let idle = search.fast_busy.iter().chain(&search.retry_busy).all(Option::is_none)
            && search.ai_busy.iter().all(Option::is_none);
        let general_done = search.order.is_empty() || (!self.recovery[0] && search.tried >= search.normal_target);
        let ai_done = search.ai_order.iter().all(VecDeque::is_empty);
        if idle
            && search.retries.is_empty()
            && general_done
            && ai_done
            && self.switching.iter().all(|busy| !busy)
            && self.pending.iter().all(Vec::is_empty)
        {
            self.state.elapsed_ms = search.started.elapsed().as_millis() as u64;
            self.rotation[0] =
                self.rotation[0].saturating_add(search.tried.saturating_sub(self.settings.mechanisms.initial_batch));
            for group in 1..4 {
                if self.recovery[group] {
                    self.rotation[group] = self.rotation[group].saturating_add(
                        self.settings
                            .mechanisms
                            .recovery_budget
                            .saturating_sub(self.settings.mechanisms.initial_batch),
                    );
                }
            }
            let recovering = self.recovery.iter().any(|needed| *needed);
            self.state.phase = if recovering { "recovering" } else { "monitoring" }.to_owned();
            self.state.reason = if recovering { "retry_backoff" } else { "healthy_pinned" }.to_owned();
            let delay = backoff(&self.settings, self.retry_attempt);
            self.next_search = Instant::now() + Duration::from_secs(delay);
            if recovering {
                self.retry_attempt = self.retry_attempt.saturating_add(1);
            }
            self.reselect = false;
            self.persist();
        } else {
            self.search = Some(search);
        }
    }

    fn defer_optimization(&mut self, route: Arc<Route>, quality: Quality) {
        if self.recovery[0]
            || self.cooling(0, &route)
            || !fresh(
                quality.checked_at,
                self.failed_at[0],
                self.settings.mechanisms.retry_timeout_ms * 4,
            )
            || !self.validated[0]
            || self.refining
            || !quality.available
            || !self.improves(&route, &quality)
        {
            return;
        }
        let Some(current) = self.state.current[0].as_ref() else {
            return;
        };
        if current.route.id == route.id {
            return;
        }
        let key = (rank(&route, &self.settings, Some(current), 0, false), quality.millis);
        if self.deferred.as_ref().is_none_or(|(previous, previous_quality, _)| {
            key < (
                rank(previous, &self.settings, Some(current), 0, false),
                previous_quality.millis,
            )
        }) {
            self.deferred = Some((route, quality, current.route.id.clone()));
        }
    }

    fn refine(&mut self) {
        let Some((route, _, anchor)) = self.deferred.as_ref() else {
            return;
        };
        let Some(current) = self.state.current[0]
            .as_ref()
            .filter(|current| current.route.id == *anchor && !self.recovery[0])
        else {
            self.deferred = None;
            return;
        };
        if self.refining
            || self.monitor_busy[0]
            || self.switching[0]
            || self.route_busy(&route.id)
            || probes::now().saturating_sub(current.switched_at) < self.settings.mechanisms.switch_min_seconds * 1000
        {
            return;
        }
        let Some((route, _, anchor)) = self.deferred.take() else {
            return;
        };
        self.refining = true;
        let tx = self.tx.clone();
        let epoch = self.epoch();
        let settings = Arc::clone(&self.settings);
        self.jobs.spawn(async move {
            let result = async {
                probes::select("CVR-Probe-12", &route.id).await?;
                let client = probes::client(settings.probe_base_port + 12).await?;
                Ok::<_, anyhow::Error>(
                    probes::general(
                        &client,
                        Duration::from_millis(settings.mechanisms.retry_timeout_ms),
                        false,
                    )
                    .await,
                )
            };
            let quality = tokio::time::timeout(
                Duration::from_millis(settings.mechanisms.retry_timeout_ms + 3000),
                result,
            )
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
            let _ = tx
                .send(Event::Refined {
                    epoch,
                    anchor,
                    route,
                    quality,
                })
                .await;
        });
    }

    fn monitors(&mut self) {
        for group in 0..4 {
            let mirrored = self.state.current[group]
                .as_ref()
                .is_some_and(|selection| selection.mirrored);
            if !self.state.enabled_groups[group]
                || (!self.validated[group] && !mirrored)
                || (group == 0 && self.refining)
                || self.monitor_busy[group]
                || self.switching[group]
                || self.mirroring[group]
                || Instant::now() < self.next_monitor[group]
            {
                continue;
            }
            let Some(selected) = self.state.current[group].clone() else {
                continue;
            };
            if self.route_busy(&selected.route.id) {
                continue;
            }
            self.monitor_busy[group] = true;
            self.cycles[group] += 1;
            let cycle = self.cycles[group];
            let tx = self.tx.clone();
            let epoch = self.epoch();
            let lane = 12 + group;
            let settings = Arc::clone(&self.settings);
            self.jobs.spawn(async move {
                let result = async {
                    probes::select(&format!("CVR-Probe-{lane}"), &selected.route.id).await?;
                    let client = probes::client(settings.probe_base_port + lane as u16).await?;
                    if group == 0 {
                        Ok::<_, anyhow::Error>((
                            Some(
                                probes::general(
                                    &client,
                                    Duration::from_millis(settings.mechanisms.retry_timeout_ms),
                                    true,
                                )
                                .await,
                            ),
                            None,
                        ))
                    } else {
                        Ok((
                            None,
                            Some(
                                probes::ai(
                                    &client,
                                    group - 1,
                                    selected.ai.as_ref(),
                                    cycle,
                                    true,
                                    settings.mechanisms.retry_timeout_ms,
                                )
                                .await,
                            ),
                        ))
                    }
                };
                let (quality, ai) = tokio::time::timeout(
                    Duration::from_millis(settings.mechanisms.retry_timeout_ms * 3 + 3000),
                    result,
                )
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or_else(|| {
                    if group == 0 {
                        (
                            Some(Quality {
                                checked_at: probes::now(),
                                ..Quality::default()
                            }),
                            None,
                        )
                    } else {
                        (
                            None,
                            Some(AiProof {
                                state: "failed".to_owned(),
                                challenged: false,
                                checked_at: probes::now(),
                                ..AiProof::default()
                            }),
                        )
                    }
                });
                let _ = tx
                    .send(Event::Monitor {
                        epoch,
                        group,
                        route: selected.route.id.clone(),
                        quality,
                        ai,
                    })
                    .await;
            });
        }
    }

    fn receive(&mut self, event: Event) {
        if !self.valid() {
            return;
        }
        match event {
            Event::General {
                round,
                route,
                quality,
                retry,
                lane,
            } if round == self.round => {
                self.on_general(route, quality, retry, lane);
            }
            Event::Ai {
                round,
                group,
                route,
                quality,
                proof,
                trace,
                lane,
            } if round == self.round => {
                self.on_ai(group, route, quality, proof, trace, lane);
            }
            // Monitor ownership belongs to the network epoch, independently of search rounds.
            Event::Monitor {
                epoch,
                group,
                route,
                quality,
                ai,
            } if epoch == self.epoch() => {
                self.on_monitor(group, route, quality, ai);
            }
            Event::Mirrored {
                epoch,
                group,
                route,
                quality,
            } => {
                self.on_mirrored(epoch, group, route, quality);
            }
            Event::Activated {
                epoch,
                group,
                route,
                quality,
                outcome,
                exit,
                restored,
                acceptance,
            } => {
                self.on_activated(epoch, group, route, quality, outcome, exit, restored, acceptance);
            }
            Event::Refined {
                epoch,
                anchor,
                route,
                quality,
            } if epoch == self.epoch() => {
                self.on_refined(anchor, route, quality);
            }
            Event::ActivationFinished {
                group,
                restored,
                reason,
            } => {
                self.on_activation_finished(group, restored, reason);
            }
            Event::ProxyError { epoch, reason } if epoch == self.epoch() => {
                self.state.last_error = reason;
            }
            _ => {}
        }
    }

    fn on_general(&mut self, route: Arc<Route>, quality: Quality, retry: bool, lane: usize) {
        if let Some(search) = self.search.as_mut() {
            if retry {
                search.retry_busy[lane - 8] = None;
                self.state.retry_done += 1;
            } else {
                search.fast_busy[lane] = None;
                self.state.fast_done += 1;
            }
            if quality.timed_out && !retry {
                search.retries.push_back(Arc::clone(&route));
                self.state.retry_total += 1;
            }
        }
        if quality.available {
            self.remember(&route, &quality);
            if self.should_replace(0, &route, &quality, None) {
                self.queue_activation(0, route, quality, None);
            } else {
                self.defer_optimization(route, quality);
            }
        } else if !quality.timed_out || retry {
            self.quality.insert(route.id.clone(), quality);
            self.cooldown.entry(route.id.clone()).or_insert([0; 4])[0] =
                probes::now() + self.settings.mechanisms.failure_cooldown_seconds * 1000;
        }
    }

    fn on_ai(
        &mut self,
        group: usize,
        route: Arc<Route>,
        quality: Quality,
        proof: AiProof,
        trace: Option<probes::ExitTrace>,
        lane: usize,
    ) {
        if let Some(trace) = trace {
            self.exit_intel.insert(route.id.clone(), trace);
        }
        if let Some(search) = self.search.as_mut() {
            search.ai_busy[lane - 10] = None;
            self.state.ai_done += 1;
        }
        if !quality.available {
            self.quality.insert(route.id.clone(), quality);
            return;
        }
        self.remember(&route, &quality);
        if proof.failed() {
            self.cooldown.entry(route.id.clone()).or_insert([0; 4])[group] =
                probes::now() + self.settings.mechanisms.failure_cooldown_seconds * 1000;
        }
        if proof.usable() && self.should_replace(group, &route, &quality, Some(&proof)) {
            self.queue_activation(group, route, quality, Some(proof));
        }
    }

    fn on_monitor(&mut self, group: usize, route: String, quality: Option<Quality>, ai: Option<AiProof>) {
        self.monitor_busy[group] = false;
        let Some(selected) = self.state.current[group]
            .as_mut()
            .filter(|selected| selected.route.id == route)
        else {
            return;
        };
        // A persistent challenge is as unusable as a hard failure for the user;
        // it accumulates toward the threshold instead of failing in one shot.
        // When the critical API endpoint answers normally, the challenge is one
        // a real browser typically passes — manual testing confirmed such single
        // hops work — so it is tolerated instead of accumulating.
        let failed = quality.as_ref().is_some_and(|quality| !quality.available)
            || ai.as_ref().is_some_and(|proof| {
                proof.failed() || (proof.challenged && !proof.critical_available())
            });
        let checked_at = quality.as_ref().map_or_else(
            || ai.as_ref().map_or(0, |proof| proof.checked_at),
            |quality| quality.checked_at,
        );
        if checked_at < selected.switched_at {
            return;
        }
        selected.last_checked = checked_at;
        // A mirrored group cannot take a line of its own in single-hop mode, so its
        // failures stay informational there; in chain mode they drive the takeover search.
        selected.failures =
            if failed && (!selected.mirrored || self.settings.chain) { selected.failures.saturating_add(1) } else { 0 };
        if let Some(quality) = quality {
            selected.quality = quality;
        }
        if let Some(ai) = ai {
            selected.ai = Some(ai);
        }
        let interval = if failed {
            if group == 0 {
                self.settings.mechanisms.general_retry_seconds
            } else {
                self.settings.mechanisms.ai_retry_seconds
            }
        } else if group == 0 {
            self.settings.health_interval
        } else {
            self.settings.ai_health_interval
        };
        self.next_monitor[group] = Instant::now() + Duration::from_secs(interval);
        let threshold = if group == 0 {
            self.settings.mechanisms.general_failures
        } else {
            self.settings.mechanisms.ai_failures
        };
        if selected.failures >= threshold && !self.recovery[group] {
            self.confirm_failure(group, &route, checked_at);
        } else if !failed && checked_at > self.failed_at[group] && self.validated[group] {
            self.recovery[group] = false;
            if let Some(until) = self.cooldown.get_mut(route.as_str()) {
                until[group] = 0;
                if *until == [0; 4] {
                    self.cooldown.remove(route.as_str());
                }
            }
        }
    }

    /// A monitored route is confirmed dead: cool it down and merge recovery into
    /// the running search, or start one. An AI group additionally drops its dead
    /// line so it rides the general line while recovery searches.
    fn confirm_failure(&mut self, group: usize, route: &str, checked_at: u64) {
        self.failed_at[group] = checked_at;
        self.recovery[group] = true;
        self.pending[group].clear();
        if group == 0 {
            self.deferred = None;
        } else {
            self.state.current[group] = None;
            self.validated[group] = false;
        }
        self.cooldown.entry(route.to_owned()).or_insert([0; 4])[group] =
            checked_at + self.settings.mechanisms.failure_cooldown_seconds * 1000;
        self.retry_attempt = 0;
        if self.search.is_none() {
            self.begin_search(false);
        } else {
            self.merge_recovery(group);
        }
    }

    #[allow(clippy::too_many_arguments, reason = "fields map one-to-one onto the activation event")]
    fn on_activated(
        &mut self,
        epoch: u64,
        group: usize,
        route: Arc<Route>,
        quality: Quality,
        outcome: std::result::Result<Option<AiProof>, String>,
        exit: Option<probes::ExitTrace>,
        restored: bool,
        acceptance: Option<oneshot::Sender<bool>>,
    ) {
        if epoch != self.epoch() {
            if let Some(acceptance) = acceptance {
                let _ = acceptance.send(false);
            }
            return;
        }
        match outcome {
            Ok(proof) => {
                if !self.should_replace(group, &route, &quality, proof.as_ref()) {
                    if let Some(acceptance) = acceptance {
                        let _ = acceptance.send(false);
                    }
                    return;
                }
                if let Some(acceptance) = acceptance {
                    let _ = acceptance.send(true);
                }
                let previous = self.state.current[group].as_ref();
                let exit_since = previous
                    .filter(|current| current.route.egress == route.egress)
                    .map_or_else(probes::now, |current| current.exit_since);
                let exit = exit.or_else(|| self.exit_intel.get(&route.id).cloned());
                if let Some(trace) = &exit {
                    self.exit_intel.insert(route.id.clone(), trace.clone());
                }
                self.state.current[group] = Some(Selection {
                    route,
                    quality,
                    ai: proof,
                    failures: 0,
                    exit_since,
                    switched_at: probes::now(),
                    last_checked: probes::now(),
                    mirrored: false,
                    exit: exit.unwrap_or_default(),
                });
                self.recovery[group] = false;
                self.validated[group] = true;
                self.retry_attempt = 0;
                // A verified activation supersedes any earlier failure note;
                // keeping it would scare users while traffic flows fine.
                self.state.last_error.clear();
                self.next_monitor[group] = Instant::now()
                    + Duration::from_secs(if group == 0 {
                        self.settings.health_interval
                    } else {
                        self.settings.ai_health_interval
                    });
                if group == 0 {
                    self.on_general_activated(epoch);
                }
                self.publish();
                Handle::refresh_clash();
                self.persist();
            }
            Err(reason) => {
                self.state.last_error = reason;
                self.cooldown.entry(route.id.clone()).or_insert([0; 4])[group] =
                    probes::now() + self.settings.mechanisms.failure_cooldown_seconds * 1000;
                if !restored {
                    self.state.current[group] = None;
                    self.validated[group] = false;
                    self.recovery[group] = true;
                    if group == 0 {
                        VERIFIED.store(false, Ordering::Release);
                    }
                }
            }
        }
    }

    /// First verified general route of this engine run: record the milestone and
    /// hand the system proxy over to the verified line.
    fn on_general_activated(&mut self, epoch: u64) {
        self.deferred = None;
        let first = !VERIFIED.swap(true, Ordering::AcqRel);
        if self.state.first_route_ms.is_none() {
            self.state.first_route_ms = Some(self.began.elapsed().as_millis() as u64);
        }
        if !first {
            return;
        }
        let tx = self.tx.clone();
        let generation = self.generation;
        self.jobs.spawn(async move {
            let _write = Config::lock_config_write().await;
            let manager = CoreManager::global();
            let _lifecycle = manager.lifecycle_lock.lock().await;
            if generation == GENERATION.load(Ordering::Acquire) && verified()
                && let Err(error) = manager.apply_proxy_after_start().await
            {
                let _ = tx
                    .send(Event::ProxyError {
                        epoch,
                        reason: error.to_string(),
                    })
                    .await;
            }
        });
    }

    fn on_refined(&mut self, anchor: String, route: Arc<Route>, quality: Quality) {
        self.refining = false;
        if self.state.current[0]
            .as_ref()
            .is_some_and(|selected| selected.route.id == anchor)
            && self.should_replace(0, &route, &quality, None)
        {
            self.remember(&route, &quality);
            self.queue_activation(0, route, quality, None);
        }
    }

    fn on_activation_finished(&mut self, group: usize, restored: bool, reason: String) {
        self.switching[group] = false;
        if !restored {
            self.state.last_error = reason;
            self.state.current[group] = None;
            self.validated[group] = false;
            self.recovery[group] = true;
            if group == 0 {
                VERIFIED.store(false, Ordering::Release);
            }
        }
    }

    fn merge_recovery(&mut self, group: usize) {
        let routes = self.ordered(group);
        let Some(search) = self.search.as_mut() else {
            return;
        };
        if group == 0 {
            search.order = routes
                .into_iter()
                .take(self.settings.mechanisms.recovery_budget)
                .collect();
            search.limit = search.tried + search.order.len();
            search.stage = search.tried + self.settings.mechanisms.initial_batch.min(search.order.len());
            self.state.fast_target = search.stage;
        } else {
            search.ai_order[group - 1] = routes
                .into_iter()
                .take(self.settings.mechanisms.recovery_budget)
                .collect();
        }
    }

    fn remember(&mut self, route: &Route, quality: &Quality) {
        self.quality.insert(route.id.clone(), quality.clone());
        if !self.successes.contains(&route.id) && self.successes.len() < 64 {
            self.successes.push(route.id.clone());
        }
    }

    fn should_replace(&self, group: usize, route: &Route, quality: &Quality, proof: Option<&AiProof>) -> bool {
        let window = self.settings.mechanisms.retry_timeout_ms * 4;
        if !self.state.enabled_groups[group]
            || !quality.available
            || self.cooling(group, route)
            || self.exit_intel.get(&route.id).is_some_and(|trace| trace.china)
            || !fresh(quality.checked_at, self.failed_at[group], window)
        {
            return false;
        }
        if group > 0
            && (!route.ai_allowed(&self.settings)
                // AI only takes a line of its own in chain mode, and only a verified
                // two-hop chain; single-hop configurations keep following the general line.
                || !self.settings.chain
                || route.hops != 2
                || !proof.is_some_and(|proof| proof.usable() && fresh(proof.checked_at, self.failed_at[group], window)))
        {
            return false;
        }
        if group > 0
            && self.state.current[group].is_some()
            // A merely usable (challenge-pending) candidate never displaces an
            // existing line — browsers may clear the challenge but non-browser
            // clients like Codex cannot. Only a fully verified line upgrades;
            // a failed group has no current line and accepts any usable one.
            && !proof.is_some_and(|proof| proof.confirmed())
        {
            return false;
        }
        let Some(current) = self.state.current[group].as_ref() else {
            return true;
        };
        if current.route.id == route.id {
            return self.recovery[group] || !self.validated[group];
        }
        if group > 0 && !self.recovery[group] {
            return false;
        }
        if self.recovery[group] {
            return true;
        }
        let mechanisms = &self.settings.mechanisms;
        if probes::now().saturating_sub(current.switched_at) < mechanisms.switch_min_seconds * 1000 {
            return false;
        }
        self.improves(route, quality)
    }

    fn improves(&self, route: &Route, quality: &Quality) -> bool {
        let Some(current) = self.state.current[0].as_ref() else {
            return false;
        };
        let mechanisms = &self.settings.mechanisms;
        let previous = rank(&current.route, &self.settings, Some(current), 0, false);
        let next = rank(route, &self.settings, Some(current), 0, false);
        next < previous
            || (next == previous
                && quality.millis.saturating_mul(100)
                    <= current
                        .quality
                        .millis
                        .saturating_mul(100 - mechanisms.improvement_percent)
                && current.quality.millis.saturating_sub(quality.millis) >= mechanisms.improvement_ms
                && quality.millis < current.quality.millis)
    }

    fn queue_activation(&mut self, group: usize, route: Arc<Route>, quality: Quality, proof: Option<AiProof>) {
        if self.pending[group].iter().any(|candidate| candidate.0.id == route.id) {
            return;
        }
        self.pending[group].push((route, quality, proof));
    }

    fn dispatch_activations(&mut self) {
        for group in 0..4 {
            if self.switching[group] {
                continue;
            }
            let mut candidates = std::mem::take(&mut self.pending[group]);
            if group == 0 {
                for (route, quality, _) in &candidates {
                    if !self.should_replace(0, route, quality, None) {
                        self.defer_optimization(Arc::clone(route), quality.clone());
                    }
                }
            }
            candidates.retain(|(route, quality, proof)| self.should_replace(group, route, quality, proof.as_ref()));
            candidates.sort_by_key(|(route, quality, proof)| {
                (
                    rank(
                        route,
                        &self.settings,
                        self.state.current[group].as_ref(),
                        group,
                        self.recovery[group] && !self.reselect,
                    ),
                    // Fully verified AI lines outrank challenge-pending ones.
                    candidate_penalty(group, proof.as_ref()),
                    quality.millis,
                )
            });
            let Some((route, mut quality, _proof)) = candidates.first().cloned() else {
                continue;
            };
            let best_rank = rank(
                &route,
                &self.settings,
                self.state.current[group].as_ref(),
                group,
                self.recovery[group] && !self.reselect,
            );
            let better_running = group > 0
                && self.search.as_ref().is_some_and(|search| {
                    search
                        .ai_busy
                        .iter()
                        .flatten()
                        .filter(|(candidate_group, _)| *candidate_group == group)
                        .any(|(_, id)| {
                            self.catalog
                                .routes
                                .iter()
                                .find(|route| route.id == *id)
                                .is_some_and(|route| {
                                    rank(
                                        route,
                                        &self.settings,
                                        self.state.current[group].as_ref(),
                                        group,
                                        self.recovery[group] && !self.reselect,
                                    ) < best_rank
                                })
                        })
                });
            if better_running {
                self.pending[group] = candidates;
                continue;
            }
            candidates.remove(0);
            self.pending[group] = candidates;
            self.switching[group] = true;
            let rollback = self.state.current[group]
                .as_ref()
                .map_or_else(|| "REJECT".to_owned(), |current| current.route.id.clone());
            let epoch = self.epoch();
            let epoch_owner = Arc::clone(&self.epoch);
            let generation = self.generation;
            let tx = self.tx.clone();
            let timeout = self.settings.mechanisms.retry_timeout_ms;
            self.activations.spawn(async move {
                let _selection = ACTIVATION_LOCKS[group].lock().await;
                let valid =
                    || epoch == epoch_owner.load(Ordering::Acquire) && generation == GENERATION.load(Ordering::Acquire);
                let mut exit: Option<probes::ExitTrace> = None;
                let outcome = async {
                    if !valid() {
                        anyhow::bail!("Routing context changed");
                    }
                    probes::select(GROUPS[group], &route.id).await?;
                    let port = crate::config::MixedPort::desired().await;
                    let client = probes::client(port).await?;
                    let public_proof = if group == 0 {
                        if !probes::public_transport(&client, timeout).await {
                            anyhow::bail!("Public proxy port verification failed");
                        }
                        let traced = probes::exit_trace(&client, timeout).await;
                        if traced.as_ref().is_some_and(|trace| trace.china) {
                            anyhow::bail!("Candidate exit resolved to a China-mainland address");
                        }
                        exit = traced;
                        None
                    } else {
                        let checked = probes::public_ai(&client, group - 1, timeout).await;
                        if !checked.usable() {
                            anyhow::bail!("AI route verification failed");
                        }
                        Some(checked)
                    };
                    if !valid() {
                        anyhow::bail!("Routing context changed during verification");
                    }
                    quality.checked_at = probes::now();
                    Ok::<_, anyhow::Error>(public_proof)
                }
                .await;
                let mut outcome = outcome.map_err(|error| error.to_string());
                let mut restored = true;
                if outcome.is_err()
                    && generation == GENERATION.load(Ordering::Acquire)
                    && let Err(error) = probes::select(GROUPS[group], &rollback).await
                {
                    restored = false;
                    if let Err(reason) = &mut outcome {
                        *reason = format!("{reason}; previous route could not be restored: {error}");
                    }
                }
                let succeeded = outcome.is_ok();
                let mut reason = outcome.as_ref().err().cloned().unwrap_or_default();
                let (acceptance, accepted) = oneshot::channel();
                let sent = tx
                    .send(Event::Activated {
                        epoch,
                        group,
                        route,
                        quality,
                        outcome,
                        exit,
                        restored,
                        acceptance: succeeded.then_some(acceptance),
                    })
                    .await
                    .is_ok();
                if succeeded {
                    let accepted = sent && accepted.await.unwrap_or(false);
                    if !accepted
                        && generation == GENERATION.load(Ordering::Acquire)
                        && let Err(error) = probes::select(GROUPS[group], &rollback).await
                    {
                        restored = false;
                        reason = format!("Previous route could not be restored: {error}");
                    }
                }
                let _ = tx
                    .send(Event::ActivationFinished {
                        group,
                        restored,
                        reason,
                    })
                    .await;
            });
        }
    }

    fn observe_network(&mut self, next: Option<String>) {
        let current = if self.network_available {
            Some(self.state.network.clone())
        } else {
            None
        };
        if next == current {
            self.network_candidate = None;
            self.state.network_pending = false;
            return;
        }
        match &self.network_candidate {
            Some((candidate, since))
                if *candidate == next
                    && since.elapsed() >= Duration::from_secs(self.settings.mechanisms.network_debounce_seconds) => {}
            Some((candidate, _)) if *candidate == next => return,
            _ => {
                self.network_candidate = Some((next, Instant::now()));
                self.state.network_pending = true;
                return;
            }
        }
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.round += 1;
        self.jobs.abort_all();
        self.search = None;
        self.monitor_busy = [false; 4];
        self.mirroring = [false; 4];
        self.deferred = None;
        self.refining = false;
        self.pending.iter_mut().for_each(Vec::clear);
        self.network_available = next.is_some();
        self.network_candidate = None;
        self.state.network_pending = false;
        self.state.network = next.unwrap_or_default();
        self.cache_key = cache_key(&self.state.network, &self.settings);
        self.recovery =
            std::array::from_fn(|group| self.state.enabled_groups[group] && (group == 0 || self.settings.chain));
        self.validated = [false; 4];
        self.failed_at = [probes::now(); 4];
        self.cooldown.clear();
        self.quality.clear();
        self.successes.clear();
        self.rotation = [0; 4];
        self.retry_attempt = 0;
        self.next_monitor = [Instant::now(); 4];
        self.reselect = false;
        for selected in self.state.current.iter_mut().flatten() {
            selected.failures = 0;
        }
        VERIFIED.store(false, Ordering::Release);
        self.began = Instant::now();
        self.state.first_route_ms = None;
        if self.network_available {
            self.begin_search(true);
        } else {
            self.state.phase = "waiting_network".to_owned();
            self.state.reason = "network_unavailable".to_owned();
        }
    }

    fn cache_snapshot(&mut self) -> Cache {
        let entry = CacheEntry {
            at: probes::now(),
            selected: self
                .state
                .current
                .iter()
                .enumerate()
                .filter(|(group, _)| self.validated[*group] && !self.recovery[*group])
                .filter_map(|(_, selected)| selected.as_ref().map(|selected| selected.route.id.clone()))
                .collect(),
            successes: self.successes.clone(),
        };
        self.cache.networks.insert(self.cache_key.clone(), entry);
        self.cache.networks.retain(|_, entry| {
            probes::now().saturating_sub(entry.at) < self.settings.mechanisms.cache_ttl_hours * 3600000
        });
        while self.cache.networks.len() > self.settings.mechanisms.cache_network_limit {
            let oldest = self
                .cache
                .networks
                .iter()
                .min_by_key(|(_, entry)| entry.at)
                .map(|(key, _)| key.clone());
            if let Some(key) = oldest {
                self.cache.networks.remove(&key);
            } else {
                break;
            }
        }
        Cache {
            networks: self.cache.networks.clone(),
        }
    }

    fn persist(&mut self) {
        let cache = self.cache_snapshot();
        let generation = self.generation;
        let revision = CACHE_REVISION.fetch_add(1, Ordering::AcqRel) + 1;
        self.jobs.spawn(async move {
            if let Err(error) = persist_cache(cache, generation, revision).await {
                tracing::warn!("Routing cache could not be saved: {error}");
            }
        });
    }

    fn publish(&mut self) {
        let now = probes::now();
        let instant = Instant::now();
        self.cooldown.retain(|_, until| until.iter().any(|at| *at > now));
        self.quality
            .retain(|_, quality| fresh(quality.checked_at, 0, self.settings.mechanisms.retry_timeout_ms * 4));
        self.state.active_probes = self.search.as_ref().map_or(0, |search| {
            search.fast_busy.iter().chain(&search.retry_busy).flatten().count()
                + search.ai_busy.iter().flatten().count()
        }) + self.monitor_busy.iter().filter(|busy| **busy).count()
            + self.switching.iter().filter(|busy| **busy).count()
            + usize::from(self.refining);
        self.state.checking = self.monitor_busy;
        self.state.checking[0] |= self.refining;
        self.state.next_optimization_at = self.deferred.as_ref().and_then(|_| {
            self.state.current[0]
                .as_ref()
                .map(|current| current.switched_at + self.settings.mechanisms.switch_min_seconds * 1000)
        });
        self.state.recovering = self.recovery;
        self.state.next_checks = std::array::from_fn(|group| {
            if self.state.enabled_groups[group] && self.state.current[group].is_some() {
                now + self.next_monitor[group].saturating_duration_since(instant).as_millis() as u64
            } else {
                0
            }
        });
        self.state.next_retry_at =
            if self.network_available && self.search.is_none() && self.recovery.iter().any(|needed| *needed) {
                Some(now + self.next_search.saturating_duration_since(instant).as_millis() as u64)
            } else {
                None
            };
        if !self.network_available {
            self.state.phase = "waiting_network".to_owned();
            self.state.reason = "network_unavailable".to_owned();
        }
        if self.search.is_some() {
            self.state.elapsed_ms = self.began.elapsed().as_millis() as u64;
        }
        if self.search.is_none() && self.network_available && !self.recovery.iter().any(|needed| *needed) {
            self.state.phase = "monitoring".to_owned();
            self.state.reason = "healthy_pinned".to_owned();
        }
        if self.search.is_none()
            && (self.deferred.is_some() || self.refining)
            && self.network_available
            && !self.recovery.iter().any(|needed| *needed)
        {
            self.state.phase = "optimizing".to_owned();
            self.state.reason = "wait_hold".to_owned();
        }
        if self.network_available && self.state.current[0].is_some() && !self.validated[0] {
            self.state.reason = "verifying_cached".to_owned();
        }
        // The verified general line already carries traffic; background AI search must
        // not keep the whole status looking like "still finding routes".
        if self.network_available
            && self.validated[0]
            && !self.recovery[0]
            && self.state.current[0].is_some()
        {
            self.state.phase = "monitoring".to_owned();
            self.state.reason = if self.search.is_some() {
                "verifying_background"
            } else if self.deferred.is_some() || self.refining {
                "wait_hold"
            } else {
                "healthy_pinned"
            }
            .to_owned();
        }
        SNAPSHOT.store(Arc::new(self.state.clone()));
    }
}

fn fresh(checked_at: u64, failed_at: u64, window: u64) -> bool {
    checked_at > failed_at && probes::now().saturating_sub(checked_at) <= window
}

fn spawn_general(
    jobs: &mut JoinSet<()>,
    tx: mpsc::Sender<Event>,
    round: u64,
    route: Arc<Route>,
    settings: Arc<RoutingConfig>,
    lane: usize,
    retry: bool,
) {
    jobs.spawn(async move {
        let deadline = Duration::from_millis(if retry {
            settings.mechanisms.retry_timeout_ms
        } else {
            settings.mechanisms.fast_timeout_ms
        });
        let started = Instant::now();
        let measured = tokio::time::timeout(deadline, async {
            probes::select(&format!("CVR-Probe-{lane}"), &route.id).await?;
            let client = probes::client(settings.probe_base_port + lane as u16).await?;
            Ok::<_, anyhow::Error>(probes::general(&client, deadline, false).await)
        })
        .await;
        let quality = measured.ok().and_then(Result::ok).unwrap_or_else(|| Quality {
            checked_at: probes::now(),
            timed_out: started.elapsed() >= deadline,
            ..Quality::default()
        });
        let _ = tx
            .send(Event::General {
                round,
                route,
                quality,
                retry,
                lane,
            })
            .await;
    });
}

async fn persist_cache(cache: Cache, generation: u64, revision: u64) -> Result<()> {
    static WRITE: tokio::sync::Mutex<u64> = tokio::sync::Mutex::const_new(0);
    {
        // The guard must span the write so revisions land on disk in order.
        let mut written = WRITE.lock().await;
        if generation != GENERATION.load(Ordering::Acquire) || revision <= *written {
            return Ok(());
        }
        help::save_yaml(
            &dirs::app_home_dir()?.join("routing-cache.yaml"),
            &cache,
            Some("# Cached candidates require fresh network verification"),
        )
        .await?;
        *written = revision;
    }
    Ok(())
}

fn cache_key(network: &str, settings: &RoutingConfig) -> String {
    catalog::digest(
        format!(
            "{network}:{}",
            serde_json::to_string(&(
                settings.chain,
                settings.allow_direct_fallback,
                &settings.profile_roles,
                &settings.node_metadata,
                &settings.residential_profiles,
                &settings.ai_excluded_countries
            ))
            .unwrap_or_default()
        )
        .as_bytes(),
    )
}

const GATEWAY_SAMPLE_TTL: Duration = Duration::from_secs(5);

struct GatewaySample {
    interfaces: String,
    value: Option<String>,
    at: Option<Instant>,
}

fn gateway_sample_expired(at: Option<Instant>, interfaces_changed: bool) -> bool {
    interfaces_changed
        || at.is_none_or(|at| Instant::now().duration_since(at) >= GATEWAY_SAMPLE_TTL)
}

fn default_gateway() -> Option<String> {
    #[cfg(target_os = "macos")]
    return std::process::Command::new("/sbin/route")
        .args(["-n", "get", "default"])
        .output()
        .ok()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| line.contains("gateway:") || line.contains("interface:"))
                .map(str::trim)
                .collect::<Vec<_>>()
                .join(";")
        });
    #[cfg(target_os = "windows")]
    return std::process::Command::new("route.exe")
        .args(["print", "-4"])
        .output()
        .ok()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| line.trim_start().starts_with("0.0.0.0"))
                .map(|line| line.split_whitespace().take(4).collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>()
                .join(";")
        });
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    std::fs::read_to_string("/proc/net/route").ok()
}

/// Reuse the previous gateway answer while interfaces are unchanged and the sample
/// is fresh, so the monitor loop does not fork a route query every second.
fn sampled_gateway(interfaces: &str) -> Option<String> {
    static GATEWAY: std::sync::Mutex<GatewaySample> = std::sync::Mutex::new(GatewaySample {
        interfaces: String::new(),
        value: None,
        at: None,
    });
    let mut sample = GATEWAY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if gateway_sample_expired(sample.at, sample.interfaces != interfaces) {
        sample.interfaces = interfaces.to_owned();
        sample.value = default_gateway();
        sample.at = Some(Instant::now());
    }
    sample.value.clone()
}

async fn network() -> Result<Option<String>> {
    tokio::task::spawn_blocking(|| {
        let mut interfaces = NetworkInterface::show()?
            .into_iter()
            .filter(|interface| {
                !interface.internal
                    && !interface.addr.is_empty()
                    && !["lo", "utun", "tun", "docker", "veth"]
                        .iter()
                        .any(|prefix| interface.name.starts_with(prefix))
            })
            .map(|interface| format!("{}:{:?}", interface.name, interface.addr))
            .collect::<Vec<_>>();
        if interfaces.is_empty() {
            return Ok(None);
        }
        interfaces.sort();
        let interfaces = format!("{interfaces:?}");
        let gateway = sampled_gateway(&interfaces);
        Ok(Some(catalog::digest(
            format!("{interfaces}:{gateway:?}").as_bytes(),
        )))
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::Mapping;

    fn route(id: &str, front: &str, rear: &str) -> Arc<Route> {
        Arc::new(Route {
            id: id.to_owned(),
            ingress: front.to_owned(),
            egress: rear.to_owned(),
            ingress_name: front.to_owned(),
            egress_name: rear.to_owned(),
            ingress_country: "JP".to_owned(),
            country: "JP".to_owned(),
            residential: true,
            rear_subscription: true,
            hops: 2,
            general: true,
            proxy: Mapping::new(),
        })
    }

    fn selection(route: Arc<Route>, millis: u64) -> Selection {
        Selection {
            route,
            quality: Quality {
                available: true,
                millis,
                checked_at: probes::now(),
                timed_out: false,
            },
            ai: Some(AiProof {
                state: "available".to_owned(),
                challenged: false,
                checked_at: probes::now(),
                endpoints: Vec::new(),
            }),
            failures: 0,
            exit_since: probes::now() - 120000,
            switched_at: probes::now() - 120000,
            last_checked: probes::now(),
            mirrored: false,
            exit: Default::default(),
        }
    }

    fn engine() -> Engine {
        let settings = Arc::new(RoutingConfig {
            chain: true,
            ..RoutingConfig::default()
        });
        let (tx, _) = mpsc::channel(64);
        let selected = selection(route("selected", "front", "rear"), 300);
        let state = RoutingStatus {
            enabled: true,
            enabled_groups: [true; 4],
            current: std::array::from_fn(|_| Some(selected.clone())),
            ..RoutingStatus::default()
        };
        Engine {
            generation: GENERATION.load(Ordering::Acquire),
            epoch: Arc::new(AtomicU64::new(1)),
            settings,
            catalog: Catalog {
                nodes: Vec::new(),
                routes: vec![Arc::clone(&selected.route)],
            },
            state,
            cache: Cache::default(),
            cache_key: "test".to_owned(),
            jobs: JoinSet::new(),
            activations: JoinSet::new(),
            tx,
            search: None,
            monitor_busy: [false; 4],
            mirroring: [false; 4],
            next_monitor: [Instant::now(); 4],
            cycles: [0; 4],
            recovery: [false; 4],
            validated: [true; 4],
            failed_at: [0; 4],
            cooldown: HashMap::new(),
            exit_intel: HashMap::new(),
            quality: HashMap::new(),
            successes: Vec::new(),
            rotation: [0; 4],
            round: 0,
            began: Instant::now(),
            next_search: Instant::now(),
            retry_attempt: 0,
            runtime_identity: 0,
            switching: [false; 4],
            pending: std::array::from_fn(|_| Vec::new()),
            deferred: None,
            refining: false,
            network_available: true,
            network_candidate: None,
            reselect: false,
        }
    }

    #[test]
    fn confirmed_general_failure_searches_for_a_verified_replacement() {
        let mut engine = engine();
        let replacement = route("replacement", "other-front", "other-rear");
        engine.catalog.routes.push(Arc::clone(&replacement));
        for failure in 1..=2 {
            engine.receive(Event::Monitor {
                epoch: 1,
                group: 0,
                route: "selected".to_owned(),
                quality: Some(Quality {
                    available: false,
                    millis: 0,
                    checked_at: probes::now(),
                    timed_out: true,
                }),
                ai: None,
            });
            assert_eq!(engine.recovery[0], failure == 2);
            assert_eq!(engine.search.is_some(), failure == 2);
        }
        assert!(
            engine
                .search
                .as_ref()
                .unwrap()
                .order
                .iter()
                .any(|route| route.id == replacement.id)
        );
        assert_eq!(engine.state.current[0].as_ref().unwrap().route.id, "selected");
        assert!(engine.should_replace(
            0,
            &replacement,
            &Quality {
                available: true,
                millis: 400,
                checked_at: engine.failed_at[0] + 1,
                timed_out: false,
            },
            None
        ));
    }

    #[test]
    fn search_round_does_not_discard_current_monitor() {
        let mut engine = engine();
        engine.monitor_busy[1] = true;
        engine.begin_search(false);
        engine.receive(Event::Monitor {
            epoch: 1,
            group: 1,
            route: "selected".to_owned(),
            quality: None,
            ai: Some(AiProof {
                state: "failed".to_owned(),
                challenged: false,
                checked_at: probes::now(),
                ..AiProof::default()
            }),
        });
        assert!(!engine.monitor_busy[1]);
        assert_eq!(engine.state.current[1].as_ref().unwrap().failures, 1);
    }

    #[test]
    fn stale_network_monitor_cannot_release_new_monitor() {
        let mut engine = engine();
        engine.monitor_busy[1] = true;
        engine.epoch.store(2, Ordering::Release);
        engine.receive(Event::Monitor {
            epoch: 1,
            group: 1,
            route: "selected".to_owned(),
            quality: None,
            ai: Some(AiProof {
                state: "failed".to_owned(),
                challenged: false,
                checked_at: probes::now(),
                ..AiProof::default()
            }),
        });
        assert!(engine.monitor_busy[1]);
        assert_eq!(engine.state.current[1].as_ref().unwrap().failures, 0);
    }

    #[test]
    fn ai_only_recovery_does_not_restart_general_search() {
        let mut engine = engine();
        engine.recovery[2] = true;
        engine.begin_search(false);
        let search = engine.search.as_ref().unwrap();
        assert!(search.order.is_empty());
        assert!(search.ai_order[0].is_empty());
        assert!(!search.ai_order[1].is_empty());
        assert!(search.ai_order[2].is_empty());
    }

    #[test]
    fn healthy_ai_stays_pinned_and_recovery_requires_new_evidence() {
        let mut engine = engine();
        let replacement = route("replacement", "other-front", "other-rear");
        let mut quality = Quality {
            available: true,
            millis: 1,
            checked_at: probes::now(),
            timed_out: false,
        };
        let mut proof = AiProof {
            state: "available".to_owned(),
            challenged: false,
            checked_at: quality.checked_at,
            endpoints: Vec::new(),
        };
        assert!(!engine.should_replace(1, &replacement, &quality, Some(&proof)));
        engine.recovery[1] = true;
        engine.failed_at[1] = probes::now();
        assert!(!engine.should_replace(1, &replacement, &quality, Some(&proof)));
        quality.checked_at = engine.failed_at[1] + 1;
        proof.checked_at = quality.checked_at;
        assert!(engine.should_replace(1, &replacement, &quality, Some(&proof)));
    }

    #[test]
    fn general_optimization_respects_hold_and_both_improvement_thresholds() {
        let mut engine = engine();
        let replacement = route("replacement", "other-front", "other-rear");
        let mut quality = Quality {
            available: true,
            millis: 240,
            checked_at: probes::now(),
            timed_out: false,
        };
        engine.state.current[0].as_mut().unwrap().switched_at = probes::now();
        assert!(!engine.should_replace(0, &replacement, &quality, None));
        engine.state.current[0].as_mut().unwrap().switched_at -= 120000;
        assert!(engine.should_replace(0, &replacement, &quality, None));
        quality.millis = 245;
        assert!(!engine.should_replace(0, &replacement, &quality, None));
        engine.state.current[0].as_mut().unwrap().quality.millis = 100;
        quality.millis = 75;
        assert!(!engine.should_replace(0, &replacement, &quality, None));
    }

    #[test]
    fn recovery_prefers_another_front_for_the_same_exit() {
        let mut engine = engine();
        engine.recovery[1] = true;
        let same_exit = route("same-exit", "other-front", "rear");
        let another_exit = route("preferred-exit", "front", "preferred-rear");
        Arc::make_mut(&mut engine.settings)
            .ai_priorities
            .insert("OPENAI".to_owned(), vec!["preferred-rear".to_owned()]);
        engine.catalog.routes = vec![another_exit, Arc::clone(&same_exit)];
        assert_eq!(engine.ordered(1)[0].id, same_exit.id);
    }

    #[test]
    fn single_hop_fallback_cannot_outrank_a_chain() {
        let mut engine = engine();
        Arc::make_mut(&mut engine.settings).allow_direct_fallback = true;
        let mut chain = route("chain", "front", "rear");
        Arc::make_mut(&mut chain).ingress_country = "US".to_owned();
        let mut single = route("single", "single-rear", "single-rear");
        Arc::make_mut(&mut single).hops = 1;
        let current = engine.state.current[1].as_ref();
        for group in 0..4 {
            assert!(
                rank(&chain, &engine.settings, current, group, false)
                    < rank(&single, &engine.settings, current, group, false)
            );
        }
        engine.state.current = std::array::from_fn(|_| None);
        engine.catalog.routes = vec![single, Arc::clone(&chain)];
        assert_eq!(engine.ordered(0)[0].id, chain.id);
    }

    #[test]
    fn repeated_bounded_rounds_cover_the_candidate_tail() {
        let mut engine = engine();
        let mechanisms = &mut Arc::make_mut(&mut engine.settings).mechanisms;
        mechanisms.initial_batch = 2;
        mechanisms.general_budget = 2;
        mechanisms.recovery_budget = 4;
        engine.recovery[0] = true;
        engine.state.current = std::array::from_fn(|_| None);
        engine.catalog.routes = (0..12)
            .map(|index| {
                route(
                    &format!("route-{index}"),
                    &format!("front-{index}"),
                    &format!("rear-{index}"),
                )
            })
            .collect();
        let mut seen = HashSet::new();
        for rotation in [0, 2, 4, 6, 8] {
            engine.rotation[0] = rotation;
            engine.begin_search(false);
            let search = engine.search.as_ref().unwrap();
            assert_eq!(search.order.len(), 4);
            seen.extend(search.order.iter().map(|route| route.id.clone()));
        }
        assert_eq!(seen.len(), 12);
    }

    #[test]
    fn network_change_preserves_exit_and_resets_failure_state_after_debounce() {
        let mut engine = engine();
        engine.state.network = "old".to_owned();
        engine.state.current[1].as_mut().unwrap().failures = 2;
        let exit_since = engine.state.current[1].as_ref().unwrap().exit_since;
        engine.observe_network(Some("new".to_owned()));
        assert_eq!(engine.state.network, "old");
        engine.network_candidate = Some((Some("new".to_owned()), Instant::now() - Duration::from_secs(4)));
        engine.observe_network(Some("new".to_owned()));
        assert_eq!(engine.state.network, "new");
        assert_eq!(engine.state.current[1].as_ref().unwrap().failures, 0);
        assert_eq!(engine.state.current[1].as_ref().unwrap().exit_since, exit_since);
        assert_eq!(engine.validated, [false; 4]);
    }

    #[test]
    fn retry_backoff_is_bounded_and_folder_changes_keep_network_cache() {
        let mut settings = RoutingConfig::default();
        assert_eq!(
            [0, 1, 2, 3, 9].map(|attempt| backoff(&settings, attempt)),
            [15, 30, 60, 120, 120]
        );
        let previous = cache_key("network", &settings);
        settings
            .profile_folders
            .insert("subscription".to_owned(), "folder".to_owned());
        assert_eq!(cache_key("network", &settings), previous);
    }
    #[test]
    fn deferred_optimization_retains_only_the_best_candidate_during_hold() {
        let mut engine = engine();
        engine.state.current[0].as_mut().unwrap().switched_at = probes::now();
        let first = route("better", "another-front", "another-rear");
        let best = route("best", "another-front", "another-rear");
        let proof = |millis| Quality {
            available: true,
            millis,
            checked_at: probes::now(),
            timed_out: false,
        };
        engine.defer_optimization(Arc::clone(&first), proof(150));
        engine.defer_optimization(Arc::clone(&best), proof(100));
        engine.defer_optimization(first, proof(200));
        assert_eq!(engine.deferred.as_ref().unwrap().0.id, best.id);
        assert!(engine.pending[0].is_empty());
    }

    #[test]
    fn stale_activation_is_rejected_until_the_old_selection_is_restored() {
        let mut engine = engine();
        engine.switching[1] = true;
        let (acceptance, accepted) = oneshot::channel();
        engine.receive(Event::Activated {
            epoch: 0,
            group: 1,
            route: route("stale", "front", "other-rear"),
            quality: Quality::default(),
            outcome: Ok(Some(AiProof::default())),
            exit: None,
            restored: true,
            acceptance: Some(acceptance),
        });
        assert_eq!(accepted.blocking_recv().unwrap(), false);
        assert_eq!(engine.state.current[1].as_ref().unwrap().route.id, "selected");
        assert!(engine.switching[1]);
        engine.receive(Event::ActivationFinished {
            group: 1,
            restored: true,
            reason: String::new(),
        });
        assert!(!engine.switching[1]);
    }

    #[test]
    fn failure_cooldown_is_scoped_per_group() {
        let mut engine = engine();
        engine
            .cooldown
            .entry("selected".to_owned())
            .or_insert([0; 4])[1] = probes::now() + 60_000;
        let selected = Arc::clone(&engine.catalog.routes[0]);
        assert!(engine.cooling(1, &selected));
        assert!(!engine.cooling(0, &selected));
        assert!(!engine.cooling(2, &selected));
    }

    #[test]
    fn gateway_sampling_reuses_fresh_results_until_interfaces_change() {
        assert!(gateway_sample_expired(None, false));
        assert!(!gateway_sample_expired(Some(Instant::now()), false));
        assert!(gateway_sample_expired(
            Some(Instant::now() - GATEWAY_SAMPLE_TTL),
            false
        ));
        assert!(gateway_sample_expired(Some(Instant::now()), true));
    }

    #[test]
    fn fresh_cache_pins_only_the_general_line() {
        let settings = RoutingConfig::default();
        let selected = route("selected", "front", "rear");
        let catalog = Catalog {
            nodes: Vec::new(),
            routes: vec![selected],
        };
        let mut cache = Cache::default();
        cache.networks.insert(
            "key".to_owned(),
            CacheEntry {
                at: probes::now(),
                selected: vec!["selected".to_owned()],
                successes: Vec::new(),
            },
        );
        assert!(optimistic_routes(&cache, "key", &settings, &catalog).is_some());

        cache.networks.insert(
            "key".to_owned(),
            CacheEntry {
                at: probes::now() - settings.mechanisms.cache_ttl_hours * 3600000 - 1,
                selected: vec!["selected".to_owned()],
                successes: Vec::new(),
            },
        );
        assert!(optimistic_routes(&cache, "key", &settings, &catalog).is_none());
    }

    #[test]
    fn ai_takes_a_line_only_for_two_hop_chains() {
        let mut engine = engine();
        engine.recovery[1] = true;
        engine.failed_at[1] = probes::now();
        engine.state.current[1] = None;
        let quality = Quality {
            available: true,
            millis: 100,
            checked_at: engine.failed_at[1] + 1,
            timed_out: false,
        };
        let proof = AiProof {
            state: "available".to_owned(),
            challenged: false,
            checked_at: quality.checked_at,
            endpoints: Vec::new(),
        };
        let mut single = route("single", "single-rear", "single-rear");
        Arc::make_mut(&mut single).hops = 1;
        assert!(!engine.should_replace(1, &single, &quality, Some(&proof)));

        let chained = route("chain", "front", "rear");
        assert!(engine.should_replace(1, &chained, &quality, Some(&proof)));

        // Without chain roles an AI group never leaves the general line.
        Arc::make_mut(&mut engine.settings).chain = false;
        assert!(!engine.should_replace(1, &chained, &quality, Some(&proof)));
    }

    #[test]
    fn mirrored_commit_follows_the_general_line_only_while_unowned() {
        let mut engine = engine();
        engine.state.current[1] = None;
        engine.mirroring[1] = true;
        let general = Arc::clone(&engine.catalog.routes[0]);
        engine.receive(Event::Mirrored {
            epoch: 0,
            group: 1,
            route: Some(Arc::clone(&general)),
            quality: Some(Quality::default()),
        });
        assert!(!engine.mirroring[1]);
        assert!(engine.state.current[1].is_none());

        engine.mirroring[1] = true;
        engine.receive(Event::Mirrored {
            epoch: 1,
            group: 1,
            route: Some(general),
            quality: Some(Quality::default()),
        });
        let mirrored = engine.state.current[1].as_ref().unwrap();
        assert!(mirrored.mirrored);
        assert_eq!(mirrored.route.id, "selected");

        // An owned line is not overwritten by a late mirror event.
        engine.state.current[1].as_mut().unwrap().mirrored = false;
        engine.receive(Event::Mirrored {
            epoch: 1,
            group: 1,
            route: Some(Arc::clone(&engine.catalog.routes[0])),
            quality: Some(Quality::default()),
        });
        assert!(!engine.state.current[1].as_ref().unwrap().mirrored);
    }

    #[test]
    fn confirmed_ai_failure_returns_to_the_general_line() {
        let mut engine = engine();
        engine.validated[1] = true;
        engine.confirm_failure(1, "selected", probes::now());
        assert!(engine.state.current[1].is_none());
        assert!(!engine.validated[1]);
        assert!(engine.recovery[1]);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                engine.sync_mirrors();
            });
        assert!(engine.mirroring[1]);
    }

    #[test]
    fn mirrored_failures_stay_informational_without_chain_roles() {
        let mut solo = engine();
        Arc::make_mut(&mut solo.settings).chain = false;
        solo.validated[1] = false;
        solo.state.current[1].as_mut().unwrap().mirrored = true;
        for _ in 0..3 {
            solo.receive(Event::Monitor {
                epoch: 1,
                group: 1,
                route: "selected".to_owned(),
                quality: None,
                ai: Some(AiProof {
                    state: "failed".to_owned(),
                    challenged: false,
                    checked_at: probes::now(),
                    ..AiProof::default()
                }),
            });
        }
        assert_eq!(solo.state.current[1].as_ref().unwrap().failures, 0);
        assert!(!solo.recovery[1]);

        // With chain roles the same evidence drives the takeover search.
        let mut chained = engine();
        chained.settings = Arc::new(RoutingConfig {
            chain: true,
            ..RoutingConfig::default()
        });
        chained.validated[1] = true;
        chained.state.current[1].as_mut().unwrap().mirrored = true;
        for failure in 1..=3 {
            chained.receive(Event::Monitor {
                epoch: 1,
                group: 1,
                route: "selected".to_owned(),
                quality: None,
                ai: Some(AiProof {
                    state: "failed".to_owned(),
                    challenged: false,
                    checked_at: probes::now(),
                    ..AiProof::default()
                }),
            });
            assert_eq!(chained.recovery[1], failure == 3);
        }
    }

    #[test]
    fn verified_general_line_shows_monitoring_while_ai_searches() {
        let mut engine = engine();
        engine.recovery = [false, true, false, false];
        engine.search = Some(Search {
            round: engine.round + 1,
            order: VecDeque::new(),
            ai_order: std::array::from_fn(|_| VecDeque::new()),
            retries: VecDeque::new(),
            fast_busy: std::array::from_fn(|_| None),
            retry_busy: std::array::from_fn(|_| None),
            ai_busy: std::array::from_fn(|_| None),
            tried: 0,
            stage: 0,
            normal_target: 0,
            limit: 0,
            ai_cursor: 0,
            started: Instant::now(),
        });
        engine.publish();
        assert_eq!(engine.state.phase, "monitoring");
        assert_eq!(engine.state.reason, "verifying_background");

        engine.search = None;
        engine.deferred = Some((
            Arc::clone(&engine.catalog.routes[0]),
            Quality::default(),
            "selected".to_owned(),
        ));
        engine.publish();
        assert_eq!(engine.state.reason, "wait_hold");
        assert_eq!(engine.state.phase, "monitoring");
    }

    #[test]
    fn persistent_challenges_accumulate_toward_recovery() {
        let mut streak = engine();
        streak.validated[1] = true;
        let challenged = AiProof {
            state: "pending".to_owned(),
            challenged: true,
            checked_at: probes::now(),
            endpoints: Vec::new(),
        };
        for round in 1..=3 {
            streak.receive(Event::Monitor {
                epoch: 1,
                group: 1,
                route: "selected".to_owned(),
                quality: None,
                ai: Some(challenged.clone()),
            });
            assert_eq!(streak.recovery[1], round == 3, "round {round}");
        }
        // A clean round resets the streak before the threshold trips.
        let mut clean = engine();
        clean.validated[1] = true;
        for _ in 0..2 {
            clean.receive(Event::Monitor {
                epoch: 1,
                group: 1,
                route: "selected".to_owned(),
                quality: None,
                ai: Some(AiProof {
                    state: "pending".to_owned(),
                    challenged: true,
                    checked_at: probes::now(),
                    endpoints: Vec::new(),
                }),
            });
        }
        clean.receive(Event::Monitor {
            epoch: 1,
            group: 1,
            route: "selected".to_owned(),
            quality: None,
            ai: Some(AiProof {
                state: "available".to_owned(),
                challenged: false,
                checked_at: probes::now(),
                endpoints: Vec::new(),
            }),
        });
        assert!(!clean.recovery[1]);
        assert_eq!(clean.state.current[1].as_ref().unwrap().failures, 0);
    }

    #[test]
    fn runtime_china_exit_intel_blocks_candidates_everywhere() {
        let mut intel = engine();
        let replacement = route("replacement", "other-front", "other-rear");
        intel.catalog.routes.push(Arc::clone(&replacement));
        intel.exit_intel.insert(
            "replacement".to_owned(),
            probes::ExitTrace {
                ip: "223.5.5.5".to_owned(),
                country: "CN".to_owned(),
                china: true,
            },
        );
        let quality = Quality {
            available: true,
            millis: 100,
            checked_at: probes::now(),
            timed_out: false,
        };
        assert!(!intel.should_replace(0, &replacement, &quality, None));
        assert!(intel.ordered(0).iter().all(|route| route.id != "replacement"));
        // The mirror follows the general line even when its exit region is
        // AI-excluded: connectivity first, the chain takes over once verified.
        let mut mirror_case = engine();
        let mut general = route("hk-exit", "front", "hk-rear");
        Arc::make_mut(&mut general).country = "HK".to_owned();
        mirror_case.catalog.routes = vec![general];
        mirror_case.state.current[0] = Some(selection(Arc::clone(&mirror_case.catalog.routes[0]), 300));
        mirror_case.state.current[1] = None;
        mirror_case.receive(Event::Mirrored {
            epoch: 1,
            group: 1,
            route: Some(Arc::clone(&mirror_case.catalog.routes[0])),
            quality: Some(Quality::default()),
        });
        assert!(mirror_case.state.current[1].as_ref().is_some_and(|s| s.mirrored));
    }

    #[test]
    fn only_fully_verified_candidates_upgrade_an_occupied_ai_group() {
        let mut engine = engine();
        engine.recovery[1] = true;
        engine.failed_at[1] = probes::now();
        let replacement = route("replacement", "front", "rear");
        let quality = Quality {
            available: true,
            millis: 100,
            checked_at: engine.failed_at[1] + 1,
            timed_out: false,
        };
        let pending = AiProof {
            state: "pending".to_owned(),
            challenged: false,
            checked_at: quality.checked_at,
            endpoints: Vec::new(),
        };
        let confirmed = AiProof {
            state: "available".to_owned(),
            challenged: false,
            checked_at: quality.checked_at,
            endpoints: Vec::new(),
        };
        // Occupied (mirrored or owned) line: a pending candidate waits, a fully
        // verified one upgrades — non-browser clients need clean endpoints.
        assert!(!engine.should_replace(1, &replacement, &quality, Some(&pending)));
        assert!(engine.should_replace(1, &replacement, &quality, Some(&confirmed)));
        // An empty group accepts any usable candidate.
        engine.state.current[1] = None;
        assert!(engine.should_replace(1, &replacement, &quality, Some(&pending)));
    }

    #[test]
    fn candidate_penalty_ranks_confirmed_ai_proofs_first() {
        let confirmed = AiProof {
            state: "available".to_owned(),
            challenged: false,
            checked_at: probes::now(),
            endpoints: Vec::new(),
        };
        let pending = AiProof {
            state: "pending".to_owned(),
            challenged: false,
            checked_at: probes::now(),
            endpoints: Vec::new(),
        };
        assert_eq!(candidate_penalty(1, Some(&confirmed)), 0);
        assert_eq!(candidate_penalty(1, Some(&pending)), 1);
        assert_eq!(candidate_penalty(0, None), 0);
        assert_eq!(candidate_penalty(1, None), 1);
    }

    #[test]
    fn unverified_restored_line_reports_cache_verification() {
        let mut engine = engine();
        engine.validated = [false; 4];
        engine.state.phase = "searching".to_owned();
        engine.search = Some(Search {
            round: engine.round + 1,
            order: VecDeque::new(),
            ai_order: std::array::from_fn(|_| VecDeque::new()),
            retries: VecDeque::new(),
            fast_busy: std::array::from_fn(|_| None),
            retry_busy: std::array::from_fn(|_| None),
            ai_busy: std::array::from_fn(|_| None),
            tried: 0,
            stage: 0,
            normal_target: 0,
            limit: 0,
            ai_cursor: 0,
            started: Instant::now(),
        });
        engine.publish();
        assert_eq!(engine.state.phase, "searching");
        assert_eq!(engine.state.reason, "verifying_cached");
    }
}
