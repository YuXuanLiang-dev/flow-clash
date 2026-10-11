use super::{CmdResult, StringifyErr as _};
use crate::{
    config::{
        Config,
        routing::{
            NodeMetadata, RoutingConfig, RoutingMechanisms, routing_store, save_routing_config, set_subscription_role,
        },
    },
    core::routing::{
        self, RoutingStatus,
        catalog::{self, Node},
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default, Deserialize)]
#[serde(default)]
pub struct NodeQuery {
    pub subscription_id: String,
    pub search: String,
    pub country: String,
    pub residential: Option<bool>,
    pub offset: usize,
    pub limit: usize,
    pub node_metadata: Option<BTreeMap<String, NodeMetadata>>,
    pub profile_roles: Option<BTreeMap<String, String>>,
    pub residential_profiles: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct NodePage {
    pub total: usize,
    pub offset: usize,
    pub nodes: Vec<Node>,
}

#[tauri::command]
pub async fn get_routing_config() -> CmdResult<Arc<RoutingConfig>> {
    Ok(routing_store().await.stringify_err()?.committed.load_full())
}

#[tauri::command]
pub fn get_routing_defaults() -> RoutingMechanisms {
    RoutingMechanisms::default()
}

#[tauri::command]
pub async fn save_routing_settings(settings: RoutingConfig) -> CmdResult<Arc<RoutingConfig>> {
    save_routing_config(settings).await.stringify_err()?;
    Ok(routing_store().await.stringify_err()?.committed.load_full())
}

#[tauri::command]
pub async fn set_routing_subscription_role(uid: String, role: String) -> CmdResult<Arc<RoutingConfig>> {
    set_subscription_role(&uid, &role).await.stringify_err()?;
    Ok(routing_store().await.stringify_err()?.committed.load_full())
}

#[tauri::command]
pub async fn get_routing_status() -> CmdResult<Arc<RoutingStatus>> {
    Ok(routing::status())
}

#[tauri::command]
pub async fn reevaluate_routing() -> CmdResult {
    let _write = Config::lock_config_write().await;
    if !routing_store().await.stringify_err()?.committed.load().enabled {
        return Ok(());
    }
    routing::reevaluate();
    Ok(())
}

#[tauri::command]
pub async fn list_routing_nodes(query: NodeQuery) -> CmdResult<NodePage> {
    let mut settings = routing_store()
        .await
        .stringify_err()?
        .committed
        .load_full()
        .as_ref()
        .clone();
    if let Some(metadata) = query.node_metadata {
        settings.node_metadata = metadata;
    }
    if let Some(roles) = query.profile_roles {
        settings.profile_roles = roles;
    }
    if let Some(profiles) = query.residential_profiles {
        settings.residential_profiles = profiles;
    }
    let profiles = Config::profiles().await.data_arc();
    let directory = catalog::directory(profiles, Arc::new(settings)).await.stringify_err()?;
    let search = query.search.to_lowercase();
    let nodes = directory
        .iter()
        .filter(|node| {
            (query.subscription_id.is_empty() || node.subscription_id == query.subscription_id)
                && (search.is_empty()
                    || node.name.to_lowercase().contains(&search)
                    || node.subscription.to_lowercase().contains(&search))
                && (query.country.is_empty() || node.country == query.country)
                && query
                    .residential
                    .is_none_or(|residential| node.residential == residential)
        })
        .collect::<Vec<_>>();
    let total = nodes.len();
    let offset = query.offset.min(total);
    let limit = if query.limit == 0 {
        80
    } else {
        query.limit.clamp(1, 250)
    };
    Ok(NodePage {
        total,
        offset,
        nodes: nodes.into_iter().skip(offset).take(limit).cloned().collect(),
    })
}
