use anyhow::Result;
use parking_lot::{Mutex, RwLock};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Signer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Health status of an RPC endpoint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Information about an RPC endpoint
#[derive(Clone)]
pub struct RpcEndpoint {
    pub url: String,
    pub client: Arc<RpcClient>,
    pub health: RpcHealth,
    pub latency_ms: f64,
    pub error_count: u64,
    pub last_check: Instant,
    /// Geographic location hint for proximity calculations
    pub location: Option<String>,
}

impl std::fmt::Debug for RpcEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcEndpoint")
            .field("url", &self.url)
            .field("health", &self.health)
            .field("latency_ms", &self.latency_ms)
            .field("error_count", &self.error_count)
            .field("location", &self.location)
            .finish()
    }
}

/// Leader information for geographic/stake weighting
#[derive(Debug, Clone)]
pub struct LeaderInfo {
    pub validator_pubkey: Pubkey,
    pub location: Option<String>,
    pub stake_weight: f64,
    pub next_slot: u64,
}

/// Scoring weights for RPC endpoint ranking
#[derive(Debug, Clone)]
pub struct ScoringWeights {
    pub geo_weight: f64,
    pub stake_weight: f64,
    pub latency_weight: f64,
}

/// The Command & Intelligence Center for the Quantum Race Architecture
/// Manages multiple RPC connections with health monitoring and intelligent routing
#[derive(Clone)]
pub struct RpcManager {
    endpoints: Arc<RwLock<Vec<RpcEndpoint>>>,
    leader_schedule: Arc<RwLock<HashMap<u64, Pubkey>>>,
    validator_info: Arc<RwLock<HashMap<Pubkey, LeaderInfo>>>,
    scoring_weights: ScoringWeights,
    health_check_interval: Duration,
    monitoring_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl RpcManager {
    /// Creates a new RPC Manager with multiple endpoints for the Quantum Race Architecture
    pub fn new(rpc_urls: &[String]) -> Self {
        let endpoints: Vec<RpcEndpoint> = rpc_urls
            .iter()
            .map(|url| RpcEndpoint {
                url: url.clone(),
                client: Arc::new(RpcClient::new(url.clone())),
                health: RpcHealth::Healthy,
                latency_ms: 0.0,
                error_count: 0,
                last_check: Instant::now(),
                location: Self::infer_location_from_url(url),
            })
            .collect();

        info!(
            "🌐 RpcManager initialized with {} endpoints",
            endpoints.len()
        );
        for endpoint in &endpoints {
            info!("   📡 {} (location: {:?})", endpoint.url, endpoint.location);
        }

        Self {
            endpoints: Arc::new(RwLock::new(endpoints)),
            leader_schedule: Arc::new(RwLock::new(HashMap::new())),
            validator_info: Arc::new(RwLock::new(HashMap::new())),
            scoring_weights: ScoringWeights {
                geo_weight: 1.0,
                stake_weight: 2.0,
                latency_weight: 0.5,
            },
            health_check_interval: Duration::from_secs(1),
            monitoring_task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates a new RPC Manager with custom scoring weights
    pub fn new_with_weights(rpc_urls: &[String], weights: ScoringWeights) -> Self {
        let mut manager = Self::new(rpc_urls);
        info!(
            "🎯 RpcManager configured with custom weights: geo={}, stake={}, latency={}",
            weights.geo_weight, weights.stake_weight, weights.latency_weight
        );
        manager.scoring_weights = weights;
        manager
    }

    /// Backward compatibility constructor
    pub fn new_single(rpc_url: &str) -> Self {
        Self::new(&[rpc_url.to_string()])
    }

    /// Legacy single-URL constructor (maintaining exact API compatibility)
    pub fn new_legacy(rpc_url: &str) -> OldRpcManager {
        OldRpcManager::new(rpc_url)
    }

    /// Starts continuous health monitoring of all RPC endpoints
    /// Performs lightweight health checks every ~1s and classifies endpoints
    pub async fn start_monitoring(&self) {
        let endpoints = self.endpoints.clone();
        let interval = self.health_check_interval;

        let handle = tokio::spawn(async move {
            info!("💓 RPC health monitoring started - continuous intelligence gathering");

            loop {
                // Snapshot endpoints list to avoid holding lock during async operations
                let endpoint_snapshots: Vec<(String, Arc<RpcClient>)> = {
                    let endpoints_guard = endpoints.read();
                    endpoints_guard
                        .iter()
                        .map(|ep| (ep.url.clone(), ep.client.clone()))
                        .collect()
                };

                // Perform health checks without holding any locks
                let mut health_updates = Vec::new();
                for (url, client) in endpoint_snapshots {
                    let start_time = Instant::now();

                    match client.get_health().await {
                        Ok(_) => {
                            let latency = start_time.elapsed().as_millis() as f64;

                            // Classify health based on latency thresholds
                            let health = if latency < 150.0 {
                                RpcHealth::Healthy
                            } else if latency < 750.0 {
                                RpcHealth::Degraded
                            } else {
                                RpcHealth::Unhealthy
                            };

                            health_updates.push((url.clone(), latency, 0, health));
                            debug!(
                                "✅ {} {} ({}ms)",
                                url,
                                match health {
                                    RpcHealth::Healthy => "healthy",
                                    RpcHealth::Degraded => "degraded",
                                    RpcHealth::Unhealthy => "unhealthy",
                                },
                                latency
                            );
                        }
                        Err(e) => {
                            warn!("❌ {} health check failed: {}", url, e);
                            health_updates.push((url, 9999.0, 1, RpcHealth::Unhealthy));
                        }
                    }
                }

                // Update endpoint states in a single critical section
                {
                    let mut endpoints_guard = endpoints.write();
                    for (url, latency, error_increment, health) in health_updates {
                        if let Some(endpoint) = endpoints_guard.iter_mut().find(|ep| ep.url == url)
                        {
                            endpoint.latency_ms = latency;
                            endpoint.last_check = Instant::now();
                            endpoint.health = health;

                            if error_increment > 0 {
                                endpoint.error_count += error_increment;
                                // Degrade health further if too many consecutive errors
                                if endpoint.error_count >= 3 {
                                    endpoint.health = RpcHealth::Unhealthy;
                                }
                            } else {
                                endpoint.error_count = 0; // Reset on success
                            }
                        }
                    }
                }

                tokio::time::sleep(interval).await;
            }
        });

        *self.monitoring_task_handle.lock() = Some(handle);
    }

    /// Updates the leader schedule for intelligent routing
    pub async fn update_leader_schedule(&self) -> Result<()> {
        // Get the first healthy client for fetching leader schedule
        let client = self.get_healthy_client().await?;

        // Fetch leader schedule for next epoch
        match client.get_leader_schedule(None).await {
            Ok(Some(schedule)) => {
                // Update leader schedule in critical section
                {
                    let mut leader_schedule = self.leader_schedule.write();
                    leader_schedule.clear();

                    for (validator_str, slots) in schedule {
                        if let Ok(validator_pubkey) = validator_str.parse::<Pubkey>() {
                            for slot in slots {
                                leader_schedule.insert(slot as u64, validator_pubkey);
                            }
                        }
                    }

                    info!(
                        "📅 Leader schedule updated with {} slots",
                        leader_schedule.len()
                    );
                }
            }
            Ok(None) => {
                warn!("⚠️ No leader schedule available");
            }
            Err(e) => {
                error!("❌ Failed to fetch leader schedule: {}", e);
                return Err(e.into());
            }
        }

        Ok(())
    }

    /// Get ranked RPC endpoints optimized for the current/next leader
    /// This is the core method for the Quantum Race Architecture
    /// Uses advanced scoring: Score = (GeoWeight * ProximityToLeader) + (StakeWeight * LeaderStake) - (LatencyWeight * CurrentPingMs)
    pub async fn get_ranked_rpc_endpoints(&self, count: usize) -> Result<Vec<Arc<RpcClient>>> {
        // Get current slot to determine next leader
        let current_slot = self.get_current_slot().await?;
        let next_leader_slot = current_slot + 1;

        // Snapshot data structures to avoid holding locks
        let (healthy_endpoints, next_leader_info) = {
            let endpoints_guard = self.endpoints.read();
            let leader_schedule = self.leader_schedule.read();
            let validator_info = self.validator_info.read();

            // Get next leader
            let next_leader = leader_schedule.get(&next_leader_slot);
            let leader_info = next_leader.and_then(|leader| validator_info.get(leader).cloned());

            // Filter and clone healthy endpoints
            let healthy_endpoints: Vec<RpcEndpoint> = endpoints_guard
                .iter()
                .filter(|ep| ep.health == RpcHealth::Healthy)
                .cloned()
                .collect();

            (healthy_endpoints, leader_info)
        };

        // Fallback to degraded endpoints if no healthy ones available
        if healthy_endpoints.is_empty() {
            warn!("⚠️ No healthy RPC endpoints available, using degraded ones");
            let degraded_endpoints: Vec<Arc<RpcClient>> = {
                let endpoints_guard = self.endpoints.read();
                endpoints_guard
                    .iter()
                    .filter(|ep| ep.health == RpcHealth::Degraded)
                    .take(count)
                    .map(|ep| ep.client.clone())
                    .collect()
            };
            return Ok(degraded_endpoints);
        }

        // Calculate scores for each healthy endpoint using the advanced algorithm
        let mut scored_endpoints: Vec<_> = healthy_endpoints
            .into_iter()
            .map(|endpoint| {
                let mut score = 100.0; // Base score

                // Latency penalty (lower latency = higher score)
                score -= self.scoring_weights.latency_weight * endpoint.latency_ms;

                // Leader-aware scoring if we have leader information
                if let Some(ref leader_info) = next_leader_info {
                    // Geographic proximity bonus
                    let geo_bonus = if endpoint.location == leader_info.location
                        && endpoint.location.is_some()
                    {
                        50.0 // Same region as leader
                    } else if endpoint.location.is_some() && leader_info.location.is_some() {
                        // Different region penalty
                        -10.0
                    } else {
                        0.0 // Unknown location
                    };
                    score += self.scoring_weights.geo_weight * geo_bonus;

                    // Stake weight bonus (higher stake leaders have better connectivity)
                    score += self.scoring_weights.stake_weight * leader_info.stake_weight;
                }

                debug!(
                    "🎯 RPC {} scored {:.2} (latency: {:.1}ms, location: {:?})",
                    endpoint.url, score, endpoint.latency_ms, endpoint.location
                );

                (endpoint, score)
            })
            .collect();

        // Sort by score (highest first) and select top N
        scored_endpoints.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let selected: Vec<_> = scored_endpoints
            .into_iter()
            .take(count)
            .map(|(endpoint, score)| {
                info!(
                    "🚀 Selected RPC {} (score: {:.1}) for Quantum Race",
                    endpoint.url, score
                );
                endpoint.client.clone()
            })
            .collect();

        info!("⚡ Quantum Race Intelligence: Selected {} optimal RPCs for leader slot {} (next leader: {:?})", 
              selected.len(), next_leader_slot, next_leader_info.as_ref().map(|l| l.validator_pubkey));

        Ok(selected)
    }

    /// Get the first healthy client (fallback method)
    pub async fn get_healthy_client(&self) -> Result<Arc<RpcClient>> {
        // Snapshot endpoints to avoid holding lock
        let endpoints_snapshot: Vec<RpcEndpoint> = {
            let endpoints = self.endpoints.read();
            endpoints.clone()
        };

        // Find healthy endpoint
        for endpoint in endpoints_snapshot.iter() {
            if endpoint.health == RpcHealth::Healthy {
                return Ok(endpoint.client.clone());
            }
        }

        // If no healthy endpoints, try degraded ones
        for endpoint in endpoints_snapshot.iter() {
            if endpoint.health == RpcHealth::Degraded {
                warn!("⚠️ Using degraded RPC endpoint: {}", endpoint.url);
                return Ok(endpoint.client.clone());
            }
        }

        Err(anyhow::anyhow!("No usable RPC endpoints available"))
    }

    /// Get current slot from the best available client
    async fn get_current_slot(&self) -> Result<u64> {
        let client = self.get_healthy_client().await?;
        Ok(client.get_slot().await?)
    }

    /// Infer geographic location from RPC URL patterns
    fn infer_location_from_url(url: &str) -> Option<String> {
        if url.contains("helius") {
            Some("us-east".to_string())
        } else if url.contains("triton") {
            Some("us-west".to_string())
        } else if url.contains("quicknode") {
            Some("global".to_string())
        } else if url.contains("alchemy") {
            Some("us-central".to_string())
        } else if url.contains("devnet") || url.contains("testnet") {
            Some("solana-labs".to_string())
        } else {
            None
        }
    }

    /// Update validator information for better routing decisions
    pub async fn update_validator_info(&self, validators: HashMap<Pubkey, LeaderInfo>) {
        let count = validators.len();

        // Update in critical section
        {
            let mut validator_info = self.validator_info.write();
            *validator_info = validators;
        }

        info!("🗂️ Updated information for {} validators", count);
    }

    /// Start optional canary probes for deep network health monitoring
    /// Requires a separate payer keypair for canary transactions
    pub async fn start_canary_probes(
        &self,
        canary_payer: Option<Arc<solana_sdk::signature::Keypair>>,
    ) {
        if let Some(payer) = canary_payer {
            let endpoints = self.endpoints.clone();
            let payer_clone = payer.clone();

            tokio::spawn(async move {
                info!("🐦 Starting canary probes for deep network health monitoring");
                let mut interval = tokio::time::interval(Duration::from_secs(60)); // Every minute

                loop {
                    interval.tick().await;

                    // Get a healthy client for canary probe
                    let client = {
                        let endpoints_guard = endpoints.read();
                        endpoints_guard
                            .iter()
                            .find(|ep| ep.health == RpcHealth::Healthy)
                            .map(|ep| ep.client.clone())
                    };

                    if let Some(client) = client {
                        // Perform a simple balance check as canary probe
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            client.get_balance(&payer_clone.pubkey()),
                        )
                        .await
                        {
                            Ok(Ok(balance)) => {
                                debug!(
                                    "🐦 Canary probe successful - payer balance: {} lamports",
                                    balance
                                );
                            }
                            Ok(Err(e)) => {
                                warn!("🐦 Canary probe failed: {}", e);
                            }
                            Err(_) => {
                                warn!("🐦 Canary probe timed out");
                            }
                        }
                    }
                }
            });
        } else {
            info!("🐦 Canary probes disabled - no canary payer configured");
        }
    }

    /// Check if network state is consistent across healthy RPC endpoints
    /// Returns true if all healthy endpoints report similar slots (within 2 slots of each other)
    pub async fn is_network_consistent(&self) -> bool {
        // Get snapshot of healthy endpoints to avoid holding lock
        let healthy_endpoints: Vec<Arc<RpcClient>> = {
            let endpoints_guard = self.endpoints.read();
            endpoints_guard
                .iter()
                .filter(|ep| ep.health == RpcHealth::Healthy)
                .map(|ep| ep.client.clone())
                .collect()
        };

        if healthy_endpoints.len() < 2 {
            // If we have less than 2 healthy endpoints, consider it consistent
            return true;
        }

        // Fetch current slot from each healthy endpoint
        let mut slots = Vec::new();
        for client in healthy_endpoints.iter().take(3) {
            // Check max 3 endpoints for efficiency
            match tokio::time::timeout(Duration::from_millis(500), client.get_slot()).await {
                Ok(Ok(slot)) => slots.push(slot),
                Ok(Err(_)) | Err(_) => {} // Skip failed requests
            }
        }

        if slots.len() < 2 {
            return true; // Not enough data to determine inconsistency
        }

        // Check if all slots are within 2 slots of each other
        let min_slot = *slots.iter().min().unwrap();
        let max_slot = *slots.iter().max().unwrap();
        let is_consistent = max_slot - min_slot <= 2;

        if !is_consistent {
            warn!(
                "⚠️ Network inconsistency detected: slot range {}-{} (diff: {})",
                min_slot,
                max_slot,
                max_slot - min_slot
            );
        }

        is_consistent
    }

    /// Get health statistics for monitoring
    pub async fn get_health_stats(&self) -> HashMap<RpcHealth, usize> {
        // Snapshot endpoints to avoid holding lock
        let endpoints_snapshot: Vec<RpcEndpoint> = {
            let endpoints = self.endpoints.read();
            endpoints.clone()
        };

        let mut stats = HashMap::new();
        for endpoint in endpoints_snapshot.iter() {
            *stats.entry(endpoint.health.clone()).or_insert(0) += 1;
        }

        stats
    }

    /// Legacy compatibility methods
    pub fn get_client(&self) -> Arc<RpcClient> {
        // This is a synchronous method, so we'll use the first endpoint
        // In practice, this should be avoided in favor of get_healthy_client
        let endpoints = self.endpoints.read();
        if let Some(first_endpoint) = endpoints.first() {
            first_endpoint.client.clone()
        } else {
            panic!("No RPC endpoints configured")
        }
    }

    pub async fn get_optimal_rpc(&self) -> OptimalRpc {
        let client = self.get_healthy_client().await.unwrap_or_else(|_| {
            let endpoints = self.endpoints.read();
            if let Some(first_endpoint) = endpoints.first() {
                first_endpoint.client.clone()
            } else {
                panic!("No RPC endpoints configured")
            }
        });

        OptimalRpc { client }
    }
}

/// Legacy structure for backward compatibility
pub struct OptimalRpc {
    pub client: Arc<RpcClient>,
}

/// Legacy RpcManager wrapper for full backward compatibility
#[derive(Clone)]
pub struct OldRpcManager {
    pub client: Arc<RpcClient>,
    quantum_manager: Arc<RpcManager>,
}

impl OldRpcManager {
    pub fn new(rpc_url: &str) -> Self {
        let client = Arc::new(RpcClient::new(rpc_url.to_string()));
        let quantum_manager = Arc::new(RpcManager::new(&[rpc_url.to_string()]));

        Self {
            client,
            quantum_manager,
        }
    }

    pub fn get_client(&self) -> &RpcClient {
        &self.client
    }

    pub async fn get_optimal_rpc(&self) -> OptimalRpc {
        OptimalRpc {
            client: self.client.clone(),
        }
    }

    /// Access to the new Quantum Race functionality
    pub fn quantum(&self) -> &RpcManager {
        &self.quantum_manager
    }
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            geo_weight: 1.0,
            stake_weight: 2.0,
            latency_weight: 0.5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_manager_initialization() {
        let rpc_urls = vec![
            "https://api.devnet.solana.com".to_string(),
            "https://api.testnet.solana.com".to_string(),
        ];

        // Test basic initialization
        let manager = RpcManager::new(&rpc_urls);
        let endpoints = manager.endpoints.read();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].url, "https://api.devnet.solana.com");
        assert_eq!(endpoints[1].url, "https://api.testnet.solana.com");
    }

    #[test]
    fn test_scoring_weights() {
        let weights = ScoringWeights::default();
        assert_eq!(weights.geo_weight, 1.0);
        assert_eq!(weights.stake_weight, 2.0);
        assert_eq!(weights.latency_weight, 0.5);

        let custom_weights = ScoringWeights {
            geo_weight: 2.5,
            stake_weight: 1.5,
            latency_weight: 0.8,
        };

        let rpc_urls = vec!["https://api.devnet.solana.com".to_string()];
        let manager = RpcManager::new_with_weights(&rpc_urls, custom_weights.clone());
        assert_eq!(manager.scoring_weights.geo_weight, 2.5);
        assert_eq!(manager.scoring_weights.stake_weight, 1.5);
        assert_eq!(manager.scoring_weights.latency_weight, 0.8);
    }

    #[test]
    fn test_location_inference() {
        assert_eq!(
            RpcManager::infer_location_from_url("https://white-polished-orb.solana-mainnet.quiknode.pro/311849bfafc79b24841bf73131a15cc5c5d3d7be/"),
            Some("us-east".to_string())
        );
        assert_eq!(
            RpcManager::infer_location_from_url("https://rpc.triton.one"),
            Some("us-west".to_string())
        );
        assert_eq!(
            RpcManager::infer_location_from_url("https://api.devnet.solana.com"),
            Some("solana-labs".to_string())
        );
        assert_eq!(
            RpcManager::infer_location_from_url("https://unknown-provider.com"),
            None
        );
    }

    #[test]
    fn test_rpc_health_copy_trait() {
        let health = RpcHealth::Healthy;
        let health_copy = health; // Should work because of Copy trait
        assert_eq!(health, health_copy);
    }

    #[tokio::test]
    async fn test_validator_info_update() {
        let rpc_urls = vec!["https://api.devnet.solana.com".to_string()];
        let manager = RpcManager::new(&rpc_urls);

        let mut validator_info = HashMap::new();
        let dummy_pubkey = Pubkey::new_unique();
        validator_info.insert(
            dummy_pubkey,
            LeaderInfo {
                validator_pubkey: dummy_pubkey,
                location: Some("us-east".to_string()),
                stake_weight: 1000.0,
                next_slot: 12345,
            },
        );

        manager.update_validator_info(validator_info).await;

        // Verify the info was stored
        let stored_info = manager.validator_info.read();
        assert!(stored_info.contains_key(&dummy_pubkey));
        assert_eq!(stored_info.get(&dummy_pubkey).unwrap().stake_weight, 1000.0);
    }
}
