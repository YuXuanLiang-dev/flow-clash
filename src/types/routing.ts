export type AiService = 'OPENAI' | 'CLAUDE' | 'GEMINI'
export type RoutingRole = 'ingress' | 'egress' | 'normal'
export const ROUTING_GROUPS: readonly string[] = [
  'CVR-Auto',
  'CVR-OpenAI',
  'CVR-Claude',
  'CVR-Gemini',
]
export interface RoutingConfig {
  enabled: boolean
  chain: boolean
  allow_direct_fallback: boolean
  ai_enabled: [boolean, boolean, boolean]
  ai_excluded_countries: string[]
  mechanisms: RoutingMechanisms
  profile_roles: Record<string, RoutingRole>
  residential_profiles: string[]
  preferred_ingress: string[]
  preferred_egress: string[]
  ai_priorities: Partial<Record<AiService, string[]>>
  prefer_residential: boolean
  health_interval: number
  ai_health_interval: number
  probe_base_port: number
  node_metadata: Record<
    string,
    { country: string; residential: boolean | null }
  >
  folders: { id: string; name: string }[]
  profile_folders: Record<string, string>
}
export interface RoutingMechanisms {
  fast_concurrency: number
  retry_concurrency: number
  ai_concurrency: number
  fast_timeout_ms: number
  retry_timeout_ms: number
  initial_batch: number
  general_budget: number
  recovery_budget: number
  general_failures: number
  ai_failures: number
  general_retry_seconds: number
  ai_retry_seconds: number
  recovery_retry_seconds: number
  recovery_max_retry_seconds: number
  failure_cooldown_seconds: number
  switch_min_seconds: number
  improvement_percent: number
  improvement_ms: number
  network_debounce_seconds: number
  cache_ttl_hours: number
  cache_network_limit: number
}
export interface RoutingNode {
  id: string
  subscription_id: string
  subscription: string
  name: string
  country: string
  residential: boolean
  role: 'ingress' | 'egress' | 'normal'
  proxy_type: string
  udp: boolean
}
export interface RoutingNodeQuery {
  subscription_id?: string
  search?: string
  country?: string
  residential?: boolean | null
  offset?: number
  limit?: number
  node_metadata?: RoutingConfig['node_metadata']
  profile_roles?: RoutingConfig['profile_roles']
  residential_profiles?: string[]
}
export interface RoutingNodePage {
  total: number
  offset: number
  nodes: RoutingNode[]
}
export interface Route {
  id: string
  ingress: string
  egress: string
  ingress_name: string
  egress_name: string
  ingress_country: string
  country: string
  residential: boolean
  rear_subscription: boolean
  hops: number
  /** 可用于通用分组(单跳线路) */
  general?: boolean
}
export interface RoutingEndpoint {
  name: string
  state: string
  code: number
  millis: number
  checked_at: number
}
export interface AiProof {
  state: string
  /** 持续出现验证页挑战(未硬封锁),按证据累积处理 */
  challenged?: boolean
  checked_at: number
  endpoints: RoutingEndpoint[]
}
export interface RoutingSelection {
  route: Route
  quality: {
    available: boolean
    millis: number
    checked_at: number
    timed_out: boolean
  }
  ai: AiProof | null
  failures: number
  exit_since: number
  switched_at: number
  last_checked: number
  /** AI 分组当前跟随通用线路,而非自己的已验证线路 */
  mirrored?: boolean
  /** 线路实际出口(IP/国家/是否中国大陆) */
  exit?: {
    ip: string
    country: string
    china: boolean
  }
}
export interface RoutingStatus {
  enabled: boolean
  phase: string
  reason: string
  last_error: string
  network_pending: boolean
  enabled_groups: boolean[]
  eligible_candidates: number[]
  checking: boolean[]
  recovering: boolean[]
  next_checks: number[]
  next_retry_at: number | null
  next_optimization_at: number | null
  network: string
  elapsed_ms: number
  first_route_ms: number | null
  candidate_total: number
  fast_done: number
  fast_target: number
  retry_done: number
  retry_total: number
  ai_done: number
  ai_target: number
  active_probes: number
  current: (RoutingSelection | null)[]
}
