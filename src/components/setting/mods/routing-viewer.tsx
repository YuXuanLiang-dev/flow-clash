import {
  AddRounded,
  ArrowDownwardRounded,
  ArrowUpwardRounded,
  DeleteOutlineRounded,
  ExpandMoreRounded,
  RemoveRounded,
} from '@mui/icons-material'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Autocomplete,
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControlLabel,
  IconButton,
  LinearProgress,
  List,
  ListItem,
  ListItemText,
  MenuItem,
  Stack,
  Tab,
  Tabs,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import { useCallback, useDeferredValue, useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { useProfiles } from '@/hooks/use-profiles'
import {
  getRoutingConfig,
  getRoutingDefaults,
  getRoutingStatus,
  listRoutingNodes,
  reevaluateRouting,
  saveRoutingSettings,
} from '@/services/cmds'
import { mutate } from '@/services/mutate'
import { errorDetail } from '@/services/notice-service'
import { setCacheData, useQuery } from '@/services/query-client'
import type { TranslationKey } from '@/types/generated/i18n-keys'
import type {
  AiService,
  RoutingConfig,
  RoutingNode,
  RoutingNodeQuery,
  RoutingMechanisms,
} from '@/types/routing'

const COUNTRIES = [
  'HK',
  'TW',
  'JP',
  'SG',
  'US',
  'KR',
  'GB',
  'DE',
  'CA',
  'AU',
  'FR',
  'NL',
  'IN',
  'TH',
  'MY',
  'PH',
  'VN',
  'ID',
  'RU',
  'BR',
  'CH',
  'SE',
  'FI',
  'IT',
  'ES',
  'AE',
  'TR',
  'CN',
]
type Preference = 'ingress' | 'egress' | AiService

function useLabels() {
  const { t, i18n } = useTranslation()
  return {
    text: (
      key: string,
      options?: { count?: number; min?: number; max?: number },
    ) => t(`routing.${key}` as TranslationKey, options),
    locale: i18n.language === 'zhtw' ? 'zh-TW' : i18n.language,
  }
}

function useCountryOptions(automatic = false) {
  const { text, locale } = useLabels()
  let names: Intl.DisplayNames | undefined
  try {
    names = new Intl.DisplayNames([locale], { type: 'region' })
  } catch {
    names = undefined
  }
  return [
    <MenuItem key="automatic" value="">
      {text(automatic ? 'automatic' : 'allCountries')}
    </MenuItem>,
    ...COUNTRIES.map((country) => (
      <MenuItem key={country} value={country}>
        {names?.of(country) ?? country} ({country})
      </MenuItem>
    )),
  ]
}

interface BrowserProps {
  draft: RoutingConfig
  onChange: (draft: RoutingConfig) => void
  subscription?: string
  preference?: Preference
  onAdd?: (node: RoutingNode) => void
  onLoaded?: (nodes: RoutingNode[]) => void
}

function NodeBrowser({
  draft,
  onChange,
  subscription = '',
  preference,
  onAdd,
  onLoaded,
}: BrowserProps) {
  const { text } = useLabels()
  const countryOptions = useCountryOptions()
  const automaticCountryOptions = useCountryOptions(true)
  const { profiles } = useProfiles()
  const [search, setSearch] = useState('')
  const deferredSearch = useDeferredValue(search)
  const [feed, setFeed] = useState(subscription)
  const [country, setCountry] = useState('')
  const [residential, setResidential] = useState(false)
  const [offset, setOffset] = useState(0)
  const query: RoutingNodeQuery = {
    subscription_id: subscription || feed,
    search: deferredSearch,
    country,
    residential: residential ? true : null,
    offset,
    limit: 80,
    node_metadata: draft.node_metadata,
    profile_roles: draft.profile_roles,
    residential_profiles: draft.residential_profiles,
  }
  const { data, error, isFetching } = useQuery({
    queryKey: ['routingNodes', query],
    queryFn: () => listRoutingNodes(query),
    refetchOnWindowFocus: false,
  })
  useEffect(() => {
    if (data) onLoaded?.(data.nodes)
  }, [data, onLoaded])
  const updateMetadata = (
    node: RoutingNode,
    patch: Partial<RoutingConfig['node_metadata'][string]>,
  ) => {
    onChange({
      ...draft,
      node_metadata: {
        ...draft.node_metadata,
        [node.id]: {
          ...(draft.node_metadata[node.id] ?? {
            country: '',
            residential: null,
          }),
          ...patch,
        },
      },
    })
  }
  return (
    <Stack spacing={1.5}>
      <Stack direction="row" sx={{ gap: 1, flexWrap: 'wrap' }}>
        <TextField
          size="small"
          label={text('search')}
          value={search}
          onChange={(event) => {
            setSearch(event.target.value)
            setOffset(0)
          }}
          sx={{ flex: '1 1 180px' }}
        />
        {!subscription && (
          <TextField
            select
            size="small"
            label={text('subscription')}
            value={feed}
            onChange={(event) => {
              setFeed(event.target.value)
              setOffset(0)
            }}
            sx={{ minWidth: 160 }}
          >
            <MenuItem value="">{text('allSubscriptions')}</MenuItem>
            {profiles?.items
              ?.filter((profile) =>
                ['local', 'remote'].includes(profile.type ?? ''),
              )
              .map((profile) => (
                <MenuItem key={profile.uid} value={profile.uid}>
                  {profile.name}
                </MenuItem>
              ))}
          </TextField>
        )}
        <TextField
          select
          size="small"
          label={text('country')}
          value={country}
          onChange={(event) => {
            setCountry(event.target.value)
            setOffset(0)
          }}
          sx={{ minWidth: 140 }}
        >
          {countryOptions}
        </TextField>
        <FormControlLabel
          control={
            <Checkbox
              checked={residential}
              onChange={(_, checked) => {
                setResidential(checked)
                setOffset(0)
              }}
              size="small"
            />
          }
          label={text('residentialOnly')}
        />
      </Stack>
      {isFetching && <LinearProgress />}
      {error && <Alert severity="error">{errorDetail(error)}</Alert>}
      {!isFetching && data?.total === 0 && (
        <Typography color="text.secondary">{text('empty')}</Typography>
      )}
      <List dense sx={{ maxHeight: 360, overflow: 'auto' }}>
        {data?.nodes.map((node) => {
          const metadata = draft.node_metadata[node.id]
          const role = draft.profile_roles[node.subscription_id] ?? 'normal'
          const excluded =
            preference &&
            (preference === 'ingress'
              ? role !== 'ingress'
              : preference === 'egress'
                ? role !== 'egress'
                : draft.ai_excluded_countries.includes(
                    metadata?.country || node.country,
                  ) ||
                  (draft.chain &&
                    !draft.allow_direct_fallback &&
                    role !== 'egress'))
          return (
            <ListItem key={node.id} divider sx={{ gap: 1, flexWrap: 'wrap' }}>
              <ListItemText
                sx={{ minWidth: 160, flex: '1 1 180px' }}
                primary={node.name}
                secondary={`${node.subscription} · ${metadata?.country || node.country || '—'} · ${node.proxy_type}`}
              />
              <TextField
                select
                size="small"
                label={text('country')}
                value={metadata?.country ?? ''}
                onChange={(event) =>
                  updateMetadata(node, { country: event.target.value })
                }
                sx={{ width: 140 }}
              >
                {automaticCountryOptions}
              </TextField>
              <FormControlLabel
                control={
                  <Checkbox
                    size="small"
                    checked={metadata?.residential ?? node.residential}
                    onChange={(_, checked) =>
                      updateMetadata(node, { residential: checked })
                    }
                  />
                }
                label={text('residentialNode')}
              />
              {onAdd && (
                <Tooltip title={text('addPriority')}>
                  <span>
                    <IconButton
                      size="small"
                      disabled={Boolean(excluded)}
                      onClick={() => onAdd(node)}
                      aria-label={text('addPriority')}
                    >
                      <AddRounded />
                    </IconButton>
                  </span>
                </Tooltip>
              )}
            </ListItem>
          )
        })}
      </List>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between' }}
      >
        <Typography variant="body2" color="text.secondary">
          {text('total', { count: data?.total ?? 0 })}
        </Typography>
        <Stack direction="row" spacing={1}>
          <Button
            size="small"
            disabled={offset === 0 || isFetching}
            onClick={() => setOffset(Math.max(0, offset - 80))}
          >
            {text('previous')}
          </Button>
          <Button
            size="small"
            disabled={offset + 80 >= (data?.total ?? 0) || isFetching}
            onClick={() => setOffset(offset + 80)}
          >
            {text('next')}
          </Button>
        </Stack>
      </Stack>
    </Stack>
  )
}

function FolderEditor({ draft, onChange }: BrowserProps) {
  const { text } = useLabels()
  const { profiles } = useProfiles()
  const [name, setName] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const subscriptions =
    profiles?.items?.filter((profile) =>
      ['local', 'remote'].includes(profile.type ?? ''),
    ) ?? []
  const folders = [{ id: '', name: text('ungrouped') }, ...draft.folders]
  const move = (id: string, delta: number) => {
    const items = [...draft.folders]
    const index = items.findIndex((folder) => folder.id === id)
    const target = index + delta
    if (target < 0 || target >= items.length) return
    ;[items[index], items[target]] = [items[target], items[index]]
    onChange({ ...draft, folders: items })
  }
  return (
    <Stack spacing={1.5}>
      <Stack direction="row" spacing={1}>
        <TextField
          size="small"
          fullWidth
          label={text('folderName')}
          value={name}
          onChange={(event) => setName(event.target.value)}
        />
        <Button
          disabled={!name.trim()}
          onClick={() => {
            onChange({
              ...draft,
              folders: [
                ...draft.folders,
                { id: crypto.randomUUID(), name: name.trim() },
              ],
            })
            setName('')
          }}
        >
          {text('createFolder')}
        </Button>
      </Stack>
      {folders.map((folder) => (
        <Accordion key={folder.id} defaultExpanded={folder.id === ''}>
          <AccordionSummary expandIcon={<ExpandMoreRounded />}>
            <Typography>
              {folder.name} ·{' '}
              {
                subscriptions.filter(
                  (profile) =>
                    (draft.profile_folders[profile.uid] ?? '') === folder.id,
                ).length
              }
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Stack spacing={1.5}>
              {folder.id && (
                <Stack
                  direction="row"
                  spacing={1}
                  sx={{ alignItems: 'center' }}
                >
                  <TextField
                    size="small"
                    label={text('rename')}
                    value={folder.name}
                    onChange={(event) =>
                      onChange({
                        ...draft,
                        folders: draft.folders.map((item) =>
                          item.id === folder.id
                            ? { ...item, name: event.target.value }
                            : item,
                        ),
                      })
                    }
                  />
                  <IconButton
                    aria-label={text('moveUp')}
                    size="small"
                    onClick={() => move(folder.id, -1)}
                  >
                    <ArrowUpwardRounded />
                  </IconButton>
                  <IconButton
                    aria-label={text('moveDown')}
                    size="small"
                    onClick={() => move(folder.id, 1)}
                  >
                    <ArrowDownwardRounded />
                  </IconButton>
                  <IconButton
                    aria-label={text('delete')}
                    size="small"
                    onClick={() => {
                      const profile_folders = Object.fromEntries(
                        Object.entries(draft.profile_folders).filter(
                          ([, id]) => id !== folder.id,
                        ),
                      )
                      onChange({
                        ...draft,
                        profile_folders,
                        folders: draft.folders.filter(
                          (item) => item.id !== folder.id,
                        ),
                      })
                    }}
                  >
                    <DeleteOutlineRounded />
                  </IconButton>
                </Stack>
              )}
              {subscriptions
                .filter(
                  (profile) =>
                    (draft.profile_folders[profile.uid] ?? '') === folder.id,
                )
                .map((profile) => (
                  <Box key={profile.uid}>
                    <Stack
                      direction="row"
                      sx={{ gap: 1, flexWrap: 'wrap', alignItems: 'center' }}
                    >
                      <Typography
                        sx={{ flex: '1 1 150px', overflowWrap: 'anywhere' }}
                      >
                        {profile.name}
                      </Typography>
                      <TextField
                        select
                        size="small"
                        label={text('subscription')}
                        value={draft.profile_roles[profile.uid] ?? 'normal'}
                        onChange={(event) =>
                          onChange({
                            ...draft,
                            profile_roles: {
                              ...draft.profile_roles,
                              [profile.uid]: event.target.value as
                                | 'normal'
                                | 'ingress'
                                | 'egress',
                            },
                          })
                        }
                        sx={{ width: 140 }}
                      >
                        {['normal', 'ingress', 'egress'].map((role) => (
                          <MenuItem key={role} value={role}>
                            {text(role)}
                          </MenuItem>
                        ))}
                      </TextField>
                      <TextField
                        select
                        size="small"
                        label={text('foldersTitle')}
                        value={draft.profile_folders[profile.uid] ?? ''}
                        onChange={(event) =>
                          onChange({
                            ...draft,
                            profile_folders: {
                              ...draft.profile_folders,
                              [profile.uid]: event.target.value,
                            },
                          })
                        }
                        sx={{ width: 140 }}
                      >
                        {folders.map((item) => (
                          <MenuItem key={item.id} value={item.id}>
                            {item.name}
                          </MenuItem>
                        ))}
                      </TextField>
                      <FormControlLabel
                        control={
                          <Checkbox
                            size="small"
                            checked={draft.residential_profiles.includes(
                              profile.uid,
                            )}
                            onChange={(_, checked) =>
                              onChange({
                                ...draft,
                                residential_profiles: checked
                                  ? [...draft.residential_profiles, profile.uid]
                                  : draft.residential_profiles.filter(
                                      (id) => id !== profile.uid,
                                    ),
                              })
                            }
                          />
                        }
                        label={text('residentialNode')}
                      />
                      <Button
                        size="small"
                        onClick={() =>
                          setExpanded((previous) => {
                            const next = new Set(previous)
                            if (next.has(profile.uid)) next.delete(profile.uid)
                            else next.add(profile.uid)
                            return next
                          })
                        }
                      >
                        {text(
                          expanded.has(profile.uid)
                            ? 'collapseNodes'
                            : 'expandNodes',
                        )}
                      </Button>
                    </Stack>
                    {expanded.has(profile.uid) && (
                      <Box sx={{ mt: 2 }}>
                        <NodeBrowser
                          draft={draft}
                          onChange={onChange}
                          subscription={profile.uid}
                        />
                      </Box>
                    )}
                    <Divider sx={{ mt: 1.5 }} />
                  </Box>
                ))}
            </Stack>
          </AccordionDetails>
        </Accordion>
      ))}
    </Stack>
  )
}

const MECHANISM_SECTIONS: {
  title: string
  fields: [keyof RoutingMechanisms, number, number][]
}[] = [
  {
    title: 'probeSettings',
    fields: [
      ['fast_concurrency', 1, 8],
      ['retry_concurrency', 1, 2],
      ['ai_concurrency', 1, 2],
      ['fast_timeout_ms', 1000, 8000],
      ['retry_timeout_ms', 1000, 20000],
      ['initial_batch', 1, 64],
      ['general_budget', 1, 512],
      ['recovery_budget', 1, 20000],
    ],
  },
  {
    title: 'recoverySettings',
    fields: [
      ['general_failures', 1, 10],
      ['ai_failures', 1, 10],
      ['general_retry_seconds', 1, 60],
      ['ai_retry_seconds', 5, 120],
      ['recovery_retry_seconds', 5, 300],
      ['recovery_max_retry_seconds', 5, 900],
      ['failure_cooldown_seconds', 10, 1800],
    ],
  },
  {
    title: 'stabilitySettings',
    fields: [
      ['switch_min_seconds', 10, 600],
      ['improvement_percent', 5, 80],
      ['improvement_ms', 0, 1000],
      ['network_debounce_seconds', 1, 30],
      ['cache_ttl_hours', 1, 720],
      ['cache_network_limit', 1, 32],
    ],
  },
]

function mechanismBounds(
  draft: RoutingConfig,
  key: keyof RoutingMechanisms,
  min: number,
  max: number,
): [number, number] {
  const minimum =
    key === 'retry_timeout_ms'
      ? draft.mechanisms.fast_timeout_ms
      : key === 'general_budget'
        ? draft.mechanisms.initial_batch
        : key === 'recovery_budget'
          ? draft.mechanisms.general_budget
          : key === 'recovery_max_retry_seconds'
            ? draft.mechanisms.recovery_retry_seconds
            : min
  return [minimum, max]
}

const invalidNumber = (value: number, min: number, max: number) =>
  !Number.isInteger(value) || value < min || value > max

function MechanismEditor({ draft, onChange }: BrowserProps) {
  const { text } = useLabels()
  const { data: defaults } = useQuery({
    queryKey: ['routingDefaults'],
    queryFn: getRoutingDefaults,
  })
  return (
    <Stack spacing={2}>
      <Alert severity="info">{text('mechanismHint')}</Alert>
      {MECHANISM_SECTIONS.map((section) => (
        <Accordion key={section.title} defaultExpanded>
          <AccordionSummary expandIcon={<ExpandMoreRounded />}>
            <Typography>{text(section.title)}</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Box
              sx={{
                display: 'grid',
                gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' },
                gap: 2,
              }}
            >
              {section.fields.map(([key, minimum, maximum]) => {
                const [min, max] = mechanismBounds(draft, key, minimum, maximum)
                return (
                  <TextField
                    key={key}
                    size="small"
                    type="number"
                    label={text(`mechanisms.${key}`)}
                    value={draft.mechanisms[key]}
                    error={invalidNumber(draft.mechanisms[key], min, max)}
                    helperText={text('parameterRange', { min, max })}
                    slotProps={{ htmlInput: { min, max, step: 1 } }}
                    onChange={(event) =>
                      onChange({
                        ...draft,
                        mechanisms: {
                          ...draft.mechanisms,
                          [key]: Number(event.target.value),
                        },
                      })
                    }
                  />
                )
              })}
            </Box>
          </AccordionDetails>
        </Accordion>
      ))}
      <TextField
        size="small"
        type="number"
        label={text('probePort')}
        helperText={text('probePortHint')}
        value={draft.probe_base_port}
        slotProps={{ htmlInput: { min: 1024, max: 65515 } }}
        onChange={(event) =>
          onChange({ ...draft, probe_base_port: Number(event.target.value) })
        }
      />
      <Button
        variant="outlined"
        disabled={!defaults}
        onClick={() => {
          if (defaults)
            onChange({
              ...draft,
              mechanisms: structuredClone(defaults),
              health_interval: 15,
              ai_health_interval: 60,
            })
        }}
      >
        {text('restoreMechanisms')}
      </Button>
    </Stack>
  )
}

function RoutingStatusPanel() {
  const { text } = useLabels()
  const { data, error } = useQuery({
    queryKey: ['routingStatus'],
    queryFn: getRoutingStatus,
    refetchInterval: 2000,
  })
  const phase = data?.phase || 'disabled'
  const groups = [
    text('general'),
    'OpenAI / Codex',
    'Claude / Claude Code',
    'Gemini / AI Studio',
  ]
  const stages: [string, number, number][] = [
    [text('fast'), data?.fast_done ?? 0, data?.fast_target ?? 0],
    [text('retry'), data?.retry_done ?? 0, data?.retry_total ?? 0],
    [text('ai'), data?.ai_done ?? 0, data?.ai_target ?? 0],
  ]
  return (
    <Stack spacing={2}>
      {error && <Alert severity="error">{errorDetail(error)}</Alert>}
      <Alert
        severity={
          phase === 'error'
            ? 'error'
            : phase === 'monitoring'
              ? 'success'
              : 'info'
        }
      >
        {text(`phase.${phase}`)}
        {phase !== 'error' && data?.reason
          ? ` · ${text(`reason.${data.reason}`)}`
          : ''}
      </Alert>
      {data?.last_error && <Alert severity="warning">{data.last_error}</Alert>}
      {data?.network_pending && (
        <Alert severity="info">{text('networkPending')}</Alert>
      )}
      {data?.next_retry_at && (
        <Typography variant="body2" color="text.secondary">
          {text('nextRecovery')} ·{' '}
          {new Date(data.next_retry_at).toLocaleTimeString()}
        </Typography>
      )}
      {data?.next_optimization_at && (
        <Typography variant="body2" color="text.secondary">
          {text('nextOptimization')} ·{' '}
          {new Date(data.next_optimization_at).toLocaleTimeString()}
        </Typography>
      )}
      <Stack direction="row" sx={{ gap: 1, flexWrap: 'wrap' }}>
        <Chip
          label={`${text('firstRoute')}: ${data?.first_route_ms == null ? text('waiting') : `${(data.first_route_ms / 1000).toFixed(1)} s`}`}
        />
        <Chip
          label={`${text('elapsed')}: ${((data?.elapsed_ms ?? 0) / 1000).toFixed(1)} s`}
        />
      </Stack>
      {stages.map(([label, done, target]) => (
        <Box key={label}>
          <Typography variant="body2" gutterBottom>
            {label} · {done} / {target}
          </Typography>
          <LinearProgress
            variant="determinate"
            value={target > 0 ? Math.min(100, (done * 100) / target) : 0}
          />
        </Box>
      ))}
      {data?.current.map((selection, index) => (
        <Box key={groups[index]}>
          <Typography variant="subtitle1">{groups[index]}</Typography>
          {!data.enabled_groups[index] ? (
            <Typography variant="body2" color="text.secondary">
              {text('serviceDisabled')}
            </Typography>
          ) : !selection ? (
            <Typography variant="body2" color="text.secondary">
              {text(
                data.eligible_candidates[index] === 0
                  ? 'noEligible'
                  : 'waiting',
              )}
            </Typography>
          ) : (
            <Stack spacing={0.75}>
              <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
                {selection.route.hops === 2
                  ? `${selection.route.ingress_name} → `
                  : ''}
                {selection.route.egress_name}
                {selection.mirrored && index > 0 ? ` · ${text('mirroredLine')}` : ''}
                {selection.exit?.ip
                  ? ` · ${text('exitIp')}: ${selection.exit.ip} (${selection.exit.country})`
                  : ''}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {selection.route.country || '—'} · {selection.quality.millis} ms
                · {text('failures')}: {selection.failures} · {text('exitSince')}
                : {new Date(selection.exit_since).toLocaleTimeString()}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {text(
                  data.checking[index]
                    ? 'checking'
                    : data.recovering[index]
                      ? 'recoveringService'
                      : selection.mirrored
                        ? 'mirroredLine'
                        : index > 0
                          ? 'holdingExit'
                          : 'healthyRoute',
                )}{' '}
                · {text('nextCheck')}:{' '}
                {data.next_checks[index]
                  ? new Date(data.next_checks[index]).toLocaleTimeString()
                  : '—'}
              </Typography>
              {selection.ai?.endpoints.map((endpoint) => (
                <Stack
                  direction="row"
                  key={endpoint.name}
                  sx={{
                    justifyContent: 'space-between',
                    gap: 1,
                    flexWrap: 'wrap',
                  }}
                >
                  <Typography variant="body2">{endpoint.name}</Typography>
                  <Typography
                    variant="body2"
                    color={
                      endpoint.state === 'failed'
                        ? 'error.main'
                        : endpoint.state === 'challenged'
                          ? 'warning.main'
                          : 'text.secondary'
                    }
                  >
                    {text(`proof.${endpoint.state}`)} · {endpoint.code || '—'} ·{' '}
                    {endpoint.millis} ms
                  </Typography>
                </Stack>
              ))}
            </Stack>
          )}
          <Divider sx={{ mt: 1.5 }} />
        </Box>
      ))}
      <Button
        variant="outlined"
        disabled={!data?.enabled || phase === 'waiting_core'}
        onClick={() => {
          void mutate(reevaluateRouting, {
            id: 'routing-settings',
            revalidate: [['routingStatus']],
          }).catch(() => {})
        }}
      >
        {text('reevaluate')}
      </Button>
    </Stack>
  )
}

export function RoutingViewer({
  open,
  onClose,
  initialTab = 0,
}: {
  open: boolean
  onClose: () => void
  initialTab?: number
}) {
  const { text } = useLabels()
  const { data, error } = useQuery({
    queryKey: ['routingConfig'],
    queryFn: getRoutingConfig,
    enabled: open,
  })
  const [draftChanges, setDraftChanges] = useState<RoutingConfig | null>(null)
  const draft = draftChanges ?? data ?? null
  const [tab, setTab] = useState(initialTab)
  const [preference, setPreference] = useState<Preference>('ingress')
  const [knownNodes, setKnownNodes] = useState<Record<string, RoutingNode>>({})
  const [saving, setSaving] = useState(false)
  const rememberNodes = useCallback(
    (nodes: RoutingNode[]) =>
      setKnownNodes((previous) => {
        const next = { ...previous }
        let changed = false
        for (const node of nodes) {
          if (previous[node.id] !== node) {
            next[node.id] = node
            changed = true
          }
        }
        return changed ? next : previous
      }),
    [],
  )
  const list = draft
    ? preference === 'ingress'
      ? draft.preferred_ingress
      : preference === 'egress'
        ? draft.preferred_egress
        : (draft.ai_priorities[preference] ?? [])
    : []
  const updateList = (list: string[]) => {
    if (!draft) return
    setDraftChanges(
      preference === 'ingress'
        ? { ...draft, preferred_ingress: list }
        : preference === 'egress'
          ? { ...draft, preferred_egress: list }
          : {
              ...draft,
              ai_priorities: { ...draft.ai_priorities, [preference]: list },
            },
    )
  }
  const invalid =
    draft &&
    (invalidNumber(draft.health_interval, 5, 60) ||
      invalidNumber(draft.ai_health_interval, 30, 300) ||
      invalidNumber(draft.probe_base_port, 1024, 65515) ||
      MECHANISM_SECTIONS.some((section) =>
        section.fields.some(([key, min, max]) => {
          const [lower, upper] = mechanismBounds(draft, key, min, max)
          return invalidNumber(draft.mechanisms[key], lower, upper)
        }),
      ))
  const save = async () => {
    if (!draft) return
    setSaving(true)
    try {
      const result = await mutate(() => saveRoutingSettings(draft), {
        id: 'routing-settings',
        onFulfilled: (value) => {
          setDraftChanges(structuredClone(value))
          void setCacheData(['routingConfig'], value)
        },
        revalidate: [['routingStatus']],
      })
      if (result.ok) setTab(3)
    } catch {
      /* The shared mutation reports the backend error. */
    } finally {
      setSaving(false)
    }
  }
  return (
    <Dialog
      open={open}
      onClose={() => {
        if (!saving) onClose()
      }}
      maxWidth="md"
      fullWidth
    >
      <DialogTitle>{text('title')}</DialogTitle>
      <DialogContent>
        <Tabs
          value={tab}
          onChange={(_, value: number) => setTab(value)}
          variant="scrollable"
          scrollButtons="auto"
          sx={{ mb: 2 }}
        >
          {['policy', 'subscriptions', 'nodes', 'status', 'mechanisms'].map(
            (key) => (
              <Tab key={key} label={text(`tabs.${key}`)} />
            ),
          )}
        </Tabs>
        {error && <Alert severity="error">{errorDetail(error)}</Alert>}
        {!draft && <LinearProgress />}
        {draft && tab === 0 && (
          <Stack spacing={2}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={draft.enabled}
                  onChange={(_, checked) =>
                    setDraftChanges({ ...draft, enabled: checked })
                  }
                />
              }
              label={text('enabled')}
            />
            <Alert severity="info">{text('roleHint')}</Alert>
            {Object.values(draft.profile_roles).includes('ingress') &&
              Object.values(draft.profile_roles).includes('egress') && (
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={draft.allow_direct_fallback}
                      onChange={(_, checked) =>
                        setDraftChanges({
                          ...draft,
                          allow_direct_fallback: checked,
                        })
                      }
                    />
                  }
                  label={text('directFallback')}
                />
              )}
            <Typography variant="body2" color="text.secondary">
              {text('chainHint')}
            </Typography>
            <Typography variant="subtitle2">{text('aiServices')}</Typography>
            {(
              [
                'OpenAI / Codex',
                'Claude / Claude Code',
                'Gemini / AI Studio',
              ] as const
            ).map((label, index) => (
              <FormControlLabel
                key={label}
                control={
                  <Checkbox
                    checked={draft.ai_enabled[index]}
                    onChange={(_, checked) => {
                      const enabled = [
                        ...draft.ai_enabled,
                      ] as RoutingConfig['ai_enabled']
                      enabled[index] = checked
                      setDraftChanges({ ...draft, ai_enabled: enabled })
                    }}
                  />
                }
                label={label}
              />
            ))}
            <Autocomplete
              multiple
              options={COUNTRIES.filter((country) => country !== 'HK')}
              value={draft.ai_excluded_countries.filter(
                (country) => country !== 'HK',
              )}
              onChange={(_, countries) =>
                setDraftChanges({
                  ...draft,
                  ai_excluded_countries: ['HK', ...countries],
                })
              }
              renderInput={(params) => (
                <TextField
                  {...params}
                  size="small"
                  label={text('aiExcluded')}
                  helperText={text('aiExcludedHint')}
                />
              )}
            />
            <FormControlLabel
              control={
                <Checkbox
                  checked={draft.prefer_residential}
                  onChange={(_, checked) =>
                    setDraftChanges({ ...draft, prefer_residential: checked })
                  }
                />
              }
              label={text('residential')}
            />
            <TextField
              size="small"
              type="number"
              label={text('health')}
              error={invalidNumber(draft.health_interval, 5, 60)}
              helperText={text('parameterRange', { min: 5, max: 60 })}
              slotProps={{ htmlInput: { min: 5, max: 60, step: 1 } }}
              value={draft.health_interval}
              onChange={(event) =>
                setDraftChanges({
                  ...draft,
                  health_interval: Number(event.target.value),
                })
              }
            />
            <TextField
              size="small"
              type="number"
              label={text('aiHealth')}
              error={invalidNumber(draft.ai_health_interval, 30, 300)}
              helperText={text('parameterRange', { min: 30, max: 300 })}
              slotProps={{ htmlInput: { min: 30, max: 300, step: 1 } }}
              value={draft.ai_health_interval}
              onChange={(event) =>
                setDraftChanges({
                  ...draft,
                  ai_health_interval: Number(event.target.value),
                })
              }
            />
            <Alert severity="info">{text('policyHint')}</Alert>
          </Stack>
        )}
        {draft && tab === 1 && (
          <FolderEditor draft={draft} onChange={setDraftChanges} />
        )}
        {draft && tab === 2 && (
          <Stack spacing={2}>
            <TextField
              select
              size="small"
              value={preference}
              onChange={(event) =>
                setPreference(event.target.value as Preference)
              }
              label={text('tabs.nodes')}
            >
              <MenuItem value="ingress">{text('ingress')}</MenuItem>
              <MenuItem value="egress">{text('egress')}</MenuItem>
              <MenuItem value="OPENAI">OpenAI / Codex</MenuItem>
              <MenuItem value="CLAUDE">Claude / Claude Code</MenuItem>
              <MenuItem value="GEMINI">Gemini / AI Studio</MenuItem>
            </TextField>
            <Typography variant="body2" color="text.secondary">
              {text('priorityHint')}
            </Typography>
            <List dense>
              {list.map((id, index) => (
                <ListItem key={id} sx={{ gap: 1 }}>
                  <ListItemText
                    primary={`${index + 1}. ${knownNodes[id]?.name ?? text('notLoaded')}`}
                    secondary={knownNodes[id]?.subscription ?? id}
                  />
                  <IconButton
                    size="small"
                    disabled={index === 0}
                    aria-label={text('moveUp')}
                    onClick={() => {
                      const next = [...list]
                      ;[next[index - 1], next[index]] = [
                        next[index],
                        next[index - 1],
                      ]
                      updateList(next)
                    }}
                  >
                    <ArrowUpwardRounded />
                  </IconButton>
                  <IconButton
                    size="small"
                    disabled={index + 1 === list.length}
                    aria-label={text('moveDown')}
                    onClick={() => {
                      const next = [...list]
                      ;[next[index + 1], next[index]] = [
                        next[index],
                        next[index + 1],
                      ]
                      updateList(next)
                    }}
                  >
                    <ArrowDownwardRounded />
                  </IconButton>
                  <IconButton
                    size="small"
                    aria-label={text('removePriority')}
                    onClick={() =>
                      updateList(list.filter((value) => value !== id))
                    }
                  >
                    <RemoveRounded />
                  </IconButton>
                </ListItem>
              ))}
            </List>
            <NodeBrowser
              draft={draft}
              onChange={setDraftChanges}
              preference={preference}
              onAdd={(node) => {
                if (!list.includes(node.id) && list.length < 64)
                  updateList([...list, node.id])
              }}
              onLoaded={rememberNodes}
            />
          </Stack>
        )}
        {tab === 3 && <RoutingStatusPanel />}
        {draft && tab === 4 && (
          <MechanismEditor draft={draft} onChange={setDraftChanges} />
        )}
      </DialogContent>
      <DialogActions>
        <Button disabled={saving} onClick={onClose}>
          {text('close')}
        </Button>
        <Button
          variant="contained"
          loading={saving}
          disabled={!draft || !!invalid}
          onClick={() => {
            void save()
          }}
        >
          {text('save')}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
