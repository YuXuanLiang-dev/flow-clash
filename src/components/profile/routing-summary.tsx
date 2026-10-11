import {
  Alert,
  Box,
  Button,
  Chip,
  LinearProgress,
  Stack,
  Typography,
} from '@mui/material'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

import { getRoutingStatus, reevaluateRouting } from '@/services/cmds'
import { mutate } from '@/services/mutate'
import { errorDetail } from '@/services/notice-service'
import { useQuery } from '@/services/query-client'
import type { TranslationKey } from '@/types/generated/i18n-keys'
import type { RoutingConfig } from '@/types/routing'

interface RoutingSummaryProps {
  config?: RoutingConfig
  profiles: IProfileItem[]
  onSettings: () => void
}

export function RoutingSummary({
  config,
  profiles,
  onSettings,
}: RoutingSummaryProps) {
  const { t } = useTranslation()
  const { data: status, error } = useQuery({
    queryKey: ['routingStatus'],
    queryFn: getRoutingStatus,
    refetchInterval: 2000,
  })
  const [searching, setSearching] = useState(false)
  const front = profiles.filter(
    (profile) => config?.profile_roles[profile.uid] === 'ingress',
  )
  const rear = profiles.filter(
    (profile) => config?.profile_roles[profile.uid] === 'egress',
  )
  const chain = front.length > 0 && rear.length > 0
  const phase =
    config?.enabled === false ? 'disabled' : status?.phase || 'waiting_core'
  const current = status?.current[0]
  const rescan = async () => {
    setSearching(true)
    try {
      await mutate(reevaluateRouting, {
        id: 'routing-settings',
        revalidate: [['routingStatus']],
      })
    } catch {
      /* The shared mutation reports the backend error. */
    } finally {
      setSearching(false)
    }
  }

  return (
    <Box
      sx={{
        mx: 0.5,
        mb: 1.5,
        p: 1.5,
        border: 1,
        borderColor: 'divider',
        borderRadius: 2,
      }}
    >
      <Stack
        direction="row"
        sx={{ alignItems: 'center', gap: 1, flexWrap: 'wrap' }}
      >
        <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
          {t('routing.autoTitle')}
        </Typography>
        <Chip
          size="small"
          label={t(chain ? 'routing.twoHops' : 'routing.singleHop')}
        />
        <Chip
          size="small"
          color={
            phase === 'error'
              ? 'error'
              : phase === 'monitoring'
                ? 'success'
                : 'default'
          }
          label={t(`routing.phase.${phase}` as TranslationKey)}
        />
        <Box sx={{ flexGrow: 1 }} />
        <Button
          size="small"
          disabled={!config?.enabled || !status?.candidate_total}
          loading={searching}
          onClick={() => {
            void rescan()
          }}
        >
          {t('routing.reevaluate')}
        </Button>
        <Button size="small" onClick={onSettings}>
          {t('routing.settings')}
        </Button>
      </Stack>
      <Typography variant="body2" color="text.secondary" sx={{ mt: 0.75 }}>
        {t('routing.roleHint')}
      </Typography>
      {(front.length > 0 || rear.length > 0) && (
        <Stack direction="row" sx={{ mt: 1, gap: 1, flexWrap: 'wrap' }}>
          {front.map((profile) => (
            <Chip
              key={profile.uid}
              size="small"
              variant="outlined"
              color="primary"
              label={`${t('routing.ingress')}: ${profile.name}`}
            />
          ))}
          {rear.map((profile) => (
            <Chip
              key={profile.uid}
              size="small"
              variant="outlined"
              color="secondary"
              label={`${t('routing.egress')}: ${profile.name}`}
            />
          ))}
        </Stack>
      )}
      {config?.enabled && current ? (
        <Typography variant="body2" sx={{ mt: 1, overflowWrap: 'anywhere' }}>
          {t('routing.currentRoute')} ·{' '}
          {current.route.hops === 2 ? `${current.route.ingress_name} → ` : ''}
          {current.route.egress_name} · {current.quality.millis} ms
        </Typography>
      ) : (
        <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
          {t('routing.autoHint')}
        </Typography>
      )}
      {config?.enabled && !current && status?.candidate_total ? (
        <LinearProgress sx={{ mt: 1 }} />
      ) : null}
      {error && (
        <Alert severity="error" sx={{ mt: 1 }}>
          {errorDetail(error)}
        </Alert>
      )}
      {status?.last_error && (
        <Alert severity="warning" sx={{ mt: 1 }}>
          {status.last_error}
        </Alert>
      )}
    </Box>
  )
}
