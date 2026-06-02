#!/usr/bin/env sh
set -eu

# One-time grouped-scheduling migration helper for legacy deployments.
#
# Run this inside the CVM host, or pipe it through `phala ssh`.
# The script is dry-run by default. It only commits DB changes when EXECUTE=true.
#
# Typical prod dry-run:
#   phala ssh <app_id> -- -i ~/.ssh/clawdi_access_ed25519 'sh -s' \
#     < deploy/migrate-ungrouped-api-keys.sh
#
# Conservative prod execution after reviewing the dry-run:
#   phala ssh <app_id> -- -i ~/.ssh/clawdi_access_ed25519 \
#     'EXECUTE=true MIXED_POLICY=skip IDLE_TARGET=leave SET_ALLOW_UNGROUPED=skip sh -s' \
#     < deploy/migrate-ungrouped-api-keys.sh
#
# Full cleanup execution, only after accepting that mixed-platform keys will be
# assigned to their dominant recent platform and idle keys will default to OpenAI:
#   phala ssh <app_id> -- -i ~/.ssh/clawdi_access_ed25519 \
#     'EXECUTE=true MIXED_POLICY=dominant DOMINANT_RATIO=0 IDLE_TARGET=openai SET_ALLOW_UNGROUPED=false sh -s' \
#     < deploy/migrate-ungrouped-api-keys.sh
#
# Policy knobs:
#   EXECUTE=false                 Dry-run unless explicitly true.
#   LOOKBACK_DAYS=30              Usage window used to classify active ungrouped keys.
#   OPENAI_GROUP_NAME=openai      Existing OpenAI target group.
#   ANTHROPIC_GROUP_NAME=kimi     Anthropic/Kimi target group to create/reuse.
#   MIXED_POLICY=fail             fail | skip | dominant.
#                                 fail: abort if one key used multiple platforms recently.
#                                 skip: leave mixed keys ungrouped for manual handling.
#                                 dominant: route mixed keys to dominant platform if ratio passes.
#   DOMINANT_RATIO=0.98           Minimum dominant-platform ratio for MIXED_POLICY=dominant.
#   IDLE_TARGET=leave             openai | anthropic | leave for active keys with no recent usage.
#   SET_ALLOW_UNGROUPED=skip      true | false | skip. Usually false only after clean migration.

EXECUTE="${EXECUTE:-false}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-30}"
OPENAI_GROUP_NAME="${OPENAI_GROUP_NAME:-openai}"
ANTHROPIC_GROUP_NAME="${ANTHROPIC_GROUP_NAME:-kimi}"
MIXED_POLICY="${MIXED_POLICY:-fail}"
DOMINANT_RATIO="${DOMINANT_RATIO:-0.98}"
IDLE_TARGET="${IDLE_TARGET:-leave}"
SET_ALLOW_UNGROUPED="${SET_ALLOW_UNGROUPED:-skip}"

case "$EXECUTE" in true|false) ;; *) echo "EXECUTE must be true or false" >&2; exit 2 ;; esac
case "$MIXED_POLICY" in fail|skip|dominant) ;; *) echo "MIXED_POLICY must be fail, skip, or dominant" >&2; exit 2 ;; esac
case "$IDLE_TARGET" in openai|anthropic|leave) ;; *) echo "IDLE_TARGET must be openai, anthropic, or leave" >&2; exit 2 ;; esac
case "$SET_ALLOW_UNGROUPED" in true|false|skip) ;; *) echo "SET_ALLOW_UNGROUPED must be true, false, or skip" >&2; exit 2 ;; esac

echo "=== migrate-ungrouped-api-keys config ==="
echo "EXECUTE=$EXECUTE"
echo "LOOKBACK_DAYS=$LOOKBACK_DAYS"
echo "OPENAI_GROUP_NAME=$OPENAI_GROUP_NAME"
echo "ANTHROPIC_GROUP_NAME=$ANTHROPIC_GROUP_NAME"
echo "MIXED_POLICY=$MIXED_POLICY"
echo "DOMINANT_RATIO=$DOMINANT_RATIO"
echo "IDLE_TARGET=$IDLE_TARGET"
echo "SET_ALLOW_UNGROUPED=$SET_ALLOW_UNGROUPED"

docker exec -i sub2api-postgres sh -lc 'psql -U "${POSTGRES_USER:-sub2api}" -d "${POSTGRES_DB:-sub2api}" -v ON_ERROR_STOP=1 \
  -v execute="'"$EXECUTE"'" \
  -v lookback_days="'"$LOOKBACK_DAYS"'" \
  -v openai_group_name="'"$OPENAI_GROUP_NAME"'" \
  -v anthropic_group_name="'"$ANTHROPIC_GROUP_NAME"'" \
  -v mixed_policy="'"$MIXED_POLICY"'" \
  -v dominant_ratio="'"$DOMINANT_RATIO"'" \
  -v idle_target="'"$IDLE_TARGET"'" \
  -v set_allow_ungrouped="'"$SET_ALLOW_UNGROUPED"'"' <<'SQL'
\echo === database_transaction ===
BEGIN;

\echo === target_group_validation ===
SELECT :'openai_group_name' AS openai_group_name,
       :'anthropic_group_name' AS anthropic_group_name,
       :'mixed_policy' AS mixed_policy,
       :'idle_target' AS idle_target,
       :'execute'::boolean AS execute;

-- OpenAI target must already exist. This prevents accidentally creating a
-- second OpenAI pool with no accounts attached.
WITH existing_openai_group AS (
  SELECT COUNT(*) AS group_count
  FROM groups
  WHERE deleted_at IS NULL
    AND status = 'active'
    AND platform = 'openai'
    AND name = :'openai_group_name'
)
SELECT 1 / CASE WHEN group_count = 1 THEN 1 ELSE 0 END AS openai_group_exists_guard
FROM existing_openai_group;

-- Create/reuse the Anthropic group for Kimi traffic. Dry-run rolls this back.
INSERT INTO groups (name, description, platform, subscription_type, rate_multiplier, is_exclusive, status, created_at, updated_at)
SELECT :'anthropic_group_name',
       'Kimi/Anthropic routing group created by deploy/migrate-ungrouped-api-keys.sh',
       'anthropic',
       'standard',
       1.0,
       FALSE,
       'active',
       NOW(),
       NOW()
WHERE NOT EXISTS (
  SELECT 1 FROM groups
  WHERE deleted_at IS NULL
    AND name = :'anthropic_group_name'
);

CREATE TEMP TABLE _target_groups AS
SELECT 'openai'::text AS platform, id AS group_id, name
FROM groups
WHERE deleted_at IS NULL
  AND status = 'active'
  AND platform = 'openai'
  AND name = :'openai_group_name'
UNION ALL
SELECT 'anthropic'::text AS platform, id AS group_id, name
FROM groups
WHERE deleted_at IS NULL
  AND status = 'active'
  AND platform = 'anthropic'
  AND name = :'anthropic_group_name';

SELECT * FROM _target_groups ORDER BY platform;

-- Attach active Kimi/Anthropic accounts to the Anthropic group. Dry-run rolls this back.
INSERT INTO account_groups (account_id, group_id, priority, created_at)
SELECT a.id, tg.group_id, COALESCE(a.priority, 50), NOW()
FROM accounts a
JOIN _target_groups tg ON tg.platform = 'anthropic'
WHERE a.deleted_at IS NULL
  AND a.platform = 'anthropic'
  AND a.status = 'active'
  AND a.schedulable = TRUE
  AND (
    a.credentials->'model_mapping' ? 'kimi-for-coding'
    OR a.credentials->'model_mapping' ? 'kimi-for-coding-thinking'
  )
ON CONFLICT (account_id, group_id) DO NOTHING;

\echo === active_ungrouped_before ===
SELECT COUNT(*) AS active_ungrouped_keys
FROM api_keys
WHERE deleted_at IS NULL
  AND status = 'active'
  AND group_id IS NULL;

CREATE TEMP TABLE _active_ungrouped_keys AS
SELECT id AS api_key_id, user_id, name
FROM api_keys
WHERE deleted_at IS NULL
  AND status = 'active'
  AND group_id IS NULL;

CREATE TEMP TABLE _recent_key_platform_usage AS
SELECT ul.api_key_id, a.platform, COUNT(*)::bigint AS requests
FROM usage_logs ul
JOIN accounts a ON a.id = ul.account_id
JOIN _active_ungrouped_keys k ON k.api_key_id = ul.api_key_id
WHERE ul.created_at >= NOW() - (:'lookback_days'::int * INTERVAL '1 day')
GROUP BY ul.api_key_id, a.platform;

CREATE TEMP TABLE _key_classification AS
WITH ranked AS (
  SELECT
    api_key_id,
    platform,
    requests,
    SUM(requests) OVER (PARTITION BY api_key_id) AS total_requests,
    COUNT(*) OVER (PARTITION BY api_key_id) AS platform_count,
    ROW_NUMBER() OVER (PARTITION BY api_key_id ORDER BY requests DESC, platform) AS rn
  FROM _recent_key_platform_usage
),
per_key AS (
  SELECT
    api_key_id,
    MAX(platform) FILTER (WHERE rn = 1) AS top_platform,
    MAX(requests) FILTER (WHERE rn = 1) AS top_requests,
    MAX(total_requests) AS total_requests,
    MAX(platform_count) AS platform_count,
    ARRAY_AGG(platform || ':' || requests ORDER BY requests DESC, platform) AS platform_counts
  FROM ranked
  GROUP BY api_key_id
)
SELECT
  k.api_key_id,
  k.user_id,
  k.name,
  COALESCE(p.total_requests, 0) AS total_requests,
  COALESCE(p.platform_count, 0) AS platform_count,
  p.platform_counts,
  CASE
    WHEN p.api_key_id IS NULL THEN
      CASE WHEN :'idle_target' IN ('openai', 'anthropic') THEN :'idle_target' ELSE 'idle' END
    WHEN p.platform_count = 1 THEN p.top_platform
    WHEN :'mixed_policy' = 'dominant'
      AND (p.top_requests::numeric / NULLIF(p.total_requests, 0)) >= :'dominant_ratio'::numeric
      THEN p.top_platform
    ELSE 'mixed'
  END AS target_platform
FROM _active_ungrouped_keys k
LEFT JOIN per_key p ON p.api_key_id = k.api_key_id;

\echo === classification_summary ===
SELECT target_platform, COUNT(*) AS keys, SUM(total_requests) AS recent_requests
FROM _key_classification
GROUP BY target_platform
ORDER BY target_platform;

\echo === mixed_key_sample ===
SELECT kc.api_key_id, ak.name, u.email, kc.total_requests, kc.platform_counts
FROM _key_classification kc
JOIN api_keys ak ON ak.id = kc.api_key_id
JOIN users u ON u.id = kc.user_id
WHERE kc.target_platform = 'mixed'
ORDER BY kc.total_requests DESC, kc.api_key_id
LIMIT 50;

-- Default policy is fail: a mixed key cannot be perfectly migrated to a single
-- group without changing one of its historical platform behaviors.
WITH mixed_keys AS (
  SELECT COUNT(*) AS key_count
  FROM _key_classification
  WHERE target_platform = 'mixed'
)
SELECT 1 / CASE
  WHEN :'mixed_policy' = 'fail' AND key_count > 0 THEN 0
  ELSE 1
END AS mixed_key_guard
FROM mixed_keys;

CREATE TEMP TABLE _planned_updates AS
SELECT kc.api_key_id, kc.user_id, kc.target_platform, tg.group_id
FROM _key_classification kc
JOIN _target_groups tg ON tg.platform = kc.target_platform
WHERE kc.target_platform IN ('openai', 'anthropic');

\echo === planned_updates ===
SELECT target_platform, group_id, COUNT(*) AS keys
FROM _planned_updates
GROUP BY target_platform, group_id
ORDER BY target_platform, group_id;

UPDATE api_keys ak
SET group_id = pu.group_id,
    updated_at = NOW()
FROM _planned_updates pu
WHERE :'execute'::boolean
  AND ak.id = pu.api_key_id
  AND ak.deleted_at IS NULL
  AND ak.status = 'active'
  AND ak.group_id IS NULL;

\echo === updated_rows ===
SELECT COUNT(*) AS updated_keys
FROM _planned_updates;

INSERT INTO settings (key, value, updated_at)
SELECT 'allow_ungrouped_key_scheduling', :'set_allow_ungrouped', NOW()
WHERE :'execute'::boolean
  AND :'set_allow_ungrouped' IN ('true', 'false')
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    updated_at = NOW();

\echo === active_ungrouped_after_if_execute ===
SELECT
  CASE
    WHEN :'execute'::boolean THEN (
      SELECT COUNT(*)
      FROM api_keys
      WHERE deleted_at IS NULL
        AND status = 'active'
        AND group_id IS NULL
    )
    ELSE NULL
  END AS active_ungrouped_keys_after;

\if :execute
COMMIT;
\echo === committed ===
\else
ROLLBACK;
\echo === dry_run_rolled_back ===
\endif
SQL

if [ "$EXECUTE" = "true" ]; then
  echo "=== clearing Redis API key auth cache ==="
  docker exec sub2api-redis sh -lc 'redis-cli EVAL "local ks=redis.call('\''keys'\'','\''apikey:auth:*'\''); if #ks > 0 then return redis.call('\''del'\'', unpack(ks)) else return 0 end" 0'
else
  echo "=== dry-run complete; Redis cache was not changed ==="
fi
