#!/usr/bin/env sh
set -eu

# One-time migration helper for deployments where the product contract is:
# every default API key can call every default model/platform.
#
# Upstream v0.1.126 group isolation cannot represent that contract with one
# api_keys.group_id, because a key can only point at one platform group. The
# compatible migration is therefore to keep default keys ungrouped and keep the
# default serving accounts ungrouped as well.
#
# Dry-run:
#   phala ssh <app_id> -- -i ~/.ssh/clawdi_access_ed25519 'sh -s' \
#     < deploy/migrate-legacy-default-pool.sh
#
# Execute:
#   phala ssh <app_id> -- -i ~/.ssh/clawdi_access_ed25519 \
#     'EXECUTE=true RESTART_APP=true sh -s' \
#     < deploy/migrate-legacy-default-pool.sh

EXECUTE="${EXECUTE:-false}"
DEFAULT_OPENAI_GROUP_NAME="${DEFAULT_OPENAI_GROUP_NAME:-openai}"
SET_ALLOW_UNGROUPED="${SET_ALLOW_UNGROUPED:-true}"
RESTART_APP="${RESTART_APP:-false}"

case "$EXECUTE" in true|false) ;; *) echo "EXECUTE must be true or false" >&2; exit 2 ;; esac
case "$SET_ALLOW_UNGROUPED" in true|false) ;; *) echo "SET_ALLOW_UNGROUPED must be true or false" >&2; exit 2 ;; esac
case "$RESTART_APP" in true|false) ;; *) echo "RESTART_APP must be true or false" >&2; exit 2 ;; esac

echo "=== migrate-legacy-default-pool config ==="
echo "EXECUTE=$EXECUTE"
echo "DEFAULT_OPENAI_GROUP_NAME=$DEFAULT_OPENAI_GROUP_NAME"
echo "SET_ALLOW_UNGROUPED=$SET_ALLOW_UNGROUPED"
echo "RESTART_APP=$RESTART_APP"

docker exec -i sub2api-postgres sh -lc 'psql -U "${POSTGRES_USER:-sub2api}" -d "${POSTGRES_DB:-sub2api}" -v ON_ERROR_STOP=1 \
  -v execute="'"$EXECUTE"'" \
  -v default_openai_group_name="'"$DEFAULT_OPENAI_GROUP_NAME"'" \
  -v set_allow_ungrouped="'"$SET_ALLOW_UNGROUPED"'"' <<'SQL'
\echo === database_transaction ===
BEGIN;

CREATE TEMP TABLE _default_groups AS
SELECT id, name, platform
FROM groups
WHERE deleted_at IS NULL
  AND status = 'active'
  AND name = :'default_openai_group_name'
  AND platform = 'openai';

SELECT 1 / CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END AS default_openai_group_guard
FROM _default_groups;

\echo === current_default_group ===
SELECT * FROM _default_groups;

\echo === active_keys_before ===
SELECT COALESCE(group_id::text, 'NULL') AS group_id, COUNT(*) AS active_keys
FROM api_keys
WHERE deleted_at IS NULL
  AND status = 'active'
GROUP BY group_id
ORDER BY group_id;

\echo === accounts_to_make_ungrouped ===
SELECT a.id, a.name, a.platform, a.type, a.status, a.schedulable, ag.group_id
FROM accounts a
JOIN account_groups ag ON ag.account_id = a.id
JOIN _default_groups dg ON dg.id = ag.group_id
WHERE a.deleted_at IS NULL
ORDER BY a.platform, a.id;

\echo === planned_ungrouped_account_pools ===
WITH planned_ungrouped_accounts AS (
  SELECT a.id, a.platform
  FROM accounts a
  WHERE a.deleted_at IS NULL
    AND a.status = 'active'
    AND a.schedulable = TRUE
    AND (
      NOT EXISTS (SELECT 1 FROM account_groups ag WHERE ag.account_id = a.id)
      OR EXISTS (
        SELECT 1
        FROM account_groups ag
        JOIN _default_groups dg ON dg.id = ag.group_id
        WHERE ag.account_id = a.id
      )
    )
)
SELECT platform, COUNT(*) AS active_schedulable_ungrouped_accounts_after_execute
FROM planned_ungrouped_accounts
GROUP BY platform
ORDER BY platform;

\echo === active_keys_to_unbind ===
SELECT ak.group_id, COUNT(*) AS keys
FROM api_keys ak
JOIN _default_groups dg ON dg.id = ak.group_id
WHERE ak.deleted_at IS NULL
  AND ak.status = 'active'
GROUP BY ak.group_id;

-- Keep default keys ungrouped so each endpoint can select the ungrouped pool
-- for its own platform. This restores legacy "all default keys can call all
-- default models" behavior.
UPDATE api_keys ak
SET group_id = NULL,
    updated_at = NOW()
FROM _default_groups dg
WHERE :'execute'::boolean
  AND ak.group_id = dg.id
  AND ak.deleted_at IS NULL
  AND ak.status = 'active';

-- Move default OpenAI accounts from the grouped pool back into the ungrouped
-- OpenAI pool. Kimi/Anthropic prod account is already ungrouped.
DELETE FROM account_groups ag
USING _default_groups dg
WHERE :'execute'::boolean
  AND ag.group_id = dg.id;

INSERT INTO settings (key, value, updated_at)
VALUES ('allow_ungrouped_key_scheduling', :'set_allow_ungrouped', NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    updated_at = NOW()
WHERE :'execute'::boolean;

\echo === active_keys_after_if_execute ===
SELECT
  CASE WHEN :'execute'::boolean THEN COALESCE(group_id::text, 'NULL') ELSE 'dry-run' END AS group_id,
  COUNT(*) AS active_keys
FROM api_keys
WHERE deleted_at IS NULL
  AND status = 'active'
GROUP BY CASE WHEN :'execute'::boolean THEN COALESCE(group_id::text, 'NULL') ELSE 'dry-run' END
ORDER BY group_id;

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
  if [ "$RESTART_APP" = "true" ]; then
    echo "=== restarting sub2api ==="
    docker restart sub2api
  else
    echo "=== app restart skipped; deployment restart will refresh scheduler state ==="
  fi
else
  echo "=== dry-run complete; DB, Redis, and app process were not changed ==="
fi
