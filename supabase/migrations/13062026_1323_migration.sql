-- CloudTMS targeted staged TIMESHEET lifecycle invariant.
-- Scope: manual_timesheet_queue active contract-week STAGED TIMESHEET candidates only.
-- Business rule: one active STAGED TIMESHEET candidate per contract_week_id.
-- Same full storage key duplicates are repaired by deactivating all but the canonical row.
-- Different full storage key conflicts are not auto-repaired; deployment stops with STAGED_TIMESHEET_CONFLICT.
-- Rerun safety: this block is idempotent. Re-running recreates the invariant DDL, but does not
-- rewrite unchanged canonical rows or refresh repair timestamps unnecessarily.

DO $migration$
DECLARE
  v_missing_storage jsonb;
  v_different_key_conflicts jsonb;
BEGIN
  EXECUTE 'LOCK TABLE public.manual_timesheet_queue IN SHARE ROW EXCLUSIVE MODE';

  EXECUTE 'DROP INDEX IF EXISTS public.uq_manual_timesheet_queue_one_active_staged_timesheet_per_contract_week';

  EXECUTE 'ALTER TABLE public.manual_timesheet_queue DROP CONSTRAINT IF EXISTS chk_manual_timesheet_queue_active_staged_timesheet_storage_key';

  WITH active_timesheet AS (
    SELECT
      mq.id,
      NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') AS contract_week_id,
      NULLIF(
        regexp_replace(
          COALESCE(
            NULLIF(BTRIM(mq.r2_key), ''),
            NULLIF(BTRIM(mq.meta_json->>'r2_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'storage_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'file_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'canonical_key'), ''),
            ''
          ),
          '^/+',
          ''
        ),
        ''
      ) AS storage_key
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') IS NOT NULL
      AND UPPER(
        COALESCE(
          NULLIF(BTRIM(mq.meta_json->>'staged_kind'), ''),
          NULLIF(BTRIM(mq.meta_json->>'kind'), ''),
          NULLIF(BTRIM(mq.meta_json->>'attached_kind'), ''),
          'TIMESHEET'
        )
      ) = 'TIMESHEET'
  )
  SELECT jsonb_agg(
           jsonb_build_object(
             'contract_week_id', active_timesheet.contract_week_id,
             'queue_id', active_timesheet.id::text,
             'reason', 'missing_storage_key'
           )
           ORDER BY active_timesheet.contract_week_id, active_timesheet.id::text
         )
    INTO v_missing_storage
  FROM active_timesheet
  WHERE active_timesheet.storage_key IS NULL;

  IF COALESCE(jsonb_array_length(v_missing_storage), 0) > 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_TIMESHEET_EVIDENCE',
      DETAIL = v_missing_storage::text,
      HINT = 'Repair active STAGED TIMESHEET rows with missing storage keys before adding the invariant.';
  END IF;

  WITH active_timesheet AS (
    SELECT
      mq.id,
      mq.uploaded_at_utc,
      NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') AS contract_week_id,
      NULLIF(
        regexp_replace(
          COALESCE(
            NULLIF(BTRIM(mq.r2_key), ''),
            NULLIF(BTRIM(mq.meta_json->>'r2_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'storage_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'file_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'canonical_key'), ''),
            ''
          ),
          '^/+',
          ''
        ),
        ''
      ) AS storage_key
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') IS NOT NULL
      AND UPPER(
        COALESCE(
          NULLIF(BTRIM(mq.meta_json->>'staged_kind'), ''),
          NULLIF(BTRIM(mq.meta_json->>'kind'), ''),
          NULLIF(BTRIM(mq.meta_json->>'attached_kind'), ''),
          'TIMESHEET'
        )
      ) = 'TIMESHEET'
  ), grouped AS (
    SELECT
      active_timesheet.contract_week_id,
      COUNT(*) AS active_count,
      COUNT(DISTINCT active_timesheet.storage_key) AS distinct_storage_key_count,
      jsonb_agg(active_timesheet.id::text ORDER BY active_timesheet.uploaded_at_utc ASC NULLS LAST, active_timesheet.id::text ASC) AS queue_ids,
      jsonb_agg(DISTINCT active_timesheet.storage_key) AS storage_keys
    FROM active_timesheet
    GROUP BY active_timesheet.contract_week_id
    HAVING COUNT(*) > 1
       AND COUNT(DISTINCT active_timesheet.storage_key) > 1
  )
  SELECT jsonb_agg(
           jsonb_build_object(
             'contract_week_id', grouped.contract_week_id,
             'active_count', grouped.active_count,
             'distinct_storage_key_count', grouped.distinct_storage_key_count,
             'queue_ids', grouped.queue_ids,
             'storage_keys', grouped.storage_keys,
             'reason', 'different_storage_key_conflict'
           )
           ORDER BY grouped.contract_week_id
         )
    INTO v_different_key_conflicts
  FROM grouped;

  IF COALESCE(jsonb_array_length(v_different_key_conflicts), 0) > 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = v_different_key_conflicts::text,
      HINT = 'Resolve different active STAGED TIMESHEET files manually; this migration only auto-repairs same-full-storage-key duplicates.';
  END IF;

  WITH active_timesheet AS (
    SELECT
      mq.id,
      mq.uploaded_at_utc,
      mq.meta_json,
      NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') AS contract_week_id,
      NULLIF(
        regexp_replace(
          COALESCE(
            NULLIF(BTRIM(mq.r2_key), ''),
            NULLIF(BTRIM(mq.meta_json->>'r2_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'storage_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'file_key'), ''),
            NULLIF(BTRIM(mq.meta_json->>'canonical_key'), ''),
            ''
          ),
          '^/+',
          ''
        ),
        ''
      ) AS storage_key
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') IS NOT NULL
      AND UPPER(
        COALESCE(
          NULLIF(BTRIM(mq.meta_json->>'staged_kind'), ''),
          NULLIF(BTRIM(mq.meta_json->>'kind'), ''),
          NULLIF(BTRIM(mq.meta_json->>'attached_kind'), ''),
          'TIMESHEET'
        )
      ) = 'TIMESHEET'
  ), ranked AS (
    SELECT
      active_timesheet.*,
      FIRST_VALUE(active_timesheet.id) OVER (
        PARTITION BY active_timesheet.contract_week_id
        ORDER BY active_timesheet.uploaded_at_utc ASC NULLS LAST, active_timesheet.id::text ASC
      ) AS canonical_id,
      ROW_NUMBER() OVER (
        PARTITION BY active_timesheet.contract_week_id
        ORDER BY active_timesheet.uploaded_at_utc ASC NULLS LAST, active_timesheet.id::text ASC
      ) AS rn,
      COUNT(*) OVER (PARTITION BY active_timesheet.contract_week_id) AS active_count
    FROM active_timesheet
  ), keepers AS (
    UPDATE public.manual_timesheet_queue AS mq
    SET meta_json = (
          COALESCE(mq.meta_json, '{}'::jsonb)
            - 'deferred_target_timesheet_id'
            - 'materialised_to_timesheet_id'
            - 'materialisation_deferred_to_backend'
            - 'materialisation_deferred_at_utc'
            - 'materialised_storage_key'
            - 'materialised_at_utc'
            - 'deferred_rotation_degrees'
        ) || jsonb_build_object(
          'contract_week_id', ranked.contract_week_id,
          'staged_kind', 'TIMESHEET',
          'staged_timesheet_invariant_cleaned_at_utc', COALESCE(
            mq.meta_json->'staged_timesheet_invariant_cleaned_at_utc',
            to_jsonb(now())
          )
        )
    FROM ranked
    WHERE mq.id = ranked.id
      AND ranked.rn = 1
      AND (
        ranked.active_count > 1
        OR NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') IS DISTINCT FROM ranked.contract_week_id
        OR NULLIF(BTRIM(mq.meta_json->>'staged_kind'), '') IS DISTINCT FROM 'TIMESHEET'
        OR mq.meta_json ?| ARRAY[
          'deferred_target_timesheet_id',
          'materialised_to_timesheet_id',
          'materialisation_deferred_to_backend',
          'materialisation_deferred_at_utc',
          'materialised_storage_key',
          'materialised_at_utc',
          'deferred_rotation_degrees'
        ]
      )
    RETURNING mq.id
  )
  UPDATE public.manual_timesheet_queue AS mq
  SET status = 'DISCARDED',
      timesheet_id = NULL,
      meta_json = (
        COALESCE(mq.meta_json, '{}'::jsonb)
          - 'deferred_target_timesheet_id'
          - 'materialised_to_timesheet_id'
          - 'materialisation_deferred_to_backend'
          - 'materialisation_deferred_at_utc'
          - 'materialised_storage_key'
          - 'materialised_at_utc'
          - 'deferred_rotation_degrees'
      ) || jsonb_build_object(
        'contract_week_id', ranked.contract_week_id,
        'staged_kind', 'TIMESHEET',
        'duplicate_timesheet_evidence_identity', true,
        'duplicate_of_queue_item_id', ranked.canonical_id::text,
        'materialisation_noop_reason', 'same_storage_key_duplicate',
        'same_storage_duplicate_deactivated_at_utc', COALESCE(
          mq.meta_json->'same_storage_duplicate_deactivated_at_utc',
          to_jsonb(now())
        ),
        'staged_timesheet_invariant_repaired_at_utc', COALESCE(
          mq.meta_json->'staged_timesheet_invariant_repaired_at_utc',
          to_jsonb(now())
        )
      )
  FROM ranked
  WHERE mq.id = ranked.id
    AND ranked.rn > 1
    AND ranked.active_count > 1;

  EXECUTE $ddl$
    ALTER TABLE public.manual_timesheet_queue
      ADD CONSTRAINT chk_manual_timesheet_queue_active_staged_timesheet_storage_key
      CHECK (
        NOT (
          status = 'STAGED'
          AND NULLIF(BTRIM(meta_json->>'contract_week_id'), '') IS NOT NULL
          AND UPPER(
            COALESCE(
              NULLIF(BTRIM(meta_json->>'staged_kind'), ''),
              NULLIF(BTRIM(meta_json->>'kind'), ''),
              NULLIF(BTRIM(meta_json->>'attached_kind'), ''),
              'TIMESHEET'
            )
          ) = 'TIMESHEET'
        )
        OR NULLIF(
          regexp_replace(
            COALESCE(
              NULLIF(BTRIM(r2_key), ''),
              NULLIF(BTRIM(meta_json->>'r2_key'), ''),
              NULLIF(BTRIM(meta_json->>'storage_key'), ''),
              NULLIF(BTRIM(meta_json->>'file_key'), ''),
              NULLIF(BTRIM(meta_json->>'canonical_key'), ''),
              ''
            ),
            '^/+',
            ''
          ),
          ''
        ) IS NOT NULL
      )
  $ddl$;

  EXECUTE $ddl$
    CREATE UNIQUE INDEX uq_manual_timesheet_queue_one_active_staged_timesheet_per_contract_week
    ON public.manual_timesheet_queue ((NULLIF(BTRIM(meta_json->>'contract_week_id'), '')))
    WHERE status = 'STAGED'
      AND NULLIF(BTRIM(meta_json->>'contract_week_id'), '') IS NOT NULL
      AND UPPER(
        COALESCE(
          NULLIF(BTRIM(meta_json->>'staged_kind'), ''),
          NULLIF(BTRIM(meta_json->>'kind'), ''),
          NULLIF(BTRIM(meta_json->>'attached_kind'), ''),
          'TIMESHEET'
        )
      ) = 'TIMESHEET'
  $ddl$;

  EXECUTE $ddl$
    COMMENT ON INDEX public.uq_manual_timesheet_queue_one_active_staged_timesheet_per_contract_week IS
      'CloudTMS invariant: at most one active STAGED TIMESHEET candidate per contract week in manual_timesheet_queue.'
  $ddl$;
END;
$migration$;
