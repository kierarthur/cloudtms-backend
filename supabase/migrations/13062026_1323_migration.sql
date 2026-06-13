-- CloudTMS staged TIMESHEET invariant repair and guard
-- Rerun-safe migration: repairs same-storage active duplicates once, blocks unsafe states,
-- and enforces one active contract-week-bound STAGED TIMESHEET candidate per contract week.

DO $staged_timesheet_invariant$
DECLARE
  v_now timestamptz := now();
  v_missing jsonb;
  v_different jsonb;
BEGIN
  LOCK TABLE public.manual_timesheet_queue IN ACCESS EXCLUSIVE MODE;

  ALTER TABLE public.manual_timesheet_queue
    DROP CONSTRAINT IF EXISTS chk_manual_timesheet_queue_active_staged_timesheet_storage_key;

  DROP INDEX IF EXISTS public.uq_manual_timesheet_queue_one_active_staged_timesheet_per_contract_week;
  DROP INDEX IF EXISTS public.uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr;

  WITH active_timesheet AS (
    SELECT
      mq.id,
      NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') AS contract_week_id,
      NULLIF(
        regexp_replace(
          COALESCE(
            NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'r2_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'storage_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'file_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'canonical_key', '')), ''),
            ''
          ),
          '^/+',
          ''
        ),
        ''
      ) AS storage_key
    FROM public.manual_timesheet_queue mq
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
             'queue_id', atx.id::text,
             'contract_week_id', atx.contract_week_id
           )
           ORDER BY atx.contract_week_id, atx.id::text
         )
    INTO v_missing
  FROM active_timesheet atx
  WHERE atx.storage_key IS NULL;

  IF v_missing IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_TIMESHEET_EVIDENCE',
      DETAIL = jsonb_build_object(
        'reason', 'active_staged_timesheet_missing_storage_key',
        'rows', v_missing
      )::text;
  END IF;

  WITH active_timesheet AS (
    SELECT
      mq.id,
      NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') AS contract_week_id,
      NULLIF(
        regexp_replace(
          COALESCE(
            NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'r2_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'storage_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'file_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'canonical_key', '')), ''),
            ''
          ),
          '^/+',
          ''
        ),
        ''
      ) AS storage_key
    FROM public.manual_timesheet_queue mq
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
      atx.contract_week_id,
      COUNT(DISTINCT atx.storage_key) AS distinct_storage_keys,
      jsonb_agg(DISTINCT atx.storage_key ORDER BY atx.storage_key) AS storage_keys,
      jsonb_agg(atx.id::text ORDER BY atx.id::text) AS queue_ids
    FROM active_timesheet atx
    GROUP BY atx.contract_week_id
    HAVING COUNT(DISTINCT atx.storage_key) > 1
  )
  SELECT jsonb_agg(
           jsonb_build_object(
             'contract_week_id', g.contract_week_id,
             'distinct_storage_keys', g.distinct_storage_keys,
             'storage_keys', g.storage_keys,
             'queue_ids', g.queue_ids
           )
           ORDER BY g.contract_week_id
         )
    INTO v_different
  FROM grouped g;

  IF v_different IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = jsonb_build_object(
        'reason', 'different_active_staged_timesheet_storage_keys',
        'conflicts', v_different
      )::text;
  END IF;

  WITH active_timesheet AS (
    SELECT
      mq.id,
      mq.uploaded_at_utc,
      NULLIF(BTRIM(mq.meta_json->>'contract_week_id'), '') AS contract_week_id,
      NULLIF(
        regexp_replace(
          COALESCE(
            NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'r2_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'storage_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'file_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json->>'canonical_key', '')), ''),
            ''
          ),
          '^/+',
          ''
        ),
        ''
      ) AS storage_key
    FROM public.manual_timesheet_queue mq
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
      atx.*,
      FIRST_VALUE(atx.id) OVER (
        PARTITION BY atx.contract_week_id, atx.storage_key
        ORDER BY atx.uploaded_at_utc ASC NULLS LAST, atx.id::text ASC
      ) AS canonical_queue_id,
      ROW_NUMBER() OVER (
        PARTITION BY atx.contract_week_id, atx.storage_key
        ORDER BY atx.uploaded_at_utc ASC NULLS LAST, atx.id::text ASC
      ) AS rn
    FROM active_timesheet atx
  )
  UPDATE public.manual_timesheet_queue mq
     SET status = 'DISCARDED',
         timesheet_id = NULL,
         meta_json =
           (
             mq.meta_json
             - 'deferred_target_timesheet_id'
             - 'deferred_rotation_degrees'
             - 'materialisation_deferred_to_backend'
             - 'materialisation_deferred_at_utc'
             - 'materialised_to_timesheet_id'
             - 'dematerialised_from_timesheet_id'
           )
           || jsonb_build_object(
             'contract_week_id', ranked.contract_week_id,
             'staged_kind', 'TIMESHEET',
             'kind', 'TIMESHEET',
             'duplicate_timesheet_evidence_identity', true,
             'duplicate_of_queue_item_id', ranked.canonical_queue_id::text,
             'materialisation_noop_reason', 'same_storage_key_duplicate',
             'same_storage_duplicate_deactivated_at_utc', COALESCE(NULLIF(mq.meta_json->>'same_storage_duplicate_deactivated_at_utc', ''), to_char(v_now AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
             'duplicate_stage_noop_at_utc', COALESCE(NULLIF(mq.meta_json->>'duplicate_stage_noop_at_utc', ''), to_char(v_now AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
           )
   FROM ranked
  WHERE mq.id = ranked.id
    AND ranked.rn > 1
    AND mq.status = 'STAGED';

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
        AND NULLIF(
          regexp_replace(
            COALESCE(
              NULLIF(BTRIM(COALESCE(r2_key, '')), ''),
              NULLIF(BTRIM(COALESCE(meta_json->>'r2_key', '')), ''),
              NULLIF(BTRIM(COALESCE(meta_json->>'storage_key', '')), ''),
              NULLIF(BTRIM(COALESCE(meta_json->>'file_key', '')), ''),
              NULLIF(BTRIM(COALESCE(meta_json->>'canonical_key', '')), ''),
              ''
            ),
            '^/+',
            ''
          ),
          ''
        ) IS NULL
      )
    );

  CREATE UNIQUE INDEX uq_manual_timesheet_queue_one_active_staged_timesheet_per_contract_week
    ON public.manual_timesheet_queue (
      NULLIF(BTRIM((meta_json->>'contract_week_id')), '')
    )
    WHERE status = 'STAGED'
      AND NULLIF(BTRIM((meta_json->>'contract_week_id')), '') IS NOT NULL
      AND UPPER(
        COALESCE(
          NULLIF(BTRIM(meta_json->>'staged_kind'), ''),
          NULLIF(BTRIM(meta_json->>'kind'), ''),
          NULLIF(BTRIM(meta_json->>'attached_kind'), ''),
          'TIMESHEET'
        )
      ) = 'TIMESHEET';
END;
$staged_timesheet_invariant$;
