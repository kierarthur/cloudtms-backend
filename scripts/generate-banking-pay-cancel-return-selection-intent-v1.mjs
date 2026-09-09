import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const cancelOwner = path.join(
  repoRoot,
  'supabase/repeatable/19072026_1816_cancel_refresh_supersede_finance_dirty.sql'
);
const publisherOwner = path.join(
  repoRoot,
  'supabase/repeatable/07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql'
);
const cancelOutput = path.join(
  repoRoot,
  'supabase/repeatable/08092026_1200_banking_pay_cancel_return_selection_intent_v1.sql'
);
const publisherOutput = path.join(
  repoRoot,
  'supabase/repeatable/08092026_1201_banking_pay_certified_preview_final_selection_count_v1.sql'
);

function replaceExactlyOnce(source, before, after, label) {
  const first = source.indexOf(before);
  if (first < 0 || source.indexOf(before, first + before.length) >= 0) {
    throw new Error(`${label}: expected exactly one source anchor`);
  }
  return source.slice(0, first) + after + source.slice(first + before.length);
}

let cancelSql = fs.readFileSync(cancelOwner, 'utf8');
cancelSql = replaceExactlyOnce(
  cancelSql,
  `-- Prevent a cancellation-generated finance dirty job from overwriting the
-- authoritative full-candidate refresh queued after the cancellation.`,
  `-- Final authority for carrying cancelled frozen Draft constituent selection intent
-- across an exact same-session Workbench rebuild.  The historical owner remains
-- byte-identical; this replacement changes selection orchestration only.
--
-- Prevent a cancellation-generated finance dirty job from overwriting the
-- authoritative full-candidate refresh queued after the cancellation.`,
  'cancel owner header'
);
cancelSql = replaceExactlyOnce(
  cancelSql,
  `  v_cancelled_row_unselected_count integer := 0;
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;`,
  `  v_cancelled_row_unselected_count integer := 0;
  v_cancelled_selection_expected_count integer := 0;
  v_cancelled_selection_intent_count integer := 0;
  v_cancelled_selection_duplicate_count integer := 0;
  v_cancelled_selection_registered_count integer := 0;
  v_cancelled_selection_reused_count integer := 0;
  v_cancelled_selection_superseded_count integer := 0;
  v_cancelled_selection_pending_count integer := 0;
  v_cancelled_selection_applied_count integer := 0;
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;`,
  'cancel selection variables'
);
cancelSql = replaceExactlyOnce(
  cancelSql,
  `  v_post_cancel_patch_digest:=NULLIF(BTRIM(COALESCE(v_result->>'post_cancel_patch_digest','')),'');

  -- Cancelling a draft returns its rows to the live workbench, but it must not`,
  `  v_post_cancel_patch_digest:=NULLIF(BTRIM(COALESCE(v_result->>'post_cancel_patch_digest','')),'');

  -- V8 Draft rows are frozen by stable financial identity. A certified rebuild can
  -- replace their public preview UUIDs, so direct patched-row IDs are not a durable
  -- selection authority. Register only the exact frozen constituents belonging to
  -- the cancelled Candidate/channel scopes. The existing carry trigger applies the
  -- UNSELECTED intent to a matching rebuilt row without deriving any economics.
  DROP TABLE IF EXISTS pg_temp._bpay_cancelled_selection_intents;
  CREATE TEMPORARY TABLE pg_temp._bpay_cancelled_selection_intents ON COMMIT DROP AS
  SELECT
    frozen_payload.candidate_id,
    frozen_payload.preview_row_id AS source_preview_row_id,
    public._pay_workbench_preview_selection_key_v1(
      frozen_payload.candidate_id,
      frozen_payload.payload_json->>'section',
      frozen_payload.timesheet_id,
      frozen_payload.payload_json->>'key_type',
      frozen_payload.payload_json->>'key_value',
      frozen_payload.row_key,
      frozen_payload.payload_json
    ) AS stable_selection_key
  FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
  JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS frozen_payload
    ON frozen_payload.operation_id = frozen_scope.operation_id
   AND frozen_payload.candidate_id = frozen_scope.candidate_id
   AND frozen_payload.resolved_pay_channel = frozen_scope.resolved_pay_channel
  JOIN pg_temp._bpay_batch_mutation_candidates AS cancelled_candidate
    ON cancelled_candidate.candidate_id = frozen_scope.candidate_id
  WHERE frozen_scope.pay_batch_id = p_pay_batch_id
  ORDER BY frozen_payload.candidate_id, frozen_payload.constituent_ordinal;

  SELECT COALESCE(pg_catalog.sum(frozen_scope.constituent_count), 0)::integer
  INTO v_cancelled_selection_expected_count
  FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
  JOIN pg_temp._bpay_batch_mutation_candidates AS cancelled_candidate
    ON cancelled_candidate.candidate_id = frozen_scope.candidate_id
  WHERE frozen_scope.pay_batch_id = p_pay_batch_id;

  SELECT pg_catalog.count(*)::integer,
         (pg_catalog.count(*) - pg_catalog.count(DISTINCT (intent.candidate_id, intent.stable_selection_key)))::integer
  INTO v_cancelled_selection_intent_count,
       v_cancelled_selection_duplicate_count
  FROM pg_temp._bpay_cancelled_selection_intents AS intent;

  IF v_cancelled_selection_expected_count > 0
     AND (
       v_cancelled_selection_intent_count IS DISTINCT FROM v_cancelled_selection_expected_count
       OR COALESCE(v_cancelled_selection_duplicate_count, 0) <> 0
       OR EXISTS (
         SELECT 1
         FROM pg_temp._bpay_cancelled_selection_intents AS invalid_intent
         WHERE invalid_intent.stable_selection_key IS NULL
       )
     ) THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_SELECTION_INTENT_IDENTITY_INCOMPLETE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_SELECTION_INTENT_IDENTITY_INCOMPLETE',
              'session_id', p_session_id,
              'pay_batch_id', p_pay_batch_id,
              'expected_count', v_cancelled_selection_expected_count,
              'actual_count', v_cancelled_selection_intent_count,
              'duplicate_count', v_cancelled_selection_duplicate_count
            )::text;
  END IF;

  WITH superseded AS (
    UPDATE public.banking_pay_workbench_selection_carry_registrations AS prior_intent
    SET status = 'SUPERSEDED',
        state_reason_code = 'NEWER_POST_CANCEL_UNSELECT_INTENT',
        updated_at_utc = v_now,
        completed_at_utc = v_now
    FROM pg_temp._bpay_cancelled_selection_intents AS current_intent
    WHERE prior_intent.target_session_id = p_session_id
      AND prior_intent.candidate_id = current_intent.candidate_id
      AND prior_intent.stable_selection_key = current_intent.stable_selection_key
      AND prior_intent.source_preview_row_id IS DISTINCT FROM current_intent.source_preview_row_id
      AND prior_intent.status IN ('PENDING', 'APPLIED')
    RETURNING prior_intent.id
  )
  SELECT pg_catalog.count(*)::integer
  INTO v_cancelled_selection_superseded_count
  FROM superseded;

  WITH inserted AS (
    INSERT INTO public.banking_pay_workbench_selection_carry_registrations(
      target_session_id,
      source_session_id,
      candidate_id,
      source_preview_row_id,
      stable_selection_key,
      selected,
      selection_state,
      source_priority,
      carry_reason,
      status,
      state_reason_code,
      source_row_snapshot_json,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      p_session_id,
      p_session_id,
      intent.candidate_id,
      intent.source_preview_row_id,
      intent.stable_selection_key,
      false,
      'UNSELECTED',
      -1000,
      'POST_CANCEL_RETURN_UNSELECTED',
      'PENDING',
      'AWAITING_STABLE_SELECTION_KEY',
      jsonb_build_object(
        'source_preview_row_id', intent.source_preview_row_id,
        'source_session_id', p_session_id,
        'candidate_id', intent.candidate_id,
        'stable_selection_key', intent.stable_selection_key,
        'selected', false,
        'selection_state', 'UNSELECTED',
        'selection_user_override', 'UNSELECTED',
        'selection_origin', 'POST_CANCEL_RETURN_UNSELECTED',
        'pay_batch_id', p_pay_batch_id,
        'policy_x_authority_scope', 'PRE_DRAFT_SELECTION_INTENT_ONLY'
      ),
      v_now,
      v_now
    FROM pg_temp._bpay_cancelled_selection_intents AS intent
    WHERE intent.stable_selection_key IS NOT NULL
    ON CONFLICT (target_session_id, source_preview_row_id) DO NOTHING
    RETURNING id
  )
  SELECT pg_catalog.count(*)::integer
  INTO v_cancelled_selection_registered_count
  FROM inserted;

  v_cancelled_selection_reused_count := GREATEST(
    COALESCE(v_cancelled_selection_intent_count, 0) - COALESCE(v_cancelled_selection_registered_count, 0),
    0
  );

  IF EXISTS (
    SELECT 1
    FROM pg_temp._bpay_cancelled_selection_intents AS intent
    LEFT JOIN public.banking_pay_workbench_selection_carry_registrations AS registered_intent
      ON registered_intent.target_session_id = p_session_id
     AND registered_intent.source_preview_row_id = intent.source_preview_row_id
    WHERE registered_intent.id IS NULL
       OR registered_intent.candidate_id IS DISTINCT FROM intent.candidate_id
       OR registered_intent.stable_selection_key IS DISTINCT FROM intent.stable_selection_key
       OR registered_intent.selected IS DISTINCT FROM false
       OR registered_intent.selection_state IS DISTINCT FROM 'UNSELECTED'
       OR registered_intent.status NOT IN ('PENDING', 'APPLIED')
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_SELECTION_INTENT_REGISTRATION_CONFLICT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_SELECTION_INTENT_REGISTRATION_CONFLICT',
              'session_id', p_session_id,
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  -- Cancelling a draft returns its rows to the live workbench, but it must not`,
  'cancel selection registration'
);
cancelSql = replaceExactlyOnce(
  cancelSql,
  `  v_progress_recompute:=public.pay_workbench_session_recompute_progress_counters(
    p_session_id,true,'POST_CANCEL_PHYSICAL_CURRENTNESS_FINALISE',true
  );`,
  `  SELECT pg_catalog.count(*) FILTER (WHERE registered_intent.status = 'PENDING')::integer,
         pg_catalog.count(*) FILTER (WHERE registered_intent.status = 'APPLIED')::integer
  INTO v_cancelled_selection_pending_count,
       v_cancelled_selection_applied_count
  FROM pg_temp._bpay_cancelled_selection_intents AS intent
  JOIN public.banking_pay_workbench_selection_carry_registrations AS registered_intent
    ON registered_intent.target_session_id = p_session_id
   AND registered_intent.source_preview_row_id = intent.source_preview_row_id
   AND registered_intent.candidate_id = intent.candidate_id
   AND registered_intent.stable_selection_key = intent.stable_selection_key;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._bpay_cancel_safe_route_candidates AS route_candidate
    JOIN pg_temp._bpay_cancelled_selection_intents AS intent
      ON intent.candidate_id = route_candidate.candidate_id
    LEFT JOIN public.banking_pay_workbench_selection_carry_registrations AS applied_intent
      ON applied_intent.target_session_id = p_session_id
     AND applied_intent.source_preview_row_id = intent.source_preview_row_id
     AND applied_intent.status = 'APPLIED'
    WHERE route_candidate.direct_current IS TRUE
      AND applied_intent.id IS NULL
  ) THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_CURRENT_SELECTION_INTENT_NOT_APPLIED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_CURRENT_SELECTION_INTENT_NOT_APPLIED',
              'session_id', p_session_id,
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_progress_recompute:=public.pay_workbench_session_recompute_progress_counters(
    p_session_id,true,'POST_CANCEL_PHYSICAL_CURRENTNESS_FINALISE',true
  );`,
  'cancel selection completion proof'
);
cancelSql = replaceExactlyOnce(
  cancelSql,
  `    'cancelled_row_unselected_count', COALESCE(v_cancelled_row_unselected_count, 0),
    'server_selected_preview_row_ids', COALESCE(v_preserved_selected_preview_row_ids, '[]'::jsonb),`,
  `    'cancelled_row_unselected_count', COALESCE(v_cancelled_row_unselected_count, 0),
    'cancelled_selection_intent_expected_count', COALESCE(v_cancelled_selection_expected_count, 0),
    'cancelled_selection_intent_count', COALESCE(v_cancelled_selection_intent_count, 0),
    'cancelled_selection_intent_registered_count', COALESCE(v_cancelled_selection_registered_count, 0),
    'cancelled_selection_intent_reused_count', COALESCE(v_cancelled_selection_reused_count, 0),
    'cancelled_selection_intent_superseded_count', COALESCE(v_cancelled_selection_superseded_count, 0),
    'cancelled_selection_intent_pending_count', COALESCE(v_cancelled_selection_pending_count, 0),
    'cancelled_selection_intent_applied_count', COALESCE(v_cancelled_selection_applied_count, 0),
    'server_selected_preview_row_ids', COALESCE(v_preserved_selected_preview_row_ids, '[]'::jsonb),`,
  'cancel result evidence'
);

let publisherSql = fs.readFileSync(publisherOwner, 'utf8');
publisherSql = replaceExactlyOnce(
  publisherSql,
  `-- Banking Pay Workbench: publish one exact, certified bounded CURRENT source
-- into the public preview read model.`,
  `-- Final authority for publishing the exact certified source after applying any
-- durable stable-identity selection carry.  The historical owner remains
-- byte-identical; this replacement changes selection accounting only.
--
-- Banking Pay Workbench: publish one exact, certified bounded CURRENT source
-- into the public preview read model.`,
  'publisher header'
);
publisherSql = replaceExactlyOnce(
  publisherSql,
  `    AND v_scope.certified_preview_publication_attestation_json->>'authority_kind'=v_authority_kind
    AND NOT EXISTS (`,
  `    AND v_scope.certified_preview_publication_attestation_json->>'authority_kind'=v_authority_kind
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_selection_carry_registrations AS pending_selection_intent
      WHERE pending_selection_intent.target_session_id = p_session_id
        AND pending_selection_intent.candidate_id = p_candidate_id
        AND pending_selection_intent.status = 'PENDING'
    )
    AND NOT EXISTS (`,
  'publisher pending intent no-op fence'
);
publisherSql = replaceExactlyOnce(
  publisherSql,
  `  FROM public.banking_pay_workbench_preview_rows AS selected_row
  WHERE selected_row.session_id = p_session_id
    AND selected_row.session_version = p_session_version
    AND selected_row.status = 'READY'
    AND selected_row.selected IS TRUE
    AND selected_row.selection_state = 'SELECTED';

  UPDATE public.banking_pay_workbench_sessions AS session_update`,
  `  FROM public.banking_pay_workbench_preview_rows AS selected_row
  WHERE selected_row.session_id = p_session_id
    AND selected_row.session_version = p_session_version
    AND selected_row.status = 'READY'
    AND selected_row.selected IS TRUE
    AND selected_row.selection_state = 'SELECTED';

  -- The carry trigger can alter selection during the upsert. Attest the final
  -- published rows, not the pre-upsert source defaults.
  v_selected_count := pg_catalog.jsonb_array_length(v_selected_ids);

  UPDATE public.banking_pay_workbench_sessions AS session_update`,
  'publisher final selected count'
);

fs.writeFileSync(cancelOutput, cancelSql);
fs.writeFileSync(publisherOutput, publisherSql);
console.log(JSON.stringify({
  cancel_output: path.relative(repoRoot, cancelOutput).replaceAll('\\\\', '/'),
  publisher_output: path.relative(repoRoot, publisherOutput).replaceAll('\\\\', '/')
}));
