\set ON_ERROR_STOP on

-- Rollback-contained payment-cancellation notice runtime proof.
-- No provider, remittance, settlement, cancellation or financial owner is
-- called.  The only exercised business write is the new idempotent mail notice,
-- and the outer transaction removes it and every synthetic source row.

begin;

-- A hostile direct EXECUTE grant must be removed by the same catalogue-driven
-- ACL algorithm used on repeatable reapply.  Re-granting it must then be
-- visible to the exact verifier census.  The outer rollback restores the
-- disposable database's original ACL after both mutations.
grant execute on function public.pay_payment_cancellation_notice_reconcile_v1(uuid,uuid,timestamptz,uuid,integer,text)
  to pg_monitor;

do $acl_reapply$
declare
  v_function_identity constant text :=
    'public.pay_payment_cancellation_notice_reconcile_v1(uuid,uuid,timestamptz,uuid,integer,text)';
  v_function_oid oid := pg_catalog.to_regprocedure(
    'public.pay_payment_cancellation_notice_reconcile_v1(uuid,uuid,timestamp with time zone,uuid,integer,text)'
  );
  v_function_owner_oid oid;
  v_service_role_oid oid := pg_catalog.to_regrole('service_role');
  v_acl record;
  v_execute_grantees text[];
  v_expected_execute_grantees text[];
begin
  select function_row.proowner
    into strict v_function_owner_oid
  from pg_catalog.pg_proc as function_row
  where function_row.oid = v_function_oid;

  for v_acl in
    select distinct acl_row.grantee
    from pg_catalog.pg_proc as function_row
    cross join lateral pg_catalog.aclexplode(
      coalesce(
        function_row.proacl,
        pg_catalog.acldefault('f', function_row.proowner)
      )
    ) as acl_row
    where function_row.oid = v_function_oid
      and acl_row.privilege_type = 'EXECUTE'
      and acl_row.grantee not in (
        v_function_owner_oid,
        v_service_role_oid
      )
  loop
    if v_acl.grantee = 0 then
      execute pg_catalog.format(
        'revoke all on function %s from public cascade',
        v_function_identity
      );
    else
      execute pg_catalog.format(
        'revoke all on function %s from %I cascade',
        v_function_identity,
        pg_catalog.pg_get_userbyid(v_acl.grantee)
      );
    end if;
  end loop;

  if exists (
    select 1
    from pg_catalog.pg_proc as function_row
    cross join lateral pg_catalog.aclexplode(
      coalesce(
        function_row.proacl,
        pg_catalog.acldefault('f', function_row.proowner)
      )
    ) as acl_row
    where function_row.oid = v_function_oid
      and acl_row.grantee = pg_catalog.to_regrole('pg_monitor')
      and acl_row.privilege_type = 'EXECUTE'
  ) then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_ROGUE_REAPPLY_NOT_REMOVED';
  end if;

  execute pg_catalog.format(
    'grant execute on function %s to %I',
    v_function_identity,
    'pg_monitor'
  );

  select coalesce(
           pg_catalog.array_agg(
             normalized_acl.grantee_name
             order by normalized_acl.grantee_name
           ),
           array[]::text[]
         )
    into v_execute_grantees
  from (
    select distinct
           case
             when acl_row.grantee = 0 then 'PUBLIC'
             else pg_catalog.pg_get_userbyid(acl_row.grantee)
           end as grantee_name
    from pg_catalog.pg_proc as function_row
    cross join lateral pg_catalog.aclexplode(
      coalesce(
        function_row.proacl,
        pg_catalog.acldefault('f', function_row.proowner)
      )
    ) as acl_row
    where function_row.oid = v_function_oid
      and acl_row.privilege_type = 'EXECUTE'
  ) as normalized_acl;

  select pg_catalog.array_agg(
           expected_grantee.grantee_name
           order by expected_grantee.grantee_name
         )
    into v_expected_execute_grantees
  from (
    select distinct expected_name.grantee_name
    from pg_catalog.unnest(
      array[
        pg_catalog.pg_get_userbyid(v_function_owner_oid),
        'service_role'
      ]::text[]
    ) as expected_name(grantee_name)
  ) as expected_grantee;

  if v_execute_grantees is not distinct from v_expected_execute_grantees then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_ROGUE_VERIFIER_NOT_DETECTED';
  end if;
end
$acl_reapply$;

do $runtime$
declare
  v_batch_id uuid;
  v_pay_batch_candidate_id uuid;
  v_candidate_id uuid;
  v_item_id uuid;
  v_request_id uuid := pg_catalog.md5(
    'payment-cancellation-notice-runtime-request:' || pg_catalog.current_database()
  )::uuid;
  v_source_mail_id uuid := pg_catalog.md5(
    'payment-cancellation-notice-runtime-mail:' || pg_catalog.current_database()
  )::uuid;
  v_result jsonb;
  v_notice_count integer;
begin
  if pg_catalog.current_database() !~ '^h12_cancel_notice_events(17|18)$' then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_DISPOSABLE_DATABASE_REQUIRED';
  end if;

  select batch_row.id,
         candidate_row.id,
         candidate_row.candidate_id,
         item_row.id
    into strict v_batch_id,
                v_pay_batch_candidate_id,
                v_candidate_id,
                v_item_id
  from public.pay_batches as batch_row
  join public.pay_batch_candidates as candidate_row
    on candidate_row.pay_batch_id = batch_row.id
  join public.pay_batch_items as item_row
    on item_row.pay_batch_candidate_id = candidate_row.id
  where batch_row.batch_kind_fixed = 'PAYE'
  order by candidate_row.id, item_row.id
  limit 1;

  insert into public.pay_payment_correction_requests (
    id,
    pay_batch_id,
    correction_kind,
    status,
    required_quantity,
    approved_count,
    golden_key_used,
    selection_json,
    selection_hash,
    plan_json,
    plan_hash,
    auto_requested,
    created_at_utc,
    applied_at_utc,
    updated_at_utc,
    cancel_notice_tracked,
    cancel_notice_reconciled_applied_at_utc
  ) values (
    v_request_id,
    v_batch_id,
    'PRE_BANK_CANCEL',
    'APPLIED',
    1,
    1,
    false,
    '{}'::jsonb,
    pg_catalog.repeat('a', 64),
    pg_catalog.jsonb_build_object(
      'candidate_scope_contract_version', '2',
      'candidate_scope_hash_version', '2',
      'source_row_count_semantics', 'FINANCIAL_ONLY',
      'communication_cleanup_contract_version', '2'
    ),
    pg_catalog.repeat('b', 64),
    false,
    '2026-09-08 00:00:00+00'::timestamptz,
    '2026-09-08 00:01:00+00'::timestamptz,
    '2026-09-08 00:01:00+00'::timestamptz,
    null,
    null
  );

  insert into public.pay_payment_correction_items (
    id,
    correction_request_id,
    pay_batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    correction_item_kind,
    status,
    created_at_utc,
    applied_at_utc
  ) values (
    pg_catalog.md5(v_request_id::text || ':' || v_item_id::text)::uuid,
    v_request_id,
    v_batch_id,
    v_pay_batch_candidate_id,
    v_candidate_id,
    v_item_id,
    'PRE_BANK_CANCEL',
    'APPLIED',
    '2026-09-08 00:00:30+00'::timestamptz,
    '2026-09-08 00:01:00+00'::timestamptz
  );

  insert into public.mail_outbox (
    id,
    type,
    "to",
    subject,
    attachments,
    status,
    created_at_utc,
    sent_at,
    reference,
    recipient_kind,
    recipient_id,
    context_kind,
    context_id,
    provider_status,
    payment_scope_json,
    deterministic_outbox_key,
    attachments_ready,
    attachment_total_bytes,
    cancel_notice_tracked
  ) values (
    v_source_mail_id,
    'REMITTANCE',
    'runtime-recipient@example.invalid',
    'Original payment notice',
    '[]'::jsonb,
    'SENT'::public.mail_status_enum,
    '2026-09-08 00:01:30+00'::timestamptz,
    '2026-09-08 00:02:00+00'::timestamptz,
    'runtime-candidate-remittance',
    'candidate',
    v_candidate_id,
    'pay_batches',
    v_batch_id,
    'ACCEPTED',
    pg_catalog.jsonb_build_object(
      'remittance_type', 'CANDIDATE_REMITTANCE',
      'pay_batch_id', v_batch_id,
      'candidate_id', v_candidate_id,
      'pay_batch_candidate_id', v_pay_batch_candidate_id,
      'pay_batch_item_ids', pg_catalog.jsonb_build_array(v_item_id::text),
      'item_count', 1
    ),
    'runtime-original:' || v_source_mail_id::text,
    true,
    0,
    true
  );

  -- This models the cutover interleaving exactly: the completed communication-
  -- V2 request is deliberately historical/untracked, while its formerly
  -- retryable payment mail is tracked and becomes provider-accepted afterward.
  -- Only the exact MAIL/item linkage may resolve the request; the request must
  -- remain excluded from request-driven recovery.
  v_result := public.pay_payment_cancellation_notice_reconcile_v1();

  if v_result->>'ok' is distinct from 'true'
     or v_result->>'mode' is distinct from 'RECOVERY'
     or v_result->>'progress_owner' is distinct from 'SERVER_ROW'
     or v_result->>'examined' is distinct from '1'
     or v_result->>'eligible' is distinct from '1'
     or v_result->>'queued' is distinct from '1'
     or v_result->>'skipped' is distinct from '0'
     or v_result->>'next_cursor' is not null then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_MAIL_RECOVERY_RESULT_INVALID',
      detail = v_result::text;
  end if;

  select pg_catalog.count(*)::integer
    into v_notice_count
  from public.mail_outbox as notice_row
  where notice_row.deterministic_outbox_key =
        'PAYMENT_CANCELLATION_NOTICE_V1:' || v_source_mail_id::text
    and notice_row.type = 'PAYMENT_CANCELLATION'
    and notice_row.status = 'QUEUED'::public.mail_status_enum
    and notice_row.context_kind = 'pay_payment_correction_requests'
    and notice_row.context_id = v_request_id
    and notice_row.recipient_id = v_candidate_id
    and notice_row."to" = 'runtime-recipient@example.invalid'
    and notice_row.attachments = '[]'::jsonb
    and notice_row.payment_scope_json->>'covered_item_count' = '1'
    and notice_row.cancel_notice_tracked is false;

  if v_notice_count <> 1 then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_OUTPUT_INVALID';
  end if;

  if not exists (
    select 1
    from public.mail_outbox as source_row
    where source_row.id = v_source_mail_id
      and source_row.cancel_notice_reconciled_sent_at_utc = source_row.sent_at
      and source_row.cancel_notice_next_attempt_at_utc is null
      and source_row.cancel_notice_result_code = 'QUEUED'
  ) then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_MAIL_EVENT_NOT_CONSUMED';
  end if;

  -- Exact replay is idempotent and the server owns all progress.
  v_result := public.pay_payment_cancellation_notice_reconcile_v1(
    p_original_mail_outbox_id => v_source_mail_id
  );
  if v_result->>'eligible' is distinct from '1'
     or v_result->>'already_present' is distinct from '1'
     or v_result->>'queued' is distinct from '0'
     or v_result->>'skipped' is distinct from '0'
     or v_result->>'next_cursor' is not null then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_REPLAY_INVALID',
      detail = v_result::text;
  end if;
end
$runtime$;

-- Saved progress is a durable server-owned cursor.  Prove exact identity,
-- fail-closed tamper handling, and TimeZone-independent partial/terminal replay.
do $saved_progress$
declare
  v_batch_id uuid;
  v_request_id uuid;
  v_result jsonb;
  v_saved jsonb;
  v_applied_at constant timestamptz := '2026-09-08 00:11:00+00';
  v_event_text constant text := '2026-09-08T00:11:00.000000Z';
  v_case text;
  v_before jsonb;
begin
  select batch_row.id
    into strict v_batch_id
  from public.pay_batches as batch_row
  where batch_row.batch_kind_fixed = 'PAYE'
  order by batch_row.id
  limit 1;

  perform pg_catalog.set_config('TimeZone', 'UTC', true);

  -- A completed event survives a different caller TimeZone byte-identically and
  -- does not reopen page one or mutate durable progress on an exact retry.
  v_request_id := pg_catalog.md5(
    'payment-cancellation-terminal-timezone:' || pg_catalog.current_database()
  )::uuid;
  v_saved := pg_catalog.jsonb_build_object(
    'template_version', 'PAYMENT_CANCELLATION_NOTICE_V1',
    'reconciliation_authority', 'ASYNCHRONOUS_SENT_ONLY',
    'event_applied_at_utc', v_event_text,
    'examined', 0,
    'eligible', 0,
    'queued', 0,
    'already_present', 0,
    'skipped', 0,
    'reason_counts', '{}'::jsonb,
    'complete', true,
    'last_source_created_at_utc', null,
    'last_source_mail_outbox_id', null,
    'result_code', 'EVENT_COMPLETE'
  );
  insert into public.pay_payment_correction_requests (
    id, pay_batch_id, correction_kind, status, required_quantity,
    approved_count, golden_key_used, selection_json, selection_hash,
    plan_json, plan_hash, auto_requested, created_at_utc, applied_at_utc,
    updated_at_utc, cancel_notice_tracked,
    cancel_notice_reconciled_applied_at_utc, cancel_notice_result_json
  ) values (
    v_request_id, v_batch_id, 'PRE_BANK_CANCEL', 'APPLIED', 1, 1, false,
    '{}'::jsonb, pg_catalog.repeat('c', 64),
    pg_catalog.jsonb_build_object(
      'candidate_scope_contract_version', '2',
      'candidate_scope_hash_version', '2',
      'source_row_count_semantics', 'FINANCIAL_ONLY',
      'communication_cleanup_contract_version', '2'
    ), pg_catalog.repeat('d', 64), false,
    '2026-09-08 00:10:00+00', v_applied_at, v_applied_at, true,
    v_applied_at, v_saved
  );
  select pg_catalog.to_jsonb(request_row)
    into v_before
  from public.pay_payment_correction_requests as request_row
  where request_row.id = v_request_id;
  perform pg_catalog.set_config('TimeZone', 'America/New_York', true);
  v_result := public.pay_payment_cancellation_notice_reconcile_v1(
    p_correction_request_id => v_request_id
  );
  perform pg_catalog.set_config('TimeZone', 'UTC', true);
  if v_result->>'already_complete' is distinct from 'true'
     or v_result->>'examined' is distinct from '0'
     or exists (
       select 1
       from public.pay_payment_correction_requests as request_row
       where request_row.id = v_request_id
         and pg_catalog.to_jsonb(request_row) is distinct from v_before
     ) then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_TERMINAL_TIMEZONE_REPLAY_INVALID',
      detail = v_result::text;
  end if;

  -- A valid no-cursor error checkpoint created in UTC resumes in another
  -- TimeZone; JSON null cursor fields are not confused with SQL NULL.
  perform pg_catalog.set_config('TimeZone', 'UTC', true);
  v_request_id := pg_catalog.md5(
    'payment-cancellation-partial-timezone:' || pg_catalog.current_database()
  )::uuid;
  v_saved := pg_catalog.jsonb_build_object(
    'template_version', 'PAYMENT_CANCELLATION_NOTICE_V1',
    'reconciliation_authority', 'ASYNCHRONOUS_SENT_ONLY',
    'event_applied_at_utc', v_event_text,
    'examined', 0,
    'eligible', 0,
    'queued', 0,
    'already_present', 0,
    'skipped', 0,
    'reason_counts', '{}'::jsonb,
    'complete', false,
    'last_source_created_at_utc', null,
    'last_source_mail_outbox_id', null,
    'result_code', 'UNEXPECTED_EVENT_ERROR_RETRY'
  );
  insert into public.pay_payment_correction_requests (
    id, pay_batch_id, correction_kind, status, required_quantity,
    approved_count, golden_key_used, selection_json, selection_hash,
    plan_json, plan_hash, auto_requested, created_at_utc, applied_at_utc,
    updated_at_utc, cancel_notice_tracked,
    cancel_notice_progress_applied_at_utc, cancel_notice_result_json
  ) values (
    v_request_id, v_batch_id, 'PRE_BANK_CANCEL', 'APPLIED', 1, 1, false,
    '{}'::jsonb, pg_catalog.repeat('e', 64),
    pg_catalog.jsonb_build_object(
      'candidate_scope_contract_version', '2',
      'candidate_scope_hash_version', '2',
      'source_row_count_semantics', 'FINANCIAL_ONLY',
      'communication_cleanup_contract_version', '2'
    ), pg_catalog.repeat('f', 64), false,
    '2026-09-08 00:10:00+00', v_applied_at, v_applied_at, true,
    v_applied_at, v_saved
  );
  perform pg_catalog.set_config('TimeZone', 'America/New_York', true);
  v_result := public.pay_payment_cancellation_notice_reconcile_v1(
    p_correction_request_id => v_request_id
  );
  if v_result->'reason_counts' ? 'SAVED_PROGRESS_INVALID'
     or v_result->'reason_counts'->>'CORRECTION_SOURCE_NOT_FOUND'
          is distinct from '1' then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_PARTIAL_TIMEZONE_REPLAY_INVALID',
      detail = v_result::text;
  end if;

  perform pg_catalog.set_config('TimeZone', 'UTC', true);
  foreach v_case in array array[
    'ROOT_SCALAR', 'ROOT_ARRAY', 'REASON_SCALAR', 'REASON_ARRAY',
    'REASON_NULL', 'EVENT_MALFORMED', 'TEMPLATE_MISMATCH',
    'CURSOR_MISMATCH', 'REASON_SUM_OVERFLOW', 'TOO_MANY_ROOT_KEYS',
    'OVERSIZED_SAVED_RESULT'
  ] loop
    v_request_id := pg_catalog.md5(
      'payment-cancellation-saved-tamper:' || v_case || ':' ||
      pg_catalog.current_database()
    )::uuid;
    v_saved := pg_catalog.jsonb_build_object(
      'template_version', 'PAYMENT_CANCELLATION_NOTICE_V1',
      'reconciliation_authority', 'ASYNCHRONOUS_SENT_ONLY',
      'event_applied_at_utc', v_event_text,
      'examined', 0,
      'eligible', 0,
      'queued', 0,
      'already_present', 0,
      'skipped', 0,
      'reason_counts', '{}'::jsonb,
      'complete', false,
      'last_source_created_at_utc', null,
      'last_source_mail_outbox_id', null,
      'result_code', 'UNEXPECTED_EVENT_ERROR_RETRY'
    );
    v_saved := case v_case
      when 'ROOT_SCALAR' then '"tampered"'::jsonb
      when 'ROOT_ARRAY' then '[]'::jsonb
      when 'REASON_SCALAR' then pg_catalog.jsonb_set(
        v_saved, '{reason_counts}', '17'::jsonb
      )
      when 'REASON_ARRAY' then pg_catalog.jsonb_set(
        v_saved, '{reason_counts}', '[]'::jsonb
      )
      when 'REASON_NULL' then pg_catalog.jsonb_set(
        v_saved, '{reason_counts}', 'null'::jsonb
      )
      when 'EVENT_MALFORMED' then pg_catalog.jsonb_set(
        v_saved, '{event_applied_at_utc}', '"not-a-timestamp"'::jsonb
      )
      when 'TEMPLATE_MISMATCH' then pg_catalog.jsonb_set(
        v_saved, '{template_version}', '"WRONG"'::jsonb
      )
      when 'CURSOR_MISMATCH' then v_saved || pg_catalog.jsonb_build_object(
        'last_source_created_at_utc', '2026-09-08T00:00:00.000001Z',
        'last_source_mail_outbox_id',
          '11111111-1111-1111-1111-111111111111'
      )
      when 'REASON_SUM_OVERFLOW' then v_saved || pg_catalog.jsonb_build_object(
        'examined', 50000,
        'skipped', 50000,
        'reason_counts', pg_catalog.jsonb_build_object(
          'ORIGINAL_CONTEXT_INVALID', 50000,
          'ORIGINAL_ITEM_SCOPE_INVALID', 50000
        )
      )
      when 'TOO_MANY_ROOT_KEYS' then v_saved || (
        select pg_catalog.jsonb_object_agg('extra_' || g::text, g)
        from pg_catalog.generate_series(1, 25) as generated(g)
      )
      when 'OVERSIZED_SAVED_RESULT' then v_saved || pg_catalog.jsonb_build_object(
        'padding', (
          select pg_catalog.string_agg(pg_catalog.md5(g::text), '')
          from pg_catalog.generate_series(1, 1000) as generated(g)
        )
      )
      else v_saved
    end;

    insert into public.pay_payment_correction_requests (
      id, pay_batch_id, correction_kind, status, required_quantity,
      approved_count, golden_key_used, selection_json, selection_hash,
      plan_json, plan_hash, auto_requested, created_at_utc, applied_at_utc,
      updated_at_utc, cancel_notice_tracked,
      cancel_notice_progress_applied_at_utc, cancel_notice_result_json
    ) values (
      v_request_id, v_batch_id, 'PRE_BANK_CANCEL', 'APPLIED', 1, 1, false,
      '{}'::jsonb, pg_catalog.repeat('1', 64),
      pg_catalog.jsonb_build_object(
        'candidate_scope_contract_version', '2',
        'candidate_scope_hash_version', '2',
        'source_row_count_semantics', 'FINANCIAL_ONLY',
        'communication_cleanup_contract_version', '2'
      ), pg_catalog.repeat('2', 64), false,
      '2026-09-08 00:10:00+00', v_applied_at, v_applied_at, true,
      v_applied_at, v_saved
    );
    v_result := public.pay_payment_cancellation_notice_reconcile_v1(
      p_correction_request_id => v_request_id
    );
    if v_result->'reason_counts'->>'SAVED_PROGRESS_INVALID'
         is distinct from '1' then
      raise exception using
        errcode = 'P0001',
        message = 'PAYMENT_CANCELLATION_NOTICE_SAVED_TAMPER_NOT_TYPED',
        detail = v_case || ': ' || v_result::text;
    end if;
  end loop;

  -- Both fail-closed request-state exits are reachable from the server-owned
  -- RECOVERY lane.  Their envelopes must therefore carry an explicit false
  -- contention flag (and a matching false has_more), so the strict Worker does
  -- not mistake a safely terminalized event for malformed transport output.
  v_request_id := pg_catalog.md5(
    'payment-cancellation-recovery-cursor-corrupt:' ||
    pg_catalog.current_database()
  )::uuid;
  insert into public.pay_payment_correction_requests (
    id, pay_batch_id, correction_kind, status, required_quantity,
    approved_count, golden_key_used, selection_json, selection_hash,
    plan_json, plan_hash, auto_requested, created_at_utc, applied_at_utc,
    updated_at_utc, cancel_notice_tracked,
    cancel_notice_progress_applied_at_utc,
    cancel_notice_after_created_at_utc,
    cancel_notice_after_mail_outbox_id,
    cancel_notice_next_attempt_at_utc
  ) values (
    v_request_id, v_batch_id, 'PRE_BANK_CANCEL', 'APPLIED', 1, 1, false,
    '{}'::jsonb, pg_catalog.repeat('3', 64),
    pg_catalog.jsonb_build_object(
      'candidate_scope_contract_version', '2',
      'candidate_scope_hash_version', '2',
      'source_row_count_semantics', 'FINANCIAL_ONLY',
      'communication_cleanup_contract_version', '2'
    ), pg_catalog.repeat('4', 64), false,
    '2000-01-01 00:00:00+00', v_applied_at, v_applied_at, true,
    v_applied_at, '2000-01-01 00:00:01+00', null,
    '2000-01-01 00:00:02+00'
  );
  v_result := public.pay_payment_cancellation_notice_reconcile_v1();
  if v_result->>'mode' is distinct from 'RECOVERY'
     or v_result->'reason_counts'->>'REQUEST_PROGRESS_CURSOR_CORRUPT'
          is distinct from '1'
     or v_result->>'has_more' is distinct from 'false'
     or v_result->>'recovery_claim_contended' is distinct from 'false' then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_RECOVERY_CURSOR_ENVELOPE_INVALID',
      detail = v_result::text;
  end if;

  v_request_id := pg_catalog.md5(
    'payment-cancellation-recovery-saved-invalid:' ||
    pg_catalog.current_database()
  )::uuid;
  insert into public.pay_payment_correction_requests (
    id, pay_batch_id, correction_kind, status, required_quantity,
    approved_count, golden_key_used, selection_json, selection_hash,
    plan_json, plan_hash, auto_requested, created_at_utc, applied_at_utc,
    updated_at_utc, cancel_notice_tracked,
    cancel_notice_progress_applied_at_utc,
    cancel_notice_next_attempt_at_utc,
    cancel_notice_result_json
  ) values (
    v_request_id, v_batch_id, 'PRE_BANK_CANCEL', 'APPLIED', 1, 1, false,
    '{}'::jsonb, pg_catalog.repeat('5', 64),
    pg_catalog.jsonb_build_object(
      'candidate_scope_contract_version', '2',
      'candidate_scope_hash_version', '2',
      'source_row_count_semantics', 'FINANCIAL_ONLY',
      'communication_cleanup_contract_version', '2'
    ), pg_catalog.repeat('6', 64), false,
    '2001-01-01 00:00:00+00', v_applied_at, v_applied_at, true,
    v_applied_at, '2001-01-01 00:00:02+00', '"tampered"'::jsonb
  );
  v_result := public.pay_payment_cancellation_notice_reconcile_v1();
  if v_result->>'mode' is distinct from 'RECOVERY'
     or v_result->'reason_counts'->>'SAVED_PROGRESS_INVALID'
          is distinct from '1'
     or v_result->>'has_more' is distinct from 'false'
     or v_result->>'recovery_claim_contended' is distinct from 'false' then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_RECOVERY_SAVED_ENVELOPE_INVALID',
      detail = v_result::text;
  end if;
end
$saved_progress$;

-- Pressure proof uses the user-approved 5,000-item maximum (never a 50,000-row
-- stress run).  Five thousand unrelated same-batch messages put the exact
-- Candidate/PBC expression access path under target-last noise.  A separate
-- 50,001-element source is parsed only far enough to prove cheap rejection.
do $scale$
declare
  v_batch_id uuid;
  v_pay_batch_candidate_id uuid;
  v_candidate_id uuid;
  v_template_item_id uuid;
  v_template_item public.pay_batch_items%rowtype;
  v_request_id uuid := pg_catalog.md5(
    'payment-cancellation-scale-request:' || pg_catalog.current_database()
  )::uuid;
  v_source_mail_id uuid := pg_catalog.md5(
    'payment-cancellation-scale-mail:' || pg_catalog.current_database()
  )::uuid;
  v_boundary_mail_id uuid := pg_catalog.md5(
    'payment-cancellation-boundary-mail:' || pg_catalog.current_database()
  )::uuid;
  v_result jsonb;
  v_started_at timestamptz;
  v_elapsed_ms numeric;
  v_plan json;
  v_plan_text text;
  v_notice_count integer;
  v_near_ceiling_before jsonb;
  v_near_ceiling_after jsonb;
  v_near_ceiling_raised boolean := false;
begin
  select batch_row.id,
         candidate_row.id,
         candidate_row.candidate_id,
         item_row.id
    into strict v_batch_id,
                v_pay_batch_candidate_id,
                v_candidate_id,
                v_template_item_id
  from public.pay_batches as batch_row
  join public.pay_batch_candidates as candidate_row
    on candidate_row.pay_batch_id = batch_row.id
  join public.pay_batch_items as item_row
    on item_row.pay_batch_candidate_id = candidate_row.id
  where batch_row.batch_kind_fixed = 'PAYE'
  order by candidate_row.id, item_row.id
  limit 1;

  select item_row.*
    into strict v_template_item
  from public.pay_batch_items as item_row
  where item_row.id = v_template_item_id;

  -- Retire only this file's earlier one-row synthetic fixture so the scale cell
  -- measures exactly one 5,000-item source mail rather than two messages for the
  -- same real fixture Candidate.  The outer rollback still owns all test state.
  delete from public.mail_outbox as mail_row
  where mail_row.id = pg_catalog.md5(
          'payment-cancellation-notice-runtime-mail:' ||
          pg_catalog.current_database()
        )::uuid
     or mail_row.deterministic_outbox_key =
        'PAYMENT_CANCELLATION_NOTICE_V1:' || pg_catalog.md5(
          'payment-cancellation-notice-runtime-mail:' ||
          pg_catalog.current_database()
        )::uuid::text;
  delete from public.pay_payment_correction_items as correction_item
  where correction_item.correction_request_id = pg_catalog.md5(
    'payment-cancellation-notice-runtime-request:' ||
    pg_catalog.current_database()
  )::uuid;
  delete from public.pay_payment_correction_requests as request_row
  where request_row.id = pg_catalog.md5(
    'payment-cancellation-notice-runtime-request:' ||
    pg_catalog.current_database()
  )::uuid;

  create temporary table cancel_notice_scale_items_2305 (
    ordinal integer primary key,
    item_id uuid not null unique
  ) on commit drop;

  insert into cancel_notice_scale_items_2305 (ordinal, item_id)
  select generated.ordinal,
         pg_catalog.md5(
           'payment-cancellation-scale-item:' ||
           pg_catalog.current_database() || ':' || generated.ordinal::text
         )::uuid
  from pg_catalog.generate_series(1, 5000) as generated(ordinal);

  insert into public.pay_batch_items
  select (
    pg_catalog.jsonb_populate_record(
      null::public.pay_batch_items,
      pg_catalog.to_jsonb(v_template_item) || pg_catalog.jsonb_build_object(
        'id', scale_item.item_id,
        'pay_batch_candidate_id', v_pay_batch_candidate_id,
        'operation_source_key',
          'payment-cancellation-scale:' || pg_catalog.current_database() || ':' ||
          scale_item.ordinal::text,
        'created_at', '2026-09-08T01:00:00.000000Z',
        'updated_at', '2026-09-08T01:00:00.000000Z'
      )
    )
  ).*
  from cancel_notice_scale_items_2305 as scale_item;

  insert into public.pay_payment_correction_requests (
    id, pay_batch_id, correction_kind, status, required_quantity,
    approved_count, golden_key_used, selection_json, selection_hash,
    plan_json, plan_hash, auto_requested, created_at_utc, applied_at_utc,
    updated_at_utc, cancel_notice_tracked
  ) values (
    v_request_id, v_batch_id, 'PRE_BANK_CANCEL', 'APPLIED', 5000, 5000, false,
    '{}'::jsonb, pg_catalog.repeat('3', 64),
    pg_catalog.jsonb_build_object(
      'candidate_scope_contract_version', '2',
      'candidate_scope_hash_version', '2',
      'source_row_count_semantics', 'FINANCIAL_ONLY',
      'communication_cleanup_contract_version', '2'
    ), pg_catalog.repeat('4', 64), false,
    '2026-09-08 01:00:00+00', '2026-09-08 01:01:00+00',
    '2026-09-08 01:01:00+00', true
  );

  insert into public.pay_payment_correction_items (
    id, correction_request_id, pay_batch_id, pay_batch_candidate_id,
    candidate_id, pay_batch_item_id, correction_item_kind, status,
    created_at_utc, applied_at_utc
  )
  select pg_catalog.md5(
           'payment-cancellation-scale-correction-item:' ||
           pg_catalog.current_database() || ':' || scale_item.ordinal::text
         )::uuid,
         v_request_id,
         v_batch_id,
         v_pay_batch_candidate_id,
         v_candidate_id,
         scale_item.item_id,
         'PRE_BANK_CANCEL',
         'APPLIED',
         '2026-09-08 01:00:30+00'::timestamptz,
         '2026-09-08 01:01:00+00'::timestamptz
  from cancel_notice_scale_items_2305 as scale_item;

  -- Same-batch noise deliberately places the exact target last in creation
  -- order.  It is not old-request compatibility data and can never qualify.
  insert into public.mail_outbox (
    id, type, "to", subject, attachments, status, created_at_utc, sent_at,
    reference, recipient_kind, recipient_id, context_kind, context_id,
    provider_status, payment_scope_json, deterministic_outbox_key,
    attachments_ready, attachment_total_bytes, cancel_notice_tracked
  )
  select pg_catalog.md5(
           'payment-cancellation-scale-noise-mail:' ||
           pg_catalog.current_database() || ':' || generated.ordinal::text
         )::uuid,
         'REMITTANCE',
         'noise-recipient@example.invalid',
         'Noise payment notice',
         '[]'::jsonb,
         'SENT'::public.mail_status_enum,
         '2026-09-08 01:02:00+00'::timestamptz +
           (generated.ordinal || ' microseconds')::interval,
         '2026-09-08 01:02:30+00'::timestamptz +
           (generated.ordinal || ' microseconds')::interval,
         'scale-noise:' || generated.ordinal::text,
         'candidate',
         v_candidate_id,
         'pay_batches',
         v_batch_id,
         'ACCEPTED',
         pg_catalog.jsonb_build_object(
           'remittance_type', 'CANDIDATE_REMITTANCE',
           'pay_batch_id', v_batch_id,
           'candidate_id', pg_catalog.md5(
             'scale-noise-candidate:' || generated.ordinal::text
           )::uuid,
           'pay_batch_candidate_id', pg_catalog.md5(
             'scale-noise-pbc:' || generated.ordinal::text
           )::uuid,
           'pay_batch_item_ids', pg_catalog.jsonb_build_array(
             pg_catalog.md5('scale-noise-item:' || generated.ordinal::text)::uuid
           ),
           'item_count', 1
         ),
         'scale-noise:' || pg_catalog.current_database() || ':' ||
           generated.ordinal::text,
         true,
         0,
         false
  from pg_catalog.generate_series(1, 5000) as generated(ordinal);

  insert into public.mail_outbox (
    id, type, "to", subject, attachments, status, created_at_utc, sent_at,
    reference, recipient_kind, recipient_id, context_kind, context_id,
    provider_status, payment_scope_json, deterministic_outbox_key,
    attachments_ready, attachment_total_bytes, cancel_notice_tracked
  ) values (
    v_source_mail_id, 'REMITTANCE', 'scale-recipient@example.invalid',
    'Original scale payment notice', '[]'::jsonb,
    'SENT'::public.mail_status_enum, '2026-09-08 01:03:00+00',
    '2026-09-08 01:03:30+00', 'scale-candidate-remittance', 'candidate',
    v_candidate_id, 'pay_batches', v_batch_id, 'ACCEPTED',
    pg_catalog.jsonb_build_object(
      'remittance_type', 'CANDIDATE_REMITTANCE',
      'pay_batch_id', v_batch_id,
      'candidate_id', v_candidate_id,
      'pay_batch_candidate_id', v_pay_batch_candidate_id,
      'pay_batch_item_ids', (
        select pg_catalog.jsonb_agg(scale_item.item_id::text order by scale_item.ordinal)
        from cancel_notice_scale_items_2305 as scale_item
      ),
      'item_count', 5000
    ),
    'scale-original:' || v_source_mail_id::text,
    true, 0, false
  );

  execute pg_catalog.format(
    $explain$
      explain (format json, costs off)
      with request_scope(candidate_id, pay_batch_candidate_id) as (
        values (%L::text, %L::text)
      ), candidate_source_per_scope as materialized (
        select exact_source.id, exact_source.created_at_utc
        from request_scope
        cross join lateral (
          select source_mail.id, source_mail.created_at_utc
          from public.mail_outbox as source_mail
          where source_mail.context_id = %L::uuid
            and source_mail.payment_scope_json->>'candidate_id' =
                request_scope.candidate_id
            and source_mail.payment_scope_json->>'pay_batch_candidate_id' =
                request_scope.pay_batch_candidate_id
            and source_mail.context_kind = 'pay_batches'
            and source_mail.type = 'REMITTANCE'
            and source_mail.status = 'SENT'::public.mail_status_enum
            and source_mail.sent_at is not null
            and source_mail.provider_status = 'ACCEPTED'
          order by source_mail.created_at_utc, source_mail.id
          limit 51
        ) as exact_source
      )
      select candidate_source_per_scope.id
      from candidate_source_per_scope
      order by candidate_source_per_scope.created_at_utc,
               candidate_source_per_scope.id
      limit 51
    $explain$,
    v_candidate_id::text,
    v_pay_batch_candidate_id::text,
    v_batch_id::text
  ) into v_plan;
  v_plan_text := v_plan::text;
  if v_plan_text !~ 'mail_outbox_payment_cancellation_sent_candidate_source_idx' then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_TARGET_LAST_INDEX_PLAN_INVALID',
      detail = v_plan_text;
  end if;

  v_started_at := pg_catalog.clock_timestamp();
  v_result := public.pay_payment_cancellation_notice_reconcile_v1(
    p_correction_request_id => v_request_id,
    p_limit => 50
  );
  v_elapsed_ms := extract(
    epoch from pg_catalog.clock_timestamp() - v_started_at
  ) * 1000;
  raise notice 'PAYMENT_CANCELLATION_NOTICE_5000_ITEM_MS=%', v_elapsed_ms;
  if v_elapsed_ms >= 6000
     or v_result->>'examined' is distinct from '1'
     or v_result->>'eligible' is distinct from '1'
     or v_result->>'queued' is distinct from '1'
     or v_result->>'has_more' is distinct from 'false' then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_5000_ITEM_RESULT_INVALID',
      detail = v_elapsed_ms::text || 'ms ' || v_result::text;
  end if;

  select pg_catalog.count(*)::integer
    into v_notice_count
  from public.mail_outbox as notice_row
  where notice_row.deterministic_outbox_key =
          'PAYMENT_CANCELLATION_NOTICE_V1:' || v_source_mail_id::text
    and notice_row.type = 'PAYMENT_CANCELLATION'
    and notice_row.subject = 'Payment cancelled'
    and notice_row.payment_scope_json->>'covered_item_count' = '5000';
  if v_notice_count <> 1 then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_5000_ITEM_OUTPUT_INVALID';
  end if;

  insert into public.mail_outbox (
    id, type, "to", subject, attachments, status, created_at_utc, sent_at,
    reference, recipient_kind, recipient_id, context_kind, context_id,
    provider_status, payment_scope_json, deterministic_outbox_key,
    attachments_ready, attachment_total_bytes, cancel_notice_tracked
  ) values (
    v_boundary_mail_id, 'REMITTANCE', 'boundary-recipient@example.invalid',
    'Boundary payment notice', '[]'::jsonb,
    'SENT'::public.mail_status_enum, '2026-09-08 01:04:00+00',
    '2026-09-08 01:04:30+00', 'boundary-candidate-remittance', 'candidate',
    v_candidate_id, 'pay_batches', v_batch_id, 'ACCEPTED',
    pg_catalog.jsonb_build_object(
      'remittance_type', 'CANDIDATE_REMITTANCE',
      'pay_batch_id', v_batch_id,
      'candidate_id', v_candidate_id,
      'pay_batch_candidate_id', v_pay_batch_candidate_id,
      'pay_batch_item_ids', (
        select pg_catalog.jsonb_agg(
                 pg_catalog.md5('boundary-item:' || generated.ordinal::text)::uuid
                 order by generated.ordinal
               )
        from pg_catalog.generate_series(1, 50001) as generated(ordinal)
      ),
      'item_count', 50001
    ),
    'boundary-original:' || v_boundary_mail_id::text,
    true, 0, true
  );
  v_started_at := pg_catalog.clock_timestamp();
  v_result := public.pay_payment_cancellation_notice_reconcile_v1(
    p_original_mail_outbox_id => v_boundary_mail_id
  );
  v_elapsed_ms := extract(
    epoch from pg_catalog.clock_timestamp() - v_started_at
  ) * 1000;
  raise notice 'PAYMENT_CANCELLATION_NOTICE_50001_REJECT_MS=%', v_elapsed_ms;
  if v_result->'reason_counts'->>'ORIGINAL_ITEM_SCOPE_SIZE_INVALID'
       is distinct from '1'
     or v_result->>'queued' is distinct from '0'
     or v_elapsed_ms >= 6000 then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_50001_REJECTION_INVALID',
      detail = v_elapsed_ms::text || 'ms ' || v_result::text;
  end if;

  -- A valid saved page at 49,950 plus another 100 exact source messages must
  -- fail atomically at the settled 50,000-event ceiling.  In particular, the
  -- routine must not persist an impossible 50,050 cursor, consume any source
  -- event marker, or leave any of the page's notices behind.
  insert into public.mail_outbox (
    id, type, "to", subject, attachments, status, created_at_utc, sent_at,
    reference, recipient_kind, recipient_id, context_kind, context_id,
    provider_status, payment_scope_json, deterministic_outbox_key,
    attachments_ready, attachment_total_bytes, cancel_notice_tracked
  )
  select pg_catalog.md5(
           'payment-cancellation-near-ceiling-mail:' ||
           pg_catalog.current_database() || ':' || generated.ordinal::text
         )::uuid,
         'REMITTANCE',
         'near-ceiling-recipient@example.invalid',
         'Original near-ceiling payment notice',
         '[]'::jsonb,
         'SENT'::public.mail_status_enum,
         '2026-09-08 02:00:00+00'::timestamptz +
           (generated.ordinal || ' microseconds')::interval,
         '2026-09-08 02:01:00+00'::timestamptz +
           (generated.ordinal || ' microseconds')::interval,
         'near-ceiling-candidate-remittance:' || generated.ordinal::text,
         'candidate',
         v_candidate_id,
         'pay_batches',
         v_batch_id,
         'ACCEPTED',
         pg_catalog.jsonb_build_object(
           'remittance_type', 'CANDIDATE_REMITTANCE',
           'pay_batch_id', v_batch_id,
           'candidate_id', v_candidate_id,
           'pay_batch_candidate_id', v_pay_batch_candidate_id,
           'pay_batch_item_ids', pg_catalog.jsonb_build_array(
             (
               select scale_item.item_id
               from cancel_notice_scale_items_2305 as scale_item
               where scale_item.ordinal = 1
             )
           ),
           'item_count', 1
         ),
         'near-ceiling-original:' || pg_catalog.md5(
           'payment-cancellation-near-ceiling-mail:' ||
           pg_catalog.current_database() || ':' || generated.ordinal::text
         )::uuid::text,
         true,
         0,
         true
  from pg_catalog.generate_series(1, 100) as generated(ordinal);

  update public.pay_payment_correction_requests as request_row
     set cancel_notice_reconciled_applied_at_utc = null,
         cancel_notice_progress_applied_at_utc = request_row.applied_at_utc,
         cancel_notice_after_created_at_utc =
           '2026-09-08 01:59:59+00'::timestamptz,
         cancel_notice_after_mail_outbox_id =
           '00000000-0000-0000-0000-000000000000'::uuid,
         cancel_notice_next_attempt_at_utc = null,
         cancel_notice_result_json = pg_catalog.jsonb_build_object(
           'template_version', 'PAYMENT_CANCELLATION_NOTICE_V1',
           'reconciliation_authority', 'ASYNCHRONOUS_SENT_ONLY',
           'event_applied_at_utc', pg_catalog.to_char(
             pg_catalog.timezone('UTC', request_row.applied_at_utc),
             'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
           ),
           'examined', 49950,
           'eligible', 0,
           'queued', 0,
           'already_present', 0,
           'skipped', 49950,
           'reason_counts', pg_catalog.jsonb_build_object(
             'ORIGINAL_CONTEXT_INVALID', 49950
           ),
           'complete', false,
           'last_source_created_at_utc', '2026-09-08T01:59:59.000000Z',
           'last_source_mail_outbox_id',
             '00000000-0000-0000-0000-000000000000',
           'result_code', 'SOURCE_PAGE_CONTINUES'
         )
   where request_row.id = v_request_id;

  select pg_catalog.to_jsonb(request_row)
    into strict v_near_ceiling_before
  from public.pay_payment_correction_requests as request_row
  where request_row.id = v_request_id;

  begin
    perform public.pay_payment_cancellation_notice_reconcile_v1(
      p_correction_request_id => v_request_id,
      p_limit => 100
    );
  exception
    when sqlstate '54000' then
      if sqlerrm is distinct from
           'PAYMENT_CANCELLATION_NOTICE_REQUEST_EVENT_CEILING_EXCEEDED' then
        raise;
      end if;
      v_near_ceiling_raised := true;
  end;

  if not v_near_ceiling_raised then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_NEAR_CEILING_NOT_REJECTED';
  end if;

  select pg_catalog.to_jsonb(request_row)
    into strict v_near_ceiling_after
  from public.pay_payment_correction_requests as request_row
  where request_row.id = v_request_id;

  select pg_catalog.count(*)::integer
    into v_notice_count
  from public.mail_outbox as notice_row
  where notice_row.deterministic_outbox_key = any (
    select 'PAYMENT_CANCELLATION_NOTICE_V1:' || pg_catalog.md5(
             'payment-cancellation-near-ceiling-mail:' ||
             pg_catalog.current_database() || ':' || generated.ordinal::text
           )::uuid::text
    from pg_catalog.generate_series(1, 100) as generated(ordinal)
  );

  if v_near_ceiling_after is distinct from v_near_ceiling_before
     or v_notice_count <> 0
     or exists (
       select 1
       from public.mail_outbox as source_mail
       where source_mail.id = any (
         select pg_catalog.md5(
                  'payment-cancellation-near-ceiling-mail:' ||
                  pg_catalog.current_database() || ':' || generated.ordinal::text
                )::uuid
         from pg_catalog.generate_series(1, 100) as generated(ordinal)
       )
         and source_mail.cancel_notice_reconciled_sent_at_utc is not null
     ) then
    raise exception using
      errcode = 'P0001',
      message = 'PAYMENT_CANCELLATION_NOTICE_NEAR_CEILING_NOT_ATOMIC';
  end if;
end
$scale$;

rollback;
