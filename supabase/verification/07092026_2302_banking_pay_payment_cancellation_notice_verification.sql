\set ON_ERROR_STOP on

do $verification$
declare
  v_function_oid oid := pg_catalog.to_regprocedure(
    'public.pay_payment_cancellation_notice_reconcile_v1(uuid,uuid,timestamp with time zone,uuid,integer,text)'
  );
  v_function_definition text;
  v_function_owner text;
  v_public_execute boolean;
  v_execute_grantees text[];
  v_expected_execute_grantees text[];
  v_execute_grant_option boolean;
  v_constraint_definition text;
  v_constraint_validated boolean;
  v_index_definition text;
  v_index_valid boolean;
  v_index_ready boolean;
  v_invalid_column_count integer;
  v_disallowed_update_target text;
  v_insert_count integer;
  v_before_count bigint;
  v_after_count bigint;
  v_expected_constraint constant text :=
    'CHECK (((type = ANY (ARRAY[''INVOICE''::text, ''REMITTANCE''::text, ''TSO_FAILURE''::text, ''BROADCAST''::text, ''TIMESHEET_QR''::text, ''TIMESHEET_REFUSAL''::text, ''TIMESHEET_GENERAL''::text, ''TIMESHEET_QUERY''::text, ''PAYMENT_CANCELLATION''::text])) OR (type = ''MAILSHOT_EMAIL''::text)))';
begin
  if v_function_oid is null then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_FUNCTION_MISSING';
  end if;

  select pg_catalog.pg_get_functiondef(function_row.oid),
         pg_catalog.pg_get_userbyid(function_row.proowner),
         exists (
           select 1
           from pg_catalog.aclexplode(
             coalesce(
               function_row.proacl,
               pg_catalog.acldefault('f', function_row.proowner)
             )
           ) as acl_row
           where acl_row.grantee = 0
             and acl_row.privilege_type = 'EXECUTE'
         )
    into v_function_definition, v_function_owner, v_public_execute
  from pg_catalog.pg_proc as function_row
  where function_row.oid = v_function_oid
    and function_row.prosecdef is true
    and function_row.provolatile = 'v'
    and function_row.proparallel = 'u'
    and function_row.proconfig @> array[
      'search_path=""',
      'statement_timeout=6000ms',
      'lock_timeout=1000ms'
    ]::text[];

  if v_function_definition is null
     or v_function_owner not in ('postgres', current_user) then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_FUNCTION_METADATA_INVALID';
  end if;

  select coalesce(
           pg_catalog.array_agg(
             normalized_acl.grantee_name
             order by normalized_acl.grantee_name
           ),
           array[]::text[]
         ),
         coalesce(
           pg_catalog.bool_or(normalized_acl.is_grantable),
           false
         )
    into v_execute_grantees, v_execute_grant_option
  from (
    select distinct
           case
             when acl_row.grantee = 0 then 'PUBLIC'
             else pg_catalog.pg_get_userbyid(acl_row.grantee)
           end as grantee_name,
           acl_row.is_grantable
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
      array[v_function_owner, 'service_role']::text[]
    ) as expected_name(grantee_name)
  ) as expected_grantee;

  if not pg_catalog.has_function_privilege('service_role', v_function_oid, 'EXECUTE')
     or not pg_catalog.has_function_privilege(v_function_owner, v_function_oid, 'EXECUTE')
     or pg_catalog.has_function_privilege('anon', v_function_oid, 'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated', v_function_oid, 'EXECUTE')
     or coalesce(pg_catalog.has_function_privilege(
          pg_catalog.to_regrole('authenticator'), v_function_oid, 'EXECUTE'
        ), false)
     or coalesce(pg_catalog.has_function_privilege(
          pg_catalog.to_regrole('supabase_admin'), v_function_oid, 'EXECUTE'
        ), false)
     or v_public_execute is true
     or v_execute_grantees is distinct from v_expected_execute_grantees
     or v_execute_grant_option is true then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_FUNCTION_ACL_INVALID';
  end if;

  select pg_catalog.pg_get_constraintdef(constraint_row.oid),
         constraint_row.convalidated
    into v_constraint_definition, v_constraint_validated
  from pg_catalog.pg_constraint as constraint_row
  where constraint_row.conrelid = 'public.mail_outbox'::pg_catalog.regclass
    and constraint_row.conname = 'mail_outbox_type_check'
    and constraint_row.contype = 'c';
  if v_constraint_definition is distinct from v_expected_constraint
     or v_constraint_validated is not true then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_MAIL_TYPE_CONTRACT_INVALID';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid, index_row.indisready
    into v_index_definition, v_index_valid, v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid = pg_catalog.to_regclass(
    'public.mail_outbox_payment_cancellation_sent_source_idx'
  );
  if v_index_definition is distinct from
       'CREATE INDEX mail_outbox_payment_cancellation_sent_source_idx ON public.mail_outbox USING btree (context_id, created_at_utc, id) WHERE ((context_kind = ''pay_batches''::text) AND (type = ''REMITTANCE''::text) AND (status = ''SENT''::mail_status_enum) AND (sent_at IS NOT NULL) AND (provider_status = ''ACCEPTED''::text))'
     or v_index_valid is not true or v_index_ready is not true then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_SOURCE_INDEX_INVALID';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid, index_row.indisready
    into v_index_definition, v_index_valid, v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid = pg_catalog.to_regclass(
    'public.mail_outbox_payment_cancellation_sent_candidate_source_idx'
  );
  if v_index_definition is distinct from
       'CREATE INDEX mail_outbox_payment_cancellation_sent_candidate_source_idx ON public.mail_outbox USING btree (context_id, ((payment_scope_json ->> ''candidate_id''::text)), ((payment_scope_json ->> ''pay_batch_candidate_id''::text)), created_at_utc, id) WHERE ((context_kind = ''pay_batches''::text) AND (type = ''REMITTANCE''::text) AND (status = ''SENT''::mail_status_enum) AND (sent_at IS NOT NULL) AND (provider_status = ''ACCEPTED''::text))'
     or v_index_valid is not true or v_index_ready is not true then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_CANDIDATE_SOURCE_INDEX_INVALID';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid, index_row.indisready
    into v_index_definition, v_index_valid, v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid = pg_catalog.to_regclass(
    'public.mail_outbox_payment_cancellation_pending_idx'
  );
  if v_index_definition is distinct from
       'CREATE INDEX mail_outbox_payment_cancellation_pending_idx ON public.mail_outbox USING btree (COALESCE(cancel_notice_next_attempt_at_utc, sent_at), sent_at, id) WHERE ((cancel_notice_tracked IS TRUE) AND (type = ''REMITTANCE''::text) AND (status = ''SENT''::mail_status_enum) AND (sent_at IS NOT NULL) AND (provider_status = ''ACCEPTED''::text) AND (cancel_notice_reconciled_sent_at_utc IS DISTINCT FROM sent_at))'
     or v_index_valid is not true or v_index_ready is not true then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_PENDING_MAIL_INDEX_INVALID';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid, index_row.indisready
    into v_index_definition, v_index_valid, v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid = pg_catalog.to_regclass(
    'public.pay_payment_cancellation_notice_recovery_request_idx'
  );
  if v_index_definition is distinct from
       'CREATE INDEX pay_payment_cancellation_notice_recovery_request_idx ON public.pay_payment_correction_requests USING btree (COALESCE(cancel_notice_next_attempt_at_utc, applied_at_utc), applied_at_utc, id) WHERE ((cancel_notice_tracked IS TRUE) AND (applied_at_utc IS NOT NULL) AND (cancel_notice_reconciled_applied_at_utc IS DISTINCT FROM applied_at_utc) AND (status = ANY (ARRAY[''APPLIED''::text, ''APPLIED_WITH_BLOCKERS''::text])) AND (correction_kind = ANY (ARRAY[''PRE_BANK_CANCEL''::text, ''NO_MONEY_UNWIND''::text])) AND ((plan_json ->> ''candidate_scope_contract_version''::text) = ''2''::text) AND ((plan_json ->> ''candidate_scope_hash_version''::text) = ''2''::text) AND ((plan_json ->> ''source_row_count_semantics''::text) = ''FINANCIAL_ONLY''::text) AND ((plan_json ->> ''communication_cleanup_contract_version''::text) = ''2''::text))'
     or v_index_valid is not true or v_index_ready is not true then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_PENDING_REQUEST_INDEX_INVALID';
  end if;

  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid, index_row.indisready
    into v_index_definition, v_index_valid, v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid = pg_catalog.to_regclass(
    'public.pay_payment_correction_items_applied_item_kind_uidx'
  );
  if v_index_definition is distinct from
       'CREATE UNIQUE INDEX pay_payment_correction_items_applied_item_kind_uidx ON public.pay_payment_correction_items USING btree (pay_batch_item_id, correction_item_kind) WHERE ((status = ''APPLIED''::text) AND (pay_batch_item_id IS NOT NULL))'
     or v_index_valid is not true or v_index_ready is not true then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_APPLIED_ITEM_UNIQUENESS_INVALID';
  end if;

  select pg_catalog.count(*)::integer
    into v_invalid_column_count
  from (
    values
      ('mail_outbox', 'cancel_notice_tracked', 'boolean', 'true'),
      ('mail_outbox', 'cancel_notice_reconciled_sent_at_utc', 'timestamp with time zone', null),
      ('mail_outbox', 'cancel_notice_next_attempt_at_utc', 'timestamp with time zone', null),
      ('mail_outbox', 'cancel_notice_result_code', 'text', null),
      ('pay_payment_correction_requests', 'cancel_notice_tracked', 'boolean', 'true'),
      ('pay_payment_correction_requests', 'cancel_notice_reconciled_applied_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_progress_applied_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_after_created_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_after_mail_outbox_id', 'uuid', null),
      ('pay_payment_correction_requests', 'cancel_notice_next_attempt_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_result_json', 'jsonb', '''{}''::jsonb')
  ) as expected(table_name, column_name, data_type, column_default)
  left join information_schema.columns as actual
    on actual.table_schema = 'public'
   and actual.table_name = expected.table_name
   and actual.column_name = expected.column_name
  where actual.column_name is null
     or actual.data_type is distinct from expected.data_type
     or actual.is_nullable is distinct from 'YES'
     or actual.column_default is distinct from expected.column_default;
  if v_invalid_column_count <> 0 then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_RECOVERY_COLUMN_INVALID';
  end if;

  if v_function_definition !~* 'PAYMENT_CANCELLATION_NOTICE_EXTERNAL_CURSOR_PROHIBITED'
     or v_function_definition !~* 'p_limit < 1 OR p_limit > 100'
     or v_function_definition !~* 'jsonb_array_length\(v_item_json\)[[:space:]]*>[[:space:]]*50000'
     or v_function_definition ~* '>128'
     or v_function_definition !~* 'status::text IS DISTINCT FROM ''SENT'''
     or v_function_definition !~* 'provider_status IS DISTINCT FROM ''ACCEPTED'''
     or v_function_definition !~* 'correction_item[.]applied_at_utc IS NOT NULL'
     or v_function_definition !~* 'correction_item[.]pay_batch_candidate_id = ANY'
     or v_function_definition !~* 'PAYMENT_CANCELLATION_NOTICE_RESULT_INVARIANT_VIOLATION'
     or v_function_definition !~* 'SAVED_PROGRESS_INVALID'
     or v_function_definition !~* 'FOR UPDATE SKIP LOCKED'
     or v_function_definition !~* 'CROSS JOIN LATERAL'
     or v_function_definition !~* 'PAYMENT_CANCELLATION_NOTICE_REQUEST_EVENT_CEILING_EXCEEDED'
     or v_function_definition !~* 'recovery_claim_contended'
     or v_function_definition !~* 'timezone\(''UTC'', v_bound_request[.]applied_at_utc\)'
     or v_function_definition !~* 'progress_owner'', ''SERVER_ROW'''
     or v_function_definition !~* 'cancel_notice_reconciled_sent_at_utc'
     or v_function_definition !~* 'cancel_notice_reconciled_applied_at_utc'
     or v_function_definition !~* '''CANDIDATE_REMITTANCE'''
     or v_function_definition !~* '''UMBRELLA_REMITTANCE'''
     or v_function_definition !~* '''PAYOUT_NOTICE_CANDIDATE'''
     or v_function_definition !~* '''ORIGINAL_RECIPIENT_IDENTITY_MISMATCH'''
     or v_function_definition !~* '''EXISTING_NOTICE_IDENTITY_CONFLICT'''
     or v_function_definition !~* '''PAYMENT_CANCELLATION'''
     or v_function_definition !~* 'v_notice_subject := ''Payment cancelled'''
     or v_function_definition ~* 'v_notice_subject[[:space:]]*:=[^;]*v_original[.]subject'
     or v_function_definition !~* '''html'''
     or v_function_definition !~* '''\[\]''::jsonb' then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_SOURCE_BOUNDARY_INVALID';
  end if;

  select (match_row.captures)[1]
    into v_disallowed_update_target
  from pg_catalog.regexp_matches(
         pg_catalog.lower(v_function_definition),
         '\mupdate\M[[:space:]]+public[.]([a-z0-9_]+)', 'g'
       ) as match_row(captures)
  where (match_row.captures)[1] not in (
    'mail_outbox', 'pay_payment_correction_requests'
  )
  limit 1;
  if v_disallowed_update_target is not null
     or v_function_definition ~* '\m(delete|truncate)\M[[:space:]]+(from[[:space:]]+|table[[:space:]]+)?public[.]' then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_DISALLOWED_WRITE_TARGET';
  end if;

  select pg_catalog.count(*)::integer
    into v_insert_count
  from pg_catalog.regexp_matches(
         pg_catalog.lower(v_function_definition),
         '\minsert\M[[:space:]]+into[[:space:]]+public[.]mail_outbox', 'g'
       );
  if v_insert_count <> 1
     or pg_catalog.regexp_count(
          pg_catalog.lower(v_function_definition),
          '\minsert\M[[:space:]]+into[[:space:]]+public[.]'
        ) <> 1 then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_INSERT_BOUNDARY_INVALID';
  end if;

  select pg_catalog.count(*) into v_before_count from public.mail_outbox;
  begin
    perform public.pay_payment_cancellation_notice_reconcile_v1(
      pg_catalog.gen_random_uuid(), pg_catalog.gen_random_uuid(),
      null, null, 50, 'PAYMENT_CANCELLATION_NOTICE_V1'
    );
    raise exception 'PAYMENT_CANCELLATION_NOTICE_MODE_CONFLICT_NOT_REJECTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'PAYMENT_CANCELLATION_NOTICE_MODE_CONFLICT' then raise; end if;
  end;

  begin
    perform public.pay_payment_cancellation_notice_reconcile_v1(
      null, null, pg_catalog.statement_timestamp(), null,
      50, 'PAYMENT_CANCELLATION_NOTICE_V1'
    );
    raise exception 'PAYMENT_CANCELLATION_NOTICE_HALF_CURSOR_NOT_REJECTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'PAYMENT_CANCELLATION_NOTICE_CURSOR_PAIR_REQUIRED' then raise; end if;
  end;

  begin
    perform public.pay_payment_cancellation_notice_reconcile_v1(
      null, null, pg_catalog.statement_timestamp(), pg_catalog.gen_random_uuid(),
      50, 'PAYMENT_CANCELLATION_NOTICE_V1'
    );
    raise exception 'PAYMENT_CANCELLATION_NOTICE_EXTERNAL_CURSOR_NOT_REJECTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'PAYMENT_CANCELLATION_NOTICE_EXTERNAL_CURSOR_PROHIBITED' then raise; end if;
  end;

  begin
    perform public.pay_payment_cancellation_notice_reconcile_v1(
      null, null, null, null, 101, 'PAYMENT_CANCELLATION_NOTICE_V1'
    );
    raise exception 'PAYMENT_CANCELLATION_NOTICE_LIMIT_NOT_REJECTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'PAYMENT_CANCELLATION_NOTICE_LIMIT_INVALID' then raise; end if;
  end;

  begin
    perform public.pay_payment_cancellation_notice_reconcile_v1(
      null, null, null, null, 50, 'UNSUPPORTED'
    );
    raise exception 'PAYMENT_CANCELLATION_NOTICE_TEMPLATE_NOT_REJECTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'PAYMENT_CANCELLATION_NOTICE_TEMPLATE_UNSUPPORTED' then raise; end if;
  end;

  select pg_catalog.count(*) into v_after_count from public.mail_outbox;
  if v_after_count is distinct from v_before_count then
    raise exception 'PAYMENT_CANCELLATION_NOTICE_INVALID_INPUT_WROTE_ROWS';
  end if;
end
$verification$;
