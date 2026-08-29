CREATE OR REPLACE FUNCTION public.rpc_changes_ping(p_last_seen jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_last_seen jsonb := COALESCE(p_last_seen, '{}'::jsonb);
  v_seqs jsonb := '{}'::jsonb;
  v_changed text[] := ARRAY[]::text[];
  v_prev bigint := 0;
  v_cur bigint := 0;
  v_counter_record record;
  v_actor_user_id uuid := NULL::uuid;
  v_last_alert_hash text := NULL::text;
  v_cached_alert_hash text := 'banking_alert_signal:v3:' || MD5('');
  v_cached_summary_hash text := 'banking_alert_summary:v3:' || MD5('');
  v_cached_unacknowledged_count integer := 0;
  v_cached_highest_severity text := NULL::text;
  v_cached_highest_label text := NULL::text;
  v_watched_batch_ids jsonb := '[]'::jsonb;
  v_watched_batch_signals jsonb := '[]'::jsonb;
  v_explicit_entity_keys jsonb := '[]'::jsonb;
  v_ping_context text := NULL::text;
  v_has_watched_batch_ids boolean := false;
  v_is_banking_pay_mode boolean := false;
  v_allow_generic_global_scan boolean := false;
  v_app_counter_mode text := 'SKIPPED';
  v_result jsonb := '{}'::jsonb;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_candidate_summary_watch boolean:=false;
  v_candidate_summary_cursor bigint:=0;
  v_candidate_summary_high_watermark bigint:=0;
  v_candidate_summary_changed_identities jsonb:='[]'::jsonb;
  v_candidate_summary_overflow boolean:=false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('CHANGES_PING');

  IF COALESCE(jsonb_typeof(v_last_seen), 'null') <> 'object' THEN
    v_last_seen := '{}'::jsonb;
  END IF;

  v_ping_context := UPPER(NULLIF(BTRIM(COALESCE(
    v_last_seen ->> '__changes_ping_context',
    v_last_seen ->> '__route_context',
    v_last_seen ->> 'route_context',
    v_last_seen ->> '__context',
    v_last_seen ->> 'context',
    v_last_seen ->> '__mode',
    v_last_seen ->> 'mode',
    ''
  )), ''));

  IF v_ping_context IS NULL THEN
    v_ping_context := 'UNSPECIFIED';
  END IF;

  BEGIN
    v_actor_user_id := NULLIF(BTRIM(COALESCE(v_last_seen ->> '__banking_actor_user_id', '')), '')::uuid;
  EXCEPTION WHEN OTHERS THEN
    v_actor_user_id := NULL::uuid;
  END;

  IF NOT v_service THEN
    IF auth.uid() IS NULL THEN
      RAISE EXCEPTION USING ERRCODE='42501',
        MESSAGE='AUTHENTICATED_ACTOR_REQUIRED';
    END IF;
    IF v_actor_user_id IS NOT NULL
       AND v_actor_user_id IS DISTINCT FROM auth.uid() THEN
      RAISE EXCEPTION USING ERRCODE='42501',
        MESSAGE='AUTHENTICATED_ACTOR_MISMATCH';
    END IF;
    v_actor_user_id:=auth.uid();
    IF NOT EXISTS(
      SELECT 1 FROM public.tms_users u
      WHERE u.id=v_actor_user_id AND u.is_active
        AND lower(btrim(coalesce(u.role,'')))='admin') THEN
      RAISE EXCEPTION USING ERRCODE='42501',
        MESSAGE='ADMINISTRATOR_PERMISSION_REQUIRED';
    END IF;
  END IF;

  v_last_alert_hash := NULLIF(BTRIM(COALESCE(
    v_last_seen ->> '__banking_alert_hash',
    v_last_seen ->> 'banking_alert_hash',
    ''
  )), '');

  IF jsonb_typeof(v_last_seen -> '__watched_pay_batch_ids') = 'array' THEN
    v_watched_batch_ids := v_last_seen -> '__watched_pay_batch_ids';
  ELSIF jsonb_typeof(v_last_seen -> 'watched_pay_batch_ids') = 'array' THEN
    v_watched_batch_ids := v_last_seen -> 'watched_pay_batch_ids';
  ELSIF jsonb_typeof(v_last_seen -> 'watched_batches') = 'array' THEN
    v_watched_batch_ids := v_last_seen -> 'watched_batches';
  ELSE
    v_watched_batch_ids := '[]'::jsonb;
  END IF;

  IF COALESCE(jsonb_typeof(v_watched_batch_ids), 'null') = 'array' THEN
    v_has_watched_batch_ids := jsonb_array_length(v_watched_batch_ids) > 0;
  ELSE
    v_has_watched_batch_ids := false;
  END IF;

  IF jsonb_typeof(v_last_seen -> '__watched_entity_keys') = 'array' THEN
    v_explicit_entity_keys := v_last_seen -> '__watched_entity_keys';
  ELSIF jsonb_typeof(v_last_seen -> 'watched_entity_keys') = 'array' THEN
    v_explicit_entity_keys := v_last_seen -> 'watched_entity_keys';
  ELSIF jsonb_typeof(v_last_seen -> '__entity_keys') = 'array' THEN
    v_explicit_entity_keys := v_last_seen -> '__entity_keys';
  ELSIF jsonb_typeof(v_last_seen -> 'entity_keys') = 'array' THEN
    v_explicit_entity_keys := v_last_seen -> 'entity_keys';
  ELSE
    v_explicit_entity_keys := '[]'::jsonb;
  END IF;

  v_is_banking_pay_mode :=
       v_actor_user_id IS NOT NULL
    OR v_has_watched_batch_ids
    OR v_ping_context IN (
      'BANKING',
      'BANKING_PAY',
      'BANKING_PAY_LIST',
      'BANKING_PAY_MODAL',
      'BANKING_PAY_PREVIEW',
      'BANKING_PAY_WATCH',
      'BANKING_PAY_BATCH_WATCH',
      'BANKING_PAY_CHANGES_PING',
      'PAY_BATCH_WATCH',
      'PAY_BATCH_MODAL',
      'PAY_BATCH_LIST'
    );

  v_allow_generic_global_scan :=
       v_is_banking_pay_mode IS NOT TRUE
   AND v_ping_context IN ('GENERIC_APP_COUNTERS', 'APP_CHANGE_COUNTERS', 'GLOBAL_APP_CHANGES', 'NON_BANKING_APP_CHANGES')
   AND LOWER(BTRIM(COALESCE(
      v_last_seen ->> '__allow_global_app_change_counter_scan',
      v_last_seen ->> 'allow_global_app_change_counter_scan',
      'false'
   ))) IN ('true', '1', 'yes', 'y', 'on');

  v_candidate_summary_watch:=lower(btrim(coalesce(
    v_last_seen->>'__candidate_timesheet_summary_watch',
    v_last_seen->>'candidate_timesheet_summary_watch',
    'false'
  ))) in ('true','1','yes','y','on');


  IF COALESCE(jsonb_typeof(v_explicit_entity_keys), 'null') = 'array'
     AND jsonb_array_length(v_explicit_entity_keys) > 0 THEN
    v_app_counter_mode := 'EXPLICIT_CAPPED_KEYS';

    FOR v_counter_record IN
      WITH raw_entity_keys AS (
        SELECT entity_key_entry.value,
               entity_key_entry.ordinality
        FROM jsonb_array_elements(COALESCE(v_explicit_entity_keys, '[]'::jsonb)) WITH ORDINALITY AS entity_key_entry(value, ordinality)
        LIMIT 25
      ),
      parsed_entity_keys AS (
        SELECT
          CASE
            WHEN jsonb_typeof(raw_entity_keys.value) = 'string'
              THEN NULLIF(BTRIM(TRIM(BOTH '"' FROM raw_entity_keys.value::text)), '')
            WHEN jsonb_typeof(raw_entity_keys.value) = 'object'
              THEN NULLIF(BTRIM(COALESCE(raw_entity_keys.value->>'entity_key', raw_entity_keys.value->>'key', raw_entity_keys.value->>'name', '')), '')
            ELSE NULL::text
          END AS entity_key,
          CASE
            WHEN jsonb_typeof(raw_entity_keys.value) = 'object'
             AND COALESCE(raw_entity_keys.value->>'known_seq', raw_entity_keys.value->>'seq', raw_entity_keys.value->>'last_seen_seq', '') ~ '^[0-9]{1,18}$'
              THEN COALESCE(raw_entity_keys.value->>'known_seq', raw_entity_keys.value->>'seq', raw_entity_keys.value->>'last_seen_seq')::bigint
            ELSE NULL::bigint
          END AS known_seq,
          raw_entity_keys.ordinality
        FROM raw_entity_keys
      ),
      requested_entity_keys AS (
        SELECT DISTINCT ON (parsed_entity_keys.entity_key)
          parsed_entity_keys.entity_key,
          parsed_entity_keys.known_seq,
          parsed_entity_keys.ordinality
        FROM parsed_entity_keys
        WHERE parsed_entity_keys.entity_key IS NOT NULL
          AND LENGTH(parsed_entity_keys.entity_key) <= 200
        ORDER BY parsed_entity_keys.entity_key, parsed_entity_keys.ordinality
      )
      SELECT requested_entity_keys.entity_key,
             requested_entity_keys.known_seq,
             COALESCE(change_counter.seq, 0) AS seq
      FROM requested_entity_keys
      LEFT JOIN public.app_change_counters AS change_counter
        ON change_counter.entity_key = requested_entity_keys.entity_key
      ORDER BY requested_entity_keys.ordinality
    LOOP
      v_cur := COALESCE(v_counter_record.seq, 0);

      IF v_counter_record.known_seq IS NOT NULL THEN
        v_prev := COALESCE(v_counter_record.known_seq, 0);
      ELSE
        BEGIN
          v_prev := COALESCE((v_last_seen ->> v_counter_record.entity_key)::bigint, 0);
        EXCEPTION WHEN OTHERS THEN
          v_prev := 0;
        END;
      END IF;

      v_seqs := v_seqs || jsonb_build_object(v_counter_record.entity_key, v_cur);

      IF v_cur > v_prev THEN
        v_changed := array_append(v_changed, v_counter_record.entity_key);
      END IF;
    END LOOP;
  ELSIF v_allow_generic_global_scan THEN
    v_app_counter_mode := 'EXPLICIT_NON_BANKING_GLOBAL_SCAN';

    FOR v_counter_record IN
      SELECT change_counter.entity_key, change_counter.seq
      FROM public.app_change_counters AS change_counter
      ORDER BY change_counter.entity_key
    LOOP
      v_cur := COALESCE(v_counter_record.seq, 0);
      v_prev := 0;

      BEGIN
        v_prev := COALESCE((v_last_seen ->> v_counter_record.entity_key)::bigint, 0);
      EXCEPTION WHEN OTHERS THEN
        v_prev := 0;
      END;

      v_seqs := v_seqs || jsonb_build_object(v_counter_record.entity_key, v_cur);

      IF v_cur > v_prev THEN
        v_changed := array_append(v_changed, v_counter_record.entity_key);
      END IF;
    END LOOP;
  ELSE
    v_app_counter_mode := CASE
      WHEN v_is_banking_pay_mode THEN 'SKIPPED_FOR_BANKING_PAY'
      ELSE 'SKIPPED_NO_EXPLICIT_KEYS'
    END;
  END IF;

  v_result := jsonb_build_object(
    'server_utc', now(),
    'seqs', v_seqs,
    'changed', to_jsonb(v_changed),
    'changes_ping_context', v_ping_context,
    'banking_pay_mode', COALESCE(v_is_banking_pay_mode, false),
    'app_change_counter_mode', v_app_counter_mode,
    'app_change_counter_cap', CASE WHEN v_app_counter_mode = 'EXPLICIT_CAPPED_KEYS' THEN 25 ELSE NULL::integer END
  );

  if v_candidate_summary_watch then
    select coalesce(max(revision_row.revision_seq),0)
    into v_candidate_summary_high_watermark
    from private.candidate_timesheet_summary_revisions as revision_row;

    if coalesce(
      v_last_seen->>'__candidate_timesheet_summary_cursor',
      v_last_seen->>'candidate_timesheet_summary_cursor',
      ''
    ) ~ '^[0-9]{1,18}$' then
      v_candidate_summary_cursor:=least(
        coalesce(
          v_last_seen->>'__candidate_timesheet_summary_cursor',
          v_last_seen->>'candidate_timesheet_summary_cursor'
        )::bigint,
        v_candidate_summary_high_watermark
      );
    else
      v_candidate_summary_cursor:=v_candidate_summary_high_watermark;
    end if;

    with bounded_changes as materialized (
      select
        revision_row.identity_kind,
        revision_row.identity_id,
        revision_row.current_timesheet_id,
        revision_row.contract_week_id,
        revision_row.revision_seq
      from private.candidate_timesheet_summary_revisions as revision_row
      where revision_row.revision_seq>v_candidate_summary_cursor
      order by revision_row.revision_seq,revision_row.identity_kind,revision_row.identity_id
      limit 101
    ), numbered_changes as (
      select bounded_changes.*,
        row_number() over(order by revision_seq,identity_kind,identity_id) as ordinal
      from bounded_changes
    )
    select
      coalesce(jsonb_agg(jsonb_build_object(
        'identity_kind',numbered_changes.identity_kind,
        'identity_id',numbered_changes.identity_id,
        'timesheet_id',case when numbered_changes.identity_kind='TIMESHEET' then numbered_changes.identity_id end,
        'contract_week_id',numbered_changes.contract_week_id,
        'current_timesheet_id',numbered_changes.current_timesheet_id,
        'revision',numbered_changes.revision_seq
      ) order by numbered_changes.revision_seq)
        filter(where numbered_changes.ordinal<=100),'[]'::jsonb),
      count(*)>100
    into v_candidate_summary_changed_identities,v_candidate_summary_overflow
    from numbered_changes;

    v_result:=v_result||jsonb_build_object(
      'candidate_timesheet_summary_cursor',v_candidate_summary_high_watermark,
      'candidate_timesheet_summary_changed_identities',v_candidate_summary_changed_identities,
      'candidate_timesheet_summary_overflow',v_candidate_summary_overflow,
      'candidate_timesheet_summary_cap',100,
      'candidate_timesheet_summary',jsonb_build_object(
        'cursor',v_candidate_summary_high_watermark,
        'changed_identities',v_candidate_summary_changed_identities,
        'overflow',v_candidate_summary_overflow,
        'cap',100
      )
    );
  end if;

  IF v_actor_user_id IS NOT NULL THEN
    SELECT
      COALESCE(alert_summary.alert_hash, v_cached_alert_hash),
      COALESCE(alert_summary.summary_hash, v_cached_summary_hash),
      COALESCE(alert_summary.unacknowledged_count, 0),
      alert_summary.highest_severity,
      alert_summary.highest_label
    INTO
      v_cached_alert_hash,
      v_cached_summary_hash,
      v_cached_unacknowledged_count,
      v_cached_highest_severity,
      v_cached_highest_label
    FROM public.banking_alert_display_summary AS alert_summary
    WHERE alert_summary.actor_user_id = v_actor_user_id;

    IF NOT FOUND THEN
      v_cached_alert_hash := 'banking_alert_signal:v3:' || MD5('');
      v_cached_summary_hash := 'banking_alert_summary:v3:' || MD5('');
      v_cached_unacknowledged_count := 0;
      v_cached_highest_severity := NULL::text;
      v_cached_highest_label := NULL::text;
    END IF;

    v_result := v_result || jsonb_build_object(
      'banking_alert_hash', v_cached_alert_hash,
      'banking_alert_summary_signature', v_cached_summary_hash,
      'banking_alert_summary_changed', COALESCE(v_last_alert_hash IS DISTINCT FROM v_cached_alert_hash, true),
      'banking_unacknowledged_alert_count', COALESCE(v_cached_unacknowledged_count, 0),
      'grouped_banking_unacknowledged_alert_count', COALESCE(v_cached_unacknowledged_count, 0),
      'banking_highest_alert_severity', COALESCE(v_cached_highest_severity, ''),
      'banking_highest_alert_label', COALESCE(v_cached_highest_label, ''),
      'banking_alert_summary_included', false,
      'banking_alert_summary', NULL::jsonb
    );
  END IF;

  IF COALESCE(jsonb_typeof(v_watched_batch_ids), 'null') = 'array' THEN
    WITH raw_watched_batches AS (
      SELECT watched_batch.value,
             watched_batch.ordinality
      FROM jsonb_array_elements(COALESCE(v_watched_batch_ids, '[]'::jsonb)) WITH ORDINALITY AS watched_batch(value, ordinality)
      LIMIT 25
    ),
    parsed_watched_batches AS (
      SELECT
        CASE
          WHEN jsonb_typeof(raw_watched_batches.value) = 'string'
           AND TRIM(BOTH '"' FROM raw_watched_batches.value::text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN TRIM(BOTH '"' FROM raw_watched_batches.value::text)::uuid
          WHEN jsonb_typeof(raw_watched_batches.value) = 'object'
           AND COALESCE(raw_watched_batches.value->>'pay_batch_id', raw_watched_batches.value->>'id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN COALESCE(raw_watched_batches.value->>'pay_batch_id', raw_watched_batches.value->>'id')::uuid
          ELSE NULL::uuid
        END AS pay_batch_id,
        CASE
          WHEN jsonb_typeof(raw_watched_batches.value) = 'object'
           AND COALESCE(raw_watched_batches.value->>'known_version', raw_watched_batches.value->>'live_signal_version', '') ~ '^[0-9]{1,18}$'
            THEN COALESCE(raw_watched_batches.value->>'known_version', raw_watched_batches.value->>'live_signal_version')::bigint
          ELSE NULL::bigint
        END AS known_version,
        raw_watched_batches.ordinality
      FROM raw_watched_batches
    ),
    requested_watched_batches AS (
      SELECT DISTINCT ON (parsed_watched_batches.pay_batch_id)
        parsed_watched_batches.pay_batch_id,
        parsed_watched_batches.known_version,
        parsed_watched_batches.ordinality
      FROM parsed_watched_batches
      WHERE parsed_watched_batches.pay_batch_id IS NOT NULL
      ORDER BY parsed_watched_batches.pay_batch_id, parsed_watched_batches.ordinality
      LIMIT 25
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'pay_batch_id', requested_watched_batches.pay_batch_id::text,
      'known_version', requested_watched_batches.known_version,
      'live_signal_version', COALESCE(batch_signal.version, 0),
      'payment_status_version', COALESCE(batch_signal.payment_status_version, 0),
      'correction_progress_version', COALESCE(batch_signal.correction_progress_version, 0),
      'alert_version', COALESCE(batch_signal.alert_version, 0),
      'overview_version', COALESCE(batch_signal.overview_version, 0),
      'last_status_hash', batch_signal.last_status_hash,
      'last_alert_hash', batch_signal.last_alert_hash,
      'changed_since_version', requested_watched_batches.known_version IS NOT NULL AND COALESCE(batch_signal.version, 0) > requested_watched_batches.known_version
    ) ORDER BY requested_watched_batches.ordinality), '[]'::jsonb)
    INTO v_watched_batch_signals
    FROM requested_watched_batches
    LEFT JOIN public.banking_pay_batch_change_signals AS batch_signal
      ON batch_signal.pay_batch_id = requested_watched_batches.pay_batch_id;

    v_result := v_result || jsonb_build_object(
      'watched_batch_signals', COALESCE(v_watched_batch_signals, '[]'::jsonb),
      'watched_batch_signal_hash', 'watched_batch_signals:v2:' || MD5(COALESCE(v_watched_batch_signals::text, '[]')),
      'watched_batch_cap', 25
    );
  END IF;

  -- Invoice operation watches are independent of Banking Pay and are resolved in one capped set query.
  WITH RECURSIVE raw_watch AS MATERIALIZED (
    SELECT value,ordinality
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(v_last_seen->'watched_invoice_operation_ids')='array'
          THEN v_last_seen->'watched_invoice_operation_ids'
        WHEN jsonb_typeof(v_last_seen->'__watched_invoice_operation_ids')='array'
          THEN v_last_seen->'__watched_invoice_operation_ids'
        ELSE '[]'::jsonb
      END
    ) WITH ORDINALITY x(value,ordinality)
    LIMIT 100
  ),
  parsed AS MATERIALIZED (
    SELECT CASE
      WHEN jsonb_typeof(value)='string'
       AND pg_input_is_valid(trim(both '"' from value::text),'uuid')
        THEN trim(both '"' from value::text)::uuid
      WHEN jsonb_typeof(value)='object'
       AND pg_input_is_valid(coalesce(value->>'operation_id',value->>'id',''),'uuid')
        THEN coalesce(value->>'operation_id',value->>'id')::uuid
      END operation_id,
      CASE WHEN jsonb_typeof(value)='object'
        AND pg_input_is_valid(coalesce(value->>'known_change_seq',value->>'change_seq',''),'bigint')
        THEN coalesce(value->>'known_change_seq',value->>'change_seq')::bigint
        WHEN pg_input_is_valid(coalesce(v_last_seen->>'invoice_operations_seq',''),'bigint')
          THEN(v_last_seen->>'invoice_operations_seq')::bigint
        ELSE 0 END known_seq,
      ordinality
    FROM raw_watch
  ),
  requested AS MATERIALIZED (
    SELECT DISTINCT ON (operation_id) operation_id,known_seq,ordinality
    FROM parsed WHERE operation_id IS NOT NULL ORDER BY operation_id,ordinality
  ),
  actor AS MATERIALIZED (
    SELECT coalesce(auth.role(),'')='service_role' is_service,auth.uid() actor_id,
      exists(select 1 from public.tms_users u
        where u.id=auth.uid() and u.is_active and lower(btrim(coalesce(u.role,'')))='admin') is_admin
  ),
  authorised_ops AS MATERIALIZED (
    SELECT r.ordinality,r.known_seq,o.*
    FROM requested r JOIN public.invoice_operations o ON o.id=r.operation_id
    CROSS JOIN actor a
    WHERE a.is_service or a.is_admin or o.actor_user_id=a.actor_id
  ),
  descendants(root_id,id,depth,path) AS MATERIALIZED (
    select o.id,ch.id,1,array[o.id,ch.id]::uuid[]
    from authorised_ops o
    join public.invoice_operations ch on ch.parent_operation_id=o.id
    union all
    select d.root_id,ch.id,d.depth+1,d.path||ch.id
    from descendants d
    join public.invoice_operations ch on ch.parent_operation_id=d.id
    where not ch.id=any(d.path)
  ),
  expected_purpose AS MATERIALIZED (
    select o.id operation_id,case
      when o.operation_type='ISSUE_INVOICES' then 'FINAL_ISSUE'
      when o.entity_type='TIMESHEET' then 'TIMESHEET'
      when upper(coalesce(o.input_json->>'purpose','')) in(
        'DRAFT_PREVIEW','FINAL_ISSUE','TIMESHEET')
        then upper(o.input_json->>'purpose')
      when o.entity_type='INVOICE' and exists(
        select 1 from public.invoices i
        where i.id=o.entity_id and i.status in('ISSUED','PAID'))
        then 'FINAL_ISSUE'
      else 'DRAFT_PREVIEW' end purpose
    from authorised_ops o
  ),
  document_candidates AS MATERIALIZED (
    select o.id operation_id,v.id,v.purpose,v.r2_key,v.status,v.created_at_utc
    from authorised_ops o join public.invoice_document_versions v on v.operation_id=o.id
    union all
    select d.root_id,v.id,v.purpose,v.r2_key,v.status,v.created_at_utc
    from descendants d
    join public.invoice_document_versions v on v.operation_id=d.id
    union all
    select o.id,v.id,v.purpose,v.r2_key,v.status,v.created_at_utc
    from authorised_ops o
    join public.invoice_operation_chunks c on c.operation_id=o.id and c.chunk_type='ISSUE_INVOICE'
    join public.invoice_document_versions v on v.id=case
      when coalesce(c.payload_json->>'final_document_version_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(c.payload_json->>'final_document_version_id')::uuid end
    union all
    select o.id,v.id,v.purpose,v.r2_key,v.status,v.created_at_utc
    from authorised_ops o join public.invoices i
      on o.entity_type='INVOICE' and i.id=o.entity_id
    join public.invoice_document_versions v
      on v.id in(i.preview_document_version_id,i.issued_document_version_id)
    union all
    select o.id,v.id,v.purpose,v.r2_key,v.status,v.created_at_utc
    from authorised_ops o join public.timesheets t
      on o.entity_type='TIMESHEET' and t.timesheet_id=o.entity_id and t.is_current
    join public.invoice_document_versions v on v.id=t.current_document_version_id
  ),
  selected_documents AS MATERIALIZED (
    select distinct on(c.operation_id) c.*
    from document_candidates c
    join expected_purpose p on p.operation_id=c.operation_id and p.purpose=c.purpose
    order by c.operation_id,case c.status when 'READY' then 0 else 1 end,
      c.created_at_utc desc,c.id desc
  ),
  descendant_change AS MATERIALIZED (
    select d.root_id,
      greatest(max(ch.change_seq),0)::bigint descendant_change_seq,
      bool_or(ch.status in(
        'COMPLETE','FAILED','DEAD_LETTER','BLOCKED','CANCELLED','SUPERSEDED'))
        descendant_terminal
    from descendants d
    join public.invoice_operations ch on ch.id=d.id
    group by d.root_id
  ),
  effective_ops AS MATERIALIZED (
    select o.*,greatest(o.change_seq,coalesce(dc.descendant_change_seq,0))
        effective_change_seq,
      coalesce(dc.descendant_terminal,false) descendant_terminal
    from authorised_ops o
    left join descendant_change dc on dc.root_id=o.id
  ),
  changed_ops AS (
    SELECT o.*,d.id document_version_id,d.r2_key,d.status document_status
    FROM effective_ops o
    LEFT JOIN selected_documents d ON d.operation_id=o.id
    WHERE o.effective_change_seq>o.known_seq
  )
  SELECT v_result||jsonb_build_object(
    'invoice_operations_seq',
      coalesce((select max(effective_change_seq) from effective_ops),0),
    'watched_invoice_operations',coalesce(jsonb_agg(jsonb_build_object(
      'operation_id',o.id,'change_seq',o.effective_change_seq,
      'root_change_seq',o.change_seq,'status',o.status,'phase',o.phase,
      'progress',o.progress_json,'entity_type',o.entity_type,'entity_id',o.entity_id,
      'document_version_id',o.document_version_id,
      'ready_key',case when o.document_status='READY' then o.r2_key end,
      'error_code',o.error_json->>'code',
      'error_summary',coalesce(o.error_json->>'summary',o.error_json->>'message'),
      'notify',o.status in(
        'COMPLETE','FAILED','DEAD_LETTER','BLOCKED','CANCELLED','SUPERSEDED')
        or o.descendant_terminal or o.document_status='READY'
    ) order by o.ordinality),'[]'::jsonb),
    'watched_invoice_operation_cap',100
  ) INTO v_result
  FROM changed_ops o;

  RETURN v_result;
END;
$function$;



revoke all on function public.rpc_changes_ping(jsonb)
  from public,anon,authenticated;
grant execute on function public.rpc_changes_ping(jsonb) to service_role;
