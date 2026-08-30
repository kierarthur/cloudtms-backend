-- LEGACY_UPGRADE-only convergence for the exact general SECURITY DEFINER
-- routine identity and ACL contract already installed on managed TEST.
-- This file is executed only after the full repeatable set. It accepts one
-- sealed pre-state, installs exact current definitions obtained read-only from
-- the current TEST authority, removes only the exact obsolete overloads, and
-- then proves the complete non-Candidate service/browser contract.

begin;
set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $legacy_general_rpc_prestate$
declare
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_hash text;
begin
  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
      and p.proname not in (
        'timesheet_break_entry_effective_get_v1',
        'daily_zero_shifts_review_create_v1'
      )
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not svc_execute),
    pg_catalog.count(*) filter (where anon_execute or auth_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_browser_executable,v_hash
  from targets;

  if v_count<>647 or v_service_missing<>85 or v_browser_executable<>0
     or v_hash<>'79c1d4349118210f3166017e4c0ff43d' then
    raise exception 'LEGACY_GENERAL_RPC_PRESTATE_DRIFT:count=% service_missing=% browser=% hash=%',
      v_count,v_service_missing,v_browser_executable,v_hash;
  end if;
end
$legacy_general_rpc_prestate$;

-- SOURCE public._banking_pay_operation_touch_updated_at()
CREATE OR REPLACE FUNCTION public._banking_pay_operation_touch_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
    new.updated_at_utc := now();
    return new;
end;
$function$;

-- SOURCE public._cloudtms_touch_updated_at_utc()
CREATE OR REPLACE FUNCTION public._cloudtms_touch_updated_at_utc()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  NEW.updated_at_utc := now();
  RETURN NEW;
END;
$function$;

-- SOURCE public._pay_bank_transfers_normalise_status_biu()
CREATE OR REPLACE FUNCTION public._pay_bank_transfers_normalise_status_biu()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  IF NEW.status IS NOT NULL THEN
    NEW.status := upper(btrim(NEW.status));

    IF NEW.status = 'CANCELED' THEN
      NEW.status := 'CANCELLED';
    END IF;
  END IF;

  RETURN NEW;
END;
$function$;

-- SOURCE public._pay_workbench_authoritative_scope_valid_v1(uuid,uuid,bigint,uuid,date,date,text)
CREATE OR REPLACE FUNCTION public._pay_workbench_authoritative_scope_valid_v1(p_session_id uuid, p_candidate_id uuid, p_session_version bigint, p_actor_user_id uuid, p_pay_date date, p_week_ending_cutoff date, p_pay_channel_scope text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS authoritative_session
    JOIN public.banking_pay_workbench_session_scope AS authoritative_scope
      ON authoritative_scope.session_id = authoritative_session.id
     AND authoritative_scope.candidate_id = p_candidate_id
    JOIN public.candidates AS authoritative_candidate
      ON authoritative_candidate.id = authoritative_scope.candidate_id
    WHERE authoritative_session.id = p_session_id
      AND UPPER(BTRIM(authoritative_session.status)) = 'OPEN'
      AND authoritative_session.discarded_at_utc IS NULL
      AND CASE
            WHEN authoritative_session.version IS NULL THEN 1
            ELSE authoritative_session.version
          END = p_session_version
      AND authoritative_session.actor_user_id = p_actor_user_id
      AND authoritative_session.pay_date = p_pay_date
      AND authoritative_session.week_ending_cutoff = p_week_ending_cutoff
      AND UPPER(BTRIM(authoritative_candidate.pay_method)) = UPPER(BTRIM(p_pay_channel_scope))
  );
$function$;

-- SOURCE public._temp_diag_log(text,text,text,jsonb)
CREATE OR REPLACE FUNCTION public._temp_diag_log(p_action text, p_object_type text, p_object_id_text text DEFAULT NULL::text, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_enabled boolean := false;
  v_payload jsonb := '{}'::jsonb;
BEGIN
  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      RETURN;
    WHEN OTHERS THEN
      RETURN;
  END;

  IF COALESCE(v_enabled, false) IS NOT TRUE THEN
    RETURN;
  END IF;

  v_payload := COALESCE(p_payload_json, '{}'::jsonb);

  IF LENGTH(v_payload::text) > 12000 THEN
    v_payload := jsonb_build_object(
      'truncated', true,
      'original_length', LENGTH(COALESCE(p_payload_json, '{}'::jsonb)::text)
    );
  END IF;

  BEGIN
    INSERT INTO public.audit_events (
      actor_user_id,
      actor_display,
      actor_role_at_time,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason,
      ip,
      user_agent,
      correlation_id
    )
    VALUES (
      NULL::uuid,
      NULL::text,
      NULL::text,
      COALESCE(NULLIF(BTRIM(p_object_type), ''), 'TEMP_DIAG'),
      NULLIF(BTRIM(COALESCE(p_object_id_text, '')), ''),
      COALESCE(NULLIF(BTRIM(p_action), ''), 'TEMP_DIAG_STAGE'),
      NULL::jsonb,
      v_payload,
      'TEMP_DIAG',
      NULL::text,
      NULL::text,
      NULL::text
    );
  EXCEPTION
    WHEN OTHERS THEN
      RETURN;
  END;
END;
$function$;

-- SOURCE public.banking_alert_display_summary_touch(uuid,text,text,integer,text,text,jsonb)
CREATE OR REPLACE FUNCTION public.banking_alert_display_summary_touch(p_actor_user_id uuid, p_alert_hash text DEFAULT NULL::text, p_summary_hash text DEFAULT NULL::text, p_unacknowledged_count integer DEFAULT NULL::integer, p_highest_severity text DEFAULT NULL::text, p_highest_label text DEFAULT NULL::text, p_summary_json jsonb DEFAULT NULL::jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  INSERT INTO public.banking_alert_display_summary (
    actor_user_id,
    alert_hash,
    summary_hash,
    unacknowledged_count,
    highest_severity,
    highest_label,
    summary_json,
    updated_at_utc
  )
  VALUES (
    p_actor_user_id,
    p_alert_hash,
    p_summary_hash,
    COALESCE(p_unacknowledged_count, 0),
    p_highest_severity,
    p_highest_label,
    COALESCE(p_summary_json, '{}'::jsonb),
    now()
  )
  ON CONFLICT (actor_user_id) DO UPDATE
  SET
    alert_hash = COALESCE(EXCLUDED.alert_hash, public.banking_alert_display_summary.alert_hash),
    summary_hash = COALESCE(EXCLUDED.summary_hash, public.banking_alert_display_summary.summary_hash),
    unacknowledged_count = COALESCE(EXCLUDED.unacknowledged_count, public.banking_alert_display_summary.unacknowledged_count),
    highest_severity = COALESCE(EXCLUDED.highest_severity, public.banking_alert_display_summary.highest_severity),
    highest_label = COALESCE(EXCLUDED.highest_label, public.banking_alert_display_summary.highest_label),
    summary_json = COALESCE(EXCLUDED.summary_json, public.banking_alert_display_summary.summary_json),
    updated_at_utc = now();
END;
$function$;

-- SOURCE public.codex_debug_activity_snapshot(integer,numeric)
CREATE OR REPLACE FUNCTION public.codex_debug_activity_snapshot(p_query_prefix_len integer DEFAULT 500, p_min_age_seconds numeric DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_len integer := greatest(100, least(coalesce(p_query_prefix_len, 500), 4000));
  v_min_age numeric := greatest(0, coalesce(p_min_age_seconds, 0));
  v_summary jsonb := '[]'::jsonb;
  v_active jsonb := '[]'::jsonb;
  v_blocking jsonb := '[]'::jsonb;
  v_locks jsonb := '[]'::jsonb;
  v_connections jsonb := '[]'::jsonb;
BEGIN
  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_summary
  FROM (
    SELECT
      a.state,
      a.wait_event_type,
      a.wait_event,
      count(*)::int AS n
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
    GROUP BY a.state, a.wait_event_type, a.wait_event
    ORDER BY n DESC
    LIMIT 50
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_active
  FROM (
    SELECT
      a.pid,
      a.usename,
      a.application_name,
      a.client_addr::text AS client_addr,
      a.state,
      a.wait_event_type,
      a.wait_event,
      round(extract(epoch from (now() - a.query_start))::numeric, 1) AS age_s,
      left(regexp_replace(coalesce(a.query, ''), '\s+', ' ', 'g'), v_len) AS query_prefix
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
      AND a.state <> 'idle'
      AND (
        a.query_start IS NULL
        OR extract(epoch from (now() - a.query_start)) >= v_min_age
      )
    ORDER BY a.query_start NULLS LAST
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_blocking
  FROM (
    SELECT
      a.pid,
      a.usename,
      a.application_name,
      a.client_addr::text AS client_addr,
      a.state,
      a.wait_event_type,
      a.wait_event,
      pg_blocking_pids(a.pid) AS blocking_pids,
      round(extract(epoch from (now() - a.query_start))::numeric, 1) AS age_s,
      left(regexp_replace(coalesce(a.query, ''), '\s+', ' ', 'g'), v_len) AS query_prefix
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
      AND (
        cardinality(pg_blocking_pids(a.pid)) > 0
        OR a.wait_event_type IS NOT NULL
        OR a.state = 'active'
      )
    ORDER BY age_s DESC NULLS LAST
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_locks
  FROM (
    SELECT
      l.locktype,
      l.mode,
      l.granted,
      count(*)::int AS n
    FROM pg_locks AS l
    GROUP BY l.locktype, l.mode, l.granted
    ORDER BY n DESC
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_connections
  FROM (
    SELECT
      a.usename,
      a.application_name,
      a.state,
      count(*)::int AS n
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
    GROUP BY a.usename, a.application_name, a.state
    ORDER BY n DESC
    LIMIT 100
  ) AS x;

  RETURN jsonb_build_object(
    'ok', true,
    'generated_at', now(),
    'database', current_database(),
    'activity_summary', v_summary,
    'active_queries', v_active,
    'blocking_activity', v_blocking,
    'lock_summary', v_locks,
    'connection_summary', v_connections
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- SOURCE public.codex_debug_bulk_process_snapshot(uuid[],uuid[])
CREATE OR REPLACE FUNCTION public.codex_debug_bulk_process_snapshot(p_contract_week_ids uuid[] DEFAULT ARRAY[]::uuid[], p_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_contract_weeks jsonb := '[]'::jsonb;
  v_timesheets jsonb := '[]'::jsonb;
  v_evidence_counts jsonb := '[]'::jsonb;
  v_queue_counts jsonb := '[]'::jsonb;
BEGIN
  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_contract_weeks
  FROM (
    SELECT
      cw.id,
      cw.status,
      cw.timesheet_id,
      cw.created_at,
      cw.updated_at
    FROM public.contract_weeks AS cw
    WHERE array_length(p_contract_week_ids, 1) IS NOT NULL
      AND cw.id = ANY(p_contract_week_ids)
    ORDER BY cw.updated_at DESC NULLS LAST, cw.id
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_timesheets
  FROM (
    SELECT
      t.timesheet_id,
      t.status,
      t.is_current,
      t.manual_pdf_r2_key,
      t.created_at,
      t.updated_at
    FROM public.timesheets AS t
    WHERE array_length(p_timesheet_ids, 1) IS NOT NULL
      AND t.timesheet_id = ANY(p_timesheet_ids)
    ORDER BY t.updated_at DESC NULLS LAST, t.timesheet_id
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_evidence_counts
  FROM (
    SELECT
      te.timesheet_id,
      coalesce(te.kind::text, 'UNKNOWN') AS kind,
      count(*)::int AS n,
      max(te.created_at) AS latest_created_at
    FROM public.timesheet_evidence AS te
    WHERE array_length(p_timesheet_ids, 1) IS NOT NULL
      AND te.timesheet_id = ANY(p_timesheet_ids)
    GROUP BY te.timesheet_id, coalesce(te.kind::text, 'UNKNOWN')
    ORDER BY te.timesheet_id, kind
    LIMIT 500
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_queue_counts
  FROM (
    SELECT
      mq.timesheet_id,
      mq.status,
      count(*)::int AS n
    FROM public.manual_timesheet_queue AS mq
    WHERE array_length(p_timesheet_ids, 1) IS NOT NULL
      AND mq.timesheet_id = ANY(p_timesheet_ids)
    GROUP BY mq.timesheet_id, mq.status
    ORDER BY mq.timesheet_id, mq.status
    LIMIT 500
  ) AS x;

  RETURN jsonb_build_object(
    'ok', true,
    'generated_at', now(),
    'contract_week_ids', to_jsonb(p_contract_week_ids),
    'timesheet_ids', to_jsonb(p_timesheet_ids),
    'contract_weeks', v_contract_weeks,
    'timesheets', v_timesheets,
    'evidence_counts', v_evidence_counts,
    'manual_queue_counts', v_queue_counts
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- SOURCE public.codex_debug_exec_sql(text,integer,integer)
CREATE OR REPLACE FUNCTION public.codex_debug_exec_sql(p_sql text, p_statement_timeout_ms integer DEFAULT 0, p_lock_timeout_ms integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_sql text := btrim(coalesce(p_sql, ''));
  v_row_count integer := NULL;
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms numeric;
BEGIN
  IF v_sql = '' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'EMPTY_SQL',
      'message', 'p_sql is empty'
    );
  END IF;

  IF coalesce(p_statement_timeout_ms, 0) > 0 THEN
    PERFORM set_config('statement_timeout', p_statement_timeout_ms::text, true);
  END IF;

  IF coalesce(p_lock_timeout_ms, 0) > 0 THEN
    PERFORM set_config('lock_timeout', p_lock_timeout_ms::text, true);
  END IF;

  EXECUTE v_sql;
  GET DIAGNOSTICS v_row_count = ROW_COUNT;

  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'exec',
    'row_count', v_row_count,
    'elapsed_ms', v_elapsed_ms
  );

EXCEPTION WHEN OTHERS THEN
  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', false,
    'mode', 'exec',
    'sqlstate', SQLSTATE,
    'message', SQLERRM,
    'elapsed_ms', v_elapsed_ms
  );
END;
$function$;

-- SOURCE public.codex_debug_explain_sql(text,boolean,integer,integer)
CREATE OR REPLACE FUNCTION public.codex_debug_explain_sql(p_sql text, p_analyze boolean DEFAULT false, p_statement_timeout_ms integer DEFAULT 0, p_lock_timeout_ms integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_sql text := btrim(coalesce(p_sql, ''));
  v_plan json;
  v_options text;
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms numeric;
BEGIN
  IF v_sql = '' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'EMPTY_SQL',
      'message', 'p_sql is empty'
    );
  END IF;

  v_sql := regexp_replace(v_sql, ';\s*$', '');

  IF coalesce(p_statement_timeout_ms, 0) > 0 THEN
    PERFORM set_config('statement_timeout', p_statement_timeout_ms::text, true);
  END IF;

  IF coalesce(p_lock_timeout_ms, 0) > 0 THEN
    PERFORM set_config('lock_timeout', p_lock_timeout_ms::text, true);
  END IF;

  v_options := CASE
    WHEN p_analyze THEN 'FORMAT JSON, ANALYZE, VERBOSE, BUFFERS'
    ELSE 'FORMAT JSON, VERBOSE'
  END;

  EXECUTE 'EXPLAIN (' || v_options || ') ' || v_sql INTO v_plan;

  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'explain',
    'analyze', p_analyze,
    'elapsed_ms', v_elapsed_ms,
    'plan', v_plan::jsonb
  );

EXCEPTION WHEN OTHERS THEN
  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', false,
    'mode', 'explain',
    'analyze', p_analyze,
    'sqlstate', SQLSTATE,
    'message', SQLERRM,
    'elapsed_ms', v_elapsed_ms
  );
END;
$function$;

-- SOURCE public.codex_debug_function_fingerprints(text[],text)
CREATE OR REPLACE FUNCTION public.codex_debug_function_fingerprints(p_function_names text[] DEFAULT NULL::text[], p_schema text DEFAULT 'public'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_schema text := coalesce(nullif(btrim(p_schema), ''), 'public');
  v_rows jsonb := '[]'::jsonb;
BEGIN
  SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.schema_name, x.function_name, x.identity_arguments), '[]'::jsonb)
  INTO v_rows
  FROM (
    SELECT
      n.nspname AS schema_name,
      p.proname AS function_name,
      p.oid::regprocedure::text AS regprocedure,
      pg_get_function_identity_arguments(p.oid) AS identity_arguments,
      l.lanname AS language,
      p.prosecdef AS security_definer,
      p.provolatile AS volatility,
      length(d.def) AS definition_chars,
      md5(d.def) AS definition_md5,
      (position('FOR UPDATE' in upper(d.def)) > 0) AS has_for_update,
      (position('LOCK_TIMEOUT' in upper(d.def)) > 0) AS has_lock_timeout,
      (position('STATEMENT_TIMEOUT' in upper(d.def)) > 0) AS has_statement_timeout,
      (position('QUERY_CANCELED' in upper(d.def)) > 0 OR position('57014' in upper(d.def)) > 0) AS has_query_canceled_handler,
      (position('LOCK_NOT_AVAILABLE' in upper(d.def)) > 0 OR position('55P03' in upper(d.def)) > 0) AS has_lock_not_available_handler,
      (position('EXCEPTION WHEN OTHERS' in upper(d.def)) > 0) AS has_exception_when_others
    FROM pg_proc AS p
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    JOIN pg_language AS l ON l.oid = p.prolang
    CROSS JOIN LATERAL (
      SELECT pg_get_functiondef(p.oid) AS def
    ) AS d
    WHERE n.nspname = v_schema
      AND (
        p_function_names IS NULL
        OR array_length(p_function_names, 1) IS NULL
        OR p.proname = ANY(p_function_names)
      )
  ) AS x;

  RETURN jsonb_build_object(
    'ok', true,
    'schema', v_schema,
    'function_names', to_jsonb(p_function_names),
    'rows', coalesce(v_rows, '[]'::jsonb)
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- SOURCE public.codex_debug_lock_activity_snapshot()
CREATE OR REPLACE FUNCTION public.codex_debug_lock_activity_snapshot()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  RETURN public.codex_debug_activity_snapshot(500, 0);
END;
$function$;

-- SOURCE public.codex_debug_pg_stat_statements_snapshot(text[],integer)
CREATE OR REPLACE FUNCTION public.codex_debug_pg_stat_statements_snapshot(p_terms text[] DEFAULT ARRAY['timesheet'::text, 'contract_week'::text, 'evidence'::text, 'manual'::text, 'tsfin'::text, 'bulk_process'::text, 'manual_timesheet_queue'::text, 'rpc_changes_ping'::text], p_limit integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_schema name;
  v_view text;
  v_limit integer := greatest(1, least(coalesce(p_limit, 50), 500));
  v_rows jsonb := '[]'::jsonb;
BEGIN
  SELECT n.nspname
  INTO v_schema
  FROM pg_extension AS e
  JOIN pg_namespace AS n ON n.oid = e.extnamespace
  WHERE e.extname = 'pg_stat_statements'
  LIMIT 1;

  IF v_schema IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'PG_STAT_STATEMENTS_NOT_INSTALLED'
    );
  END IF;

  v_view := format('%I.pg_stat_statements', v_schema);

  IF to_regclass(v_view) IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'PG_STAT_STATEMENTS_VIEW_NOT_FOUND',
      'extension_schema', v_schema
    );
  END IF;

  EXECUTE format(
    'select coalesce(jsonb_agg(to_jsonb(s)), ''[]''::jsonb)
       from (
         select
           userid::text as userid,
           dbid::text as dbid,
           queryid::text as queryid,
           calls,
           round(total_exec_time::numeric, 1) as total_exec_time_ms,
           round(mean_exec_time::numeric, 1) as mean_exec_time_ms,
           round(max_exec_time::numeric, 1) as max_exec_time_ms,
           rows,
           left(regexp_replace(query, ''\s+'', '' '', ''g''), 700) as query_prefix
         from %s as p
         where (
           $1 is null
           or array_length($1, 1) is null
           or exists (
             select 1
             from unnest($1) as term(value)
             where p.query ilike ''%%'' || term.value || ''%%''
           )
         )
         order by total_exec_time desc
         limit %s
       ) as s',
    v_view,
    v_limit
  )
  INTO v_rows
  USING p_terms;

  RETURN jsonb_build_object(
    'ok', true,
    'extension_schema', v_schema,
    'limit', v_limit,
    'terms', to_jsonb(p_terms),
    'rows', coalesce(v_rows, '[]'::jsonb)
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- SOURCE public.codex_debug_query_sql(text,integer,integer,integer)
CREATE OR REPLACE FUNCTION public.codex_debug_query_sql(p_sql text, p_limit integer DEFAULT 500, p_statement_timeout_ms integer DEFAULT 0, p_lock_timeout_ms integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_sql text := btrim(coalesce(p_sql, ''));
  v_limit integer := greatest(1, least(coalesce(p_limit, 500), 10000));
  v_rows jsonb := '[]'::jsonb;
  v_row_count integer := 0;
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms numeric;
BEGIN
  IF v_sql = '' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'EMPTY_SQL',
      'message', 'p_sql is empty'
    );
  END IF;

  -- Strip one trailing semicolon for easier wrapping.
  v_sql := regexp_replace(v_sql, ';\s*$', '');

  IF coalesce(p_statement_timeout_ms, 0) > 0 THEN
    PERFORM set_config('statement_timeout', p_statement_timeout_ms::text, true);
  END IF;

  IF coalesce(p_lock_timeout_ms, 0) > 0 THEN
    PERFORM set_config('lock_timeout', p_lock_timeout_ms::text, true);
  END IF;

  EXECUTE format(
    'select coalesce(jsonb_agg(to_jsonb(_codex_limited)), ''[]''::jsonb), count(*)::int
       from (
         select *
         from (%s) as _codex_inner
         limit %s
       ) as _codex_limited',
    v_sql,
    v_limit
  )
  INTO v_rows, v_row_count;

  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'query',
    'row_count', coalesce(v_row_count, 0),
    'limit', v_limit,
    'elapsed_ms', v_elapsed_ms,
    'rows', coalesce(v_rows, '[]'::jsonb)
  );

EXCEPTION WHEN OTHERS THEN
  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', false,
    'mode', 'query',
    'sqlstate', SQLSTATE,
    'message', SQLERRM,
    'elapsed_ms', v_elapsed_ms
  );
END;
$function$;

-- SOURCE public.outbox_unified_list(text,text,text,integer,integer)
CREATE OR REPLACE FUNCTION public.outbox_unified_list(p_status text, p_channel text, p_search text, p_limit integer, p_offset integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_status text := nullif(upper(btrim(coalesce(p_status,''))), '');
  v_channel text := nullif(upper(btrim(coalesce(p_channel,''))), '');
  v_search text := nullif(btrim(coalesce(p_search,'')), '');

  v_limit int := coalesce(p_limit, 50);
  v_offset int := coalesce(p_offset, 0);

  v_total bigint := 0;
  v_items jsonb := '[]'::jsonb;
begin
  if v_limit < 1 then v_limit := 1; end if;
  if v_limit > 500 then v_limit := 500; end if;
  if v_offset < 0 then v_offset := 0; end if;

  with filtered as (
    select
      u.channel,
      u.outbox_id,
      u.outbox_type,
      u.status,
      u.delivery_status,
      u.created_at_utc,
      u.sent_at,
      u.delivered_at,
      u.read_at,
      u.failed_at,
      u.to_address,
      u.subject,
      u.body_text,
      u.reference,
      u.provider_message_id,
      u.last_error,
      u.created_by,
      u.recipient_kind,
      u.recipient_id,
      u.context_kind,
      u.context_id,
      u.mailshot_run_id,
      u.document_template_id
    from public.v_outbox_unified u
    where
      (v_status is null or upper(coalesce(u.status,'')) = v_status)
      and (v_channel is null or upper(coalesce(u.channel,'')) = v_channel)
      and (
        v_search is null
        or coalesce(u.to_address,'') ilike ('%' || v_search || '%')
        or coalesce(u.subject,'') ilike ('%' || v_search || '%')
        or coalesce(u.body_text,'') ilike ('%' || v_search || '%')
        or coalesce(u.reference,'') ilike ('%' || v_search || '%')
        or coalesce(u.last_error,'') ilike ('%' || v_search || '%')
      )
  ),
  counted as (
    select count(*)::bigint as total_count
    from filtered
  ),
  paged as (
    select *
    from filtered
    order by created_at_utc desc, outbox_id::text desc
    limit v_limit offset v_offset
  )
  select
    (select total_count from counted),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'channel', p.channel,
          'outbox_id', p.outbox_id::text,
          'outbox_type', p.outbox_type,
          'status', p.status,
          'delivery_status', p.delivery_status,
          'created_at_utc', p.created_at_utc::text,
          'sent_at', case when p.sent_at is null then null else p.sent_at::text end,
          'delivered_at', case when p.delivered_at is null then null else p.delivered_at::text end,
          'read_at', case when p.read_at is null then null else p.read_at::text end,
          'failed_at', case when p.failed_at is null then null else p.failed_at::text end,
          'to_address', p.to_address,
          'subject', p.subject,
          'body_preview', case
            when p.body_text is null then null
            when char_length(p.body_text) <= 200 then p.body_text
            else left(p.body_text, 200) || '…'
          end,
          'reference', p.reference,
          'provider_message_id', p.provider_message_id,
          'last_error', p.last_error,
          'created_by', case when p.created_by is null then null else p.created_by::text end,
          'recipient_kind', p.recipient_kind,
          'recipient_id', case when p.recipient_id is null then null else p.recipient_id::text end,
          'context_kind', p.context_kind,
          'context_id', case when p.context_id is null then null else p.context_id::text end,
          'mailshot_run_id', case when p.mailshot_run_id is null then null else p.mailshot_run_id::text end,
          'document_template_id', case when p.document_template_id is null then null else p.document_template_id::text end
        )
        order by p.created_at_utc desc, p.outbox_id::text desc
      ),
      '[]'::jsonb
    )
  into v_total, v_items
  from paged p;

  return jsonb_build_object(
    'ok', true,
    'filters', jsonb_build_object(
      'status', v_status,
      'channel', v_channel,
      'search', v_search
    ),
    'limit', v_limit,
    'offset', v_offset,
    'total_count', v_total,
    'items', v_items
  );
end;
$function$;

-- SOURCE public.pay_workbench_session_change_bump()
CREATE OR REPLACE FUNCTION public.pay_workbench_session_change_bump()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_should_bump boolean := false;
  v_entity_key text;
BEGIN
  IF TG_OP = 'INSERT' THEN
    v_should_bump := true;
    v_entity_key := 'banking_pay_workbench_session:' || NEW.id::text;

  ELSIF TG_OP = 'UPDATE' THEN
    v_should_bump := (
      NEW.progress_counter_version IS DISTINCT FROM OLD.progress_counter_version
      OR NEW.progress_state IS DISTINCT FROM OLD.progress_state
      OR NEW.status IS DISTINCT FROM OLD.status
      OR NEW.version IS DISTINCT FROM OLD.version
      OR NEW.discarded_at_utc IS DISTINCT FROM OLD.discarded_at_utc
      OR NEW.replacement_session_id IS DISTINCT FROM OLD.replacement_session_id
      OR NEW.scope_seed_complete IS DISTINCT FROM OLD.scope_seed_complete
      OR NEW.scope_total_count IS DISTINCT FROM OLD.scope_total_count
      OR NEW.scope_seeded_count IS DISTINCT FROM OLD.scope_seeded_count
      OR NEW.scope_ready_count IS DISTINCT FROM OLD.scope_ready_count
      OR NEW.scope_pending_count IS DISTINCT FROM OLD.scope_pending_count
      OR NEW.scope_failed_count IS DISTINCT FROM OLD.scope_failed_count
      OR NEW.line_units_total IS DISTINCT FROM OLD.line_units_total
      OR NEW.line_units_ready IS DISTINCT FROM OLD.line_units_ready
      OR NEW.line_units_pending IS DISTINCT FROM OLD.line_units_pending
      OR NEW.line_units_failed IS DISTINCT FROM OLD.line_units_failed
      OR NEW.preview_row_count IS DISTINCT FROM OLD.preview_row_count
      OR NEW.selected_row_count IS DISTINCT FROM OLD.selected_row_count
      OR NEW.section_counts_json IS DISTINCT FROM OLD.section_counts_json
      OR NEW.candidate_sample_rows_json IS DISTINCT FROM OLD.candidate_sample_rows_json
      OR NEW.progress_json IS DISTINCT FROM OLD.progress_json
      OR NEW.server_selected_preview_row_ids IS DISTINCT FROM OLD.server_selected_preview_row_ids
      OR NEW.server_selected_preview_row_ids_provided IS DISTINCT FROM OLD.server_selected_preview_row_ids_provided
    );

    v_entity_key := 'banking_pay_workbench_session:' || NEW.id::text;
  END IF;

  IF v_should_bump THEN
    PERFORM public._change_bump(v_entity_key);
  END IF;

  RETURN NEW;
END;
$function$;

create trigger trg_bpay_workbench_sessions_change_bump
  after insert or update of progress_counter_version,progress_state,status,version,
    discarded_at_utc,replacement_session_id,scope_seed_complete,scope_total_count,
    scope_seeded_count,scope_ready_count,scope_pending_count,scope_failed_count,
    line_units_total,line_units_ready,line_units_pending,line_units_failed,
    preview_row_count,selected_row_count,section_counts_json,
    candidate_sample_rows_json,progress_json,server_selected_preview_row_ids,
    server_selected_preview_row_ids_provided
  on public.banking_pay_workbench_sessions
  for each row execute function public.pay_workbench_session_change_bump();

do $legacy_general_trigger_convergence$
declare
  v_count integer;
begin
  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_trigger
  where tgrelid='public.banking_pay_workbench_sessions'::pg_catalog.regclass
    and tgname='trg_bpay_workbench_sessions_change_bump' and not tgisinternal;
  if v_count<>1 then
    raise exception 'LEGACY_GENERAL_TRIGGER_CONVERGENCE_FAILED:%',v_count;
  end if;
end
$legacy_general_trigger_convergence$;

drop function public.pay_batch_auth_start(uuid,text,timestamptz,text,jsonb,uuid,text);
drop function public.pay_batch_cancel(uuid,uuid,text);
drop function public.pay_batch_prepare(uuid,uuid);
drop function public.pay_batch_schedule(uuid,text,timestamptz,text,jsonb,uuid);
drop function public.pay_batch_validate_freshness(uuid,uuid);
drop function public.pay_preview(date,date,uuid,uuid,uuid);
drop function public.pay_settle_rail(uuid,jsonb,uuid);

do $legacy_general_rpc_acl$
declare
  v_identity text;
  v_signature pg_catalog.regprocedure;
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_hash text;
begin
  foreach v_identity in array array[
      'public._banking_pay_operation_touch_updated_at()',
      'public._cloudtms_touch_updated_at_utc()',
      'public._pay_bank_transfers_normalise_status_biu()',
      'public._pay_workbench_authoritative_scope_valid_v1(uuid,uuid,bigint,uuid,date,date,text)',
      'public._temp_diag_log(text,text,text,jsonb)',
      'public.banking_alert_display_summary_touch(uuid,text,text,integer,text,text,jsonb)',
      'public.codex_debug_activity_snapshot(integer,numeric)',
      'public.codex_debug_bulk_process_snapshot(uuid[],uuid[])',
      'public.codex_debug_exec_sql(text,integer,integer)',
      'public.codex_debug_explain_sql(text,boolean,integer,integer)',
      'public.codex_debug_function_fingerprints(text[],text)',
      'public.codex_debug_lock_activity_snapshot()',
      'public.codex_debug_pg_stat_statements_snapshot(text[],integer)',
      'public.codex_debug_query_sql(text,integer,integer,integer)',
      'public.outbox_unified_list(text,text,text,integer,integer)',
      'public.pay_workbench_session_change_bump()',
      'public._banking_alert_user_filter_allows(uuid,jsonb)',
      'public._pay_timesheet_rotation_scope(uuid[])',
      'public.banking_alert_success_event_capture_pay_batch()',
      'public.banking_pay_hot_path_budget_apply(text)',
      'public.invoice_line_archived_timesheet_guard_v1()',
      'public.pay_batch_display_summary_refresh(uuid)',
      'public.pay_batch_display_summary_touch(uuid)',
      'public.pay_batch_validate_freshness_chunk(uuid,uuid,uuid,uuid,integer)',
      'public.pay_workbench_case_resolution_origin_guard_v1()',
      'public.pay_workbench_mark_contract_client_dirty()',
      'public.timesheet_archive_row_guard_v1()',
      'public.timesheet_archived_evidence_guard_v1()',
      'public.trg_invoice_document_invalidate()',
      'public.trg_timesheet_document_invalidate()'
  ]::text[]
  loop
    v_signature:=pg_catalog.to_regprocedure(v_identity);
    if v_signature is null then
      raise exception 'LEGACY_GENERAL_RPC_TARGET_MISSING:%',v_identity;
    end if;
    execute pg_catalog.format(
      'revoke all privileges on function %s from PUBLIC, anon, authenticated, service_role, authenticator, supabase_admin',
      v_signature
    );
    execute pg_catalog.format(
      'grant execute on function %s to current_user, service_role',
      v_signature
    );
  end loop;

  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
      and p.proname not in (
        'timesheet_break_entry_effective_get_v1',
        'daily_zero_shifts_review_create_v1'
      )
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not svc_execute),
    pg_catalog.count(*) filter (where anon_execute or auth_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_browser_executable,v_hash
  from targets;

  if v_count<>656 or v_service_missing<>72 or v_browser_executable<>0
     or v_hash<>'a992e2c09a147974a745e7a9073db1c2' then
    raise exception 'LEGACY_GENERAL_RPC_CONVERGENCE_FAILED:count=% service_missing=% browser=% hash=%',
      v_count,v_service_missing,v_browser_executable,v_hash;
  end if;
end
$legacy_general_rpc_acl$;


do $legacy_candidate_rpc_prestate$
declare
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_hash text;
begin
  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public' and p.prosecdef and p.proname ilike '%candidate%'
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not svc_execute),
    pg_catalog.count(*) filter (where anon_execute or auth_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_browser_executable,v_hash
  from targets;

  if v_count<>114 or v_service_missing<>13 or v_browser_executable<>0
     or v_hash<>'426cb25afcca047d21c729af91ca6fe5' then
    raise exception 'LEGACY_CANDIDATE_RPC_PRESTATE_DRIFT:count=% service_missing=% browser=% hash=%',
      v_count,v_service_missing,v_browser_executable,v_hash;
  end if;
end
$legacy_candidate_rpc_prestate$;

-- SOURCE public._pay_workbench_candidate_serial_candidate_ids(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_candidate_ids(p_candidate_id uuid DEFAULT NULL::uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS SETOF uuid
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
      DECLARE
        v_payload jsonb := CASE
            WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object'
                  THEN COALESCE(p_payload_json, '{}'::jsonb)
                      ELSE '{}'::jsonb
                        END;
                          v_uuid_regex constant text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
                          BEGIN
                            /*
                                Compatibility rule used by the existing singular wrapper:

                                      _pay_workbench_candidate_serial_candidate_id(p_candidate_id, payload)

                                          already returns p_candidate_id immediately when supplied. Keep the plural
                                              helper aligned with that behaviour: an explicit argument is authoritative.
                                                */
                                                  IF p_candidate_id IS NOT NULL THEN
                                                      RETURN NEXT p_candidate_id;
                                                          RETURN;
                                                            END IF;

                                                              RETURN QUERY
                                                                WITH scalar_values(raw_value, source_rank) AS (
                                                                    SELECT v_payload->>'candidate_serial_candidate_id', 10
                                                                        UNION ALL SELECT v_payload->>'candidate_id', 20
                                                                            UNION ALL SELECT v_payload->>'candidateId', 30
                                                                                UNION ALL SELECT v_payload->>'candidate_uuid', 40
                                                                                    UNION ALL SELECT v_payload#>>'{candidate,id}', 50
                                                                                        UNION ALL SELECT v_payload#>>'{candidate,candidate_id}', 60
                                                                                            UNION ALL SELECT v_payload#>>'{candidate,candidate_uuid}', 70

                                                                                                UNION ALL SELECT v_payload#>>'{scope,candidate_id}', 80
                                                                                                    UNION ALL SELECT v_payload#>>'{scope,candidate,id}', 90

                                                                                                        UNION ALL SELECT v_payload#>>'{source,candidate_id}', 100
                                                                                                            UNION ALL SELECT v_payload#>>'{source,candidate,id}', 110

                                                                                                                UNION ALL SELECT v_payload#>>'{source_build,candidate_id}', 120
                                                                                                                    UNION ALL SELECT v_payload#>>'{classifier_result,candidate_id}', 130
                                                                                                                        UNION ALL SELECT v_payload#>>'{preview_decisions_json,candidate_id}', 140
                                                                                                                            UNION ALL SELECT v_payload#>>'{result,candidate_id}', 150

                                                                                                                                UNION ALL SELECT v_payload#>>'{cursor,candidate_id}', 160
                                                                                                                                    UNION ALL SELECT v_payload#>>'{cursor,candidate,id}', 170
                                                                                                                                        UNION ALL SELECT v_payload#>>'{cursor,cursor_candidate_id}', 180

                                                                                                                                            UNION ALL SELECT v_payload#>>'{cursor_json,candidate_id}', 190
                                                                                                                                                UNION ALL SELECT v_payload#>>'{cursor_json,candidate,id}', 200
                                                                                                                                                    UNION ALL SELECT v_payload#>>'{cursor_json,cursor_candidate_id}', 210

                                                                                                                                                        UNION ALL
                                                                                                                                                            SELECT
                                                                                                                                                                  CASE
                                                                                                                                                                          WHEN UPPER(BTRIM(COALESCE(v_payload->>'scope_kind', v_payload->>'scope_type', ''))) = 'CANDIDATE'
                                                                                                                                                                                    THEN v_payload->>'scope_id'
                                                                                                                                                                                            ELSE NULL::text
                                                                                                                                                                                                  END,
                                                                                                                                                                                                        220
                                                                                                                                                                                                          ),
                                                                                                                                                                                                            array_sources(key_name, source_rank) AS (
                                                                                                                                                                                                                VALUES
                                                                                                                                                                                                                      ('candidate_ids'::text, 1000),
                                                                                                                                                                                                                            ('candidateIds'::text, 1100),
                                                                                                                                                                                                                                  ('candidate_uuids'::text, 1200),
                                                                                                                                                                                                                                        ('targeted_candidate_ids'::text, 1300),
                                                                                                                                                                                                                                              ('targeted_refresh_candidate_ids'::text, 1400),
                                                                                                                                                                                                                                                    ('affected_candidate_ids'::text, 1500)
                                                                                                                                                                                                                                                      ),
                                                                                                                                                                                                                                                        array_values(raw_value, source_rank) AS (
                                                                                                                                                                                                                                                            SELECT
                                                                                                                                                                                                                                                                  CASE
                                                                                                                                                                                                                                                                          WHEN jsonb_typeof(array_item.value) IN ('string', 'number') THEN
                                                                                                                                                                                                                                                                                    array_item.value #>> '{}'
                                                                                                                                                                                                                                                                                            WHEN jsonb_typeof(array_item.value) = 'object' THEN
                                                                                                                                                                                                                                                                                                      COALESCE(
                                                                                                                                                                                                                                                                                                                  array_item.value->>'candidate_serial_candidate_id',
                                                                                                                                                                                                                                                                                                                              array_item.value->>'candidate_id',
                                                                                                                                                                                                                                                                                                                                          array_item.value->>'candidateId',
                                                                                                                                                                                                                                                                                                                                                      array_item.value->>'candidate_uuid',
                                                                                                                                                                                                                                                                                                                                                                  array_item.value->>'id',
                                                                                                                                                                                                                                                                                                                                                                              array_item.value#>>'{candidate,id}',
                                                                                                                                                                                                                                                                                                                                                                                          array_item.value#>>'{candidate,candidate_id}',
                                                                                                                                                                                                                                                                                                                                                                                                      array_item.value#>>'{candidate,candidate_uuid}'
                                                                                                                                                                                                                                                                                                                                                                                                                )
                                                                                                                                                                                                                                                                                                                                                                                                                        ELSE NULL::text
                                                                                                                                                                                                                                                                                                                                                                                                                              END AS raw_value,
                                                                                                                                                                                                                                                                                                                                                                                                                                    array_sources.source_rank + array_item.ordinality::integer AS source_rank
                                                                                                                                                                                                                                                                                                                                                                                                                                        FROM array_sources
                                                                                                                                                                                                                                                                                                                                                                                                                                            CROSS JOIN LATERAL jsonb_array_elements(
                                                                                                                                                                                                                                                                                                                                                                                                                                                  CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                          WHEN jsonb_typeof(v_payload -> array_sources.key_name) = 'array'
                                                                                                                                                                                                                                                                                                                                                                                                                                                                    THEN v_payload -> array_sources.key_name
                                                                                                                                                                                                                                                                                                                                                                                                                                                                            WHEN jsonb_typeof(v_payload -> array_sources.key_name) IN ('string', 'number')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      THEN jsonb_build_array(v_payload -> array_sources.key_name)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              ELSE '[]'::jsonb
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    END
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        ) WITH ORDINALITY AS array_item(value, ordinality)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          ),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            raw_values AS (
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                SELECT scalar_values.raw_value, scalar_values.source_rank
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    FROM scalar_values

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        UNION ALL

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            SELECT array_values.raw_value, array_values.source_rank
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                FROM array_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  ),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    cleaned_values AS (
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        SELECT DISTINCT
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              NULLIF(BTRIM(raw_values.raw_value), '') AS raw_value
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  FROM raw_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      WHERE raw_values.raw_value IS NOT NULL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        ),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          parsed_values AS (
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              SELECT
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    cleaned_values.raw_value::uuid AS candidate_uuid
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        FROM cleaned_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            WHERE cleaned_values.raw_value ~* v_uuid_regex
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              )
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                SELECT DISTINCT parsed_values.candidate_uuid
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  FROM parsed_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    WHERE parsed_values.candidate_uuid IS NOT NULL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ORDER BY parsed_values.candidate_uuid;

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        RETURN;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        END;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        $function$;

-- SOURCE public.candidate_list_ids(jsonb)
CREATE OR REPLACE FUNCTION public.candidate_list_ids(p_filters jsonb)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_timezone_id text := null;
  v_first_name text := null;
  v_last_name text := null;
  v_email text := null;
  v_phone text := null;
  v_notes text := null;
  v_pay_method text := null;
  v_active text := null;
  v_created_from date := null;
  v_created_to date := null;
  v_updated_from date := null;
  v_updated_to date := null;
  v_job_title_include_node_ids text[] := null;
  v_job_title_exclude_node_ids text[] := null;
  v_job_title_role_ids uuid[] := null;
  v_job_title_primary_only boolean := false;
  v_prof_reg_number text := null;
  v_prof_reg_type text := null;
  v_dob date := null;
  v_gender text := null;
  v_town_city text := null;
  v_postcode text := null;
  v_sort_code text := null;
  v_account_number text := null;
  v_umbrella_name text := null;
  v_tms_ref text := null;
  v_q text := null;
  v_work_status text := null;
  v_recent_months_raw text := null;
  v_recent_months int := 3;
  v_recent_all boolean := false;
  v_cutoff_ymd date := null;
  v_ids uuid[] := null;
  v_roles_any text[] := null;
  v_roles_all text[] := null;
begin
  if p_filters is null then
    p_filters := '{}'::jsonb;
  end if;

  begin
    select sd.timezone_id
      into v_timezone_id
    from public.settings_defaults as sd
    limit 1;
  exception when others then
    v_timezone_id := null;
  end;

  if nullif(btrim(coalesce(v_timezone_id, '')), '') is null then
    v_timezone_id := 'UTC';
  end if;

  v_first_name := nullif(btrim(coalesce(p_filters->>'first_name', '')), '');
  v_last_name := nullif(btrim(coalesce(p_filters->>'last_name', '')), '');
  v_email := nullif(btrim(coalesce(p_filters->>'email', '')), '');
  v_phone := nullif(btrim(coalesce(p_filters->>'phone', '')), '');
  v_notes := nullif(btrim(coalesce(p_filters->>'notes', '')), '');
  v_pay_method := nullif(upper(btrim(coalesce(p_filters->>'pay_method', ''))), '');
  v_active := nullif(lower(btrim(coalesce(p_filters->>'active', ''))), '');
  v_prof_reg_number := nullif(btrim(coalesce(p_filters->>'prof_reg_number', '')), '');
  v_prof_reg_type := nullif(upper(btrim(coalesce(p_filters->>'prof_reg_type', ''))), '');
  v_gender := nullif(btrim(coalesce(p_filters->>'gender', '')), '');
  v_town_city := nullif(btrim(coalesce(p_filters->>'town_city', '')), '');
  v_postcode := nullif(btrim(coalesce(p_filters->>'postcode', '')), '');
  v_sort_code := nullif(btrim(coalesce(p_filters->>'sort_code', '')), '');
  v_account_number := nullif(btrim(coalesce(p_filters->>'account_number', '')), '');
  v_umbrella_name := nullif(btrim(coalesce(p_filters->>'umbrella_name', '')), '');
  v_tms_ref := nullif(btrim(coalesce(p_filters->>'tms_ref', '')), '');
  v_q := nullif(btrim(coalesce(p_filters->>'q', coalesce(p_filters->>'name', ''))), '');
  v_work_status := nullif(upper(btrim(coalesce(p_filters->>'work_status', ''))), '');
  v_recent_months_raw := nullif(upper(btrim(coalesce(p_filters->>'recent_months', ''))), '');

  begin
    if nullif(btrim(coalesce(p_filters->>'created_from', '')), '') is not null then
      v_created_from := (p_filters->>'created_from')::date;
    end if;
  exception when others then
    v_created_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'created_to', '')), '') is not null then
      v_created_to := (p_filters->>'created_to')::date;
    end if;
  exception when others then
    v_created_to := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'updated_from', '')), '') is not null then
      v_updated_from := (p_filters->>'updated_from')::date;
    end if;
  exception when others then
    v_updated_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'updated_to', '')), '') is not null then
      v_updated_to := (p_filters->>'updated_to')::date;
    end if;
  exception when others then
    v_updated_to := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'dob', '')), '') is not null then
      v_dob := (p_filters->>'dob')::date;
    end if;
  exception when others then
    v_dob := null;
  end;

  begin
    if p_filters ? 'job_title_include_node_ids' then
      if jsonb_typeof(p_filters->'job_title_include_node_ids') = 'array' then
        select array_agg(parsed.include_node_id_text order by parsed.include_node_id_text)
        into v_job_title_include_node_ids
        from (
          select distinct nullif(btrim(include_nodes.value), '') as include_node_id_text
          from jsonb_array_elements_text(p_filters->'job_title_include_node_ids') as include_nodes(value)
          where nullif(btrim(include_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_include_node_ids', '')), '') is not null then
        select array_agg(parsed.include_node_id_text order by parsed.include_node_id_text)
        into v_job_title_include_node_ids
        from (
          select distinct nullif(btrim(include_tokens.token), '') as include_node_id_text
          from unnest(regexp_split_to_array(p_filters->>'job_title_include_node_ids', '\s*,\s*')) as include_tokens(token)
          where nullif(btrim(include_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_job_title_include_node_ids := null;
  end;

  begin
    if p_filters ? 'job_title_exclude_node_ids' then
      if jsonb_typeof(p_filters->'job_title_exclude_node_ids') = 'array' then
        select array_agg(parsed.exclude_node_id_text order by parsed.exclude_node_id_text)
        into v_job_title_exclude_node_ids
        from (
          select distinct nullif(btrim(exclude_nodes.value), '') as exclude_node_id_text
          from jsonb_array_elements_text(p_filters->'job_title_exclude_node_ids') as exclude_nodes(value)
          where nullif(btrim(exclude_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_exclude_node_ids', '')), '') is not null then
        select array_agg(parsed.exclude_node_id_text order by parsed.exclude_node_id_text)
        into v_job_title_exclude_node_ids
        from (
          select distinct nullif(btrim(exclude_tokens.token), '') as exclude_node_id_text
          from unnest(regexp_split_to_array(p_filters->>'job_title_exclude_node_ids', '\s*,\s*')) as exclude_tokens(token)
          where nullif(btrim(exclude_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_job_title_exclude_node_ids := null;
  end;

  begin
    if p_filters ? 'job_title_role_ids' then
      if jsonb_typeof(p_filters->'job_title_role_ids') = 'array' then
        select array_agg(parsed.role_id order by parsed.role_id)
        into v_job_title_role_ids
        from (
          select distinct nullif(btrim(role_nodes.value), '')::uuid as role_id
          from jsonb_array_elements_text(p_filters->'job_title_role_ids') as role_nodes(value)
          where nullif(btrim(role_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'job_title_role_ids', '')), '') is not null then
        select array_agg(parsed.role_id order by parsed.role_id)
        into v_job_title_role_ids
        from (
          select distinct nullif(btrim(role_tokens.token), '')::uuid as role_id
          from unnest(regexp_split_to_array(p_filters->>'job_title_role_ids', '\s*,\s*')) as role_tokens(token)
          where nullif(btrim(role_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_job_title_role_ids := null;
  end;

  begin
    if p_filters ? 'job_title_primary_only' then
      v_job_title_primary_only := case lower(btrim(coalesce(p_filters->>'job_title_primary_only', '')))
        when 'true' then true
        when '1' then true
        when 'yes' then true
        when 'y' then true
        when 'on' then true
        else false
      end;
    else
      v_job_title_primary_only := false;
    end if;
  exception when others then
    v_job_title_primary_only := false;
  end;

  begin
    if p_filters ? 'ids' then
      if jsonb_typeof(p_filters->'ids') = 'array' then
        select array_agg(parsed.selected_id order by parsed.selected_id)
        into v_ids
        from (
          select distinct nullif(btrim(id_nodes.value), '')::uuid as selected_id
          from jsonb_array_elements_text(p_filters->'ids') as id_nodes(value)
          where nullif(btrim(id_nodes.value), '') is not null
        ) as parsed;
      elsif nullif(btrim(coalesce(p_filters->>'ids', '')), '') is not null then
        select array_agg(parsed.selected_id order by parsed.selected_id)
        into v_ids
        from (
          select distinct nullif(btrim(id_tokens.token), '')::uuid as selected_id
          from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as id_tokens(token)
          where nullif(btrim(id_tokens.token), '') is not null
        ) as parsed;
      end if;
    end if;
  exception when others then
    v_ids := null;
  end;

  begin
    if p_filters ? 'roles_any' then
      if jsonb_typeof(p_filters->'roles_any') = 'array' then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_any
        from (
          select distinct upper(nullif(btrim(role_any_nodes.value), '')) as role_code
          from jsonb_array_elements_text(p_filters->'roles_any') as role_any_nodes(value)
        ) as parsed
        where parsed.role_code is not null;
      elsif nullif(btrim(coalesce(p_filters->>'roles_any', '')), '') is not null then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_any
        from (
          select distinct upper(nullif(btrim(role_any_tokens.token), '')) as role_code
          from unnest(regexp_split_to_array(p_filters->>'roles_any', '\s*,\s*')) as role_any_tokens(token)
        ) as parsed
        where parsed.role_code is not null;
      end if;
    end if;
  exception when others then
    v_roles_any := null;
  end;

  begin
    if p_filters ? 'roles_all' then
      if jsonb_typeof(p_filters->'roles_all') = 'array' then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_all
        from (
          select distinct upper(nullif(btrim(role_all_nodes.value), '')) as role_code
          from jsonb_array_elements_text(p_filters->'roles_all') as role_all_nodes(value)
        ) as parsed
        where parsed.role_code is not null;
      elsif nullif(btrim(coalesce(p_filters->>'roles_all', '')), '') is not null then
        select array_agg(parsed.role_code order by parsed.role_code)
        into v_roles_all
        from (
          select distinct upper(nullif(btrim(role_all_tokens.token), '')) as role_code
          from unnest(regexp_split_to_array(p_filters->>'roles_all', '\s*,\s*')) as role_all_tokens(token)
        ) as parsed
        where parsed.role_code is not null;
      end if;
    end if;
  exception when others then
    v_roles_all := null;
  end;

  if v_recent_months_raw = 'ALL' then
    v_recent_all := true;
    v_recent_months := 3;
  elsif v_recent_months_raw is not null then
    begin
      v_recent_months := greatest(1, least(120, (v_recent_months_raw)::int));
    exception when others then
      v_recent_months := 3;
    end;
  else
    v_recent_months := 3;
  end if;

  if v_work_status in ('RECENT', 'NOT') and not v_recent_all then
    v_cutoff_ymd := ((now() at time zone v_timezone_id)::date - make_interval(months => v_recent_months))::date;
  end if;

  return query
  with recursive job_title_tree as (
    select
      root_job_titles.id,
      root_job_titles.parent_id,
      root_job_titles.is_role,
      case
        when root_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then true
        when root_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then false
        else false
      end as checked,
      case
        when root_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then 'included'
        when root_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then 'excluded'
        else 'none'
      end as inherited_mode
    from public.default_job_titles as root_job_titles
    where root_job_titles.parent_id is null
       or not exists (
         select 1
         from public.default_job_titles as parent_job_titles
         where parent_job_titles.id = root_job_titles.parent_id
       )

    union all

    select
      child_job_titles.id,
      child_job_titles.parent_id,
      child_job_titles.is_role,
      case
        when child_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then true
        when child_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then false
        when job_title_tree.inherited_mode = 'included' then true
        when job_title_tree.inherited_mode = 'excluded' then false
        else false
      end as checked,
      case
        when child_job_titles.id::text = any(coalesce(v_job_title_include_node_ids, '{}'::text[])) then 'included'
        when child_job_titles.id::text = any(coalesce(v_job_title_exclude_node_ids, '{}'::text[])) then 'excluded'
        when job_title_tree.inherited_mode = 'included' then 'included'
        when job_title_tree.inherited_mode = 'excluded' then 'excluded'
        else 'none'
      end as inherited_mode
    from public.default_job_titles as child_job_titles
    join job_title_tree
      on job_title_tree.id = child_job_titles.parent_id
  ),
  tree_selected_roles as (
    select distinct
      job_title_tree.id as role_id
    from job_title_tree
    where job_title_tree.is_role = true
      and job_title_tree.checked = true
  ),
  effective_role_ids as (
    select distinct
      tree_selected_roles.role_id
    from tree_selected_roles
    where (
      coalesce(array_length(v_job_title_include_node_ids, 1), 0) > 0
      or coalesce(array_length(v_job_title_exclude_node_ids, 1), 0) > 0
    )
      and (
        coalesce(array_length(v_job_title_role_ids, 1), 0) = 0
        or tree_selected_roles.role_id = any(coalesce(v_job_title_role_ids, '{}'::uuid[]))
      )

    union

    select distinct
      direct_role_ids.role_id
    from unnest(coalesce(v_job_title_role_ids, '{}'::uuid[])) as direct_role_ids(role_id)
    where coalesce(array_length(v_job_title_include_node_ids, 1), 0) = 0
      and coalesce(array_length(v_job_title_exclude_node_ids, 1), 0) = 0
      and coalesce(array_length(v_job_title_role_ids, 1), 0) > 0
  ),
  candidate_filter_flags as (
    select
      (
        coalesce(array_length(v_job_title_include_node_ids, 1), 0) > 0
        or coalesce(array_length(v_job_title_exclude_node_ids, 1), 0) > 0
        or coalesce(array_length(v_job_title_role_ids, 1), 0) > 0
      ) as has_job_title_filter,
      v_job_title_primary_only as primary_only
  ),
  filtered as (
    select csa.id
    from public.candidates_summary_activity as csa
    cross join candidate_filter_flags as candidate_flags
    where (v_ids is null or csa.id = any(v_ids))
      and (v_first_name is null or csa.first_name ilike ('%' || v_first_name || '%'))
      and (v_last_name is null or csa.last_name ilike ('%' || v_last_name || '%'))
      and (v_email is null or csa.email ilike ('%' || v_email || '%'))
      and (v_phone is null or csa.phone ilike ('%' || v_phone || '%'))
      and (
        v_notes is null
        or csa.notes ilike ('%' || replace(replace(replace(v_notes, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%') escape '\\'
      )
      and (
        v_pay_method is null
        or (v_pay_method = 'BLANK' and csa.pay_method is null)
        or (v_pay_method <> 'BLANK' and upper(coalesce(csa.pay_method::text, '')) = v_pay_method)
      )
      and (
        v_active is null
        or (v_active = 'true' and csa.active = true)
        or (v_active = 'false' and csa.active = false)
      )
      and (v_created_from is null or (csa.created_at AT TIME ZONE v_timezone_id)::date >= v_created_from)
      and (v_created_to is null or (csa.created_at AT TIME ZONE v_timezone_id)::date <= v_created_to)
      and (v_updated_from is null or (csa.updated_at AT TIME ZONE v_timezone_id)::date >= v_updated_from)
      and (v_updated_to is null or (csa.updated_at AT TIME ZONE v_timezone_id)::date <= v_updated_to)
      and (
        candidate_flags.has_job_title_filter = false
        or (
          candidate_flags.primary_only = true
          and exists (
            select 1
            from effective_role_ids as effective_roles
            where effective_roles.role_id = csa.primary_job_title_id
          )
        )
        or (
          candidate_flags.primary_only = false
          and (
            exists (
              select 1
              from effective_role_ids as effective_roles
              where effective_roles.role_id = csa.primary_job_title_id
            )
            or exists (
              select 1
              from unnest(coalesce(csa.job_title_ids, '{}'::uuid[])) as candidate_job_titles(job_title_id)
              join effective_role_ids as effective_roles
                on effective_roles.role_id = candidate_job_titles.job_title_id
            )
          )
        )
      )
      and (v_prof_reg_number is null or csa.prof_reg_number ilike ('%' || v_prof_reg_number || '%'))
      and (v_prof_reg_type is null or upper(coalesce(csa.prof_reg_type::text, '')) = v_prof_reg_type)
      and (v_dob is null or csa.date_of_birth = v_dob)
      and (v_gender is null or csa.gender = v_gender)
      and (v_town_city is null or csa.town_city ilike ('%' || v_town_city || '%'))
      and (v_postcode is null or csa.postcode ilike ('%' || v_postcode || '%'))
      and (v_sort_code is null or csa.sort_code ilike ('%' || v_sort_code || '%'))
      and (v_account_number is null or csa.account_number ilike ('%' || v_account_number || '%'))
      and (v_umbrella_name is null or csa.umbrella_name ilike ('%' || v_umbrella_name || '%'))
      and (v_tms_ref is null or csa.tms_ref ilike ('%' || v_tms_ref || '%'))
      and (
        v_q is null
        or (
          csa.first_name ilike ('%' || v_q || '%')
          or csa.last_name ilike ('%' || v_q || '%')
          or csa.display_name ilike ('%' || v_q || '%')
          or csa.email ilike ('%' || v_q || '%')
          or csa.phone ilike ('%' || v_q || '%')
          or csa.tms_ref ilike ('%' || v_q || '%')
          or csa.job_titles_display ilike ('%' || v_q || '%')
        )
      )
      and (
        v_roles_any is null
        or exists (
          select 1
          from jsonb_array_elements(coalesce(csa.roles, '[]'::jsonb)) as roles_any_json(role_elem)
          where upper(coalesce(roles_any_json.role_elem->>'code', '')) = any(v_roles_any)
        )
      )
      and (
        v_roles_all is null
        or not exists (
          select 1
          from unnest(v_roles_all) as required_roles(required_code)
          where not exists (
            select 1
            from jsonb_array_elements(coalesce(csa.roles, '[]'::jsonb)) as roles_all_json(role_elem)
            where upper(coalesce(roles_all_json.role_elem->>'code', '')) = required_roles.required_code
          )
        )
      )
      and (
        v_work_status is null
        or v_work_status = 'ALL'
        or (v_work_status = 'CURRENT' and csa.is_currently_working = true)
        or (
          v_work_status = 'RECENT'
          and (
            csa.is_currently_working = true
            or (
              csa.is_currently_working = false
              and (
                (v_recent_all and csa.last_timesheet_week_ending is not null)
                or (not v_recent_all and v_cutoff_ymd is not null and csa.last_timesheet_week_ending >= v_cutoff_ymd)
              )
            )
          )
        )
        or (
          v_work_status = 'NOT'
          and csa.is_currently_working = false
          and (
            csa.last_timesheet_week_ending is null
            or (v_cutoff_ymd is not null and csa.last_timesheet_week_ending < v_cutoff_ymd)
          )
        )
      )
  )
  select filtered.id
  from filtered
  order by filtered.id;
end;
$function$;

-- SOURCE public.candidate_picker_search(text,integer,integer,boolean)
CREATE OR REPLACE FUNCTION public.candidate_picker_search(p_query text, p_limit integer DEFAULT 25, p_offset integer DEFAULT 0, p_include_inactive boolean DEFAULT false)
 RETURNS TABLE(id uuid, display_name text, first_name text, last_name text, email text, phone text, tms_ref text, roles jsonb, job_titles_display text, active boolean, match_rank integer)
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog', 'public'
AS $function$
declare
  v_query_raw text := coalesce(p_query, '');
  v_query text := btrim(v_query_raw);
  v_query_lc text := lower(btrim(v_query_raw));
  v_limit integer := greatest(1, least(coalesce(p_limit, 25), 100));
  v_offset integer := greatest(coalesce(p_offset, 0), 0);
  v_q_digits text := regexp_replace(coalesce(p_query, ''), '[^0-9]+', '', 'g');
begin
  if v_query = '' or char_length(v_query) < 2 then
    return;
  end if;

  return query
  with base as (
    select
      cs.id,
      cs.display_name,
      cs.first_name,
      cs.last_name,
      cs.email,
      cs.phone,
      cs.tms_ref,
      cs.roles,
      cs.job_titles_display,
      cs.active,
      concat_ws(
        ' ',
        coalesce(cs.first_name, ''),
        coalesce(cs.last_name, '')
      ) as full_name
    from public.candidates_summary as cs
    where
      (p_include_inactive is true or cs.active is true)
      and (
        lower(coalesce(cs.tms_ref, '')) = v_query_lc
        or lower(coalesce(cs.email, '')) = v_query_lc
        or lower(coalesce(cs.display_name, '')) = v_query_lc
        or lower(
          concat_ws(
            ' ',
            coalesce(cs.first_name, ''),
            coalesce(cs.last_name, '')
          )
        ) = v_query_lc
        or lower(coalesce(cs.first_name, '')) = v_query_lc
        or lower(coalesce(cs.last_name, '')) = v_query_lc
        or lower(coalesce(cs.phone, '')) = v_query_lc
        or (
          v_q_digits <> ''
          and regexp_replace(coalesce(cs.phone, ''), '[^0-9]+', '', 'g') = v_q_digits
        )
        or lower(coalesce(cs.tms_ref, '')) like v_query_lc || '%'
        or lower(coalesce(cs.email, '')) like v_query_lc || '%'
        or lower(coalesce(cs.display_name, '')) like v_query_lc || '%'
        or lower(coalesce(cs.first_name, '')) like v_query_lc || '%'
        or lower(coalesce(cs.last_name, '')) like v_query_lc || '%'
        or lower(
          concat_ws(
            ' ',
            coalesce(cs.first_name, ''),
            coalesce(cs.last_name, '')
          )
        ) like v_query_lc || '%'
        or lower(coalesce(cs.phone, '')) like v_query_lc || '%'
        or (
          v_q_digits <> ''
          and regexp_replace(coalesce(cs.phone, ''), '[^0-9]+', '', 'g') like v_q_digits || '%'
        )
        or position(v_query_lc in lower(coalesce(cs.tms_ref, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.email, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.display_name, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.first_name, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.last_name, ''))) > 0
        or position(
          v_query_lc in lower(
            concat_ws(
              ' ',
              coalesce(cs.first_name, ''),
              coalesce(cs.last_name, '')
            )
          )
        ) > 0
        or position(v_query_lc in lower(coalesce(cs.job_titles_display, ''))) > 0
        or (
          cs.roles is not null
          and position(v_query_lc in lower(cs.roles::text)) > 0
        )
        or position(v_query_lc in lower(coalesce(cs.phone, ''))) > 0
        or (
          v_q_digits <> ''
          and position(v_q_digits in regexp_replace(coalesce(cs.phone, ''), '[^0-9]+', '', 'g')) > 0
        )
      )
  ),
  ranked as (
    select
      b.id,
      b.display_name,
      b.first_name,
      b.last_name,
      b.email,
      b.phone,
      b.tms_ref,
      b.roles,
      b.job_titles_display,
      b.active,
      case
        when lower(coalesce(b.tms_ref, '')) = v_query_lc then 10
        when lower(coalesce(b.email, '')) = v_query_lc then 20
        when lower(coalesce(b.display_name, '')) = v_query_lc then 30
        when lower(coalesce(b.full_name, '')) = v_query_lc then 40
        when lower(coalesce(b.first_name, '')) = v_query_lc then 50
        when lower(coalesce(b.last_name, '')) = v_query_lc then 60
        when lower(coalesce(b.tms_ref, '')) like v_query_lc || '%' then 70
        when lower(coalesce(b.email, '')) like v_query_lc || '%' then 80
        when lower(coalesce(b.display_name, '')) like v_query_lc || '%' then 90
        when lower(coalesce(b.full_name, '')) like v_query_lc || '%' then 100
        when lower(coalesce(b.first_name, '')) like v_query_lc || '%' then 110
        when lower(coalesce(b.last_name, '')) like v_query_lc || '%' then 120
        when position(v_query_lc in lower(coalesce(b.tms_ref, ''))) > 0 then 130
        when position(v_query_lc in lower(coalesce(b.email, ''))) > 0 then 140
        when position(v_query_lc in lower(coalesce(b.display_name, ''))) > 0 then 150
        when position(v_query_lc in lower(coalesce(b.full_name, ''))) > 0 then 160
        when position(v_query_lc in lower(coalesce(b.job_titles_display, ''))) > 0 then 170
        when b.roles is not null and position(v_query_lc in lower(b.roles::text)) > 0 then 180
        when v_q_digits <> '' and regexp_replace(coalesce(b.phone, ''), '[^0-9]+', '', 'g') = v_q_digits then 190
        when v_q_digits <> '' and regexp_replace(coalesce(b.phone, ''), '[^0-9]+', '', 'g') like v_q_digits || '%' then 200
        when v_q_digits <> '' and position(v_q_digits in regexp_replace(coalesce(b.phone, ''), '[^0-9]+', '', 'g')) > 0 then 210
        when position(v_query_lc in lower(coalesce(b.phone, ''))) > 0 then 220
        else 999
      end as match_rank
    from base as b
  )
  select
    r.id,
    r.display_name,
    r.first_name,
    r.last_name,
    r.email,
    r.phone,
    r.tms_ref,
    r.roles,
    r.job_titles_display,
    r.active,
    r.match_rank
  from ranked as r
  order by
    r.match_rank asc,
    lower(coalesce(r.display_name, '')) asc,
    r.id asc
  offset v_offset
  limit v_limit;
end;
$function$;

do $legacy_candidate_rpc_acl$
declare
  v_identity text;
  v_signature pg_catalog.regprocedure;
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_hash text;
begin
  foreach v_identity in array array[
      'public._pay_workbench_candidate_serial_candidate_ids(uuid,jsonb)',
      'public._pay_candidate_week_totals(uuid[],date)',
      'public._trg_candidates_set_bank_hash()',
      'public.pay_preview_build_candidate_rollup(jsonb,jsonb)',
      'public.set_candidate_job_titles_updated_at()',
      'public.trg_candidates_tombstone()'
  ]::text[]
  loop
    v_signature:=pg_catalog.to_regprocedure(v_identity);
    if v_signature is null then
      raise exception 'LEGACY_CANDIDATE_RPC_TARGET_MISSING:%',v_identity;
    end if;
    execute pg_catalog.format(
      'revoke all privileges on function %s from PUBLIC, anon, authenticated, service_role, authenticator, supabase_admin',
      v_signature
    );
    execute pg_catalog.format(
      'grant execute on function %s to current_user, service_role',
      v_signature
    );
  end loop;

  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public' and p.prosecdef and p.proname ilike '%candidate%'
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not svc_execute),
    pg_catalog.count(*) filter (where anon_execute or auth_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_browser_executable,v_hash
  from targets;

  if v_count<>115 or v_service_missing<>8 or v_browser_executable<>0
     or v_hash<>'9eb64f67054303d3d292ecfa07f432e3' then
    raise exception 'LEGACY_CANDIDATE_RPC_CONVERGENCE_FAILED:count=% service_missing=% browser=% hash=%',
      v_count,v_service_missing,v_browser_executable,v_hash;
  end if;
end
$legacy_candidate_rpc_acl$;

commit;
