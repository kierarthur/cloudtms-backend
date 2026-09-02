\set ON_ERROR_STOP on

do $verification$
declare
  v_definition text;
  v_valid boolean;
begin
  select pg_get_indexdef(index_row.oid),index_state.indisvalid and index_state.indisready
  into v_definition,v_valid
  from pg_catalog.pg_class index_row
  join pg_catalog.pg_namespace namespace_row on namespace_row.oid=index_row.relnamespace
  join pg_catalog.pg_index index_state on index_state.indexrelid=index_row.oid
  where namespace_row.nspname='public'
    and index_row.relname='idx_mail_outbox_candidate_paper_workflow_v1';
  if not coalesce(v_valid,false)
     or v_definition not like '%payment_scope_json ->> ''candidate_workflow_id''%'
     or v_definition not like '%payment_scope_json ->> ''candidate_workflow_generation''%'
     or v_definition not like '%WHERE ((type = ''TIMESHEET_QR''::text) AND (context_kind = ''timesheets''::text))%'
  then
    raise exception 'CANDIDATE_PAPER_WORKFLOW_LOOKUP_INDEX_INVALID';
  end if;

  select pg_get_indexdef(index_row.oid),index_state.indisvalid and index_state.indisready
  into v_definition,v_valid
  from pg_catalog.pg_class index_row
  join pg_catalog.pg_namespace namespace_row on namespace_row.oid=index_row.relnamespace
  join pg_catalog.pg_index index_state on index_state.indexrelid=index_row.oid
  where namespace_row.nspname='public'
    and index_row.relname='idx_mail_outbox_candidate_paper_token_v1';
  if not coalesce(v_valid,false)
     or v_definition not like '%lower(COALESCE((payment_scope_json ->> ''qr_token_hash''::text), ''''::text))%'
     or v_definition not like '%payment_scope_json ->> ''candidate_workflow_id''%'
     or v_definition not like '%payment_scope_json ->> ''candidate_workflow_generation''%'
  then
    raise exception 'CANDIDATE_PAPER_TOKEN_LOOKUP_INDEX_INVALID';
  end if;

  select pg_get_indexdef(index_row.oid),index_state.indisvalid and index_state.indisready
  into v_definition,v_valid
  from pg_catalog.pg_class index_row
  join pg_catalog.pg_namespace namespace_row on namespace_row.oid=index_row.relnamespace
  join pg_catalog.pg_index index_state on index_state.indexrelid=index_row.oid
  where namespace_row.nspname='public'
    and index_row.relname='idx_timesheets_candidate_paper_source_v1';
  if not coalesce(v_valid,false)
     or v_definition not like '%(booking_id, contract_id, week_ending_date, timesheet_id)%'
     or v_definition not like '%WHERE ((is_current = true) AND (archived_at_utc IS NULL))%'
  then
    raise exception 'CANDIDATE_PAPER_SOURCE_LOOKUP_INDEX_INVALID';
  end if;
end;
$verification$;
