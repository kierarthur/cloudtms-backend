create or replace function public.timesheet_correction_pair_transition_v1(
  p_timesheet_id uuid,
  p_action text,
  p_actor_user_id uuid,
  p_operation_id uuid default null,
  p_expected_chain_fingerprint text default null,
  p_lock_rows boolean default true,
  p_max_members integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_chain jsonb;
  v_unit jsonb;
  v_envelope jsonb;
  v_ids uuid[]:=array[]::uuid[];
  v_expected_count integer;
  v_rows jsonb:='[]'::jsonb;
  v_items jsonb:='[]'::jsonb;
  v_errors jsonb:='[]'::jsonb;
  v_authorised integer:=0;
  v_unauthorised integer:=0;
  v_ready integer:=0;
  v_processed integer:=0;
  v_unprocessed integer:=0;
  v_paid integer:=0;
  v_invoiced integer:=0;
  v_action_ready boolean:=false;
  v_idempotent boolean:=false;
  v_required_rpc text;
  r record;
  v_leg jsonb;
  v_actual_envelope_fingerprint text;
  v_actual_leg_fingerprint text;
  v_values_match boolean;
begin
  if p_timesheet_id is null or p_actor_user_id is null then
    raise exception 'CORRECTION_TRANSITION_REQUIRED_ARGUMENT_MISSING' using errcode='22023';
  end if;
  if v_action not in ('AUTHORISE','UNAUTHORISE','PROCESS','UNPROCESS') then
    raise exception 'CORRECTION_TRANSITION_ACTION_INVALID' using errcode='22023';
  end if;
  if p_max_members<1 or p_max_members>100 then
    raise exception 'CORRECTION_TRANSITION_MEMBER_LIMIT_INVALID' using errcode='22023';
  end if;
  perform 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false);
  if not found then raise exception 'CORRECTION_TRANSITION_ACTOR_INVALID' using errcode='42501'; end if;

  if p_operation_id is not null then
    perform 1 from public.import_apply_operations o
    where o.id=p_operation_id and o.actor_user_id=p_actor_user_id
      and o.state in ('PREPARED','SOURCE_COMMITTED_TSFIN_PENDING','FINANCIALISED_PENDING_FINALISATION')
    for update;
    if not found then raise exception 'CORRECTION_TRANSITION_OPERATION_INVALID' using errcode='P0001'; end if;
  end if;

  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,p_lock_rows,32,p_max_members);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    raise exception 'CORRECTION_TRANSITION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
  end if;
  if nullif(btrim(coalesce(p_expected_chain_fingerprint,'')),'') is not null
     and p_expected_chain_fingerprint is distinct from v_chain->>'chain_fingerprint' then
    raise exception 'CORRECTION_TRANSITION_CHAIN_STALE' using errcode='40001';
  end if;
  v_unit:=v_chain->'requested_correction_unit';
  if jsonb_typeof(v_unit)<>'object' then
    raise exception 'CORRECTION_TRANSITION_UNIT_NOT_FOUND' using errcode='22023';
  end if;
  if coalesce((v_unit->>'valid')::boolean,false) is not true then
    raise exception 'CORRECTION_TRANSITION_UNIT_INVALID' using errcode='P0001',detail=v_unit::text;
  end if;
  v_envelope:=v_unit->'policy_envelope';
  v_expected_count:=(v_unit->>'expected_member_count')::integer;
  select coalesce(array_agg(value::uuid order by value::text),array[]::uuid[])
  into v_ids from jsonb_array_elements_text(v_unit->'member_ids');
  if cardinality(v_ids)<>v_expected_count then
    raise exception 'CORRECTION_TRANSITION_MEMBER_COUNT_MISMATCH' using errcode='P0001';
  end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id for update;
    perform 1 from public.timesheets_financials tf
    where tf.timesheet_id=any(v_ids) and tf.is_current=true
    order by tf.timesheet_id,tf.id for update;
  end if;

  for r in
    select ts.*,tf.id tsfin_id,tf.processing_status,tf.processed_at_utc,
      tf.authorised_at_utc,tf.is_stale,tf.paid_at_utc,tf.locked_by_invoice_id,
      tf.candidate_id,tf.client_id,tf.pay_method,tf.has_rate_issue,tf.has_pay_channel_issue,
      tf.policy_snapshot_json,tf.rate_source_refs_json,tf.pay_vat_rate_pct_snapshot,
      exists(select 1 from public.invoice_lines il where il.timesheet_id=ts.timesheet_id) invoice_lined
    from public.timesheets ts
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id
  loop
    v_leg:=public._ctms_correction_policy_leg_read_v1(r.timesheet_id);
    v_actual_envelope_fingerprint:=coalesce(
      r.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
      r.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
      r.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint'
    );
    v_actual_leg_fingerprint:=coalesce(
      r.policy_snapshot_json->>'correction_leg_fingerprint',
      r.rate_source_refs_json->>'correction_leg_fingerprint'
    );
    v_values_match:=
      v_actual_envelope_fingerprint is not distinct from v_envelope->>'envelope_fingerprint'
      and v_actual_leg_fingerprint is not distinct from v_leg->>'leg_fingerprint'
      and case when coalesce(r.policy_snapshot_json->>'erni_pct','')~'^-?[0-9]+([.][0-9]+)?$'
        then (r.policy_snapshot_json->>'erni_pct')::numeric is not distinct from (v_leg#>>'{tsfin_policy,erni_pct}')::numeric else false end
      and upper(btrim(coalesce(r.policy_snapshot_json->>'apply_erni_to','')))=upper(btrim(coalesce(v_leg#>>'{tsfin_policy,apply_erni_to}','')))
      and coalesce(r.pay_vat_rate_pct_snapshot,
        case when coalesce(r.policy_snapshot_json->>'pay_vat_rate_pct','')~'^-?[0-9]+([.][0-9]+)?$'
          then (r.policy_snapshot_json->>'pay_vat_rate_pct')::numeric else null end)
        is not distinct from (v_leg#>>'{tsfin_policy,applied_pay_vat_rate_pct}')::numeric
      and coalesce(r.policy_snapshot_json->>'correction_tsfin_policy_fingerprint','')
        is not distinct from v_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
      and coalesce(r.policy_snapshot_json->>'correction_invoice_policy_fingerprint','')
        is not distinct from v_leg#>>'{invoice_policy,invoice_policy_fingerprint}';

    if r.authorised_at_server is not null and r.authorised_at_utc is not null then v_authorised:=v_authorised+1; end if;
    if r.authorised_at_server is null and r.authorised_at_utc is null then v_unauthorised:=v_unauthorised+1; end if;
    if r.tsfin_id is not null and not coalesce(r.is_stale,false)
       and r.candidate_id is not null and r.client_id is not null
       and nullif(btrim(coalesce(r.pay_method,'')),'') is not null
       and not coalesce(r.has_rate_issue,false) and not coalesce(r.has_pay_channel_issue,false)
       and v_values_match then v_ready:=v_ready+1; end if;
    if r.processed_at_utc is not null or r.processing_status in (
      'READY_FOR_HR'::public.ts_fin_processing_status_enum,
      'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
      'PENDING_AUTH'::public.ts_fin_processing_status_enum) then v_processed:=v_processed+1; end if;
    if r.processed_at_utc is null and r.processing_status='UNPROCESSED'::public.ts_fin_processing_status_enum then v_unprocessed:=v_unprocessed+1; end if;
    if r.paid_at_utc is not null then v_paid:=v_paid+1; end if;
    if r.invoice_lined or r.locked_by_invoice_id is not null then v_invoiced:=v_invoiced+1; end if;
    if not v_values_match then
      v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_LEG_POLICY_NOT_FROZEN','timesheet_id',r.timesheet_id));
    end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'correction_kind',r.correction_kind,
      'timesheet_authorised',r.authorised_at_server is not null,
      'tsfin_authorised',r.authorised_at_utc is not null,'tsfin_id',r.tsfin_id,
      'processing_status',r.processing_status,'policy_ready',v_values_match,
      'leg_fingerprint',v_leg->>'leg_fingerprint'));
    v_items:=v_items||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'expected_version',r.version,
      'expected_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
      'expected_leg_fingerprint',v_leg->>'leg_fingerprint'));
  end loop;

  case v_action
    when 'AUTHORISE' then
      v_idempotent:=v_authorised=v_expected_count;
      v_action_ready:=v_idempotent or (v_unauthorised=v_expected_count and v_ready=v_expected_count);
      v_required_rpc:=case when v_idempotent then null else 'timesheet_authorise_bulk_atomic' end;
    when 'UNAUTHORISE' then
      v_idempotent:=v_unauthorised=v_expected_count;
      v_action_ready:=v_idempotent or v_authorised=v_expected_count;
      v_required_rpc:=case when v_idempotent then null else 'timesheet_unauthorise_bulk_atomic' end;
    when 'PROCESS' then
      v_idempotent:=v_processed=v_expected_count;
      v_action_ready:=v_idempotent or v_unprocessed=v_expected_count;
      v_required_rpc:=case when v_idempotent then null else 'contract_week_manual_upsert_bulk_process_atomic' end;
    when 'UNPROCESS' then
      v_idempotent:=v_unprocessed=v_expected_count;
      v_action_ready:=v_idempotent or v_processed=v_expected_count;
      v_required_rpc:=case when v_idempotent then null else 'contract_week_manual_unprocess_atomic' end;
  end case;
  if not v_action_ready then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_UNIT_ACTION_STATE_INVALID')); end if;

  return jsonb_build_object(
    'ok',true,'valid',jsonb_array_length(v_errors)=0,'action',v_action,
    'action_ready',v_action_ready and jsonb_array_length(v_errors)=0,'idempotent_state',v_idempotent,
    'operation_id',p_operation_id,'root_timesheet_id',v_chain->>'root_timesheet_id',
    'correction_id',v_unit->>'correction_id','correction_shape',v_unit->>'correction_shape',
    'expected_member_count',v_expected_count,'pair_timesheet_ids',to_jsonb(v_ids),
    'correction_financials_policy_envelope',v_envelope,
    'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
    'chain_fingerprint',v_chain->>'chain_fingerprint','pair_rows',v_rows,
    'transition_items',v_items,'required_backend_rpc',v_required_rpc,
    'authorised_count',v_authorised,'unauthorised_count',v_unauthorised,
    'ready_count',v_ready,'processed_count',v_processed,'unprocessed_count',v_unprocessed,
    'paid_count',v_paid,'invoice_lined_count',v_invoiced,'errors',v_errors);
end;
$function$;

comment on function public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer) is
  'Validates one import correction unit and returns one complete bulk lifecycle plan. Supports reversal/replacement and reversal-only; performs no lifecycle mutation.';
revoke all on function public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer) from public,anon,authenticated;
grant execute on function public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer) to service_role;
