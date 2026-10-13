-- Adjacent request authority; no widening of the import correction classifier.
create or replace function private.timesheet_correction_whole_transition_v1(
  p_timesheet_id uuid,p_action text,p_actor_user_id uuid,p_expected_chain_fingerprint text,p_lock_rows boolean,p_max_members integer
) returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare
  v_request private.timesheet_correction_requests%rowtype; v_chain jsonb; v_ids uuid[];
  v_rows jsonb; v_items jsonb; v_authorised integer; v_unauthorised integer; v_ready integer; v_paid integer; v_invoiced integer;
  v_action text:=upper(btrim(p_action)); v_errors jsonb:='[]'::jsonb; v_include_original boolean:=false; v_count integer;
begin
  select request.* into v_request from private.timesheet_correction_requests request
    where p_timesheet_id in(request.replacement_timesheet_id,request.reversal_timesheet_id)
      and request.correction_id=(select t.correction_id from public.timesheets t where t.timesheet_id=p_timesheet_id);
  if not found then return null; end if;
  if private.timesheet_correction_actor_allowed_v1(p_actor_user_id) is distinct from true then
    raise exception 'CORRECTION_ACTOR_NOT_ALLOWED' using errcode='42501';
  end if;
  if v_action not in('AUTHORISE','UNAUTHORISE','PROCESS','UNPROCESS') then
    raise exception 'CORRECTION_TRANSITION_ACTION_INVALID' using errcode='22023';
  end if;
  if p_lock_rows then
    perform 1 from public.invoices i where i.id in(v_request.original_invoice_id,v_request.return_invoice_id) order by i.id for update;
    perform pg_advisory_xact_lock(hashtextextended('CORRECTION_CHAIN|'||v_request.root_timesheet_id::text,21072026));
    select request.* into v_request from private.timesheet_correction_requests request where request.id=v_request.id for update;
  end if;
  if v_request.state<>'APPLIED' then
    return jsonb_build_object('ok',true,'valid',false,'action_ready',false,'correction_id',v_request.correction_id,
      'expected_member_count',2,'errors',jsonb_build_array(jsonb_build_object('code','CORRECTION_REQUEST_NOT_APPLIED')));
  end if;
  v_include_original:=v_action='AUTHORISE'
    and coalesce((v_request.terminal_receipt_json->>'original_authorisation_required')::boolean,false)
    and exists(select 1 from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id
      and t.authorised_at_server is null);
  v_ids:=array[v_request.reversal_timesheet_id,v_request.replacement_timesheet_id];
  if v_include_original then v_ids:=array_append(v_ids,v_request.reversed_timesheet_id); end if;
  v_count:=cardinality(v_ids);
  if v_count>p_max_members then raise exception 'CORRECTION_TRANSITION_MEMBER_LIMIT_INVALID' using errcode='22023'; end if;
  if p_lock_rows then
    perform 1 from public.candidate_submission_workflows w where w.id in(select t.candidate_workflow_id from public.timesheets t
      where t.timesheet_id=any(v_ids)) order by w.id for update;
    perform 1 from public.timesheets t where t.timesheet_id=any(v_ids) order by t.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.timesheet_id=any(v_ids) and tf.is_current
      order by tf.timesheet_id,tf.id for update;
  end if;
  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,false,32,p_max_members);
  if (v_chain->>'valid')::boolean is distinct from true
    or (p_expected_chain_fingerprint is not null and p_expected_chain_fingerprint is distinct from v_chain->>'chain_fingerprint') then
    raise exception 'CORRECTION_TRANSITION_CHAIN_STALE' using errcode='40001';
  end if;
  select count(*) filter(where t.authorised_at_server is not null and tf.authorised_at_utc is not null),
    count(*) filter(where t.authorised_at_server is null and tf.authorised_at_utc is null),
    count(*) filter(where tf.id is not null and not coalesce(tf.is_stale,true) and tf.candidate_id=v_request.candidate_id
      and tf.client_id=v_request.client_id and nullif(btrim(tf.pay_method),'') is not null
      and not coalesce(tf.has_rate_issue,true) and not coalesce(tf.has_pay_channel_issue,true)),
    count(*) filter(where tf.paid_at_utc is not null),
    count(*) filter(where tf.locked_by_invoice_id is not null or exists(select 1 from public.invoice_lines l where l.timesheet_id=t.timesheet_id)
      or exists(select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
        then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) s where nullif(s->>'invoice_locked_invoice_id','') is not null)),
    jsonb_agg(jsonb_build_object('timesheet_id',t.timesheet_id,'correction_kind',t.correction_kind,
      'timesheet_authorised',t.authorised_at_server is not null,'tsfin_authorised',tf.authorised_at_utc is not null,
      'tsfin_id',tf.id,'processing_status',tf.processing_status,'policy_ready',not coalesce(tf.is_stale,true)) order by t.timesheet_id),
    jsonb_agg(jsonb_build_object('timesheet_id',t.timesheet_id,'expected_timesheet_id',t.timesheet_id,
      'expected_version',t.version) order by t.timesheet_id)
    into v_authorised,v_unauthorised,v_ready,v_paid,v_invoiced,v_rows,v_items
  from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.timesheet_id=any(v_ids) and t.is_current and t.archived_at_utc is null;
  if jsonb_array_length(coalesce(v_rows,'[]'::jsonb))<>v_count or v_paid<>0 or v_invoiced<>0
    or (v_action='AUTHORISE' and not(v_authorised=v_count or (v_unauthorised=v_count and v_ready=v_count)))
    or (v_action='UNAUTHORISE' and not(v_authorised=v_count or v_unauthorised=v_count)) then
    v_errors:=jsonb_build_array(jsonb_build_object('code','CORRECTION_UNIT_ACTION_STATE_INVALID'));
  end if;
  if v_action in('PROCESS','UNPROCESS') then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','WHOLE_CORRECTION_FINANCE_FROZEN'));
  end if;
  return jsonb_build_object('ok',true,'valid',jsonb_array_length(v_errors)=0,'action',v_action,
    'action_ready',jsonb_array_length(v_errors)=0,'root_timesheet_id',v_request.root_timesheet_id,
    'correction_id',v_request.correction_id,'request_id',v_request.id,'correction_shape','WHOLE_REVERSAL_REPLACEMENT',
    'expected_member_count',v_count,'pair_timesheet_ids',to_jsonb(v_ids),'pair_rows',coalesce(v_rows,'[]'::jsonb),
    'transition_items',coalesce(v_items,'[]'::jsonb),'chain_fingerprint',v_chain->>'chain_fingerprint',
    'authorised_count',v_authorised,'unauthorised_count',v_unauthorised,'ready_count',v_ready,
    'paid_count',v_paid,'invoice_lined_count',v_invoiced,'errors',v_errors,
    'correction_financials_policy_envelope',null,'correction_financials_policy_envelope_fingerprint',null,
    'required_backend_rpc',case when v_action='AUTHORISE' then 'timesheet_authorise_bulk_atomic' else 'timesheet_unauthorise_bulk_atomic' end);
end;
$function$;
