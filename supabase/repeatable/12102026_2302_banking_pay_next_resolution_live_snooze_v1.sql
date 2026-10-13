-- WB29 C29-W1-S01: native live date/basis snooze qualification only.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_resolution_live_snooze_v1(p_work_id uuid,p_revision_id uuid,p_component_key text)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select exists(select 1 from private.bpay_next_snooze sn
  join private.bpay_next_revision_worker_basis b on b.revision_id=p_revision_id
  where sn.work_id=p_work_id and sn.active
   and (sn.until_date is null or sn.until_date>=(current_timestamp at time zone 'Europe/London')::date)
   and ((sn.scope_kind='WHOLE_TIMESHEET' and sn.worker_pay_basis=b.whole_basis)
    or (sn.scope_kind='SEGMENT' and sn.component_key=p_component_key and exists(select 1
     from private.bpay_next_approved_segment s where s.revision_id=p_revision_id and s.component_key=p_component_key
      and s.segment_key=sn.segment_key and sn.worker_pay_basis=private.bpay_next_segment_pay_basis_v1(s.id)))))
$function$;

create or replace function private.bpay_next_resolution_context_v1(p_actor_id uuid,p_scope jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare
 w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;pos private.bpay_next_position%rowtype;
 c public.contracts%rowtype;t public.timesheets%rowtype;s private.bpay_next_rate_schedule%rowtype;
 d private.bpay_next_component_resolution%rowtype;v_compatible record;v_rate record;
 v_intent private.bpay_next_component_resolution;v_current_basis jsonb;v_position jsonb;v_base jsonb;
 v_works jsonb:='[]';v_contexts jsonb:='[]';v_rows jsonb;v_row_contexts jsonb;
 v_work jsonb;v_row jsonb;v_decisions jsonb;v_incompatible jsonb;v_expected jsonb;v_available jsonb;
 v_suggestion jsonb;v_preview jsonb;v_saved jsonb;v_ref jsonb;v_work_context jsonb;v_response jsonb;
 v_code text;v_preview_code text;v_saved_code text;v_date date;v_owner_count integer:=0;v_row_count integer;
 v_pos_residual numeric;v_zero boolean;v_daily boolean;v_basis_ok boolean;v_saved_quote jsonb;v_quote_ok boolean;
 v_decimal_objects jsonb;v_decimal_object jsonb;v_decimal_quote jsonb;v_decimal_member record;
begin
 perform private.bpay_next_resolution_scope_validate_v1(p_scope,'READ');
 v_base:=private.bpay_next_resolution_offer_context_v1(p_actor_id,(p_scope->>'candidate_id')::uuid);
 v_date:=(v_base->>'official_pay_date')::date;
 for v_work in select value from jsonb_array_elements(p_scope->'works') loop
  select x.* into w from private.bpay_next_work x where x.id=(v_work->>'work_id')::uuid and x.candidate_id=(p_scope->>'candidate_id')::uuid;
  select x.* into r from private.bpay_next_work_revision x where x.id=(v_work->>'expected_revision_id')::uuid and x.work_id=w.id;
  select x.* into t from public.timesheets x where x.timesheet_id=r.physical_timesheet_id;
  if w.id is null or r.id is null or w.approval_state is distinct from 'APPROVED' or r.approved_at_utc is null or r.sealed_at_utc is null
   or w.current_revision_id is distinct from r.id or w.applied_revision_id is distinct from r.id
   or t.is_current is distinct from true or t.version is distinct from r.physical_timesheet_version or t.booking_id is distinct from w.booking_id
   or t.contract_id is distinct from w.contract_id then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
  select x.* into c from public.contracts x where x.id=w.contract_id;
  v_daily:=w.origin_kind='DAILY' or (r.daily_approval_evidence_json->>'contract_version'='BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
    and r.daily_approval_evidence_json->>'sheet_scope'='DAILY');
  v_work_context:=jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'applied_revision_id',w.applied_revision_id,
   'revision_no',r.revision_no::text,'physical_timesheet_id',r.physical_timesheet_id,'physical_timesheet_version',r.physical_timesheet_version::text,
   'original_timesheet_id',w.original_timesheet_id,'contract_id',w.contract_id,'week_ending_date',w.week_ending_date,
   'valuation_policy_id',r.valuation_policy_id,'source_pay_channel',r.source_pay_channel,'source_umbrella_id',r.source_umbrella_id);
  v_rows:='[]';v_row_contexts:='[]';
  for v_row in select value from jsonb_array_elements(v_work->'rows') loop
   v_current_basis:=private.bpay_next_component_resolution_basis_v1(r.id,(v_row->>'approved_line_id')::uuid,v_row->>'rate_family',v_row->>'rate_code');
   select x.* into pos from private.bpay_next_position x where x.work_id=w.id and x.component_key=v_current_basis->>'component_key';
   if pos.applied_revision_id is distinct from r.id or pos.approved_source_ex_vat is distinct from (v_current_basis->>'component_source_ex_vat')::numeric then
    raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED';end if;
   v_position:=jsonb_build_object('approved_source_ex_vat',pos.approved_source_ex_vat,'realised_source_ex_vat',pos.realised_source_ex_vat,
    'held_source_ex_vat',pos.held_source_ex_vat,'source_basis_channel',pos.source_basis_channel);
   v_pos_residual:=greatest(pos.approved_source_ex_vat-pos.realised_source_ex_vat-pos.held_source_ex_vat,0);
   select x.* into s from private.bpay_next_rate_schedule x where x.revision_id=r.id and x.rate_family=v_row->>'rate_family' and x.rate_code=v_row->>'rate_code';
   v_available:=jsonb_build_object('provenance','UNAVAILABLE','paye_rate',null,'umbrella_rate',null,'charge_rate',null,'unit_label',null);
   if v_daily then
    if r.daily_approval_evidence_json->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
     or r.daily_approval_evidence_json->>'sheet_scope' is distinct from 'DAILY' then
     raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';end if;
    v_available:=jsonb_build_object('provenance','APPROVAL_FROZEN_DAILY','paye_rate',s.paye_rate,'umbrella_rate',s.umbrella_rate,
     'charge_rate',s.charge_rate,'unit_label',s.unit_label);
   elsif c.id is not null and v_row->>'rate_family' in ('STANDARD','ADDITIONAL') then
    if octet_length(jsonb_build_array(c.rates_json,c.bucket_labels_json,c.additional_rates_json)::text)>65536 then
     raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
    for v_rate in select x.* from private.bpay_next_contract_rate_rows_v1(coalesce(c.rates_json,'{}'::jsonb),c.bucket_labels_json,c.additional_rates_json,c.mileage_pay_rate,c.mileage_charge_rate) x
      where x.rate_family=v_row->>'rate_family' and x.rate_code=v_row->>'rate_code' loop
     v_available:=jsonb_build_object('provenance','CURRENT_CONTRACT','paye_rate',v_rate.paye_rate,'umbrella_rate',v_rate.umbrella_rate,
      'charge_rate',v_rate.charge_rate,'unit_label',v_rate.unit_label);
    end loop;
   end if;
   select x.* into v_compatible from private.bpay_next_component_resolution_active_v1(w.id,r.id,(v_row->>'approved_line_id')::uuid,
    v_row->>'rate_family',v_row->>'rate_code',v_base->>'target_pay_channel',(v_base->>'target_umbrella_id')::uuid,
    (v_base->>'target_umbrella_vat_chargeable')::boolean,r.valuation_policy_id,v_date) x;
   v_expected:=case when v_compatible.id is null then 'null'::jsonb else jsonb_build_object('decision_id',v_compatible.id,'decision_version',v_compatible.decision_version::text) end;
   v_decisions:='[]';v_incompatible:='[]';v_saved:=null;v_row_count:=0;
   for d in select x.* from private.bpay_next_component_resolution x where x.work_id=w.id and x.state='ACTIVE'
    and x.rate_family=v_row->>'rate_family' and x.rate_code=v_row->>'rate_code'
    and ((x.based_on_revision_id=r.id and x.approved_line_id=(v_row->>'approved_line_id')::uuid)
     or (x.published_revision_id=r.id and x.published_line_id=(v_row->>'approved_line_id')::uuid)) order by x.id limit 101 loop
    v_owner_count:=v_owner_count+1;v_row_count:=v_row_count+1;
    if v_row_count>100 or v_owner_count>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
    v_basis_ok:=(case when d.based_on_revision_id=r.id then d.original_basis else d.published_basis end) is not distinct from v_current_basis;
    if d.candidate_id is distinct from w.candidate_id or not v_basis_ok then
     raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
    v_ref:=jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version::text,'target_pay_channel',d.target_pay_channel,
     'target_umbrella_id',d.target_umbrella_id,'target_umbrella_vat_chargeable',d.target_umbrella_vat_chargeable,
     'valuation_policy_id',d.valuation_policy_id,'valuation_date',d.valuation_date,'resolution_mode',d.resolution_mode,
     'manual_replacement_rate',d.manual_replacement_rate::text,'manual_amount_ex_vat',d.manual_amount_ex_vat::text,'reason',d.reason,'clearable',true);
    v_decisions:=v_decisions||jsonb_build_array(v_ref);
    if d.id is not distinct from v_compatible.id then v_saved:=v_ref;
    else v_incompatible:=v_incompatible||jsonb_build_array(v_ref);end if;
   end loop;
   v_intent:=null;v_intent.work_id:=w.id;v_intent.candidate_id:=w.candidate_id;v_intent.based_on_revision_id:=r.id;
   v_intent.approved_line_id:=(v_row->>'approved_line_id')::uuid;v_intent.physical_timesheet_id:=r.physical_timesheet_id;
   v_intent.component_key:=v_current_basis->>'component_key';v_intent.rate_family:=v_row->>'rate_family';v_intent.rate_code:=v_row->>'rate_code';
   v_intent.original_basis:=v_current_basis;v_intent.original_position_basis:=v_position;v_intent.valuation_policy_id:=r.valuation_policy_id;
   v_intent.valuation_date:=v_date;v_intent.target_pay_channel:=v_base->>'target_pay_channel';
   v_intent.target_umbrella_id:=(v_base->>'target_umbrella_id')::uuid;v_intent.target_umbrella_vat_chargeable:=(v_base->>'target_umbrella_vat_chargeable')::boolean;
   v_intent.resolution_mode:='SUGGESTED_EQUIVALENT_BASIS';v_suggestion:=null;v_preview:=null;v_saved_quote:=null;
   v_code:=null;v_preview_code:=null;v_saved_code:=null;v_zero:=false;
   v_quote_ok:=not private.bpay_next_resolution_live_snooze_v1(w.id,r.id,v_intent.component_key);
   if v_quote_ok then
    begin v_suggestion:=private.bpay_next_component_resolution_input_quote_v1(v_intent,r.id,v_position,v_pos_residual);
    exception when check_violation then
     get stacked diagnostics v_code=MESSAGE_TEXT;
     if v_code not in ('BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
    end;
    if v_compatible.id is not null then
     begin v_saved_quote:=private.bpay_next_component_resolution_quote_v1(v_compatible.id,v_compatible.decision_version,r.id,null);
     exception when check_violation then
      get stacked diagnostics v_saved_code=MESSAGE_TEXT;
      if v_saved_code not in ('BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
     end;
    end if;
    if v_row->'intent'<>'null'::jsonb then
     v_intent.resolution_mode:=(v_row->'intent'->>'resolution_mode')::public.pay_finance_component_resolution_mode_enum;
     v_intent.manual_replacement_rate:=(v_row->'intent'->>'manual_replacement_rate')::numeric;
     v_intent.manual_amount_ex_vat:=(v_row->'intent'->>'manual_amount_ex_vat')::numeric;v_intent.reason:=p_scope->>'reason';
     begin v_preview:=private.bpay_next_component_resolution_input_quote_v1(v_intent,r.id,v_position,v_pos_residual);
     exception when check_violation then
      get stacked diagnostics v_preview_code=MESSAGE_TEXT;
      if v_preview_code not in ('BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE') then raise;end if;
     end;
     v_zero:=v_preview is not null and (v_intent.manual_replacement_rate=0 or v_intent.manual_amount_ex_vat=0 or (v_preview->'quote'->>'ex')::numeric=0);
     v_zero:=coalesce(v_zero,false);
    end if;
   else v_code:='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';v_preview_code:=case when v_row->'intent'<>'null'::jsonb then v_code end;
    v_saved_code:=case when v_saved is not null then v_code end;
   end if;
   -- BD160: finite DTO projection only, after all native numeric qualification.
   -- JavaScript never rounds native pennies, units, rates or decision versions.
   v_decimal_objects:='[]';
   for v_decimal_object in select value from jsonb_array_elements(jsonb_build_array(v_current_basis,v_position,v_available,v_suggestion,v_preview,v_saved_quote)) loop
    if jsonb_typeof(v_decimal_object)='object' then
     for v_decimal_member in select key,value from jsonb_each(v_decimal_object) loop
      if jsonb_typeof(v_decimal_member.value)='number' then
       v_decimal_object:=jsonb_set(v_decimal_object,array[v_decimal_member.key],to_jsonb(v_decimal_member.value#>>'{}'));
      end if;
     end loop;
     if jsonb_typeof(v_decimal_object->'quote')='object' then
      v_decimal_quote:=v_decimal_object->'quote';
      for v_decimal_member in select key,value from jsonb_each(v_decimal_quote) loop
       if jsonb_typeof(v_decimal_member.value)='number' then
        v_decimal_quote:=jsonb_set(v_decimal_quote,array[v_decimal_member.key],to_jsonb(v_decimal_member.value#>>'{}'));
       end if;
      end loop;
      v_decimal_object:=jsonb_set(v_decimal_object,'{quote}',v_decimal_quote);
     end if;
    end if;
    v_decimal_objects:=v_decimal_objects||jsonb_build_array(v_decimal_object);
   end loop;
   v_current_basis:=v_decimal_objects->0;v_position:=v_decimal_objects->1;v_available:=v_decimal_objects->2;
   v_suggestion:=nullif(v_decimal_objects->3,'null'::jsonb);v_preview:=nullif(v_decimal_objects->4,'null'::jsonb);v_saved_quote:=nullif(v_decimal_objects->5,'null'::jsonb);
   v_row_contexts:=v_row_contexts||jsonb_build_array(jsonb_build_object('approved_line_id',v_intent.approved_line_id,
    'rate_family',v_intent.rate_family,'rate_code',v_intent.rate_code,'original_basis',v_current_basis,'position_basis',v_position,
    'position_applied_revision_id',pos.applied_revision_id,'source_book_state',pos.source_book_state,'source_book_revision',pos.source_book_revision::text,
    'source_binding_version_id',(select b.current_binding_version_id from private.bpay_next_source_component_binding b where b.canonical_work_id=w.id and b.canonical_component_key=pos.component_key),
    'available_rates',v_available,'decisions',v_decisions));
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('approved_line_id',v_intent.approved_line_id,'rate_family',v_intent.rate_family,'rate_code',v_intent.rate_code,
    'original_basis',v_current_basis,'available_rates',v_available,'suggestion',v_suggestion,'suggestion_unavailable_code',v_code,
    'expected_decision',v_expected,'saved_decision',v_saved,'saved_quote',v_saved_quote,'saved_unavailable_code',v_saved_code,
    'incompatible_decisions',v_incompatible,'preview',v_preview,'preview_unavailable_code',v_preview_code,'requires_zero_confirmation',v_zero));
   if octet_length(jsonb_build_array(v_contexts,v_works,v_row_contexts,v_rows)::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  end loop;
  v_contexts:=v_contexts||jsonb_build_array(v_work_context||jsonb_build_object('rows',v_row_contexts));
  v_works:=v_works||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'rows',v_rows));
 end loop;
 v_response:=jsonb_build_object('ok',true,'kind','RESOLUTION_CONTEXT','context',v_base||jsonb_build_object('works',v_contexts),'works',v_works);
 if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 return v_response;
end
$function$;

create or replace function private.bpay_next_component_resolution_quote_v1(
 p_decision_id uuid,p_version bigint,p_boundary_revision_id uuid,p_run_position_id uuid default null
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare d private.bpay_next_component_resolution%rowtype;rp private.bpay_next_run_position%rowtype;
 pos private.bpay_next_position%rowtype;w private.bpay_next_work%rowtype;
 rw private.bpay_next_run_worker%rowtype;run private.bpay_next_pay_run%rowtype;
 b jsonb;v_line uuid;v_approved numeric;v_realised numeric;v_held numeric;v_residual numeric;v_included numeric;
begin
 select wb45src.* into d from private.bpay_next_component_resolution wb45src where id=p_decision_id and decision_version=p_version;
 if d.id is null or p_boundary_revision_id is null then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_VERSION_MISSING';end if;
 if p_boundary_revision_id=d.based_on_revision_id then v_line:=d.approved_line_id;
 elsif p_boundary_revision_id=d.published_revision_id then v_line:=d.published_line_id;
 else raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_PUBLICATION_INCOMPATIBLE';end if;
 b:=private.bpay_next_component_resolution_basis_v1(p_boundary_revision_id,v_line,d.rate_family,d.rate_code);
 if b is distinct from (case when p_boundary_revision_id=d.based_on_revision_id then d.original_basis else d.published_basis end) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
 if p_run_position_id is null then
  select wb45src.* into w from private.bpay_next_work wb45src where id=d.work_id;
  select wb45src.* into pos from private.bpay_next_position wb45src where work_id=d.work_id and component_key=d.component_key;
  if w.approval_state is distinct from 'APPROVED' or w.current_revision_id is distinct from p_boundary_revision_id
   or w.applied_revision_id is distinct from p_boundary_revision_id or pos.applied_revision_id is distinct from p_boundary_revision_id then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
  if private.bpay_next_resolution_live_snooze_v1(d.work_id,p_boundary_revision_id,d.component_key) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
  v_approved:=pos.approved_source_ex_vat;v_realised:=pos.realised_source_ex_vat;v_held:=pos.held_source_ex_vat;
  if (v_realised<>0 or v_held<>0) and pos.source_basis_channel is distinct from d.original_basis->>'source_pay_channel' then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
  v_residual:=v_approved-v_realised-v_held;v_included:=greatest(v_residual,0);
 else
  select wb45src.* into rp from private.bpay_next_run_position wb45src where id=p_run_position_id;
  select wb45src.* into rw from private.bpay_next_run_worker wb45src where id=rp.run_worker_id;
  select wb45src.* into run from private.bpay_next_pay_run wb45src where id=rw.run_id;
  if (rp.work_id,rp.captured_revision_id,rp.component_key,rp.approved_line_id)
   is distinct from (d.work_id,p_boundary_revision_id,d.component_key,v_line) or not rp.segment_attribution_complete
   or (rw.candidate_id,rw.target_pay_channel,rw.target_umbrella_id,rw.target_umbrella_vat_chargeable,run.pay_date)
   is distinct from (d.candidate_id,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,d.valuation_date) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_approved:=rp.approved_source_ex_vat;v_realised:=rp.realised_source_ex_vat;v_held:=rp.held_source_ex_vat;
  v_residual:=rp.residual_source_ex_vat;
  v_included:=case when rp.segment_member_count>0 then rp.segment_included_positive_ex_vat
   else greatest(v_residual,0)-rp.snooze_excluded_positive_ex_vat end;
  if (v_realised<>0 or v_held<>0) and rp.source_basis_channel is distinct from d.original_basis->>'source_pay_channel' then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 end if;
 if v_approved is distinct from (d.original_basis->>'component_source_ex_vat')::numeric
  or v_residual is distinct from v_approved-v_realised-v_held or v_residual<0
  or v_included is null or v_included<0 or v_included>greatest(v_residual,0) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 return private.bpay_next_component_resolution_input_quote_v1(d,p_boundary_revision_id,
  jsonb_build_object('approved_source_ex_vat',v_approved,'realised_source_ex_vat',v_realised,
   'held_source_ex_vat',v_held,'source_basis_channel',case when p_run_position_id is null then pos.source_basis_channel else rp.source_basis_channel end),
  v_included)||jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version);
end
$function$;

alter function private.bpay_next_resolution_live_snooze_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_resolution_live_snooze_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_resolution_context_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_resolution_context_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid) from public,anon,authenticated,service_role;
commit;
