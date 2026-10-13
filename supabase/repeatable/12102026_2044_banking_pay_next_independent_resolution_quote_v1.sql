-- WB-45 / BD153: owner-only independent basis, audit and compatible residual quote.
-- WB-29 consumes these contracts; no existing money/Source writer is replaced.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_component_resolution_basis_v1(
 p_revision_id uuid,p_line_id uuid,p_family text,p_code text
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare r private.bpay_next_work_revision%rowtype;a private.bpay_next_approved_line%rowtype;
 d private.bpay_next_rate_detail%rowtype;s private.bpay_next_rate_schedule%rowtype;
 v_units numeric;v_rate numeric;v_charge numeric;v_semantics text;
begin
 if p_revision_id is null or p_line_id is null or p_family is null or p_code is null
  or p_family not in ('STANDARD','ADDITIONAL','ADJUSTMENT') or length(p_code) not between 1 and 256 then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
 select wb45src.* into r from private.bpay_next_work_revision wb45src where id=p_revision_id;
 select wb45src.* into a from private.bpay_next_approved_line wb45src where id=p_line_id and revision_id=p_revision_id;
 if r.id is null or a.id is null or r.approved_at_utc is null or r.sealed_at_utc is null
  or a.tax_treatment is distinct from 'TAXABLE' or a.source_pay_ex_vat<0
  or a.source_pay_ex_vat::text in ('NaN','Infinity','-Infinity') then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 if p_family='STANDARD' then
  if a.component_kind not in ('WORK','PROTECTED_WORK') then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
  select wb45src.* into d from private.bpay_next_rate_detail wb45src where approved_line_id=a.id and bucket=p_code;
  if d.id is null then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BUCKET_MISSING';end if;
  select wb45src.* into s from private.bpay_next_rate_schedule wb45src where revision_id=r.id and rate_family=p_family and rate_code=p_code;
  v_units:=d.approved_hours;v_rate:=d.source_pay_rate;v_charge:=s.approved_charge_rate;
 elsif p_family='ADDITIONAL' then
  if a.component_kind<>'ADDITIONAL' or a.component_key<>'ADDITIONAL:'||p_code
   or exists(select 1 from private.bpay_next_rate_detail where approved_line_id=a.id) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
  select wb45src.* into s from private.bpay_next_rate_schedule wb45src where revision_id=r.id and rate_family=p_family and rate_code=p_code;
  v_units:=a.approved_quantity;v_rate:=a.approved_unit_rate;v_charge:=s.approved_charge_rate;
 else
  if a.component_kind<>'ADJUSTMENT' or a.component_key<>p_code
   or exists(select 1 from private.bpay_next_rate_detail where approved_line_id=a.id) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
  v_units:=a.approved_quantity;v_rate:=a.approved_unit_rate;
 end if;
 if (v_units is not null and (v_units::text in ('NaN','Infinity','-Infinity') or v_units<0 or v_units>=1000000000000 or v_units<>trunc(v_units,6)))
  or (v_rate is not null and (v_rate::text in ('NaN','Infinity','-Infinity') or v_rate<0 or v_rate>=1000000000000 or v_rate<>trunc(v_rate,6)))
  or (v_charge is not null and (v_charge::text in ('NaN','Infinity','-Infinity') or v_charge<0 or v_charge>=1000000000000 or v_charge<>trunc(v_charge,6))) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 -- Exact original1157:439–451 admission. Available charge is never used here.
 v_semantics:=case when p_family in ('STANDARD','ADDITIONAL') and v_units>0
  and v_rate is not null and v_charge is not null then 'RATE_BEARING' else 'AMOUNT_LED' end;
 if p_family='STANDARD' and v_semantics<>'RATE_BEARING' then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 return jsonb_build_object('work_id',r.work_id,'physical_timesheet_id',r.physical_timesheet_id,
  'physical_timesheet_version',r.physical_timesheet_version,'financial_snapshot_id',r.financial_snapshot_id,
  'source_kind',r.source_kind,'source_head_id',r.source_head_id,'source_event_id',r.source_event_id,
  'source_pay_channel',r.source_pay_channel,'source_umbrella_id',r.source_umbrella_id,
  'valuation_policy_id',r.valuation_policy_id,'component_key',a.component_key,
  'component_kind',a.component_kind,'source_component_id',a.source_component_id,'work_date',a.work_date,
  'unit_label',a.unit_label,'tax_treatment',a.tax_treatment,'rate_family',p_family,'rate_code',p_code,
  'source_units',v_units,'source_rate',v_rate,'source_charge_rate',v_charge,
  'component_source_ex_vat',a.source_pay_ex_vat,'semantics',v_semantics);
end
$function$;

create or replace function private.bpay_next_component_resolution_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare b jsonb;v_publication private.bpay_next_publication%rowtype;
begin
 if TG_OP='UPDATE' then
  if (to_jsonb(new)-'state') is distinct from (to_jsonb(old)-'state')
   or old.state<>'ACTIVE' or new.state<>'CLEARED'
   or not exists(select 1 from private.bpay_next_component_resolution_event e
    where e.decision_id=old.id and e.decision_version=old.decision_version and e.event_kind='CLEARED') then
   raise exception using errcode='55000',message='BPAY_NEXT_RESOLUTION_VERSION_IMMUTABLE';end if;
  return new;
 elsif TG_OP<>'INSERT' then
  raise exception using errcode='55000',message='BPAY_NEXT_RESOLUTION_VERSION_IMMUTABLE';end if;
 if new.state<>'ACTIVE' then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STATE_INVALID';end if;
 b:=private.bpay_next_component_resolution_basis_v1(new.based_on_revision_id,new.approved_line_id,new.rate_family,new.rate_code);
 if b is distinct from new.original_basis or (b->>'physical_timesheet_id')::uuid<>new.physical_timesheet_id
  or (b->>'valuation_policy_id')::uuid<>new.valuation_policy_id
  or (new.resolution_mode='MANUAL_AMOUNT' and b->>'semantics'<>'AMOUNT_LED')
  or (new.resolution_mode='MANUAL_REPLACEMENT_RATE' and b->>'semantics'<>'RATE_BEARING') then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
 if new.published_revision_id is not null then
  b:=private.bpay_next_component_resolution_basis_v1(new.published_revision_id,new.published_line_id,new.rate_family,new.rate_code);
  select wb45src.* into v_publication from private.bpay_next_publication wb45src
   where work_id=new.work_id and revision_id=new.published_revision_id;
  if b is distinct from new.published_basis or v_publication.command_id is distinct from new.command_id
   or v_publication.predecessor_revision_id is distinct from new.based_on_revision_id
   or b->'physical_timesheet_id' is distinct from new.original_basis->'physical_timesheet_id'
   or b->'component_kind' is distinct from new.original_basis->'component_kind'
   or b->'source_component_id' is distinct from new.original_basis->'source_component_id'
   or b->'work_date' is distinct from new.original_basis->'work_date'
   or b->'unit_label' is distinct from new.original_basis->'unit_label'
   or b->'tax_treatment' is distinct from new.original_basis->'tax_treatment'
   or b->'source_units' is distinct from new.original_basis->'source_units'
   or b->'valuation_policy_id' is distinct from new.original_basis->'valuation_policy_id' then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_PUBLICATION_INCOMPATIBLE';end if;
 end if;
 return new;
end
$function$;

create or replace function private.bpay_next_component_resolution_event_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare d private.bpay_next_component_resolution%rowtype;m private.bpay_next_pay_method_change_member%rowtype;
 r private.bpay_next_pay_method_change_receipt%rowtype;a private.bpay_next_approved_line%rowtype;
 rev private.bpay_next_work_revision%rowtype;
begin
 if TG_OP<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_RESOLUTION_EVENT_IMMUTABLE';end if;
 if new.event_kind='NEED' then
  -- Retaining an explicit unresolved scope must not require a quotable rate.
  select wb45src.* into a from private.bpay_next_approved_line wb45src
   where id=new.approved_line_id and revision_id=new.revision_id;
  select wb45src.* into rev from private.bpay_next_work_revision wb45src where id=new.revision_id;
  if a.id is null or rev.sealed_at_utc is null or rev.approved_at_utc is null
   or(new.rate_family='STANDARD' and (a.component_kind not in ('WORK','PROTECTED_WORK')
    or not exists(select 1 from private.bpay_next_rate_detail where approved_line_id=a.id and bucket=new.rate_code)))
   or(new.rate_family='ADDITIONAL' and (a.component_kind<>'ADDITIONAL' or a.component_key<>'ADDITIONAL:'||new.rate_code))
   or(new.rate_family='ADJUSTMENT' and (a.component_kind<>'ADJUSTMENT' or a.component_key<>new.rate_code)) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_NEED_IDENTITY_CHANGED';end if;
  select wb45src.* into m from private.bpay_next_pay_method_change_member wb45src where operation_id=new.need_operation_id and member_key=new.need_member_key;
  select wb45src.* into r from private.bpay_next_pay_method_change_receipt wb45src where operation_id=new.need_operation_id;
  if m.classification is distinct from 'NEEDS_DECISION' or m.work_id is distinct from new.work_id
   or m.revision_id is distinct from new.revision_id or m.candidate_id is distinct from new.candidate_id
   or (m.approved_line_id is not null and m.approved_line_id<>new.approved_line_id)
   or r.post_metadata_version is distinct from new.need_version or r.actor_user_id is distinct from new.actor_user_id then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_NEED_IDENTITY_CHANGED';end if;
 else
  perform private.bpay_next_component_resolution_basis_v1(new.revision_id,new.approved_line_id,new.rate_family,new.rate_code);
  select wb45src.* into d from private.bpay_next_component_resolution wb45src where id=new.decision_id and decision_version=new.decision_version;
  if (d.work_id,d.candidate_id,d.based_on_revision_id,d.approved_line_id,d.rate_family,d.rate_code)
   is distinct from (new.work_id,new.candidate_id,new.revision_id,new.approved_line_id,new.rate_family,new.rate_code)
   or d.state is distinct from 'ACTIVE'
   or(new.event_kind='SAVED' and (d.command_id,d.actor_user_id) is distinct from (new.command_id,new.actor_user_id)) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_EVENT_IDENTITY_CHANGED';end if;
 end if;
 return new;
end
$function$;

create or replace function private.bpay_next_component_resolution_audit_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare d private.bpay_next_component_resolution%rowtype;
begin
 select wb45src.* into strict d from private.bpay_next_component_resolution wb45src where id=new.id;
 if not exists(select 1 from private.bpay_next_component_resolution_event e
   where e.decision_id=d.id and e.decision_version=d.decision_version and e.event_kind='SAVED')
  or ((d.state='CLEARED') is distinct from exists(select 1 from private.bpay_next_component_resolution_event e
   where e.decision_id=d.id and e.decision_version=d.decision_version and e.event_kind='CLEARED')) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_AUDIT_MISSING';end if;
 return null;
end
$function$;

create or replace function private.bpay_next_component_resolution_event_audit_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if new.event_kind='CLEARED' and not exists(select 1 from private.bpay_next_component_resolution d
   where d.id=new.decision_id and d.decision_version=new.decision_version and d.state='CLEARED') then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_AUDIT_MISSING';end if;
 return null;
end
$function$;

create or replace function private.bpay_next_component_resolution_active_v1(
 p_work_id uuid,p_boundary_revision_id uuid,p_line_id uuid,p_family text,p_code text,
 p_target_channel text,p_target_umbrella_id uuid,p_target_vat boolean,p_policy_id uuid,p_valuation_date date
) returns setof private.bpay_next_component_resolution
language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare b jsonb;d private.bpay_next_component_resolution%rowtype;v_count integer:=0;
begin
 b:=private.bpay_next_component_resolution_basis_v1(p_boundary_revision_id,p_line_id,p_family,p_code);
 if p_work_id is null or (b->>'work_id')::uuid is distinct from p_work_id or p_target_channel is null
  or p_target_channel not in ('PAYE','UMBRELLA') or p_policy_id is null or p_valuation_date is null
  or not isfinite(p_valuation_date)
  or (p_target_channel='PAYE' and (p_target_umbrella_id is not null or p_target_vat is not null))
  or (p_target_channel='UMBRELLA' and (p_target_umbrella_id is null or p_target_vat is null)) then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
 for d in select x.* from private.bpay_next_component_resolution x
  where x.work_id=p_work_id and x.rate_family=p_family and x.rate_code=p_code and x.state='ACTIVE'
   and x.target_pay_channel=p_target_channel and x.target_umbrella_id is not distinct from p_target_umbrella_id
   and x.target_umbrella_vat_chargeable is not distinct from p_target_vat
   and x.valuation_policy_id=p_policy_id and x.valuation_date=p_valuation_date
   and ((x.based_on_revision_id=p_boundary_revision_id and x.approved_line_id=p_line_id and x.original_basis=b)
    or (x.published_revision_id=p_boundary_revision_id and x.published_line_id=p_line_id and x.published_basis=b))
  limit 2 loop
  v_count:=v_count+1;
  if v_count>1 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_COMPATIBILITY_AMBIGUOUS';end if;
  return next d;
 end loop;
end
$function$;

create or replace function private.bpay_next_component_resolution_quote_v1(
 p_decision_id uuid,p_version bigint,p_boundary_revision_id uuid,p_run_position_id uuid default null
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare d private.bpay_next_component_resolution%rowtype;rp private.bpay_next_run_position%rowtype;
 pos private.bpay_next_position%rowtype;w private.bpay_next_work%rowtype;win private.bpay_next_valuation_policy_window%rowtype;
 rw private.bpay_next_run_worker%rowtype;run private.bpay_next_pay_run%rowtype;
 b jsonb;v_line uuid;v_approved numeric;v_realised numeric;v_held numeric;v_residual numeric;v_included numeric;
 v_units numeric;v_source numeric;v_full boolean;v_buckets integer;v_quote jsonb;
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
  if exists(select 1 from private.bpay_next_snooze where work_id=d.work_id and active
   and (scope_kind='WHOLE_TIMESHEET' or component_key=d.component_key)) then
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
 select wb45src.* into win from private.bpay_next_valuation_policy_window wb45src
  where policy_id=d.valuation_policy_id and d.valuation_date>=date_from and (date_to is null or d.valuation_date<=date_to)
  order by date_from desc,source_window_id desc limit 1;
 if win.source_window_id is null then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POLICY_WINDOW_MISSING';end if;
 if v_included=0 then
  return jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version,'source_ex_vat',0,
   'source_component_residual_ex_vat',v_residual,'remaining_units',0,'quote',jsonb_build_object('ex',0,'vat',0,'inc',0,'rate',null),
   'valuation_policy_id',d.valuation_policy_id,'valuation_window_id',win.source_window_id);end if;
 v_full:=v_realised=0 and v_held=0 and v_included=v_approved;
 select count(*) into v_buckets from private.bpay_next_rate_detail where approved_line_id=d.approved_line_id and approved_hours<>0;
 if d.rate_family='STANDARD' and v_buckets>1 then
  if not v_full or d.resolution_mode<>'MANUAL_REPLACEMENT_RATE' then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
  -- Untouched native bucket units are exact; bucket SOURCE money is not invented.
  v_source:=null;
 else v_source:=v_included;end if;
 v_units:=case when v_full then (d.original_basis->>'source_units')::numeric end;
 if d.resolution_mode in ('MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT') and not v_full then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS';end if;
 if (d.resolution_mode='MANUAL_REPLACEMENT_RATE' and (d.original_basis->>'semantics'<>'RATE_BEARING' or v_units is null or v_units<=0))
  or (d.resolution_mode='MANUAL_AMOUNT' and d.original_basis->>'semantics'<>'AMOUNT_LED') then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_MODE_INVALID';end if;
 v_quote:=private.pay_finance_selected_quote_v1(d.resolution_mode,v_source,
  d.original_basis->>'source_pay_channel',d.target_pay_channel,v_units,d.manual_replacement_rate,d.manual_amount_ex_vat,
  win.erni_pct,win.vat_rate_pct,coalesce(d.target_umbrella_vat_chargeable,false));
 if exists(select 1 from jsonb_each_text(v_quote) q where q.key in ('ex','vat','inc')
   and(q.value is null or q.value in ('NaN','Infinity','-Infinity') or q.value::numeric<0
    or q.value::numeric>=10000000000000000 or q.value::numeric<>trunc(q.value::numeric,2)))
  or (v_quote->>'inc')::numeric is distinct from (v_quote->>'ex')::numeric+(v_quote->>'vat')::numeric then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_QUOTE_RANGE_INVALID';end if;
 return jsonb_build_object('decision_id',d.id,'decision_version',d.decision_version,'source_ex_vat',v_source,
  'source_component_residual_ex_vat',v_residual,'included_source_component_ex_vat',v_included,'remaining_units',v_units,
  'quote',v_quote,'valuation_policy_id',d.valuation_policy_id,'valuation_window_id',win.source_window_id);
end
$function$;

alter function private.bpay_next_component_resolution_basis_v1(uuid,uuid,text,text) owner to postgres;
alter function private.bpay_next_component_resolution_guard_v1() owner to postgres;
alter function private.bpay_next_component_resolution_event_guard_v1() owner to postgres;
alter function private.bpay_next_component_resolution_audit_guard_v1() owner to postgres;
alter function private.bpay_next_component_resolution_event_audit_guard_v1() owner to postgres;
alter function private.bpay_next_component_resolution_active_v1(uuid,uuid,uuid,text,text,text,uuid,boolean,uuid,date) owner to postgres;
alter function private.bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_component_resolution_basis_v1(uuid,uuid,text,text),
 private.bpay_next_component_resolution_guard_v1(),private.bpay_next_component_resolution_event_guard_v1(),
 private.bpay_next_component_resolution_audit_guard_v1(),private.bpay_next_component_resolution_event_audit_guard_v1(),
 private.bpay_next_component_resolution_active_v1(uuid,uuid,uuid,text,text,text,uuid,boolean,uuid,date),
 private.bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid) from public,anon,authenticated,service_role;

-- Repeatable guard installation on these two WB-45 private tables only.
drop trigger if exists bpay_next_component_resolution_guard on private.bpay_next_component_resolution;
create trigger bpay_next_component_resolution_guard before insert or update or delete
 on private.bpay_next_component_resolution for each row execute function private.bpay_next_component_resolution_guard_v1();
drop trigger if exists bpay_next_component_resolution_no_truncate on private.bpay_next_component_resolution;
create trigger bpay_next_component_resolution_no_truncate before truncate
 on private.bpay_next_component_resolution for each statement execute function private.bpay_next_component_resolution_guard_v1();
drop trigger if exists bpay_next_component_resolution_event_guard on private.bpay_next_component_resolution_event;
create trigger bpay_next_component_resolution_event_guard before insert or update or delete
 on private.bpay_next_component_resolution_event for each row execute function private.bpay_next_component_resolution_event_guard_v1();
drop trigger if exists bpay_next_component_resolution_event_no_truncate on private.bpay_next_component_resolution_event;
create trigger bpay_next_component_resolution_event_no_truncate before truncate
 on private.bpay_next_component_resolution_event for each statement execute function private.bpay_next_component_resolution_event_guard_v1();
drop trigger if exists bpay_next_component_resolution_audit_guard on private.bpay_next_component_resolution;
create constraint trigger bpay_next_component_resolution_audit_guard after insert or update
 on private.bpay_next_component_resolution deferrable initially deferred
 for each row execute function private.bpay_next_component_resolution_audit_guard_v1();
drop trigger if exists bpay_next_component_resolution_event_audit_guard on private.bpay_next_component_resolution_event;
create constraint trigger bpay_next_component_resolution_event_audit_guard after insert
 on private.bpay_next_component_resolution_event deferrable initially deferred
 for each row execute function private.bpay_next_component_resolution_event_audit_guard_v1();

notify pgrst,'reload schema';

commit;
