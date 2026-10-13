-- WB29 / BD161: exact native independent carry; fresh Position guards retained.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_component_resolution_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare b jsonb;v_publication private.bpay_next_publication%rowtype;pos private.bpay_next_position%rowtype;
 v_carry boolean:=false;v_old private.bpay_next_component_resolution%rowtype;
 v_event private.bpay_next_component_resolution_event%rowtype;v_body jsonb;
 v_old_id uuid;v_old_version bigint;
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
 -- A carry body is authored only by the closed native W2 constructor. Its
 -- immutable request UUID is audit metadata; actual child/event identity owns admission.
 if new.request_body ? 'carry' then
  v_body:=new.request_body->'carry';
  if jsonb_typeof(new.request_body) is distinct from 'object'
   or new.request_body-'carry'<>'{}'::jsonb or jsonb_typeof(v_body) is distinct from 'object'
   or v_body-array['decision_id','decision_version','request_id']<>'{}'::jsonb
   or not(v_body ?& array['decision_id','decision_version','request_id'])
   or jsonb_typeof(v_body->'decision_id') is distinct from 'string'
   or jsonb_typeof(v_body->'decision_version') is distinct from 'string'
   or jsonb_typeof(v_body->'request_id') is distinct from 'string'
   or (v_body->>'decision_id') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   or (v_body->>'request_id') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   or (v_body->>'decision_version') !~ '^[1-9][0-9]{0,18}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
  if (v_body->>'decision_version')::numeric>=9223372036854775807 then
   raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
  v_old_id:=(v_body->>'decision_id')::uuid;v_old_version:=(v_body->>'decision_version')::bigint;
  select x.* into v_old from private.bpay_next_component_resolution x
   where x.id=v_old_id and x.decision_version=v_old_version;
  select x.* into v_event from private.bpay_next_component_resolution_event x
   where x.decision_id=v_old_id and x.decision_version=v_old_version and x.event_kind='CLEARED';
  select x.* into v_publication from private.bpay_next_publication x
   where x.work_id=new.work_id and x.revision_id=new.published_revision_id;
  if v_old.id is null or v_old.state is distinct from 'CLEARED' or new.id=v_old.id
   or new.decision_version is distinct from v_old.decision_version+1
   or (v_old.work_id,v_old.candidate_id,v_old.physical_timesheet_id,v_old.component_key,v_old.rate_family,v_old.rate_code)
    is distinct from (new.work_id,new.candidate_id,new.physical_timesheet_id,new.component_key,new.rate_family,new.rate_code)
   or v_event.id is null
   or (v_event.command_id,v_event.actor_user_id,v_event.work_id,v_event.candidate_id,v_event.revision_id,v_event.approved_line_id,v_event.rate_family,v_event.rate_code)
    is distinct from (new.command_id,new.actor_user_id,v_old.work_id,v_old.candidate_id,v_old.based_on_revision_id,v_old.approved_line_id,v_old.rate_family,v_old.rate_code)
   or new.published_revision_id is null or new.published_line_id is null
   or (v_publication.command_id,v_publication.candidate_id,v_publication.predecessor_revision_id)
    is distinct from (new.command_id,new.candidate_id,new.based_on_revision_id)
   or not exists(select 1 from private.bpay_next_work_revision r
    where r.id=new.published_revision_id and r.work_id=new.work_id
     and r.payment_treatment_approval_revision_id is not null and r.sealed_at_utc is not null)
   or not exists(select 1 from private.bpay_next_command c
    join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=new.candidate_id
    where c.id=new.command_id and c.command_kind='POSITION_APPLY' and c.sealed_at_utc is not null
     and c.expected_member_count=1 and m.member_no=1)
   or not (((v_old.based_on_revision_id,v_old.approved_line_id)=(new.based_on_revision_id,new.approved_line_id)
      and v_old.original_basis=new.original_basis)
    or (v_old.published_revision_id is not null and v_old.published_line_id is not null
     and (v_old.published_revision_id,v_old.published_line_id)=(new.based_on_revision_id,new.approved_line_id)
     and v_old.published_basis=new.original_basis))
   or (v_old.resolution_mode,v_old.manual_replacement_rate,v_old.manual_amount_ex_vat,
    v_old.target_pay_channel,v_old.target_umbrella_id,v_old.target_umbrella_vat_chargeable,
    v_old.valuation_policy_id,v_old.valuation_date,v_old.reason,v_old.original_position_basis)
    is distinct from (new.resolution_mode,new.manual_replacement_rate,new.manual_amount_ex_vat,
    new.target_pay_channel,new.target_umbrella_id,new.target_umbrella_vat_chargeable,
    new.valuation_policy_id,new.valuation_date,new.reason,new.original_position_basis) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_PUBLICATION_INCOMPATIBLE';end if;
  perform private.bpay_next_treatment_approval_revision_v1(new.published_revision_id);
  v_carry:=true;
 end if;
 select wb45src.* into pos from private.bpay_next_position wb45src
  where work_id=new.work_id and component_key=new.component_key;
 if pos.applied_revision_id is distinct from new.based_on_revision_id
  or pos.approved_source_ex_vat is distinct from (new.original_basis->>'component_source_ex_vat')::numeric
  or (not v_carry and new.original_position_basis is distinct from jsonb_build_object(
   'approved_source_ex_vat',pos.approved_source_ex_vat,'realised_source_ex_vat',pos.realised_source_ex_vat,
   'held_source_ex_vat',pos.held_source_ex_vat,'source_basis_channel',pos.source_basis_channel))
  or pos.realised_source_ex_vat::text in ('NaN','Infinity','-Infinity')
  or pos.held_source_ex_vat::text in ('NaN','Infinity','-Infinity') then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED';end if;
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
   or b->'valuation_policy_id' is distinct from new.original_basis->'valuation_policy_id'
   -- Treatment publication changes TARGET only, never republishes SOURCE earnings.
   or b->'source_kind' is distinct from new.original_basis->'source_kind'
   or b->'source_head_id' is distinct from new.original_basis->'source_head_id'
   or b->'source_pay_channel' is distinct from new.original_basis->'source_pay_channel'
   or b->'source_umbrella_id' is distinct from new.original_basis->'source_umbrella_id'
   or b->'component_source_ex_vat' is distinct from new.original_basis->'component_source_ex_vat'
   or b->'source_rate' is distinct from new.original_basis->'source_rate'
   or b->'source_charge_rate' is distinct from new.original_basis->'source_charge_rate' then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_PUBLICATION_INCOMPATIBLE';end if;
 end if;
 return new;
end
$function$;

alter function private.bpay_next_component_resolution_guard_v1() owner to postgres;
revoke all on function private.bpay_next_component_resolution_guard_v1() from public,anon,authenticated,service_role;
commit;
