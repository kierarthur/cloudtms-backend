-- WB-29 successor2116: VAT setting changes cannot erase an unresolved explicit need.
-- Current VAT still qualifies exact ACTIVE decisions; native transition replaces needs.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_component_resolution_need_publish_v1(
 p_operation_id uuid,p_work_id uuid default null
) returns void language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare r private.bpay_next_pay_method_change_receipt%rowtype;n record;v_count integer:=0;
begin
 if p_operation_id is null then raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_NEED_INPUT_INVALID';end if;
 select x.* into r from private.bpay_next_pay_method_change_receipt x where x.operation_id=p_operation_id;
 if r.operation_id is null or r.affected_member_count>1000
  or(p_work_id is not null and not exists(select 1 from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=r.candidate_id)) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_NEED_IDENTITY_CHANGED';end if;
 -- The original envelope is <=1000 members, each with <=5 native Standard
 -- buckets or one nonbucket code. Overflow fails the whole transaction.
 for n in
  select m.member_key,m.work_id,m.candidate_id,m.revision_id,a.id approved_line_id,
   bucket.rate_family,bucket.rate_code
  from private.bpay_next_pay_method_change_member m
  join private.bpay_next_approved_line a on a.revision_id=m.revision_id
   and a.id=m.approved_line_id and a.tax_treatment='TAXABLE'
  left join private.bpay_next_approved_segment s on s.revision_id=m.revision_id
   and s.id=m.approved_segment_id and s.approved_line_id=a.id
  cross join lateral (
   select 'STANDARD'::text rate_family,d.bucket rate_code
    from private.bpay_next_rate_detail d where d.approved_line_id=a.id
     and a.component_kind in ('WORK','PROTECTED_WORK') and d.approved_hours>0
     and(m.approved_segment_id is null or case d.bucket when 'DAY' then s.hours_day
      when 'NIGHT' then s.hours_night when 'SAT' then s.hours_sat when 'SUN' then s.hours_sun
      when 'BH' then s.hours_bh end>0)
   union all
   select 'ADDITIONAL',substring(a.component_key from 12)
    where a.component_kind='ADDITIONAL' and a.component_key like 'ADDITIONAL:%'
   union all select 'ADJUSTMENT',a.component_key where a.component_kind='ADJUSTMENT'
  ) bucket
  where m.operation_id=r.operation_id and m.member_kind='WORK' and m.classification='NEEDS_DECISION'
   and m.candidate_id=r.candidate_id and(p_work_id is null or m.work_id=p_work_id)
  order by m.member_key,a.id,bucket.rate_family,bucket.rate_code limit 5001
 loop
  v_count:=v_count+1;
  if v_count>5000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_NEED_SCOPE_TOO_LARGE';end if;
  insert into private.bpay_next_component_resolution_event(event_kind,work_id,candidate_id,revision_id,approved_line_id,
   rate_family,rate_code,need_operation_id,need_member_key,need_version,actor_user_id,reason)
  values('NEED',n.work_id,n.candidate_id,n.revision_id,n.approved_line_id,n.rate_family,n.rate_code,
   r.operation_id,n.member_key,r.post_metadata_version,r.actor_user_id,'PAY_METHOD_CHANGED')
  on conflict(need_operation_id,need_member_key,approved_line_id,rate_family,rate_code,need_version)
   where event_kind='NEED' do nothing;
 end loop;
end
$function$;

create or replace function private.bpay_next_component_resolution_need_v1(
 p_work_id uuid,p_revision_id uuid,p_line_id uuid,p_segment_id uuid,
 p_target_channel text,p_target_umbrella_id uuid,p_target_vat boolean,p_valuation_date date
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare w private.bpay_next_work%rowtype;rev private.bpay_next_work_revision%rowtype;
 r private.bpay_next_pay_method_change_receipt%rowtype;n record;v_count integer:=0;v_reason text;
begin
 select x.* into w from private.bpay_next_work x where x.id=p_work_id;
 select x.* into rev from private.bpay_next_work_revision x where x.id=p_revision_id and x.work_id=p_work_id;
 if w.id is null or rev.id is null or rev.sealed_at_utc is null or rev.approved_at_utc is null
  or not exists(select 1 from private.bpay_next_approved_line a where a.id=p_line_id
   and a.revision_id=p_revision_id and a.tax_treatment='TAXABLE') then return null;end if;
 -- Greatest native metadata version, never clock/UUID order or a target-filtered
 -- historical need. A later compatible or protected transition replaces it.
 select x.* into r from private.bpay_next_pay_method_change_receipt x where x.candidate_id=w.candidate_id
  order by x.post_metadata_version desc limit 1;
 if r.operation_id is null or(r.target_pay_channel,r.target_umbrella_id)
  is distinct from(p_target_channel,p_target_umbrella_id) then return null;end if;
 for n in
  select e.rate_family,e.rate_code
  from private.bpay_next_pay_method_change_member m
  join private.bpay_next_component_resolution_event e on e.need_operation_id=m.operation_id
   and e.need_member_key=m.member_key and e.event_kind='NEED' and e.need_version=r.post_metadata_version
   and e.work_id=m.work_id and e.candidate_id=m.candidate_id and e.revision_id=m.revision_id
   and e.approved_line_id=m.approved_line_id
  where m.operation_id=r.operation_id and m.member_kind='WORK' and m.classification='NEEDS_DECISION'
   and m.work_id=p_work_id and m.revision_id=p_revision_id and m.approved_line_id=p_line_id
   and m.approved_segment_id is not distinct from p_segment_id
  order by e.rate_family,e.rate_code limit 6
 loop
  if p_valuation_date is not null and exists(select 1 from private.bpay_next_component_resolution_active_v1(
    p_work_id,p_revision_id,p_line_id,n.rate_family,n.rate_code,p_target_channel,p_target_umbrella_id,
    p_target_vat,rev.valuation_policy_id,p_valuation_date)) then continue;end if;
  v_count:=v_count+1;
  if v_count>5 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_NEED_IDENTITY_CHANGED';end if;
  v_reason:=case when n.rate_family='STANDARD' then 'RATE_RESOLUTION_REQUIRED' else 'GROSS_RESOLUTION_REQUIRED' end;
 end loop;
 if v_count=0 then return null;end if;
 return jsonb_build_object('reason_code',v_reason,'work_reason_code','CHANNEL_RESOLUTION_REQUIRED',
  'need_operation_id',r.operation_id,'need_version',r.post_metadata_version::text,'unresolved_bucket_count',v_count::text);
end
$function$;
alter function private.bpay_next_component_resolution_need_publish_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_component_resolution_need_v1(uuid,uuid,uuid,uuid,text,uuid,boolean,date) owner to postgres;
revoke all on function private.bpay_next_component_resolution_need_publish_v1(uuid,uuid),
 private.bpay_next_component_resolution_need_v1(uuid,uuid,uuid,uuid,text,uuid,boolean,date) from public,anon,authenticated,service_role;

commit;
