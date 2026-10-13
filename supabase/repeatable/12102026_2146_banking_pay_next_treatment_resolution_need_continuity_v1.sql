-- WB-29 / BD155: exact native Source approval qualification only.
-- Actual current/applied/selected/Position/job identities and all money remain native.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_component_resolution_need_v1(
 p_work_id uuid,p_revision_id uuid,p_line_id uuid,p_segment_id uuid,
 p_target_channel text,p_target_umbrella_id uuid,p_target_vat boolean,p_valuation_date date
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare w private.bpay_next_work%rowtype;rev private.bpay_next_work_revision%rowtype;
 r private.bpay_next_pay_method_change_receipt%rowtype;n record;v_count integer:=0;v_reason text;v_approval uuid;
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
 v_approval:=private.bpay_next_treatment_approval_revision_v1(p_revision_id);
 for n in
  select e.rate_family,e.rate_code
  from private.bpay_next_pay_method_change_member m
  join private.bpay_next_work_revision mr on mr.id=m.revision_id and mr.work_id=m.work_id and mr.sealed_at_utc is not null
  join private.bpay_next_approved_line ma on ma.id=m.approved_line_id and ma.revision_id=m.revision_id
  join private.bpay_next_approved_line actual on actual.id=p_line_id and actual.revision_id=p_revision_id and actual.component_key=ma.component_key
  left join private.bpay_next_approved_segment ms on ms.id=m.approved_segment_id and ms.revision_id=m.revision_id and ms.approved_line_id=ma.id
  left join private.bpay_next_approved_segment actual_s on actual_s.id=p_segment_id and actual_s.revision_id=p_revision_id and actual_s.approved_line_id=actual.id
  join private.bpay_next_component_resolution_event e on e.need_operation_id=m.operation_id
   and e.need_member_key=m.member_key and e.event_kind='NEED' and e.need_version=r.post_metadata_version
   and e.work_id=m.work_id and e.candidate_id=m.candidate_id and e.revision_id=m.revision_id
   and e.approved_line_id=m.approved_line_id
  where m.operation_id=r.operation_id and m.member_kind='WORK' and m.classification='NEEDS_DECISION'
   and m.work_id=p_work_id and (case when mr.payment_treatment_approval_revision_id is not null
    then private.bpay_next_treatment_approval_revision_v1(mr.id) else mr.id end)=v_approval
   and ((m.approved_segment_id is null and p_segment_id is null)
    or (ms.id is not null and actual_s.id is not null and ms.component_key=actual_s.component_key and ms.segment_key=actual_s.segment_key))
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


alter function private.bpay_next_component_resolution_need_v1(uuid,uuid,uuid,uuid,text,uuid,boolean,date) owner to postgres;
revoke all on function private.bpay_next_component_resolution_need_v1(uuid,uuid,uuid,uuid,text,uuid,boolean,date) from public,anon,authenticated,service_role;
commit;
