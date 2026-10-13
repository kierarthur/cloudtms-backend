-- WB32 independent review: native Ready fact is independent of compatible owner.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_channel_resolution_review_v1(p_actor_id uuid,p_scope jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare i private.bpay_next_workbench_item%rowtype;g private.bpay_next_workbench_group%rowtype;
 w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
 root_w private.bpay_next_work%rowtype;root_r private.bpay_next_work_revision%rowtype;
 receipt private.bpay_next_pay_method_change_receipt%rowtype;n record;
 v_base jsonb;v_works jsonb:='[]';v_read jsonb;v_meta jsonb:='[]';v_row jsonb;v_work jsonb;v_owner jsonb;v_decisions jsonb;
 v_seen text[]:='{}';v_resolver_keys text[]:='{}';v_key text;v_count integer:=0;v_physical integer:=0;v_bytes bigint:=0;v_index integer;v_reason text;
 v_need boolean;v_saved boolean;v_resolver boolean;v_root boolean:=false;
begin
 if jsonb_typeof(p_scope) is distinct from 'object' or octet_length(p_scope::text)>32768
  or (select count(*) from jsonb_object_keys(p_scope))<>3 or not(p_scope ?& array['candidate_id','work_id','expected_revision_id'])
  or jsonb_typeof(p_scope->'candidate_id') is distinct from 'string' or jsonb_typeof(p_scope->'work_id') is distinct from 'string'
  or jsonb_typeof(p_scope->'expected_revision_id') is distinct from 'string' then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_REQUEST_INVALID';end if;
 v_base:=private.bpay_next_resolution_offer_context_v1(p_actor_id,(p_scope->>'candidate_id')::uuid);
 select q.* into root_w from private.bpay_next_work q where q.id=(p_scope->>'work_id')::uuid and q.candidate_id=(p_scope->>'candidate_id')::uuid;
 select q.* into root_r from private.bpay_next_work_revision q where q.id=(p_scope->>'expected_revision_id')::uuid and q.work_id=root_w.id;
 if root_w.id is null or root_r.id is null or(root_w.current_revision_id,root_w.applied_revision_id) is distinct from(root_r.id,root_r.id) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 select q.* into receipt from private.bpay_next_pay_method_change_receipt q where q.candidate_id=root_w.candidate_id order by q.post_metadata_version desc limit 1;
 -- Existing Candidate/section/ID index drives physical prefix before semantic filtering.
 for i in select q.* from private.bpay_next_workbench_item q where q.candidate_id=root_w.candidate_id order by q.section,q.id limit 1001 loop
  v_physical:=v_physical+1;v_bytes:=v_bytes+octet_length(to_jsonb(i)::text);
  if v_physical>1000 or v_bytes>1048576 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  if i.work_id is null or i.approved_line_id is null or i.portion_kind<>'AVAILABLE' or i.direction<>'PAYMENT'
   or not i.amount_known or i.included_source_ex_vat<=0 or i.tax_treatment is distinct from 'TAXABLE' or i.section not in('ACTION','READY')
   or i.progress_state<>'CURRENT' or i.accepted_run_id is not null or i.is_selected then continue;end if;
  if exists(select 1 from private.bpay_next_run_choice_member cm join private.bpay_next_pay_run pr on pr.id=cm.run_id and pr.accepted_choice_descriptor is not null
   left join private.bpay_next_run_worker rw on rw.run_id=cm.run_id and rw.candidate_id=cm.candidate_id
   where cm.candidate_id=i.candidate_id and cm.work_id=i.work_id and cm.expected_revision_id=i.revision_id and cm.member_kind='WORK' and cm.selected
    and cm.skip_code is null and pr.status in('PREPARING','REVIEW','DRAFT','EXECUTING') and(rw.id is null or rw.status not in('CANCELLED','COMPLETE'))
    and(cm.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component sc where sc.run_id=cm.run_id and sc.work_id=cm.work_id and sc.approved_line_id=i.approved_line_id)
     or exists(select 1 from jsonb_array_elements(pr.accepted_choice_descriptor#>'{intent,components}') dl where dl->>'work_id'=cm.work_id::text and dl->>'approved_line_id'=i.approved_line_id::text and dl->'selected'='true'::jsonb))) then continue;end if;
  select q.* into w from private.bpay_next_work q where q.id=i.work_id and q.candidate_id=root_w.candidate_id;
  select q.* into r from private.bpay_next_work_revision q where q.id=i.revision_id and q.work_id=w.id;
  select q.* into g from private.bpay_next_workbench_group q where q.group_key=i.group_key and q.work_id=w.id;
  if w.id is null or r.id is null or g.group_key is null or not g.complete or g.dirty or g.progress_state<>'CURRENT'
   or(w.current_revision_id,w.applied_revision_id,g.current_revision_id) is distinct from(r.id,r.id,r.id)
   or(g.financial_view_revision,g.metadata_version) is distinct from((v_base->>'financial_view_revision')::bigint,(v_base->>'metadata_version')::bigint)
   or(i.projection_revision,i.financial_view_revision,i.metadata_version) is distinct from(g.projection_revision,g.financial_view_revision,g.metadata_version) then continue;end if;
  if(r.source_pay_channel,r.source_umbrella_id) is not distinct from(v_base->>'target_pay_channel',(v_base->>'target_umbrella_id')::uuid) then continue;end if;
  -- A native Source UUID is not a rate code: never omit an affected taxable amount.
  if i.component_kind='ADDITIONAL' and((i.section='ACTION' and i.reason_code='GROSS_RESOLUTION_REQUIRED')
   or(i.section='READY' and i.channel_note_code='CHANNEL_RESOLVER')) and exists(select 1 from private.bpay_next_approved_line unbound
    where unbound.id=i.approved_line_id and unbound.revision_id=r.id and unbound.component_kind='ADDITIONAL' and unbound.tax_treatment='TAXABLE'
     and(unbound.component_key not like 'ADDITIONAL:%' or length(unbound.component_key)<=11)) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
  for n in
   select a.id as line_id,'STANDARD'::text as rate_family,d.bucket as rate_code from private.bpay_next_approved_line a
    join private.bpay_next_rate_detail d on d.approved_line_id=a.id and d.approved_hours>0
    left join private.bpay_next_approved_segment seg on seg.id=i.approved_segment_id and seg.revision_id=r.id and seg.approved_line_id=a.id
    where a.id=i.approved_line_id and a.revision_id=r.id and a.tax_treatment='TAXABLE' and a.component_kind in('WORK','PROTECTED_WORK')
     and(i.approved_segment_id is null or case d.bucket when 'DAY' then seg.hours_day when 'NIGHT' then seg.hours_night when 'SAT' then seg.hours_sat when 'SUN' then seg.hours_sun when 'BH' then seg.hours_bh end>0)
   union all
   select a.id,'ADDITIONAL',substring(a.component_key from 12) from private.bpay_next_approved_line a
    where a.id=i.approved_line_id and a.revision_id=r.id and a.tax_treatment='TAXABLE' and a.component_kind='ADDITIONAL' and a.component_key like 'ADDITIONAL:%' and length(a.component_key)>11
   union all
   select a.id,'ADJUSTMENT',a.component_key from private.bpay_next_approved_line a
    where a.id=i.approved_line_id and a.revision_id=r.id and a.tax_treatment='TAXABLE' and a.component_kind='ADJUSTMENT'
   order by rate_family,rate_code,line_id limit 101
  loop
   v_key:=jsonb_build_array(w.id,r.id,n.line_id,n.rate_family,n.rate_code)::text;if v_key=any(v_seen) then continue;end if;
   v_saved:=exists(select 1 from private.bpay_next_component_resolution_active_v1(w.id,r.id,n.line_id,n.rate_family,n.rate_code,
    v_base->>'target_pay_channel',(v_base->>'target_umbrella_id')::uuid,(v_base->>'target_umbrella_vat_chargeable')::boolean,r.valuation_policy_id,(v_base->>'official_pay_date')::date));
   v_reason:=case when n.rate_family='STANDARD' then 'RATE_RESOLUTION_REQUIRED' else 'GROSS_RESOLUTION_REQUIRED' end;
   v_need:=exists(
  select 1 from private.bpay_next_pay_method_change_member m
  join private.bpay_next_work_revision mr on mr.id=m.revision_id and mr.work_id=m.work_id and mr.sealed_at_utc is not null
  join private.bpay_next_approved_line old_a on old_a.id=m.approved_line_id and old_a.revision_id=m.revision_id
  join private.bpay_next_approved_line a on a.id=n.line_id and a.revision_id=r.id and a.component_key=old_a.component_key
  left join private.bpay_next_approved_segment old_s on old_s.id=m.approved_segment_id and old_s.revision_id=m.revision_id and old_s.approved_line_id=old_a.id
  left join private.bpay_next_approved_segment actual_s on actual_s.revision_id=r.id and actual_s.approved_line_id=a.id and actual_s.component_key=old_s.component_key and actual_s.segment_key=old_s.segment_key
  join private.bpay_next_component_resolution_event e on e.need_operation_id=m.operation_id and e.need_member_key=m.member_key and e.event_kind='NEED'
   and e.need_version=receipt.post_metadata_version and e.candidate_id=m.candidate_id and e.work_id=m.work_id and e.revision_id=m.revision_id and e.approved_line_id=m.approved_line_id
   and e.rate_family=n.rate_family and e.rate_code=n.rate_code
  join private.bpay_next_workbench_item proof_i on proof_i.work_id=w.id and proof_i.revision_id=r.id and proof_i.approved_line_id=a.id
   and ((m.approved_segment_id is null and proof_i.approved_segment_id is null) or(actual_s.id is not null and proof_i.approved_segment_id=actual_s.id))
  join private.bpay_next_workbench_group proof_g on proof_g.group_key=proof_i.group_key
  where m.operation_id=receipt.operation_id and m.candidate_id=w.candidate_id and m.work_id=w.id and m.member_kind='WORK' and m.classification='NEEDS_DECISION'
   and(receipt.target_pay_channel,receipt.target_umbrella_id) is not distinct from(v_base->>'target_pay_channel',(v_base->>'target_umbrella_id')::uuid)
   and(case when mr.payment_treatment_approval_revision_id is not null then private.bpay_next_treatment_approval_revision_v1(mr.id) else mr.id end)=private.bpay_next_treatment_approval_revision_v1(r.id)
   and proof_i.candidate_id=w.candidate_id and proof_i.section='ACTION' and proof_i.reason_code=v_reason and proof_i.portion_kind='AVAILABLE' and proof_i.direction='PAYMENT'
   and proof_i.amount_known and proof_i.included_source_ex_vat>0 and proof_i.progress_state='CURRENT' and proof_i.accepted_run_id is null and not proof_i.is_selected
   and proof_g.complete and not proof_g.dirty and proof_g.progress_state='CURRENT' and proof_g.current_revision_id=r.id
   and proof_g.financial_view_revision=(v_base->>'financial_view_revision')::bigint and proof_g.metadata_version=(v_base->>'metadata_version')::bigint
   and proof_i.projection_revision=proof_g.projection_revision and proof_i.financial_view_revision=proof_g.financial_view_revision and proof_i.metadata_version=proof_g.metadata_version
   and(n.rate_family<>'STANDARD' or exists(select 1 from private.bpay_next_rate_detail rd where rd.approved_line_id=a.id and rd.bucket=n.rate_code and rd.approved_hours>0))
   and(m.approved_segment_id is null or n.rate_family<>'STANDARD' or case n.rate_code when 'DAY' then actual_s.hours_day when 'NIGHT' then actual_s.hours_night when 'SAT' then actual_s.hours_sat when 'SUN' then actual_s.hours_sun when 'BH' then actual_s.hours_bh end>0)
   );
   v_resolver:=i.section='READY' and i.channel_note_code='CHANNEL_RESOLVER';
   if not((i.section='ACTION' and i.reason_code=v_reason and v_need) or(i.section='READY' and(v_saved or v_resolver))) then continue;end if;
   v_count:=v_count+1;if v_count>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
   v_seen:=array_append(v_seen,v_key);v_root:=v_root or w.id=root_w.id;
   if v_resolver and not v_need and not v_saved then v_resolver_keys:=array_append(v_resolver_keys,v_key);end if;
   select ordinality::integer-1 into v_index from jsonb_array_elements(v_works) with ordinality where value->>'work_id'=w.id::text;
   v_row:=jsonb_build_object('approved_line_id',n.line_id,'rate_family',n.rate_family,'rate_code',n.rate_code,'intent',null);
   if v_index is null then v_works:=v_works||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',r.id,'rows',jsonb_build_array(v_row)));
   else v_works:=jsonb_set(v_works,array[v_index::text,'rows'],(v_works->v_index->'rows')||jsonb_build_array(v_row));end if;
   if octet_length(v_works::text)>30000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  end loop;
 end loop;
 if not v_root or v_count=0 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
 v_read:=private.bpay_next_resolution_context_v1(p_actor_id,jsonb_build_object('candidate_id',root_w.candidate_id,'works',v_works,'reason',null));
 for v_work in select value from jsonb_array_elements(v_read->'works') loop
  select q.* into r from private.bpay_next_work_revision q where q.id=(v_work->>'expected_revision_id')::uuid;
  for v_row in select value from jsonb_array_elements(v_work->'rows') loop
   v_decisions:=v_row->'incompatible_decisions';
   if v_row->'saved_decision'<>'null'::jsonb then v_decisions:=v_decisions||jsonb_build_array(v_row->'saved_decision');end if;
   if jsonb_array_length(v_decisions)=0 then v_decisions:='[null]'::jsonb;end if;
   for v_owner in select value from jsonb_array_elements(v_decisions) loop
    v_meta:=v_meta||jsonb_build_array(jsonb_build_object('work_id',v_work->'work_id','expected_revision_id',v_work->'expected_revision_id',
     'approved_line_id',v_row->'approved_line_id','rate_family',v_row->'rate_family','rate_code',v_row->'rate_code',
     'owner_family',case when v_row->>'rate_family'='STANDARD' then 'WORK_BUCKET' else 'GROSS' end,
     'owner_id',v_owner->'decision_id','owner_version',v_owner->'decision_version',
     'fingerprint',jsonb_build_object('original_basis',v_row->'original_basis','target_pay_channel',coalesce(v_owner->'target_pay_channel',v_base->'target_pay_channel'),
      'target_umbrella_id',coalesce(v_owner->'target_umbrella_id',v_base->'target_umbrella_id'),
      'target_umbrella_vat_chargeable',coalesce(v_owner->'target_umbrella_vat_chargeable',v_base->'target_umbrella_vat_chargeable'),
      'valuation_policy_id',coalesce(v_owner->'valuation_policy_id',v_row#>'{original_basis,valuation_policy_id}'),'valuation_date',coalesce(v_owner->'valuation_date',v_base->'official_pay_date')),
     'from_channel',v_row#>'{original_basis,source_pay_channel}','from_umbrella_id',v_row#>'{original_basis,source_umbrella_id}',
     'to_channel',v_base->'target_pay_channel','to_umbrella_id',v_base->'target_umbrella_id',
     'clear_class',case when v_owner<>'null'::jsonb then 'RESOLVED_AND_CLEARABLE' else 'NOT_CLEARABLE' end,
     'clear_reason',case when v_owner='null'::jsonb then 'NO_SAVED_DECISION' end,
     'client_display_name',r.client_display_name,'timesheet_reference',r.timesheet_reference,
     'resolver_ready',jsonb_build_array(v_work->'work_id',v_work->'expected_revision_id',v_row->'approved_line_id',v_row->'rate_family',v_row->'rate_code')::text=any(v_resolver_keys)));
   end loop;
  end loop;
 end loop;
 v_read:=v_read||jsonb_build_object('channel_review',jsonb_build_object('candidate_id',root_w.candidate_id,'work_id',root_w.id,'expected_revision_id',root_r.id,
  'complete',true,'header',jsonb_build_object('candidate_display_name',root_r.candidate_display_name,'candidate_reference',root_r.candidate_reference),'owners',v_meta));
 if octet_length(v_read::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 return v_read;
end
$function$;

alter function private.bpay_next_channel_resolution_review_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_channel_resolution_review_v1(uuid,jsonb) from public,anon,authenticated,service_role;
commit;
